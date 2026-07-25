package listener

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mileusna/viber"

	"sailstream/internal/config"
	"sailstream/internal/enviroment"
	"sailstream/internal/session"
	"sailstream/internal/shared"
)

var debugVB = os.Getenv("VB_DEBUG") == "1"

func vbDbg(accountID, format string, args ...interface{}) {
	if !debugVB {
		return
	}
	log.Printf("[VB:DBG:%s] "+format, append([]interface{}{accountID}, args...)...)
}

const (
	vbNotifBufferSize      = 500
	vbMsgDedupeWindow      = 24 * time.Hour
	vbCollectDrain         = 100 * time.Millisecond
	vbMaxBatchSize         = 200
	vbPauseAckTimeout      = 15 * time.Second
	vbDefaultPort          = ":8087"
	vbDefaultMediaDir      = "./media/viber"
	vbDefaultWebhookPath   = "/viber/webhook/"
	vbWebhookCheckInterval = 5 * time.Minute
	vbSilenceThreshold     = 30 * time.Minute
	vbHTTPTimeout          = 10 * time.Second
	vbIPDetectRetries      = 3
)

var publicIPServices = []string{
	"https://api.ipify.org",
	"https://icanhazip.com",
	"https://ifconfig.me/ip",
	"https://api4.my-ip.io/ip",
}

type ViberCollector struct {
	platformID string
	subtypeID  string
	accountID  string
	subtype    string

	config     *ListenerConfig
	db         *sql.DB
	configMgr  *config.ConfigManager
	envMgr     *enviroment.Environment
	sessionMgr *session.Manager

	viberClient *viber.Viber
	clientMu    sync.RWMutex

	webhookServer *http.Server

	registeredWebhookURL string
	webhookMu            sync.Mutex
	lastWebhookCheck     time.Time
	lastMessageAt        time.Time
	lastMsgMu            sync.Mutex

	connected atomic.Bool

	webhookRegistered atomic.Bool

	notifBuffer      chan *Notification
	instructionQueue chan *shared.AutomationInstruction
	errorChan        chan *PlatformError

	collectRunning atomic.Bool
	// pauseCount is a reference count, not a bool — see fix N4 in whatsapp.go
	// for the rationale (concurrent instructions shouldn't be able to undo
	// each other's pause).
	pauseCount atomic.Int32

	pauseAck  chan struct{}
	resumeMu  sync.Mutex
	resumeReq chan struct{}

	executionMu sync.Mutex

	seenMsgMu sync.Mutex
	seenMsgs  map[string]time.Time

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func NewViberCollector(
	platformID, subtypeID, accountID, subtype string,
	listenerConfig *ListenerConfig,
	db *sql.DB,
	configMgr *config.ConfigManager,
	envMgr *enviroment.Environment,
	sessionMgr *session.Manager,
) *ViberCollector {
	log.Printf("[VB:INIT] NewViberCollector platformID=%s subtypeID=%s accountID=%s subtype=%s",
		platformID, subtypeID, accountID, subtype)

	vc := &ViberCollector{
		platformID:       platformID,
		subtypeID:        subtypeID,
		accountID:        accountID,
		subtype:          subtype,
		config:           listenerConfig,
		db:               db,
		configMgr:        configMgr,
		envMgr:           envMgr,
		sessionMgr:       sessionMgr,
		notifBuffer:      make(chan *Notification, vbNotifBufferSize),
		instructionQueue: make(chan *shared.AutomationInstruction, 100),
		errorChan:        make(chan *PlatformError, 50),
		pauseAck:         make(chan struct{}, 1),
		resumeReq:        make(chan struct{}),
		seenMsgs:         make(map[string]time.Time),
		shutdown:         make(chan struct{}),
		lastMessageAt:    time.Now(),
	}

	return vc
}

func (vc *ViberCollector) getUserCursor(userID string) (uint64, error) {
	var cursorValue string
	err := vc.db.QueryRow(`
		SELECT cursor_value FROM global_cursors
		WHERE platform = 'viber' AND subtype = ? AND account_id = ? AND subtype_id = ? AND cursor_type = 'message_token'
	`, vc.subtype, vc.accountID, userID).Scan(&cursorValue)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(cursorValue, 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (vc *ViberCollector) updateUserCursor(userID string, token uint64) error {
	_, err := vc.db.Exec(`
		INSERT INTO global_cursors (platform, subtype, account_id, subtype_id, cursor_type, cursor_value, updated_at)
		VALUES ('viber', ?, ?, ?, 'message_token', ?, CURRENT_TIMESTAMP)
		ON CONFLICT(platform, subtype, account_id, subtype_id, cursor_type) DO UPDATE SET
			cursor_value = excluded.cursor_value,
			updated_at = CURRENT_TIMESTAMP
	`, vc.subtype, vc.accountID, userID, fmt.Sprintf("%d", token))
	return err
}

func (vc *ViberCollector) SetClient(client *viber.Viber) error {
	if client == nil {
		return fmt.Errorf("SetClient: nil viber client")
	}
	vc.clientMu.Lock()
	vc.viberClient = client
	vc.clientMu.Unlock()

	client.Message = vc.handleMessage
	client.Subscribed = vc.handleSubscribed
	client.Unsubscribed = vc.handleUnsubscribed
	client.Delivered = vc.handleDelivered
	client.Seen = vc.handleSeen
	client.Failed = vc.handleFailed
	client.ConversationStarted = vc.handleConversationStarted

	if err := vc.startWebhookServer(client); err != nil {
		return fmt.Errorf("SetClient: start webhook server: %w", err)
	}

	vc.connected.Store(true)
	log.Printf("[VB:%s] ✓ webhook HTTP server running on %s", vc.accountID, vc.getListenPort())

	go vc.ensureWebhookRegistered(context.Background())

	return nil
}

func (vc *ViberCollector) ensureWebhookRegistered(ctx context.Context) {
	url, err := vc.resolveWebhookURL(ctx)
	if err != nil {
		log.Printf("[VB:%s] ⚠ could not resolve webhook URL: %v", vc.accountID, err)
		vc.printWebhookHelp()
		return
	}

	vc.webhookMu.Lock()
	already := vc.registeredWebhookURL
	vc.webhookMu.Unlock()

	if already == url && vc.webhookRegistered.Load() {
		vbDbg(vc.accountID, "webhook URL unchanged (%s), skipping re-register", url)
		return
	}

	log.Printf("[VB:%s] registering webhook: %s", vc.accountID, url)
	client := vc.safeClient()
	if client == nil {
		log.Printf("[VB:%s] ⚠ client not available for SetWebhook", vc.accountID)
		return
	}

	_, err = client.SetWebhook(url, nil)
	if err != nil {
		log.Printf("[VB:%s] ⚠ SetWebhook(%s) failed: %v", vc.accountID, url, err)
		vc.reportError("WEBHOOK_REGISTER_FAILED", err.Error(), "warning")
		vc.printWebhookHelp()
		return
	}

	vc.webhookMu.Lock()
	vc.registeredWebhookURL = url
	vc.webhookMu.Unlock()
	vc.webhookRegistered.Store(true)
	vc.persistWebhookURL(url)

	log.Printf("[VB:%s] ✓ webhook registered: %s", vc.accountID, url)
}

func (vc *ViberCollector) resolveWebhookURL(ctx context.Context) (string, error) {
	port := strings.TrimPrefix(vc.getListenPort(), ":")

	if u := os.Getenv("VIBER_WEBHOOK_URL"); u != "" {
		log.Printf("[VB:%s] webhook URL from env: %s", vc.accountID, u)
		return vc.normaliseWebhookURL(u), nil
	}

	if saved := vc.getConfigWebhookURL(); saved != "" {
		log.Printf("[VB:%s] webhook URL from config: %s", vc.accountID, saved)
		if vc.testURLReachable(ctx, saved) {
			return vc.normaliseWebhookURL(saved), nil
		}
		log.Printf("[VB:%s] saved webhook URL not reachable, re-detecting", vc.accountID)
	}

	if domain := vc.getConfigWebhookDomain(); domain != "" {
		u := fmt.Sprintf("https://%s%s", domain, vbDefaultWebhookPath)
		log.Printf("[VB:%s] webhook URL from domain config: %s", vc.accountID, u)
		return u, nil
	}

	ip, err := vc.detectPublicIP(ctx)
	if err != nil {
		return "", fmt.Errorf("auto-detect public IP: %w", err)
	}
	u := fmt.Sprintf("http://%s:%s%s", ip, port, vbDefaultWebhookPath)
	log.Printf("[VB:%s] webhook URL auto-detected: %s", vc.accountID, u)

	log.Printf("[VB:%s] ⚠ using plain HTTP webhook — set viber.webhook_domain for HTTPS (required for production)", vc.accountID)

	return u, nil
}

func (vc *ViberCollector) detectPublicIP(ctx context.Context) (string, error) {
	client := &http.Client{Timeout: vbHTTPTimeout}
	var lastErr error

	for attempt := 0; attempt < vbIPDetectRetries; attempt++ {
		for _, svc := range publicIPServices {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, svc, nil)
			if err != nil {
				lastErr = err
				continue
			}
			resp, err := client.Do(req)
			if err != nil {
				lastErr = err
				vbDbg(vc.accountID, "IP service %s failed: %v", svc, err)
				continue
			}
			body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK {
				lastErr = fmt.Errorf("service %s: HTTP %d", svc, resp.StatusCode)
				continue
			}
			ip := strings.TrimSpace(string(body))
			if net.ParseIP(ip) != nil {
				log.Printf("[VB:%s] public IP detected via %s: %s", vc.accountID, svc, ip)
				return ip, nil
			}
			lastErr = fmt.Errorf("service %s: invalid IP response: %q", svc, ip)
		}
		if attempt < vbIPDetectRetries-1 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	return "", fmt.Errorf("all IP detection services failed: %v", lastErr)
}

func (vc *ViberCollector) testURLReachable(ctx context.Context, u string) bool {
	client := &http.Client{Timeout: vbHTTPTimeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		vbDbg(vc.accountID, "reachability test failed for %s: %v", u, err)
		return false
	}
	resp.Body.Close()
	reachable := resp.StatusCode < 500
	vbDbg(vc.accountID, "reachability test %s → HTTP %d reachable=%v", u, resp.StatusCode, reachable)
	return reachable
}

func (vc *ViberCollector) normaliseWebhookURL(u string) string {
	u = strings.TrimRight(u, "/")
	if !strings.HasSuffix(u, strings.TrimRight(vbDefaultWebhookPath, "/")) {
		u = u + vbDefaultWebhookPath
	}
	return u
}

func (vc *ViberCollector) persistWebhookURL(url string) {
	if vc.configMgr == nil {
		return
	}
	cfg := vc.configMgr.GetConfig()
	if cfg == nil {
		return
	}
	p, ok := cfg.Platforms[vc.platformID]
	if !ok {
		return
	}
	if p.Viber == nil {
		p.Viber = &config.ViberConfig{}
	}
	if p.Viber.WebhookURL == url {
		return
	}
	p.Viber.WebhookURL = url
	cfg.Platforms[vc.platformID] = p
	if err := vc.configMgr.Save(); err != nil {
		log.Printf("[VB:%s] ⚠ could not persist webhook URL to config: %v", vc.accountID, err)
	} else {
		log.Printf("[VB:%s] ✓ webhook URL persisted to config", vc.accountID)
	}
}

func (vc *ViberCollector) printWebhookHelp() {
	port := strings.TrimPrefix(vc.getListenPort(), ":")
	log.Printf("[VB:%s] ─────────────────────────────────────────────────────────", vc.accountID)
	log.Printf("[VB:%s] Viber webhook not registered. The bot will not receive messages.", vc.accountID)
	log.Printf("[VB:%s] To fix, choose one of:", vc.accountID)
	log.Printf("[VB:%s]   A) Set env:    VIBER_WEBHOOK_URL=https://yourdomain.com%s", vc.accountID, vbDefaultWebhookPath)
	log.Printf("[VB:%s]   B) Set config: platforms.%s.viber.webhook_domain = yourdomain.com", vc.accountID, vc.platformID)
	log.Printf("[VB:%s]   C) Open port %s on your firewall/router (HTTP, auto-detected IP)", vc.accountID, port)
	log.Printf("[VB:%s]      Then Cloudflare proxy in front for HTTPS (free)", vc.accountID)
	log.Printf("[VB:%s] ─────────────────────────────────────────────────────────", vc.accountID)
}

func (vc *ViberCollector) startWebhookServer(client *viber.Viber) error {
	port := vc.getListenPort()
	mux := http.NewServeMux()

	mux.Handle(vbDefaultWebhookPath, client)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","platform":"viber","account":"%s"}`, vc.accountID)
	})

	vc.webhookServer = &http.Server{
		Addr:         port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ready := make(chan error, 1)
	vc.wg.Add(1)
	go func() {
		defer vc.wg.Done()
		ln, err := net.Listen("tcp", port)
		if err != nil {
			ready <- err
			return
		}
		ready <- nil
		log.Printf("[VB:%s] webhook HTTP server listening on %s%s",
			vc.accountID, port, vbDefaultWebhookPath)
		if err := vc.webhookServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[VB:%s] webhook server error: %v", vc.accountID, err)
			vc.reportError("WEBHOOK_SERVER_ERROR", err.Error(), "critical")
		}
		log.Printf("[VB:%s] webhook server stopped", vc.accountID)
	}()

	go func() {
		<-vc.shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := vc.webhookServer.Shutdown(ctx); err != nil {
			log.Printf("[VB:%s] webhook server shutdown error: %v", vc.accountID, err)
		}
	}()

	select {
	case err := <-ready:
		return err
	case <-time.After(3 * time.Second):
		return fmt.Errorf("webhook server did not bind within 3s on %s", port)
	}
}

func (vc *ViberCollector) checkWebhookHealth(ctx context.Context) {
	now := time.Now()

	if now.Sub(vc.lastWebhookCheck) < vbWebhookCheckInterval {
		return
	}
	vc.lastWebhookCheck = now

	if !vc.webhookRegistered.Load() {
		log.Printf("[VB:%s] webhook not registered, retrying...", vc.accountID)
		go vc.ensureWebhookRegistered(ctx)
		return
	}

	vc.lastMsgMu.Lock()
	silence := now.Sub(vc.lastMessageAt)
	vc.lastMsgMu.Unlock()

	if silence > vbSilenceThreshold {
		log.Printf("[VB:%s] no messages for %v — re-registering webhook to recover", vc.accountID, silence)
		vc.webhookRegistered.Store(false)
		go vc.ensureWebhookRegistered(ctx)
	}
}

func (vc *ViberCollector) handleMessage(v *viber.Viber, u viber.User, m viber.Message, token uint64, t time.Time) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[VB:CRASH:%s] handleMessage panic: %v", vc.accountID, r)
		}
	}()
	vbDbg(vc.accountID, "handleMessage: user=%s token=%d type=%T", u.ID, token, m)

	lastToken, err := vc.getUserCursor(u.ID)
	if err != nil {
		log.Printf("[VB:%s] getUserCursor error: %v", vc.accountID, err)
	}
	if token <= lastToken {
		vbDbg(vc.accountID, "duplicate token %d (last %d), skipping", token, lastToken)
		return
	}

	vc.lastMsgMu.Lock()
	vc.lastMessageAt = time.Now()
	vc.lastMsgMu.Unlock()

	if vc.config != nil && !vc.config.ListenMessages {
		vbDbg(vc.accountID, "ListenMessages disabled, dropping")
		return
	}

	dedupeKey := fmt.Sprintf("msg_%d", token)
	if !vc.checkAndMarkSeen(dedupeKey) {
		vbDbg(vc.accountID, "duplicate token %d (in‑memory), skipping", token)
		return
	}

	notif, err := vc.processMessage(u, m, token, t)
	if err != nil {
		log.Printf("[VB:MSG:%s] processMessage error: %v", vc.accountID, err)
		return
	}
	if notif != nil {
		if vc.pushNotification(notif) {
			if err := vc.updateUserCursor(u.ID, token); err != nil {
				log.Printf("[VB:%s] updateUserCursor error: %v", vc.accountID, err)
			}
		}
	}
}

func (vc *ViberCollector) handleSubscribed(v *viber.Viber, u viber.User, token uint64, t time.Time) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[VB:CRASH:%s] handleSubscribed panic: %v", vc.accountID, r)
		}
	}()
	log.Printf("[VB:%s] user subscribed: %s (%s)", vc.accountID, u.Name, u.ID)

	vc.lastMsgMu.Lock()
	vc.lastMessageAt = time.Now()
	vc.lastMsgMu.Unlock()

	if vc.config == nil || !vc.config.ListenMessages {
		return
	}
	notif := &Notification{
		ID:         fmt.Sprintf("vb_subscribed_%s_%d", vc.accountID, time.Now().UnixNano()),
		PlatformID: vc.platformID,
		SubtypeID:  vc.subtypeID,
		AccountID:  vc.accountID,
		Type:       NotificationTypeFollow,
		Timestamp:  t,
		Message: &MessageData{
			ConversationID: u.ID,
			MessageID:      fmt.Sprintf("%d", token),
			Sender: UserInfo{
				UserID:      vc.generateUserID(u.ID),
				Username:    u.ID,
				DisplayName: u.Name,
				ProfileURL:  u.Avatar,
			},
			Text:           "[subscribed]",
			Timestamp:      t,
			DeliveryStatus: "delivered",
		},
		RawData: map[string]interface{}{
			"event":     "subscribed",
			"user_id":   u.ID,
			"user_name": u.Name,
			"platform":  "viber",
			"subtype":   vc.subtype,
		},
		CollectedAt: time.Now(),
	}
	vc.pushNotification(notif)
}

func (vc *ViberCollector) handleUnsubscribed(v *viber.Viber, userID string, token uint64, t time.Time) {
	log.Printf("[VB:%s] user unsubscribed: %s", vc.accountID, userID)
}

func (vc *ViberCollector) handleDelivered(v *viber.Viber, userID string, token uint64, t time.Time) {
	vbDbg(vc.accountID, "delivered: user=%s token=%d", userID, token)
}

func (vc *ViberCollector) handleSeen(v *viber.Viber, userID string, token uint64, t time.Time) {
	vbDbg(vc.accountID, "seen: user=%s token=%d", userID, token)
}

func (vc *ViberCollector) handleFailed(v *viber.Viber, userID string, token uint64, descr string, t time.Time) {
	log.Printf("[VB:%s] send failed: user=%s token=%d reason=%s", vc.accountID, userID, token, descr)
	vc.reportError("SEND_FAILED",
		fmt.Sprintf("message to %s failed (token %d): %s", userID, token, descr), "warning")
}

func (vc *ViberCollector) handleConversationStarted(
	v *viber.Viber, u viber.User,
	conversationType, ctx string,
	subscribed bool, token uint64, t time.Time,
) viber.Message {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[VB:CRASH:%s] handleConversationStarted panic: %v", vc.accountID, r)
		}
	}()
	log.Printf("[VB:%s] conversation started: user=%s (%s) subscribed=%v",
		vc.accountID, u.Name, u.ID, subscribed)

	vc.lastMsgMu.Lock()
	vc.lastMessageAt = time.Now()
	vc.lastMsgMu.Unlock()

	if vc.config == nil || !vc.config.ListenMessages {
		return nil
	}
	dedupeKey := fmt.Sprintf("conv_started_%s", u.ID)
	if !vc.checkAndMarkSeen(dedupeKey) {
		return nil
	}
	notif := &Notification{
		ID:         fmt.Sprintf("vb_conv_%s_%d", vc.accountID, time.Now().UnixNano()),
		PlatformID: vc.platformID,
		SubtypeID:  vc.subtypeID,
		AccountID:  vc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  t,
		Message: &MessageData{
			ConversationID: u.ID,
			MessageID:      fmt.Sprintf("%d", token),
			Sender: UserInfo{
				UserID:      vc.generateUserID(u.ID),
				Username:    u.ID,
				DisplayName: u.Name,
				ProfileURL:  u.Avatar,
			},
			Text:           "[conversation_started]",
			Timestamp:      t,
			DeliveryStatus: "delivered",
		},
		RawData: map[string]interface{}{
			"event":             "conversation_started",
			"conversation_type": conversationType,
			"context":           ctx,
			"subscribed":        subscribed,
			"platform":          "viber",
			"subtype":           vc.subtype,
		},
		CollectedAt: time.Now(),
	}
	vc.pushNotification(notif)
	return nil
}

func (vc *ViberCollector) extractMessageContent(m viber.Message) (
	text string,
	remoteURLs []string,
	mediaType string,
	extraRaw map[string]interface{},
) {
	extraRaw = make(map[string]interface{})

	switch msg := m.(type) {
	case *viber.TextMessage:
		text = msg.Text
		mediaType = "text"

	case *viber.PictureMessage:
		text = msg.Text
		mediaType = "image"
		if msg.Media != "" {
			remoteURLs = append(remoteURLs, msg.Media)
		}
		if msg.Thumbnail != "" {
			extraRaw["thumbnail_url"] = msg.Thumbnail
		}

	case *viber.VideoMessage:
		text = msg.Text
		mediaType = "video"
		if msg.Media != "" {
			remoteURLs = append(remoteURLs, msg.Media)
		}
		if msg.Thumbnail != "" {
			extraRaw["thumbnail_url"] = msg.Thumbnail
		}
		extraRaw["video_size"] = msg.Size
		extraRaw["video_duration"] = msg.Duration

	case *viber.URLMessage:
		text = msg.Text
		mediaType = "url"
		if msg.Media != "" {
			extraRaw["url"] = msg.Media
		}

	default:
		log.Printf("[VB:UNKNOWN:%s] unsupported message type %T", vc.accountID, m)
		text = "[Unsupported message type]"
		mediaType = "unknown"
	}
	return
}

func (vc *ViberCollector) processMessage(
	u viber.User,
	m viber.Message,
	token uint64,
	t time.Time,
) (*Notification, error) {
	text, remoteURLs, mediaType, extraRaw := vc.extractMessageContent(m)

	log.Printf("[VB:PROCESS:%s] msg token=%d user=%s(%s) type=%T text_len=%d",
		vc.accountID, token, u.Name, u.ID, m, len(text))

	localPaths := vc.downloadMediaURLs(remoteURLs, mediaType)

	productID := vc.extractProductID(text)
	var productData map[string]interface{}
	if productID != "" {
		if data, err := vc.getProductData(productID); err == nil {
			productData = data
		} else {
			log.Printf("[VB:PROCESS:%s] product lookup %q failed: %v", vc.accountID, productID, err)
		}
	}

	ud, recent, isNew, err := vc.getUserInfoForNotification(context.Background(), u.ID)
	if err != nil {
		log.Printf("[VB:USER:%s] user lookup error for %s: %v – dropping for safety", vc.accountID, u.ID, err)
		return nil, nil
	}

	if !isNew && ud != nil {
		if blocked, _ := ud["is_blocked"].(bool); blocked {
			log.Printf("[VB:BLOCKED:%s] user %s is blocked, dropping", vc.accountID, u.ID)
			return nil, nil
		}
	}

	attachments := vc.buildAttachments(remoteURLs, localPaths, mediaType, token)

	raw := map[string]interface{}{
		"viber_token":       token,
		"user_id":           u.ID,
		"user_country":      u.Country,
		"user_language":     u.Language,
		"message_type":      fmt.Sprintf("%T", m),
		"image_urls":        remoteURLs,
		"downloaded_images": localPaths,
		"platform":          "viber",
		"subtype":           vc.subtype,
		"collected_at":      time.Now().Format(time.RFC3339),
	}
	if productData != nil {
		raw["product_id"] = productID
		raw["product_data"] = productData
	}
	for k, v := range extraRaw {
		raw[k] = v
	}
	if isNew {
		raw["is_new_user"] = true
	} else {
		raw["user_data"] = ud
		if len(recent) > 0 {
			raw["recent_messages"] = recent
		}
	}

	return &Notification{
		ID:         fmt.Sprintf("vb_msg_%s_%d", vc.accountID, time.Now().UnixNano()),
		PlatformID: vc.platformID,
		SubtypeID:  vc.subtypeID,
		AccountID:  vc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  t,
		Message: &MessageData{
			ConversationID:   u.ID,
			ConversationName: u.Name,
			IsGroup:          false,
			MessageID:        fmt.Sprintf("%d", token),
			Sender: UserInfo{
				UserID:      vc.generateUserID(u.ID),
				Username:    u.ID,
				DisplayName: u.Name,
				ProfileURL:  u.Avatar,
			},
			Text:           text,
			Timestamp:      t,
			IsRead:         false,
			DeliveryStatus: "delivered",
			MediaAttached:  attachments,
		},
		RawData:     raw,
		CollectedAt: time.Now(),
	}, nil
}

func (vc *ViberCollector) downloadMediaURLs(remoteURLs []string, mediaType string) []string {
	if !vc.imageRecognitionEnabled() {
		return nil
	}
	if mediaType != "image" && mediaType != "video" && mediaType != "document" {
		return nil
	}
	if len(remoteURLs) == 0 {
		return nil
	}
	dir := vc.getMediaDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("[VB:%s] media dir create failed: %v", vc.accountID, err)
		return nil
	}
	var localPaths []string
	for _, u := range remoteURLs {
		if u == "" {
			continue
		}
		path, err := vc.downloadRemoteMedia(u, mediaType)
		if err != nil {
			log.Printf("[VB:%s] media download failed (%s): %v", vc.accountID, u, err)
			continue
		}
		localPaths = append(localPaths, path)
	}
	return localPaths
}

func (vc *ViberCollector) downloadRemoteMedia(mediaURL, mediaType string) (string, error) {
	h := sha256.Sum256([]byte(mediaURL))
	ext := extensionForViberMedia(mediaType)
	fullPath := filepath.Join(vc.getMediaDir(), hex.EncodeToString(h[:])[:16]+ext)

	if info, err := os.Stat(fullPath); err == nil && info.Size() > 0 {
		vbDbg(vc.accountID, "media already cached: %s", fullPath)
		return fullPath, nil
	}

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if os.IsExist(err) {
		return fullPath, nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(mediaURL)
	if err != nil {
		os.Remove(fullPath)
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Remove(fullPath)
		return "", fmt.Errorf("HTTP %d from %s", resp.StatusCode, mediaURL)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		os.Remove(fullPath)
		return "", err
	}
	log.Printf("[VB:%s] ✓ media cached: %s", vc.accountID, fullPath)
	return fullPath, nil
}

func extensionForViberMedia(mediaType string) string {
	switch mediaType {
	case "image":
		return ".jpg"
	case "video":
		return ".mp4"
	default:
		return ".bin"
	}
}

func (vc *ViberCollector) buildAttachments(
	remoteURLs, localPaths []string,
	mediaType string,
	token uint64,
) []MediaAttachment {
	var attachments []MediaAttachment
	for idx, u := range remoteURLs {
		local := ""
		if idx < len(localPaths) {
			local = localPaths[idx]
		}
		attachments = append(attachments, MediaAttachment{
			ID:        fmt.Sprintf("vb_%d_%d", token, idx),
			Type:      mediaType,
			URL:       u,
			Thumbnail: local,
		})
	}
	return attachments
}

func (vc *ViberCollector) imageRecognitionEnabled() bool {
	if vc.configMgr == nil {
		return false
	}
	cfg := vc.configMgr.GetConfig()
	return cfg != nil && cfg.ImageRecognition.Enabled
}

func (vc *ViberCollector) pushNotification(n *Notification) bool {
	select {
	case vc.notifBuffer <- n:
		vbDbg(vc.accountID, "notification pushed (buf=%d/%d)", len(vc.notifBuffer), vbNotifBufferSize)
		return true
	default:
		log.Printf("[VB:WARN:%s] notification buffer full, message dropped", vc.accountID)
		vc.reportError("BUFFER_FULL", "notification buffer full", "warning")
		return false
	}
}

func (vc *ViberCollector) checkAndMarkSeen(key string) bool {
	vc.seenMsgMu.Lock()
	defer vc.seenMsgMu.Unlock()
	vc.pruneSeenMsgs()
	if _, seen := vc.seenMsgs[key]; seen {
		return false
	}
	vc.seenMsgs[key] = time.Now().Add(vbMsgDedupeWindow)
	return true
}

func (vc *ViberCollector) pruneSeenMsgs() {
	now := time.Now()
	for key, exp := range vc.seenMsgs {
		if now.After(exp) {
			delete(vc.seenMsgs, key)
		}
	}
}

func (vc *ViberCollector) pauseCollection() error {
	if vc.pauseCount.Add(1) > 1 {
		return nil
	}
	log.Printf("[VB:PAUSE:%s] pause requested", vc.accountID)
	if !vc.collectRunning.Load() {
		return nil
	}
	select {
	case <-vc.pauseAck:
		log.Printf("[VB:PAUSE:%s] ✓ pause ack received", vc.accountID)
	case <-time.After(vbPauseAckTimeout):
		log.Printf("[VB:PAUSE:%s] pause ack timeout (proceeding)", vc.accountID)
	}
	return nil
}

func (vc *ViberCollector) resumeCollection() {
	if vc.pauseCount.Add(-1) > 0 {
		return
	}
	newResume := make(chan struct{})
	vc.resumeMu.Lock()
	old := vc.resumeReq
	vc.resumeReq = newResume
	vc.resumeMu.Unlock()
	close(old)
	log.Printf("[VB:RESUME:%s] ✓ collection resumed", vc.accountID)
}

func (vc *ViberCollector) checkPause(ctx context.Context) bool {
	if vc.pauseCount.Load() <= 0 {
		return true
	}
	log.Printf("[VB:PAUSE:%s] paused, sending ack and waiting for resume", vc.accountID)
	select {
	case vc.pauseAck <- struct{}{}:
	default:
	}
	vc.resumeMu.Lock()
	resumeCh := vc.resumeReq
	vc.resumeMu.Unlock()
	select {
	case <-resumeCh:
		log.Printf("[VB:PAUSE:%s] resumed", vc.accountID)
		return true
	case <-ctx.Done():
		return false
	}
}

func (vc *ViberCollector) Collect(ctx context.Context, _ []*CookieData) ([]*Notification, error) {
	log.Printf("[VB:COLLECT:%s] starting collection", vc.accountID)

	if !vc.collectRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("collect already in progress for %s", vc.accountID)
	}
	defer vc.collectRunning.Store(false)

	if vc.config != nil && !vc.config.ListenMessages {
		return []*Notification{}, nil
	}

	if vc.viberClient == nil {
		return nil, fmt.Errorf("viber client not set for %s", vc.accountID)
	}
	if !vc.connected.Load() {
		return []*Notification{}, nil
	}

	vc.checkWebhookHealth(ctx)

	if !vc.checkPause(ctx) {
		return nil, ctx.Err()
	}

	vc.ProcessPendingInstructions()

	var notifications []*Notification
	for len(notifications) < vbMaxBatchSize {
		if !vc.checkPause(ctx) {
			break
		}
		select {
		case n := <-vc.notifBuffer:
			notifications = append(notifications, n)
		case <-ctx.Done():
			goto done
		default:
			select {
			case n := <-vc.notifBuffer:
				notifications = append(notifications, n)
			case <-time.After(vbCollectDrain):
				goto done
			case <-ctx.Done():
				goto done
			}
		}
	}

done:
	vc.ProcessPendingInstructions()
	log.Printf("[VB:COLLECT:%s] ✓ returning %d notifications (webhook_registered=%v)",
		vc.accountID, len(notifications), vc.webhookRegistered.Load())
	return notifications, nil
}

func (vc *ViberCollector) ReceiveInstructions(inst *shared.AutomationInstruction) error {
	if inst.Platform != "viber" && inst.Platform != vc.platformID {
		return fmt.Errorf("wrong platform: %s", inst.Platform)
	}
	if inst.TicketID == "" {
		return fmt.Errorf("empty ticket ID")
	}
	if inst.CreatedAt.IsZero() {
		inst.CreatedAt = time.Now()
	}
	select {
	case vc.instructionQueue <- inst:
		log.Printf("[VB:INSTR:%s] queued ticket=%s action=%s steps=%d",
			vc.accountID, inst.TicketID, inst.Action, len(inst.Steps))
		return nil
	case <-time.After(2 * time.Second):
		vc.reportError("QUEUE_FULL", "instruction queue full", "warning")
		return fmt.Errorf("instruction queue full for %s", vc.accountID)
	}
}

func (vc *ViberCollector) ProcessPendingInstructions() {
	vc.executionMu.Lock()
	defer vc.executionMu.Unlock()
	count := 0
	for {
		select {
		case inst := <-vc.instructionQueue:
			count++
			log.Printf("[VB:INSTR:%s] executing ticket=%s action=%s",
				vc.accountID, inst.TicketID, inst.Action)
			if err := vc.executeInstruction(inst); err != nil {
				log.Printf("[VB:INSTR:%s] ticket=%s failed: %v", vc.accountID, inst.TicketID, err)
			}
		default:
			if count > 0 {
				log.Printf("[VB:INSTR:%s] processed %d instructions", vc.accountID, count)
			}
			return
		}
	}
}

func (vc *ViberCollector) executeInstruction(inst *shared.AutomationInstruction) error {
	start := time.Now()
	if err := vc.pauseCollection(); err != nil {
		log.Printf("[VB:EXEC:%s] pause (proceeding): %v", vc.accountID, err)
	}
	defer vc.resumeCollection()

	timeout := inst.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var lastErr error
	for idx, step := range inst.Steps {
		if ctx.Err() != nil {
			lastErr = fmt.Errorf("instruction timed out before step %d/%d: %w", idx+1, len(inst.Steps), ctx.Err())
			break
		}
		log.Printf("[VB:EXEC:%s] step %d/%d type=%s", vc.accountID, idx+1, len(inst.Steps), step.Type)

		maxAttempts := step.RetryCount
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		var stepErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			stepErr = vc.executeStep(ctx, step)
			if stepErr == nil {
				break
			}
			log.Printf("[VB:EXEC:%s] step %d/%d attempt %d/%d error: %v",
				vc.accountID, idx+1, len(inst.Steps), attempt, maxAttempts, stepErr)
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if stepErr != nil {
			lastErr = stepErr
			if len(inst.FallbackSteps) > 0 {
				log.Printf("[VB:EXEC:%s] step %d/%d failed after %d attempt(s), running fallback steps",
					vc.accountID, idx+1, len(inst.Steps), maxAttempts)
				lastErr = vc.runFallbackSteps(ctx, inst.FallbackSteps)
			}
			break
		}

		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	log.Printf("[VB:EXEC:%s] ticket=%s done in %v (err=%v)",
		vc.accountID, inst.TicketID, time.Since(start), lastErr)
	return lastErr
}

// runFallbackSteps runs an instruction's FallbackSteps after its main steps
// failed; its result replaces the main loop's error.
func (vc *ViberCollector) runFallbackSteps(ctx context.Context, steps []shared.InstructionStep) error {
	var lastErr error
	for i, step := range steps {
		if err := vc.executeStep(ctx, step); err != nil {
			log.Printf("[VB:EXEC:%s] fallback step %d/%d error: %v", vc.accountID, i+1, len(steps), err)
			lastErr = err
		}
		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	return lastErr
}

func (vc *ViberCollector) executeStep(ctx context.Context, step shared.InstructionStep) error {
	if step.DelayBefore > 0 {
		time.Sleep(time.Duration(step.DelayBefore) * time.Millisecond)
	}
	switch step.Type {
	case shared.StepTypeSendMessage:
		return vc.stepSendMessage(ctx, step)
	case shared.StepTypeReply:
		return vc.stepReply(ctx, step)
	case shared.StepTypeUpload:
		return vc.stepUpload(ctx, step)
	case shared.StepTypeDownload:
		return vc.stepDownload(ctx, step)
	case shared.StepTypeWait:
		return vc.stepWait(step)
	case shared.StepTypeAPICall:
		return vc.stepAPICall(ctx, step)
	case shared.StepTypeLog:
		log.Printf("[VB:LOG:%s] %s", vc.accountID, step.Value)
		return nil
	case shared.StepTypeRateLimitCheck:
		log.Printf("[VB:RATELIMIT:%s] rate-limit check: OK", vc.accountID)
		return nil
	case shared.StepTypeBlock, shared.StepTypeUnfollow:
		log.Printf("[VB:UNSUPPORTED:%s] step=%s — no block/unblock endpoint in Viber Bot API", vc.accountID, step.Type)
		return nil
	case shared.StepTypeReact, shared.StepTypeLike:
		log.Printf("[VB:UNSUPPORTED:%s] step=%s — Viber bots cannot send reactions", vc.accountID, step.Type)
		return nil
	case shared.StepTypeFollow:
		log.Printf("[VB:UNSUPPORTED:%s] step=%s — Viber subscriptions are user-initiated only", vc.accountID, step.Type)
		return nil
	case shared.StepTypeSearch:
		log.Printf("[VB:UNSUPPORTED:%s] step=%s — no contacts search in Viber Bot API", vc.accountID, step.Type)
		return nil
	case shared.StepTypeSave:
		log.Printf("[VB:UNSUPPORTED:%s] step=%s — no pin/save endpoint in Viber Bot API", vc.accountID, step.Type)
		return nil
	case shared.StepTypeShare:
		log.Printf("[VB:UNSUPPORTED:%s] step=%s — Viber bots cannot forward messages", vc.accountID, step.Type)
		return nil
	case shared.StepTypeDBUpdate, shared.StepTypeDBRecord, shared.StepTypeAIGenerate:
		log.Printf("[VB:SKIP:%s] step=%s is orchestrator-side, skipping", vc.accountID, step.Type)
		return nil
	case shared.StepTypeNavigate, shared.StepTypeClick, shared.StepTypeType,
		shared.StepTypeScroll, shared.StepTypeJavaScript, shared.StepTypePress:
		log.Printf("[VB:SKIP:%s] step=%s is browser-only, not applicable to Viber", vc.accountID, step.Type)
		return nil
	default:
		return fmt.Errorf("unknown step type: %s", step.Type)
	}
}

func (vc *ViberCollector) stepSendMessage(ctx context.Context, step shared.InstructionStep) error {
	if step.Value == "" && step.Options["image_url"] == nil {
		return fmt.Errorf("stepSendMessage: message text or image_url required")
	}
	recipientID, err := vc.resolveRecipient(step.Options)
	if err != nil {
		return fmt.Errorf("stepSendMessage: %w", err)
	}
	client := vc.safeClient()
	if client == nil {
		return fmt.Errorf("stepSendMessage: viber client not connected")
	}

	if imageURL, _ := step.Options["image_url"].(string); imageURL != "" {
		thumbURL, _ := step.Options["thumb_url"].(string)
		log.Printf("[VB:SEND:%s] sending picture to %s", vc.accountID, recipientID)
		if _, err := client.SendPictureMessage(recipientID, step.Value, imageURL, thumbURL); err != nil {
			return fmt.Errorf("stepSendMessage SendPicture: %w", err)
		}
		log.Printf("[VB:SEND:%s] ✓ picture sent to %s", vc.accountID, recipientID)
		return nil
	}

	if linkURL, _ := step.Options["url"].(string); linkURL != "" {
		log.Printf("[VB:SEND:%s] sending URL message to %s", vc.accountID, recipientID)
		if _, err := client.SendURLMessage(recipientID, step.Value, linkURL); err != nil {
			return fmt.Errorf("stepSendMessage SendURL: %w", err)
		}
		log.Printf("[VB:SEND:%s] ✓ URL message sent to %s", vc.accountID, recipientID)
		return nil
	}

	log.Printf("[VB:SEND:%s] sending text (len=%d) to %s", vc.accountID, len(step.Value), recipientID)
	if _, err := client.SendTextMessage(recipientID, step.Value); err != nil {
		return fmt.Errorf("stepSendMessage SendText: %w", err)
	}
	log.Printf("[VB:SEND:%s] ✓ text sent to %s", vc.accountID, recipientID)
	return nil
}

func (vc *ViberCollector) stepReply(ctx context.Context, step shared.InstructionStep) error {
	return vc.stepSendMessage(ctx, step)
}

func (vc *ViberCollector) stepUpload(ctx context.Context, step shared.InstructionStep) error {
	recipientID, err := vc.resolveRecipient(step.Options)
	if err != nil {
		return fmt.Errorf("stepUpload: %w", err)
	}
	client := vc.safeClient()
	if client == nil {
		return fmt.Errorf("stepUpload: viber client not connected")
	}

	imageURL, _ := step.Options["image_url"].(string)
	if imageURL == "" {
		imageURL, _ = step.Options["url"].(string)
	}
	if imageURL == "" {
		if fp, _ := step.Options["file_path"].(string); fp != "" {
			return fmt.Errorf("stepUpload: Viber Bot API requires a public URL — "+
				"cannot upload local file %q. Host the file and set options.image_url instead", fp)
		}
		return fmt.Errorf("stepUpload: image_url or url required")
	}

	caption, _ := step.Options["caption"].(string)
	if caption == "" {
		caption = step.Value
	}
	thumbURL, _ := step.Options["thumb_url"].(string)
	mediaType, _ := step.Options["media_type"].(string)

	if strings.ToLower(mediaType) == "url" {
		if _, err := client.SendURLMessage(recipientID, caption, imageURL); err != nil {
			return fmt.Errorf("stepUpload SendURL: %w", err)
		}
	} else {
		if _, err := client.SendPictureMessage(recipientID, caption, imageURL, thumbURL); err != nil {
			return fmt.Errorf("stepUpload SendPicture: %w", err)
		}
	}
	log.Printf("[VB:UPLOAD:%s] ✓ media sent to %s", vc.accountID, recipientID)
	return nil
}

func (vc *ViberCollector) stepDownload(_ context.Context, step shared.InstructionStep) error {
	u, _ := step.Options["url"].(string)
	if u == "" {
		u = step.Value
	}
	if u == "" {
		return fmt.Errorf("stepDownload: url required")
	}
	savePath, _ := step.Options["save_path"].(string)
	log.Printf("[VB:DOWNLOAD:%s] url=%s save_path=%q", vc.accountID, u, savePath)

	data, err := vc.fetchURL(u)
	if err != nil {
		return fmt.Errorf("stepDownload fetch: %w", err)
	}

	if savePath == "" {
		h := sha256.Sum256([]byte(u))
		ext := extensionFromURLorMime(u)
		if err := os.MkdirAll(vc.getMediaDir(), 0755); err != nil {
			return err
		}
		savePath = filepath.Join(vc.getMediaDir(), hex.EncodeToString(h[:])[:16]+ext)
	}

	if err := os.WriteFile(savePath, data, 0644); err != nil {
		return fmt.Errorf("stepDownload write: %w", err)
	}
	log.Printf("[VB:DOWNLOAD:%s] ✓ saved %s → %s", vc.accountID, u, savePath)
	return nil
}

func (vc *ViberCollector) stepWait(step shared.InstructionStep) error {
	ms := step.DelayAfter
	if ms <= 0 {
		ms = 2000
	}
	log.Printf("[VB:WAIT:%s] sleeping %dms", vc.accountID, ms)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return nil
}

func (vc *ViberCollector) stepAPICall(ctx context.Context, step shared.InstructionStep) error {
	targetURL := step.Value
	if u, ok := step.Options["url"].(string); ok && u != "" {
		targetURL = u
	}
	if targetURL == "" {
		return fmt.Errorf("stepAPICall: url required")
	}
	method, _ := step.Options["method"].(string)
	if method == "" {
		method = http.MethodGet
	}
	method = strings.ToUpper(method)

	var bodyReader io.Reader
	if body, ok := step.Options["body"].(string); ok && body != "" {
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	if err != nil {
		return fmt.Errorf("stepAPICall build request: %w", err)
	}
	if headers, ok := step.Options["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			if vs, ok := v.(string); ok {
				req.Header.Set(k, vs)
			}
		}
	}
	if req.Header.Get("Content-Type") == "" && bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stepAPICall request: %w", err)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("stepAPICall read response: %w", err)
	}
	log.Printf("[VB:API:%s] %s %s → HTTP %d (%d bytes)",
		vc.accountID, method, targetURL, resp.StatusCode, len(respBytes))
	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	step.Options["result"] = string(respBytes)
	step.Options["status_code"] = resp.StatusCode
	if resp.StatusCode >= 400 {
		return fmt.Errorf("stepAPICall: server returned HTTP %d: %s",
			resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	return nil
}

func (vc *ViberCollector) resolveRecipient(opts map[string]interface{}) (string, error) {
	for _, key := range []string{"to", "recipient", "user_id", "chat_id"} {
		if v, ok := opts[key].(string); ok && v != "" {
			return v, nil
		}
	}
	return "", fmt.Errorf("no recipient in step options (to/recipient/user_id/chat_id)")
}

func (vc *ViberCollector) getUserInfoForNotification(
	ctx context.Context,
	viberUserID string,
) (userData map[string]interface{}, recentMessages []string, isNew bool, err error) {
	if vc.db == nil {
		return nil, nil, true, nil
	}
	const q = `
		SELECT id, display_name, is_blocked, last_intent
		  FROM platform_users
		 WHERE platform = ? AND platform_user_id = ?`
	var id, displayName, lastIntent sql.NullString
	var blocked sql.NullBool
	err = vc.db.QueryRowContext(ctx, q, "viber", viberUserID).Scan(
		&id, &displayName, &blocked, &lastIntent,
	)
	if err == sql.ErrNoRows {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("getUserInfoForNotification: %w", err)
	}
	userData = map[string]interface{}{
		"display_name": displayName.String,
		"is_blocked":   blocked.Bool,
		"last_intent":  lastIntent.String,
	}
	if id.Valid && id.String != "" {
		recentMessages, _ = vc.getRecentMessages(ctx, id.String, 3)
	}
	return userData, recentMessages, false, nil
}

func (vc *ViberCollector) getRecentMessages(ctx context.Context, userID string, limit int) ([]string, error) {
	const q = `
		SELECT message_text FROM (
			SELECT message_text, received_at
			  FROM messages
			 WHERE user_id = ? AND direction = 'incoming'
			 ORDER BY received_at DESC
			 LIMIT ?
		) sub ORDER BY received_at ASC`
	rows, err := vc.db.QueryContext(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var texts []string
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err == nil && text != "" {
			texts = append(texts, text)
		}
	}
	return texts, rows.Err()
}

func (vc *ViberCollector) getProductData(productID string) (map[string]interface{}, error) {
	if vc.db == nil || productID == "" {
		return nil, fmt.Errorf("no db or empty productID")
	}
	const exactQ = `
		SELECT id, sku, name, description, category, subcategory, tags,
		       price, price_per_pack, quantity_per_pack, currency,
		       stock, reserved_stock, low_stock_threshold,
		       image_url, thumbnail_url, weight_kg, dimensions,
		       is_active, is_featured, metadata, created_at, updated_at
		FROM products
		WHERE sku = ? AND is_active = 1
		LIMIT 1`
	const fuzzyQ = `
		SELECT id, sku, name, description, category, subcategory, tags,
		       price, price_per_pack, quantity_per_pack, currency,
		       stock, reserved_stock, low_stock_threshold,
		       image_url, thumbnail_url, weight_kg, dimensions,
		       is_active, is_featured, metadata, created_at, updated_at
		FROM products
		WHERE name LIKE ? AND is_active = 1
		LIMIT 1`
	var (
		id, sku, name, desc, cat, subcat sql.NullString
		tags, currency                   sql.NullString
		price, pricePerPack, weightKg    sql.NullFloat64
		qtyPerPack                       sql.NullInt64
		stock, reservedStock, lowStock   sql.NullInt64
		imageURL, thumbURL, dims         sql.NullString
		isActive, isFeatured             sql.NullBool
		metadata                         sql.NullString
		createdAt, updatedAt             sql.NullTime
	)
	scanInto := func(row *sql.Row) error {
		return row.Scan(
			&id, &sku, &name, &desc, &cat, &subcat, &tags,
			&price, &pricePerPack, &qtyPerPack, &currency,
			&stock, &reservedStock, &lowStock,
			&imageURL, &thumbURL, &weightKg, &dims,
			&isActive, &isFeatured, &metadata, &createdAt, &updatedAt,
		)
	}

	// Exact match on sku first: cheap, indexed lookup, no false positives.
	err := scanInto(vc.db.QueryRow(exactQ, productID))

	// Fall back to a fuzzy name match only when nothing exact was found and the
	// identifier is specific enough (>=4 chars) to keep the table scan rare and
	// avoid matching unrelated products on short/common substrings.
	if err == sql.ErrNoRows && len(productID) >= 4 {
		err = scanInto(vc.db.QueryRow(fuzzyQ, "%"+productID+"%"))
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	parseJSON := func(s sql.NullString) interface{} {
		if !s.Valid || s.String == "" {
			return nil
		}
		var v interface{}
		json.Unmarshal([]byte(s.String), &v)
		return v
	}
	return map[string]interface{}{
		"id": id.String, "sku": sku.String, "name": name.String,
		"description": desc.String, "category": cat.String, "subcategory": subcat.String,
		"tags": parseJSON(tags), "price": price.Float64,
		"price_per_pack": pricePerPack.Float64, "quantity_per_pack": qtyPerPack.Int64,
		"currency": currency.String, "stock": stock.Int64,
		"reserved_stock": reservedStock.Int64, "low_stock_threshold": lowStock.Int64,
		"image_url": imageURL.String, "thumbnail_url": thumbURL.String,
		"weight_kg": weightKg.Float64, "dimensions": dims.String,
		"is_active": isActive.Bool, "is_featured": isFeatured.Bool,
		"metadata": parseJSON(metadata), "created_at": createdAt.Time, "updated_at": updatedAt.Time,
	}, nil
}

var vbProductPatterns = []*regexp.Regexp{
	regexp.MustCompile(`SKU\s*=\s*["']([^"']+)["']`),
	regexp.MustCompile(`SKU\s*=\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)product\s*id\s*:\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)sku\s*:\s*([^\s<>"']+)`),
}

func (vc *ViberCollector) extractProductID(text string) string {
	for _, p := range vbProductPatterns {
		if m := p.FindStringSubmatch(text); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

func (vc *ViberCollector) getConfigWebhookURL() string {
	if vc.configMgr == nil {
		return ""
	}
	cfg := vc.configMgr.GetConfig()
	if cfg == nil {
		return ""
	}
	p, ok := cfg.Platforms[vc.platformID]
	if !ok || p.Viber == nil {
		return ""
	}
	return p.Viber.WebhookURL
}

func (vc *ViberCollector) getConfigWebhookDomain() string {
	if vc.configMgr == nil {
		return ""
	}
	cfg := vc.configMgr.GetConfig()
	if cfg == nil {
		return ""
	}
	p, ok := cfg.Platforms[vc.platformID]
	if !ok || p.Viber == nil {
		return ""
	}
	return p.Viber.WebhookDomain
}

func (vc *ViberCollector) getListenPort() string {
	if port := os.Getenv("VIBER_WEBHOOK_PORT"); port != "" {
		if !strings.HasPrefix(port, ":") {
			port = ":" + port
		}
		return port
	}
	if vc.configMgr != nil {
		if cfg := vc.configMgr.GetConfig(); cfg != nil {
			if p, ok := cfg.Platforms[vc.platformID]; ok && p.Viber != nil {
				if p.Viber.WebhookPort != "" {
					port := p.Viber.WebhookPort
					if !strings.HasPrefix(port, ":") {
						port = ":" + port
					}
					return port
				}
			}
		}
	}
	return vbDefaultPort
}

func (vc *ViberCollector) generateUserID(viberUserID string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(viberUserID))))
	return "viber_" + hex.EncodeToString(h[:8])
}

func (vc *ViberCollector) safeClient() *viber.Viber {
	vc.clientMu.RLock()
	defer vc.clientMu.RUnlock()
	return vc.viberClient
}

func (vc *ViberCollector) getMediaDir() string {
	if vc.configMgr != nil {
		if cfg := vc.configMgr.GetConfig(); cfg != nil && cfg.Paths.PostImages != "" {
			return cfg.Paths.PostImages
		}
	}
	return vbDefaultMediaDir
}

func (vc *ViberCollector) fetchURL(u string) ([]byte, error) {
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, u)
	}
	return io.ReadAll(resp.Body)
}

func extensionFromURLorMime(rawURL string) string {
	base := filepath.Base(rawURL)
	if idx := strings.Index(base, "?"); idx >= 0 {
		base = base[:idx]
	}
	if ext := filepath.Ext(base); ext != "" {
		return ext
	}
	exts, _ := mime.ExtensionsByType("application/octet-stream")
	if len(exts) > 0 {
		return exts[0]
	}
	return ".bin"
}

func (vc *ViberCollector) reportError(code, msg, severity string) {
	log.Printf("[VB:ERROR:%s] code=%s msg=%q severity=%s", vc.accountID, code, msg, severity)
	select {
	case vc.errorChan <- &PlatformError{
		PlatformID: vc.platformID,
		SubtypeID:  vc.subtypeID,
		AccountID:  vc.accountID,
		ErrorCode:  code,
		ErrorMsg:   msg,
		Timestamp:  time.Now(),
		Severity:   severity,
	}:
	default:
		log.Printf("[VB:ERROR:%s] error chan full, dropped: code=%s msg=%s", vc.accountID, code, msg)
	}
}

func (vc *ViberCollector) GetErrorChannel() <-chan *PlatformError {
	return vc.errorChan
}

func (vc *ViberCollector) Close() {
	log.Printf("[VB:CLOSE:%s] shutting down Viber collector", vc.accountID)
	select {
	case <-vc.shutdown:
	default:
		close(vc.shutdown)
	}
	vc.connected.Store(false)
	vc.wg.Wait()
	log.Printf("[VB:CLOSE:%s] ✓ shutdown complete", vc.accountID)
}

func (vc *ViberCollector) WebhookStatus() map[string]interface{} {
	vc.webhookMu.Lock()
	url := vc.registeredWebhookURL
	vc.webhookMu.Unlock()

	vc.lastMsgMu.Lock()
	lastMsg := vc.lastMessageAt
	vc.lastMsgMu.Unlock()

	return map[string]interface{}{
		"connected":          vc.connected.Load(),
		"webhook_registered": vc.webhookRegistered.Load(),
		"webhook_url":        url,
		"listen_port":        vc.getListenPort(),
		"last_message_at":    lastMsg.Format(time.RFC3339),
		"silence_duration":   time.Since(lastMsg).String(),
	}
}
