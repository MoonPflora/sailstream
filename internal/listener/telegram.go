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
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"sailstream/internal/config"
	"sailstream/internal/enviroment"
	sessionmgr "sailstream/internal/session"
	"sailstream/internal/shared"
)

var debugTG = os.Getenv("TG_DEBUG") == "1"

func tgDbg(accountID, format string, args ...interface{}) {
	if !debugTG {
		return
	}
	log.Printf("[TG:DBG:%s] "+format, append([]interface{}{accountID}, args...)...)
}

const (
	tgNotifBufferSize    = 500
	tgMsgDedupeWindow    = 24 * time.Hour
	tgCollectDrainWindow = 100 * time.Millisecond
	tgMaxBatchSize       = 200
	tgPauseAckTimeout    = 15 * time.Second
	tgMediaDirDefault    = "./media/telegram"
	tgConnectTimeout     = 30 * time.Second
)

type TelegramCollector struct {
	platformID string
	subtypeID  string
	accountID  string
	subtype    string

	config     *ListenerConfig
	db         *sql.DB
	configMgr  *config.ConfigManager
	envMgr     *enviroment.Environment
	sessionMgr *sessionmgr.Manager

	client    *telegram.Client
	clientMu  sync.Mutex
	connected atomic.Bool

	notifBuffer      chan *Notification
	instructionQueue chan *shared.AutomationInstruction
	errorChan        chan *PlatformError

	collectRunning atomic.Bool

	drainPaused atomic.Bool
	// pauseCount is a reference count, not a bool — see fix N4 in whatsapp.go.
	pauseCount atomic.Int32
	pauseAck   chan struct{}
	resumeMu   sync.Mutex
	resumeReq  chan struct{}

	executionMu sync.Mutex

	seenMsgMu   sync.Mutex
	seenMsgs    map[string]time.Time
	seenMsgFile string

	// pendingCursor holds the highest message timestamp seen in the current
	// batch. It is written by processMessage (called from event handlers /
	// fetchMissedMessages) and flushed atomically to global_cursors at the
	// end of each Collect drain, making cursor advancement batch-safe.
	pendingCursorMu   sync.Mutex
	pendingCursorTime time.Time

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func NewTelegramCollector(
	platformID, subtypeID, accountID, subtype string,
	listenerConfig *ListenerConfig,
	db *sql.DB,
	configMgr *config.ConfigManager,
	envMgr *enviroment.Environment,
	sessionMgr *sessionmgr.Manager,
) *TelegramCollector {
	log.Printf("[TG:INIT] NewTelegramCollector platformID=%s subtypeID=%s accountID=%s subtype=%s",
		platformID, subtypeID, accountID, subtype)
	log.Printf("[TG:INIT] Transport: gogram MTProto (no browser/Chrome)")

	tc := &TelegramCollector{
		platformID:       platformID,
		subtypeID:        subtypeID,
		accountID:        accountID,
		subtype:          subtype,
		config:           listenerConfig,
		db:               db,
		configMgr:        configMgr,
		envMgr:           envMgr,
		sessionMgr:       sessionMgr,
		notifBuffer:      make(chan *Notification, tgNotifBufferSize),
		instructionQueue: make(chan *shared.AutomationInstruction, 100),
		errorChan:        make(chan *PlatformError, 50),
		pauseAck:         make(chan struct{}, 1),
		resumeReq:        make(chan struct{}),
		seenMsgs:         make(map[string]time.Time),
		shutdown:         make(chan struct{}),
	}

	if err := os.MkdirAll("./cache/sessions/telegram", 0750); err == nil {
		tc.seenMsgFile = filepath.Join("./cache/sessions/telegram", fmt.Sprintf("seen_%s.json", accountID))
		tc.loadSeenMessages()
	}

	return tc
}

func (tc *TelegramCollector) loadSeenMessages() {
	if tc.seenMsgFile == "" {
		return
	}
	data, err := os.ReadFile(tc.seenMsgFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[TG:SEEN:%s] failed to read dedupe file: %v", tc.accountID, err)
		}
		return
	}
	var m map[string]time.Time
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("[TG:SEEN:%s] failed to parse dedupe file: %v", tc.accountID, err)
		return
	}
	tc.seenMsgMu.Lock()
	defer tc.seenMsgMu.Unlock()
	tc.seenMsgs = m
	log.Printf("[TG:SEEN:%s] loaded %d dedupe entries from disk", tc.accountID, len(m))
}

func (tc *TelegramCollector) saveSeenMessages() {
	if tc.seenMsgFile == "" {
		return
	}
	tc.seenMsgMu.Lock()
	now := time.Now()
	for k, exp := range tc.seenMsgs {
		if now.After(exp) {
			delete(tc.seenMsgs, k)
		}
	}
	mCopy := make(map[string]time.Time, len(tc.seenMsgs))
	for k, v := range tc.seenMsgs {
		mCopy[k] = v
	}
	tc.seenMsgMu.Unlock()

	data, err := json.Marshal(mCopy)
	if err != nil {
		log.Printf("[TG:SEEN:%s] failed to marshal dedupe: %v", tc.accountID, err)
		return
	}
	if err := os.WriteFile(tc.seenMsgFile, data, 0600); err != nil {
		log.Printf("[TG:SEEN:%s] failed to save dedupe: %v", tc.accountID, err)
	} else {
		tgDbg(tc.accountID, "saved %d dedupe entries", len(mCopy))
	}
}

// getCursorSubtypeID returns the subtype_id used for the global_cursors row.
// Mirrors WhatsApp's getCursorSubtypeID().
func (tc *TelegramCollector) getCursorSubtypeID() string {
	return "global"
}

// getCursorType returns the cursor_type stored in global_cursors.
func (tc *TelegramCollector) getCursorType() string {
	return "timestamp"
}

// getLastCollectionTimestamp reads the last persisted cursor from global_cursors.
// Returns zero time if no row exists yet.
func (tc *TelegramCollector) getLastCollectionTimestamp() (time.Time, error) {
	var cursorValue string
	err := tc.db.QueryRow(`
		SELECT cursor_value FROM global_cursors
		WHERE platform = 'telegram' AND subtype = ? AND account_id = ? AND subtype_id = ? AND cursor_type = ?
	`, tc.subtype, tc.accountID, tc.getCursorSubtypeID(), tc.getCursorType()).Scan(&cursorValue)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, cursorValue)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

// updateLastCollectionTimestamp upserts the cursor into global_cursors.
func (tc *TelegramCollector) updateLastCollectionTimestamp(ts time.Time) error {
	_, err := tc.db.Exec(`
		INSERT INTO global_cursors (platform, subtype, account_id, subtype_id, cursor_type, cursor_value, updated_at)
		VALUES ('telegram', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(platform, subtype, account_id, subtype_id, cursor_type) DO UPDATE SET
			cursor_value = excluded.cursor_value,
			updated_at = CURRENT_TIMESTAMP
	`, tc.subtype, tc.accountID, tc.getCursorSubtypeID(), tc.getCursorType(), ts.UTC().Format(time.RFC3339Nano))
	return err
}

// stageCursorUpdate records ts as a candidate for the next cursor flush.
// It only advances the pending value, never moves it backward.
// Call this from processMessage; the cursor is not written to the DB until
// flushPendingCursor() is called at the end of a successful Collect drain,
// making advancement batch-safe.
func (tc *TelegramCollector) stageCursorUpdate(ts time.Time) {
	if ts.IsZero() {
		return
	}
	tc.pendingCursorMu.Lock()
	defer tc.pendingCursorMu.Unlock()
	if ts.After(tc.pendingCursorTime) {
		tc.pendingCursorTime = ts
	}
}

// flushPendingCursor writes the highest staged timestamp to global_cursors and
// resets the in-memory buffer. It is called once per Collect cycle, after the
// entire batch has been drained, so the cursor only advances when all messages
// in the batch are safely in the caller's hands.
func (tc *TelegramCollector) flushPendingCursor() {
	tc.pendingCursorMu.Lock()
	ts := tc.pendingCursorTime
	tc.pendingCursorTime = time.Time{}
	tc.pendingCursorMu.Unlock()

	if ts.IsZero() {
		return
	}
	if err := tc.updateLastCollectionTimestamp(ts); err != nil {
		log.Printf("[TG:CURSOR:%s] failed to flush cursor: %v", tc.accountID, err)
	} else {
		tgDbg(tc.accountID, "cursor flushed → %s", ts.UTC().Format(time.RFC3339Nano))
	}
}

func (tc *TelegramCollector) checkAndMarkSeen(key string, chatID int64, msgID int32, msgTimestamp int64) bool {
	tc.seenMsgMu.Lock()
	defer tc.seenMsgMu.Unlock()
	now := time.Now()
	for k, exp := range tc.seenMsgs {
		if now.After(exp) {
			delete(tc.seenMsgs, k)
		}
	}
	if _, seen := tc.seenMsgs[key]; seen {
		return false
	}
	tc.seenMsgs[key] = now.Add(tgMsgDedupeWindow)
	go tc.saveSeenMessages()
	return true
}

func (tc *TelegramCollector) pauseCollection() error {
	if tc.pauseCount.Add(1) > 1 {
		return nil
	}
	tc.drainPaused.Store(true)
	log.Printf("[TG:PAUSE:%s] pause requested (drainPaused=true)", tc.accountID)

	if !tc.collectRunning.Load() {
		log.Printf("[TG:PAUSE:%s] collect not running, skipping wait", tc.accountID)
		return nil
	}

	select {
	case <-tc.pauseAck:
		log.Printf("[TG:PAUSE:%s] ✓ pause ack received", tc.accountID)
		return nil
	case <-time.After(tgPauseAckTimeout):
		log.Printf("[TG:PAUSE:%s] pause ack timeout (drainPaused remains set)", tc.accountID)
		return nil
	}
}

func (tc *TelegramCollector) resumeCollection() {
	if tc.pauseCount.Add(-1) > 0 {
		return
	}
	newResume := make(chan struct{})
	tc.resumeMu.Lock()
	old := tc.resumeReq
	tc.resumeReq = newResume
	tc.resumeMu.Unlock()
	tc.drainPaused.Store(false)
	close(old)
	log.Printf("[TG:RESUME:%s] ✓ collection resumed", tc.accountID)
}

func (tc *TelegramCollector) checkPause(ctx context.Context) bool {
	if !tc.drainPaused.Load() {
		return true
	}
	log.Printf("[TG:PAUSE:%s] paused, sending ack and waiting for resume", tc.accountID)
	select {
	case tc.pauseAck <- struct{}{}:
	default:
	}
	tc.resumeMu.Lock()
	resumeCh := tc.resumeReq
	tc.resumeMu.Unlock()
	select {
	case <-resumeCh:
		log.Printf("[TG:PAUSE:%s] resumed from pause", tc.accountID)
		return true
	case <-ctx.Done():
		log.Printf("[TG:PAUSE:%s] context cancelled while paused", tc.accountID)
		return false
	}
}

func (tc *TelegramCollector) extractSender(msg *telegram.NewMessage) (displayName, username string, senderID int64) {
	if msg.Sender != nil {
		senderID = msg.Sender.ID
		username = msg.Sender.Username
		displayName = strings.TrimSpace(msg.Sender.FirstName + " " + msg.Sender.LastName)
		if displayName == "" {
			displayName = username
		}
	} else {
		senderID = 0
		username = fmt.Sprintf("user_%d", senderID)
		displayName = username
	}
	if username == "" {
		username = fmt.Sprintf("user_%d", senderID)
	}
	return
}

func (tc *TelegramCollector) resolveChatName(msg *telegram.NewMessage) string {
	if msg.Chat == nil {
		return ""
	}
	if msg.Chat.Title != "" {
		return msg.Chat.Title
	}
	name, _, _ := tc.extractSender(msg)
	return name
}

func (tc *TelegramCollector) extractReplyContext(msg *telegram.NewMessage) (replyTo *string, replyText string) {
	if msg.ReplyToMsgID() == 0 {
		return nil, ""
	}
	replyMsg, err := msg.GetReplyMessage()
	if err != nil || replyMsg == nil {
		return nil, ""
	}
	id := fmt.Sprintf("%d", replyMsg.ID)
	return &id, replyMsg.Text()
}

func (tc *TelegramCollector) extractAndDownloadMedia(
	ctx context.Context,
	msg *telegram.NewMessage,
) (remoteURLs, localPaths []string, attachments []MediaAttachment) {
	if msg.Media() == nil {
		return
	}
	mediaDir := tc.getMediaDir()
	if err := os.MkdirAll(mediaDir, 0755); err != nil {
		log.Printf("[TG:MEDIA:%s] mkdir %s failed: %v", tc.accountID, mediaDir, err)
		return
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("tg_%d_%d", msg.Chat.ID, msg.ID)))
	baseName := hex.EncodeToString(h[:8])
	mediaType := tc.detectMediaType(msg)
	ext := tc.extensionForType(mediaType)
	localPath := filepath.Join(mediaDir, baseName+ext)
	remoteURL := fmt.Sprintf("telegram://media/%d/%d", msg.Chat.ID, msg.ID)
	remoteURLs = append(remoteURLs, remoteURL)
	downloaded := ""
	if tc.imageRecognitionEnabled() {
		log.Printf("[TG:MEDIA:%s] downloading %s → %s (image recognition enabled)", tc.accountID, mediaType, localPath)
		opts := &telegram.DownloadOptions{FileName: localPath}
		d, err := msg.Download(opts)
		if err != nil {
			log.Printf("[TG:MEDIA:%s] download failed (type=%s): %v", tc.accountID, mediaType, err)
		} else {
			downloaded = d
			localPath = d
			log.Printf("[TG:MEDIA:%s] ✓ downloaded %s → %s", tc.accountID, mediaType, downloaded)
		}
	} else {
		log.Printf("[TG:MEDIA:%s] image recognition disabled, skipping download for %s", tc.accountID, mediaType)
	}
	if downloaded != "" {
		localPaths = append(localPaths, downloaded)
	}
	attachments = append(attachments, MediaAttachment{
		ID:        fmt.Sprintf("%d_%d", msg.Chat.ID, msg.ID),
		Type:      mediaType,
		URL:       remoteURL,
		Thumbnail: downloaded,
	})
	return
}

func (tc *TelegramCollector) imageRecognitionEnabled() bool {
	if tc.configMgr == nil {
		return false
	}
	cfg := tc.configMgr.GetConfig()
	if cfg == nil {
		return false
	}
	return cfg.ImageRecognition.Enabled
}

func (tc *TelegramCollector) detectMediaType(msg *telegram.NewMessage) string {
	switch msg.Media().(type) {
	case *telegram.MessageMediaPhoto:
		return "image"
	case *telegram.MessageMediaDocument:
		doc := msg.Media().(*telegram.MessageMediaDocument)
		if inner, ok := doc.Document.(*telegram.DocumentObj); ok {
			for _, attr := range inner.Attributes {
				switch attr.(type) {
				case *telegram.DocumentAttributeSticker:
					return "sticker"
				case *telegram.DocumentAttributeVideo:
					return "video"
				case *telegram.DocumentAttributeAudio:
					return "audio"
				case *telegram.DocumentAttributeAnimated:
					return "gif"
				}
			}
		}
		return "document"
	}
	return "document"
}

func (tc *TelegramCollector) extensionForType(mediaType string) string {
	switch mediaType {
	case "image":
		return ".jpg"
	case "video":
		return ".mp4"
	case "voice", "audio":
		return ".ogg"
	case "gif":
		return ".gif"
	case "sticker":
		return ".webp"
	default:
		return ".bin"
	}
}

func (tc *TelegramCollector) downloadRemoteImage(imageURL string) (string, error) {
	if imageURL == "" {
		return "", fmt.Errorf("empty URL")
	}
	dir := tc.getMediaDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(imageURL))
	fullPath := filepath.Join(dir, hex.EncodeToString(h[:])[:16]+".jpg")
	if info, err := os.Stat(fullPath); err == nil && info.Size() > 0 {
		tgDbg(tc.accountID, "image already cached: %s", fullPath)
		return fullPath, nil
	}
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if os.IsExist(err) {
		return fullPath, nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	log.Printf("[TG:FETCH:%s] downloading remote image: %s", tc.accountID, imageURL)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(imageURL)
	if err != nil {
		os.Remove(fullPath)
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Remove(fullPath)
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		os.Remove(fullPath)
		return "", err
	}
	log.Printf("[TG:FETCH:%s] ✓ image saved: %s", tc.accountID, fullPath)
	return fullPath, nil
}

func (tc *TelegramCollector) pushNotification(n *Notification) bool {
	select {
	case tc.notifBuffer <- n:
		tgDbg(tc.accountID, "notification pushed (buf=%d/%d)", len(tc.notifBuffer), tgNotifBufferSize)
		return true
	default:
		log.Printf("[TG:WARN:%s] notification buffer full (%d), message dropped", tc.accountID, tgNotifBufferSize)
		tc.reportError("BUFFER_FULL", "notification buffer full, message dropped", "warning")
		return false
	}
}

func (tc *TelegramCollector) reportError(code, msg, severity string) {
	log.Printf("[TG:ERROR:%s] code=%s msg=%q severity=%s", tc.accountID, code, msg, severity)
	select {
	case tc.errorChan <- &PlatformError{
		PlatformID: tc.platformID,
		SubtypeID:  tc.subtypeID,
		AccountID:  tc.accountID,
		ErrorCode:  code,
		ErrorMsg:   msg,
		Timestamp:  time.Now(),
		Severity:   severity,
	}:
	default:
		log.Printf("[TG:ERROR:%s] error chan full, dropped: code=%s msg=%s", tc.accountID, code, msg)
	}
}

func (tc *TelegramCollector) GetErrorChannel() <-chan *PlatformError {
	return tc.errorChan
}

// authStr extracts a string value from a PlatformSubtype.Auth map[string]interface{}.
func authStr(auth map[string]interface{}, key string) string {
	if auth == nil {
		return ""
	}
	v, ok := auth[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func (tc *TelegramCollector) getAPICredentials() (int32, string) {
	apiIDStr := os.Getenv("TELEGRAM_API_ID")
	apiHash := os.Getenv("TELEGRAM_API_HASH")
	if (apiIDStr == "" || apiHash == "") && tc.configMgr != nil {
		if cfg := tc.configMgr.GetConfig(); cfg != nil {
			if p, ok := cfg.Platforms[tc.platformID]; ok {
				// 1. Try TelegramConfig top-level fields (legacy, rarely set).
				if p.Telegram != nil {
					if p.Telegram.APIID != "" {
						apiIDStr = p.Telegram.APIID
					}
					if p.Telegram.APIHash != "" {
						apiHash = p.Telegram.APIHash
					}
				}
				// 2. Scan PlatformConfig.Subtypes for api_id / api_hash in Auth map.
				//    Prefer exact subtypeID match (ignoring enabled); fall back to
				//    first enabled subtype. This is the normal path when credentials
				//    live in subtypes[].auth.
				if apiIDStr == "" || apiHash == "" {
					var fallbackIdx = -1
					for i := range p.Subtypes {
						sub := &p.Subtypes[i]
						if tc.subtypeID != "" && sub.ID == tc.subtypeID {
							apiIDStr = authStr(sub.Auth, "api_id")
							apiHash = authStr(sub.Auth, "api_hash")
							log.Printf("[TG:CREDS:%s] api_id/api_hash from subtype %s (exact match)", tc.accountID, sub.ID)
							break
						}
						if fallbackIdx == -1 && sub.Enabled {
							fallbackIdx = i
						}
					}
					if (apiIDStr == "" || apiHash == "") && fallbackIdx >= 0 {
						sub := &p.Subtypes[fallbackIdx]
						apiIDStr = authStr(sub.Auth, "api_id")
						apiHash = authStr(sub.Auth, "api_hash")
						log.Printf("[TG:CREDS:%s] api_id/api_hash from first enabled subtype %s", tc.accountID, sub.ID)
					}
				}
			}
		}
	}
	if apiIDStr == "" || apiHash == "" {
		log.Printf("[TG:CREDS:%s] ERROR: TELEGRAM_API_ID or TELEGRAM_API_HASH not set", tc.accountID)
		tc.reportError("MISSING_API_CREDENTIALS", "TELEGRAM_API_ID / TELEGRAM_API_HASH not set", "critical")
		return 0, ""
	}
	id, err := strconv.ParseInt(strings.TrimSpace(apiIDStr), 10, 32)
	if err != nil {
		log.Printf("[TG:CREDS:%s] ERROR: TELEGRAM_API_ID is not a valid integer: %q", tc.accountID, apiIDStr)
		tc.reportError("INVALID_API_ID", "TELEGRAM_API_ID is not a valid integer", "critical")
		return 0, ""
	}
	return int32(id), strings.TrimSpace(apiHash)
}

func (tc *TelegramCollector) getBotToken() string {
	if tc.configMgr != nil {
		if cfg := tc.configMgr.GetConfig(); cfg != nil {
			if p, ok := cfg.Platforms[tc.platformID]; ok && p.Telegram != nil && p.Telegram.Bot != nil {
				return p.Telegram.Bot.BotToken
			}
		}
	}
	return os.Getenv("TELEGRAM_BOT_TOKEN")
}

func (tc *TelegramCollector) getPhoneNumber() string {
	if tc.configMgr != nil {
		if cfg := tc.configMgr.GetConfig(); cfg != nil {
			if p, ok := cfg.Platforms[tc.platformID]; ok {
				// Legacy: TelegramConfig.Account.PhoneNumber.
				if p.Telegram != nil && p.Telegram.Account != nil && p.Telegram.Account.PhoneNumber != "" {
					return p.Telegram.Account.PhoneNumber
				}
				// Modern: subtypes[].auth.phone_number.
				var fallbackPhone string
				for i := range p.Subtypes {
					sub := &p.Subtypes[i]
					if tc.subtypeID != "" && sub.ID == tc.subtypeID {
						if ph := authStr(sub.Auth, "phone_number"); ph != "" {
							return ph
						}
					}
					if fallbackPhone == "" && sub.Enabled {
						if ph := authStr(sub.Auth, "phone_number"); ph != "" {
							fallbackPhone = ph
						}
					}
				}
				if fallbackPhone != "" {
					return fallbackPhone
				}
			}
		}
	}
	return os.Getenv("TELEGRAM_PHONE")
}

func (tc *TelegramCollector) getSessionPath() string {
	if tc.sessionMgr != nil {
		store := tc.sessionMgr.GetStorage()
		if store != nil {
			dir := filepath.Join(store.GetSessionsDir(), "telegram")
			return filepath.Join(dir, fmt.Sprintf("tg_%s_%s.session", tc.subtypeID, tc.accountID))
		}
	}
	return filepath.Join("./cache/sessions/telegram", fmt.Sprintf("tg_%s_%s.session", tc.subtypeID, tc.accountID))
}

func (tc *TelegramCollector) getMediaDir() string {
	if tc.configMgr != nil {
		if cfg := tc.configMgr.GetConfig(); cfg != nil && cfg.Paths.PostImages != "" {
			return cfg.Paths.PostImages
		}
	}
	return tgMediaDirDefault
}

func (tc *TelegramCollector) safeClient() *telegram.Client {
	tc.clientMu.Lock()
	defer tc.clientMu.Unlock()
	return tc.client
}

func (tc *TelegramCollector) generateUserID(raw string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(raw))))
	return "tg_" + hex.EncodeToString(h[:8])
}

func (tc *TelegramCollector) extractMsgID(opts map[string]interface{}, key string) int {
	v, ok := opts[key]
	if !ok {
		return 0
	}
	switch id := v.(type) {
	case int:
		return id
	case int32:
		return int(id)
	case int64:
		return int(id)
	case float64:
		return int(id)
	case string:
		n, _ := strconv.Atoi(id)
		return n
	}
	return 0
}

var tgProductPatterns = []*regexp.Regexp{
	regexp.MustCompile(`SKU\s*=\s*["']([^"']+)["']`),
	regexp.MustCompile(`SKU\s*=\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)product\s*id\s*:\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)sku\s*:\s*([^\s<>"']+)`),
}

func (tc *TelegramCollector) extractProductID(text string) string {
	for _, p := range tgProductPatterns {
		if m := p.FindStringSubmatch(text); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

func (tc *TelegramCollector) getProductData(productID string) (map[string]interface{}, error) {
	if tc.db == nil || productID == "" {
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
		LIMIT 1
	`
	const fuzzyQ = `
		SELECT id, sku, name, description, category, subcategory, tags,
		       price, price_per_pack, quantity_per_pack, currency,
		       stock, reserved_stock, low_stock_threshold,
		       image_url, thumbnail_url, weight_kg, dimensions,
		       is_active, is_featured, metadata, created_at, updated_at
		FROM products
		WHERE name LIKE ? AND is_active = 1
		LIMIT 1
	`
	var (
		id, sku, name, desc, cat, subcat, tags sql.NullString
		price, pricePerPack, weightKg          sql.NullFloat64
		qtyPerPack                             sql.NullInt64
		currency                               sql.NullString
		stock, reservedStock, lowStockThr      sql.NullInt64
		imageURL, thumbURL                     sql.NullString
		dimensions                             sql.NullString
		isActive, isFeatured                   sql.NullBool
		metadata                               sql.NullString
		createdAt, updatedAt                   sql.NullTime
	)
	scanInto := func(row *sql.Row) error {
		return row.Scan(
			&id, &sku, &name, &desc, &cat, &subcat, &tags,
			&price, &pricePerPack, &qtyPerPack, &currency,
			&stock, &reservedStock, &lowStockThr,
			&imageURL, &thumbURL, &weightKg, &dimensions,
			&isActive, &isFeatured, &metadata, &createdAt, &updatedAt,
		)
	}

	// Exact match on sku first: cheap, indexed lookup, no false positives.
	err := scanInto(tc.db.QueryRow(exactQ, productID))

	// Fall back to a fuzzy name match only when nothing exact was found and the
	// identifier is specific enough (>=4 chars) to keep the table scan rare and
	// avoid matching unrelated products on short/common substrings.
	if err == sql.ErrNoRows && len(productID) >= 4 {
		err = scanInto(tc.db.QueryRow(fuzzyQ, "%"+productID+"%"))
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	unmarshal := func(s sql.NullString) interface{} {
		if !s.Valid || s.String == "" {
			return nil
		}
		var v interface{}
		_ = json.Unmarshal([]byte(s.String), &v)
		return v
	}
	return map[string]interface{}{
		"id":                  id.String,
		"sku":                 sku.String,
		"name":                name.String,
		"description":         desc.String,
		"category":            cat.String,
		"subcategory":         subcat.String,
		"tags":                unmarshal(tags),
		"price":               price.Float64,
		"price_per_pack":      pricePerPack.Float64,
		"quantity_per_pack":   qtyPerPack.Int64,
		"currency":            currency.String,
		"stock":               stock.Int64,
		"reserved_stock":      reservedStock.Int64,
		"low_stock_threshold": lowStockThr.Int64,
		"image_url":           imageURL.String,
		"thumbnail_url":       thumbURL.String,
		"weight_kg":           weightKg.Float64,
		"dimensions":          dimensions.String,
		"is_active":           isActive.Bool,
		"is_featured":         isFeatured.Bool,
		"metadata":            unmarshal(metadata),
		"created_at":          createdAt.Time,
		"updated_at":          updatedAt.Time,
	}, nil
}

func (tc *TelegramCollector) getUserInfoForNotification(
	ctx context.Context,
	platformUserID string,
) (userData map[string]interface{}, recentMessages []string, isNew bool, err error) {
	if tc.db == nil {
		return nil, nil, true, nil
	}
	const q = `
		SELECT id, display_name, is_blocked, last_intent
		  FROM platform_users
		 WHERE platform = ? AND platform_user_id = ?
	`
	var id, displayName, lastIntent sql.NullString
	var blocked sql.NullBool
	err = tc.db.QueryRowContext(ctx, q, "telegram", platformUserID).Scan(
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
		recentMessages, _ = tc.getRecentMessages(ctx, id.String, 3)
	}
	return userData, recentMessages, false, nil
}

func (tc *TelegramCollector) getRecentMessages(ctx context.Context, userID string, limit int) ([]string, error) {
	const q = `
		SELECT message_text FROM (
			SELECT message_text, received_at
			FROM messages
			WHERE user_id = ? AND direction = 'incoming'
			ORDER BY received_at DESC
			LIMIT ?
		) sub
		ORDER BY received_at ASC
	`
	rows, err := tc.db.QueryContext(ctx, q, userID, limit)
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

func (tc *TelegramCollector) fetchURL(url string) ([]byte, error) {
	log.Printf("[TG:FETCH:%s] GET %s", tc.accountID, url)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	log.Printf("[TG:FETCH:%s] ✓ fetched %d bytes from %s", tc.accountID, len(data), url)
	return data, nil
}

func (tc *TelegramCollector) ensureClient(ctx context.Context) error {
	tc.clientMu.Lock()
	defer tc.clientMu.Unlock()

	if tc.client != nil && tc.connected.Load() {
		return nil
	}
	log.Printf("[TG:CONNECT:%s] initialising gogram client (subtype=%s)", tc.accountID, tc.subtype)

	apiID, apiHash := tc.getAPICredentials()
	if apiID == 0 {
		return fmt.Errorf("missing or invalid TELEGRAM_API_ID")
	}
	sessionPath := tc.getSessionPath()
	if err := os.MkdirAll(filepath.Dir(sessionPath), 0750); err != nil {
		return fmt.Errorf("session dir: %w", err)
	}

	// If the session file is missing or empty, trigger authentication via the
	// session manager (mirrors how WhatsApp calls sessionMgr.StartWhatsApp).
	// Without this, authenticateTelegram in session_manager.go is never reached.
	sessionData, err := os.ReadFile(sessionPath)
	if err != nil || len(sessionData) == 0 {
		if tc.sessionMgr == nil {
			return fmt.Errorf("no session file for %s and no session manager available", tc.accountID)
		}
		log.Printf("[TG:CONNECT:%s] no session file – triggering authenticateTelegram via session manager", tc.accountID)
		// Release the lock while waiting for interactive auth (may take minutes).
		tc.clientMu.Unlock()
		_, authErr := tc.sessionMgr.GetSession(ctx, sessionmgr.SessionRequest{
			PlatformID: tc.platformID,
			Subtype:    tc.subtypeID,
			AccountID:  tc.accountID,
		})
		tc.clientMu.Lock()
		if authErr != nil {
			return fmt.Errorf("telegram authentication failed: %w", authErr)
		}
		// Re-read the session file written by authenticateTelegram.
		sessionData, err = os.ReadFile(sessionPath)
		if err != nil || len(sessionData) == 0 {
			return fmt.Errorf("session file still missing after authentication for %s", tc.accountID)
		}
	}
	sessionString := strings.TrimSpace(string(sessionData))

	cfg := telegram.ClientConfig{
		AppID:         apiID,
		AppHash:       apiHash,
		StringSession: sessionString,
		MemorySession: true,
	}
	client, err := telegram.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("create gogram client: %w", err)
	}

	connectCtx, connectCancel := context.WithTimeout(ctx, tgConnectTimeout)
	defer connectCancel()
	connectDone := make(chan error, 1)
	go func() { _, connErr := client.Conn(); connectDone <- connErr }()

	select {
	case connectErr := <-connectDone:
		if connectErr != nil {
			return fmt.Errorf("MTProto connect: %w", connectErr)
		}
	case <-connectCtx.Done():
		return fmt.Errorf("MTProto connect timed out after %v", tgConnectTimeout)
	}

	if exported := client.ExportSession(); exported != "" && exported != sessionString {
		_ = os.WriteFile(sessionPath, []byte(exported), 0600)
	}
	tc.client = client
	tc.connected.Store(true)
	time.Sleep(500 * time.Millisecond)
	tc.registerHandlers()
	go tc.fetchMissedMessages()
	log.Printf("[TG:CONNECT:%s] ✓ gogram client ready", tc.accountID)
	return nil
}

func (tc *TelegramCollector) registerHandlers() {
	log.Printf("[TG:HANDLERS:%s] registering gogram event handlers", tc.accountID)

	tc.client.On(telegram.OnMessage, func(msg *telegram.NewMessage) error {
		if msg.Message != nil && msg.Message.Out {
			return nil
		}

		if msg.Sender != nil && msg.Sender.Bot {
			tgDbg(tc.accountID, "skipping message from bot (sender=%d)", msg.Sender.ID)
			return nil
		}

		notif, err := tc.processMessage(context.Background(), msg)
		if err != nil {
			return nil
		}
		if notif != nil && tc.pushNotification(notif) {
			tc.stageCursorUpdate(notif.Timestamp)
		}
		return nil
	})

	tc.client.On(telegram.OnEdit, func(msg *telegram.NewMessage) error {
		return nil
	})

	tc.client.On(telegram.OnAlbum, func(album *telegram.Album) error {
		if len(album.Messages) == 0 {
			return nil
		}
		first := album.Messages[0]
		if first.Message != nil && first.Message.Out {
			return nil
		}
		if first.Sender != nil && first.Sender.Bot {
			tgDbg(tc.accountID, "skipping album from bot (sender=%d)", first.Sender.ID)
			return nil
		}
		notif, err := tc.processAlbum(context.Background(), *album)
		if err != nil {
			return nil
		}
		if notif != nil && tc.pushNotification(notif) {
			tc.stageCursorUpdate(notif.Timestamp)
		}
		return nil
	})

	tc.wg.Add(1)
	go func() {
		defer tc.wg.Done()
		defer tc.connected.Store(false)
		tc.client.Idle()
	}()
	go func() {
		<-tc.shutdown
		tc.disconnectClient()
	}()
}

func (tc *TelegramCollector) disconnectClient() {
	tc.clientMu.Lock()
	defer tc.clientMu.Unlock()
	if tc.client != nil {
		tc.client.Disconnect()
		tc.client = nil
	}
	tc.connected.Store(false)
}

func (tc *TelegramCollector) processMessage(ctx context.Context, msg *telegram.NewMessage) (*Notification, error) {
	if tc.config != nil && !tc.config.ListenMessages {
		return nil, nil
	}
	if msg.ChatType() == "channel" {
		tgDbg(tc.accountID, "channel message ignored (chat=%d)", msg.ChatID())
		return nil, nil
	}
	chatID := msg.Chat.ID
	msgID := msg.ID
	msgTimestamp := int64(msg.Date())
	dedupeKey := fmt.Sprintf("%d:%d", chatID, msgID)

	if !tc.checkAndMarkSeen(dedupeKey, chatID, msgID, msgTimestamp) {
		tgDbg(tc.accountID, "duplicate message key=%s, skipping", dedupeKey)
		return nil, nil
	}

	text := msg.Text()
	senderName, username, senderID := tc.extractSender(msg)
	chatName := tc.resolveChatName(msg)
	isGroup := chatID < 0
	if isGroup && tc.config != nil && !tc.config.ListenGroupMessages {
		tgDbg(tc.accountID, "ListenGroupMessages disabled, dropping group message")
		return nil, nil
	}
	platform := "telegram"
	if isGroup {
		platform = "telegram_group"
	}
	log.Printf("[TG:PROCESS:%s] msg id=%d chat=%d(%s) sender=%s group=%v text_len=%d timestamp=%d",
		tc.accountID, msgID, chatID, chatName, username, isGroup, len(text), msgTimestamp)

	remoteURLs, localPaths, attachments := tc.extractAndDownloadMedia(ctx, msg)
	replyTo, replyText := tc.extractReplyContext(msg)
	productID := tc.extractProductID(text)
	var productData map[string]interface{}
	if productID != "" {
		log.Printf("[TG:PROCESS:%s] product ID detected: %s", tc.accountID, productID)
		if data, err := tc.getProductData(productID); err == nil {
			productData = data
		} else {
			log.Printf("[TG:PROCESS:%s] product lookup %q failed: %v", tc.accountID, productID, err)
		}
	}
	platformUserID := fmt.Sprintf("%d", senderID)
	ud, recent, isNew, err := tc.getUserInfoForNotification(ctx, platformUserID)
	if err != nil {
		log.Printf("[TG:USER:%s] user lookup error for %s: %v – dropping message for safety",
			tc.accountID, platformUserID, err)
		return nil, nil
	}
	isNewUser := isNew
	userData := ud
	recentMsgs := recent
	if !isNewUser && userData != nil {
		if blocked, ok := userData["is_blocked"].(bool); ok && blocked {
			log.Printf("[TG:BLOCKED:%s] user %s is blocked, dropping message", tc.accountID, platformUserID)
			return nil, nil
		}
	}
	raw := map[string]interface{}{
		"chat_id":           chatID,
		"chat_name":         chatName,
		"sender_id":         senderID,
		"message_id":        msgID,
		"is_group":          isGroup,
		"is_forwarded":      msg.Message != nil && msg.Message.FwdFrom != nil,
		"image_urls":        remoteURLs,
		"downloaded_images": localPaths,
		"platform":          platform,
		"subtype":           tc.subtype,
		"collected_at":      time.Now().Format(time.RFC3339),
	}
	if productData != nil {
		raw["product_id"] = productID
		raw["product_data"] = productData
	}
	if isNewUser {
		raw["is_new_user"] = true
		raw["user_data"] = nil
	} else {
		raw["user_data"] = userData
		if len(recentMsgs) > 0 {
			raw["recent_messages"] = recentMsgs
		}
	}
	notif := &Notification{
		ID:         fmt.Sprintf("tg_msg_%s_%d_%d", tc.accountID, msgID, time.Now().UnixNano()),
		PlatformID: tc.platformID,
		SubtypeID:  tc.subtypeID,
		AccountID:  tc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  time.Unix(msgTimestamp, 0),
		Message: &MessageData{
			ConversationID:   fmt.Sprintf("%d", chatID),
			ConversationName: chatName,
			IsGroup:          isGroup,
			MessageID:        fmt.Sprintf("%d", msgID),
			Sender: UserInfo{
				UserID:      tc.generateUserID(fmt.Sprintf("%d", senderID)),
				Username:    username,
				DisplayName: senderName,
			},
			Text:           text,
			Timestamp:      time.Unix(msgTimestamp, 0),
			IsRead:         false,
			IsForwarded:    msg.Message != nil && msg.Message.FwdFrom != nil,
			DeliveryStatus: "delivered",
			ReplyTo:        replyTo,
			ReplyText:      replyText,
			MediaAttached:  attachments,
		},
		RawData:     raw,
		CollectedAt: time.Now(),
	}
	return notif, nil
}

func (tc *TelegramCollector) processAlbum(ctx context.Context, album telegram.Album) (*Notification, error) {
	if len(album.Messages) == 0 {
		return nil, nil
	}
	first := album.Messages[0]
	if tc.config != nil && !tc.config.ListenMessages {
		return nil, nil
	}
	if first.ChatType() == "channel" {
		return nil, nil
	}
	chatID := first.Chat.ID
	firstMsgID := first.ID
	firstTimestamp := int64(first.Date())
	dedupeKey := fmt.Sprintf("album_%d_%d", chatID, firstMsgID)

	if !tc.checkAndMarkSeen(dedupeKey, chatID, firstMsgID, firstTimestamp) {
		return nil, nil
	}
	log.Printf("[TG:ALBUM:%s] processing album first_id=%d size=%d timestamp=%d", tc.accountID, firstMsgID, len(album.Messages), firstTimestamp)

	var remoteURLs, localPaths []string
	var attachments []MediaAttachment
	for _, msg := range album.Messages {
		r, l, a := tc.extractAndDownloadMedia(ctx, msg)
		remoteURLs = append(remoteURLs, r...)
		localPaths = append(localPaths, l...)
		attachments = append(attachments, a...)
	}
	caption := first.Text()
	senderName, username, senderID := tc.extractSender(first)
	chatName := tc.resolveChatName(first)
	isGroup := chatID < 0
	if isGroup && tc.config != nil && !tc.config.ListenGroupMessages {
		return nil, nil
	}
	platformUserID := fmt.Sprintf("%d", senderID)
	ud, recent, isNew, err := tc.getUserInfoForNotification(ctx, platformUserID)
	if err != nil {
		log.Printf("[TG:USER:%s] user lookup error for album sender %s: %v – dropping",
			tc.accountID, platformUserID, err)
		return nil, nil
	}
	isNewUser := isNew
	userData := ud
	recentMsgs := recent
	if !isNewUser && userData != nil {
		if blocked, _ := userData["is_blocked"].(bool); blocked {
			return nil, nil
		}
	}
	raw := map[string]interface{}{
		"album":             true,
		"album_size":        len(album.Messages),
		"image_urls":        remoteURLs,
		"downloaded_images": localPaths,
		"platform":          "telegram",
		"subtype":           tc.subtype,
		"collected_at":      time.Now().Format(time.RFC3339),
	}
	if isNewUser {
		raw["is_new_user"] = true
		raw["user_data"] = nil
	} else {
		raw["user_data"] = userData
		if len(recentMsgs) > 0 {
			raw["recent_messages"] = recentMsgs
		}
	}
	return &Notification{
		ID:         fmt.Sprintf("tg_album_%s_%d_%d", tc.accountID, firstMsgID, time.Now().UnixNano()),
		PlatformID: tc.platformID,
		SubtypeID:  tc.subtypeID,
		AccountID:  tc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  time.Unix(firstTimestamp, 0),
		Message: &MessageData{
			ConversationID:   fmt.Sprintf("%d", chatID),
			ConversationName: chatName,
			IsGroup:          isGroup,
			MessageID:        fmt.Sprintf("%d", firstMsgID),
			Sender: UserInfo{
				UserID:      tc.generateUserID(fmt.Sprintf("%d", senderID)),
				Username:    username,
				DisplayName: senderName,
			},
			Text:           caption,
			Timestamp:      time.Unix(firstTimestamp, 0),
			IsRead:         false,
			DeliveryStatus: "delivered",
			MediaAttached:  attachments,
		},
		RawData:     raw,
		CollectedAt: time.Now(),
	}, nil
}

func (tc *TelegramCollector) extractChatIDFromNotification(n *Notification) int64 {
	if n.RawData != nil {
		if raw, ok := n.RawData["chat_id"]; ok {
			switch v := raw.(type) {
			case int64:
				return v
			case float64:
				return int64(v)
			case int:
				return int64(v)
			case string:
				if id, err := strconv.ParseInt(v, 10, 64); err == nil {
					return id
				}
			}
		}
	}
	if n.Message != nil && n.Message.ConversationID != "" {
		if id, err := strconv.ParseInt(n.Message.ConversationID, 10, 64); err == nil {
			return id
		}
	}
	return 0
}

func (tc *TelegramCollector) extractMsgIDFromNotification(n *Notification) int {
	if n.Message == nil || n.Message.MessageID == "" {
		return 0
	}
	if id, err := strconv.Atoi(n.Message.MessageID); err == nil {
		return id
	}
	return 0
}

func (tc *TelegramCollector) Collect(ctx context.Context, _ []*CookieData) ([]*Notification, error) {
	log.Printf("[TG:COLLECT:%s] starting collection (subtype=%s)", tc.accountID, tc.subtype)

	if !tc.collectRunning.CompareAndSwap(false, true) {
		log.Printf("[TG:COLLECT:%s] collect already in progress, skipping", tc.accountID)
		return nil, fmt.Errorf("collect already in progress for %s", tc.accountID)
	}
	defer tc.collectRunning.Store(false)

	if tc.config != nil && !tc.config.ListenMessages {
		log.Printf("[TG:COLLECT:%s] ListenMessages disabled, returning empty", tc.accountID)
		return []*Notification{}, nil
	}

	if err := tc.ensureClient(ctx); err != nil {
		tc.reportError("CLIENT_ERROR", err.Error(), "error")
		return nil, fmt.Errorf("gogram client setup: %w", err)
	}

	if !tc.connected.Load() {
		log.Printf("[TG:COLLECT:%s] not connected, skipping collect", tc.accountID)
		return []*Notification{}, nil
	}

	if !tc.checkPause(ctx) {
		return nil, ctx.Err()
	}

	log.Printf("[TG:COLLECT:%s] processing pending instructions before drain", tc.accountID)
	tc.ProcessPendingInstructions()

	var notifications []*Notification
	for len(notifications) < tgMaxBatchSize {
		if !tc.checkPause(ctx) {
			break
		}
		select {
		case n := <-tc.notifBuffer:
			notifications = append(notifications, n)
		case <-ctx.Done():
			goto done
		default:
			select {
			case n := <-tc.notifBuffer:
				notifications = append(notifications, n)
			case <-time.After(tgCollectDrainWindow):
				goto done
			case <-ctx.Done():
				goto done
			}
		}
	}

done:
	// Commit the batch-safe cursor now that every notification staged during
	// this drain is sitting in `notifications`, about to be handed back to the
	// caller. This was previously missing — stageCursorUpdate() populated
	// pendingCursorTime but nothing ever flushed it, so the cursor never
	// advanced past zero.
	tc.flushPendingCursor()
	tc.ProcessPendingInstructions()
	log.Printf("[TG:COLLECT:%s] ✓ returning %d notifications", tc.accountID, len(notifications))
	return notifications, nil
}

func (tc *TelegramCollector) fetchMissedMessages() {
	client := tc.safeClient()
	if client == nil {
		return
	}
	log.Printf("[TG:HISTORY:%s] fetching missed messages from dialogs...", tc.accountID)

	// Use the global cursor as a hard cutoff so we never re-process messages
	// that were already committed in a previous Collect cycle.
	lastTimestamp, err := tc.getLastCollectionTimestamp()
	if err != nil {
		log.Printf("[TG:HISTORY:%s] could not read cursor, will rely on dedupe only: %v", tc.accountID, err)
	}
	if !lastTimestamp.IsZero() {
		log.Printf("[TG:HISTORY:%s] cursor at %s – skipping older messages", tc.accountID, lastTimestamp.UTC().Format(time.RFC3339))
	}

	dialogs, err := client.GetDialogs(&telegram.DialogOptions{Limit: 50})
	if err != nil {
		log.Printf("[TG:HISTORY:%s] GetDialogs failed: %v", tc.accountID, err)
		return
	}

	total := 0
	const historyLimit = 100

	for _, d := range dialogs {
		if d.TopMessage <= 0 || d.IsChannel() {
			continue
		}
		peerID := d.GetID()

		msgs, err := client.GetHistory(peerID, &telegram.HistoryOption{Limit: historyLimit})
		if err != nil {
			log.Printf("[TG:HISTORY:%s] GetHistory peer=%d failed: %v", tc.accountID, peerID, err)
			continue
		}

		for i := range msgs {
			m := &msgs[i]
			if m.Message == nil || m.Message.Out {
				continue
			}
			// Hard timestamp filter: skip anything at or before the last
			// committed cursor so we don't re-deliver already-processed messages.
			if !lastTimestamp.IsZero() {
				msgTs := time.Unix(int64(m.Date()), 0)
				if !msgTs.After(lastTimestamp) {
					tgDbg(tc.accountID, "history: skipping msg %d (ts %s ≤ cursor %s)",
						m.ID, msgTs.UTC().Format(time.RFC3339), lastTimestamp.UTC().Format(time.RFC3339))
					continue
				}
			}
			notif, err := tc.processMessage(context.Background(), m)
			if err != nil {
				log.Printf("[TG:HISTORY:%s] processMessage error: %v", tc.accountID, err)
				continue
			}
			if notif != nil && tc.pushNotification(notif) {
				tc.stageCursorUpdate(notif.Timestamp)
				total++
			}
		}
	}

	log.Printf("[TG:HISTORY:%s] ✓ fetched %d missed message(s) from %d dialog(s)",
		tc.accountID, total, len(dialogs))

	tc.clientMu.Lock()
	if tc.client != nil {
		sessionPath := tc.getSessionPath()
		if exported := tc.client.ExportSession(); exported != "" {
			_ = os.WriteFile(sessionPath, []byte(exported), 0600)
		}
	}
	tc.clientMu.Unlock()
}

func (tc *TelegramCollector) ReceiveInstructions(inst *shared.AutomationInstruction) error {
	if inst.Platform != "telegram" && inst.Platform != tc.platformID {
		return fmt.Errorf("wrong platform: %s (expected telegram or %s)", inst.Platform, tc.platformID)
	}
	if inst.TicketID == "" {
		return fmt.Errorf("empty ticket ID")
	}
	if inst.CreatedAt.IsZero() {
		inst.CreatedAt = time.Now()
	}
	select {
	case tc.instructionQueue <- inst:
		log.Printf("[TG:INSTR:%s] queued ticket=%s action=%s steps=%d",
			tc.accountID, inst.TicketID, inst.Action, len(inst.Steps))
		return nil
	case <-time.After(2 * time.Second):
		tc.reportError("QUEUE_FULL", "instruction queue full", "warning")
		return fmt.Errorf("instruction queue full for %s", tc.accountID)
	}
}

func (tc *TelegramCollector) ProcessPendingInstructions() {
	tc.executionMu.Lock()
	defer tc.executionMu.Unlock()
	count := 0
	for {
		select {
		case inst := <-tc.instructionQueue:
			count++
			log.Printf("[TG:INSTR:%s] executing ticket=%s action=%s", tc.accountID, inst.TicketID, inst.Action)
			if err := tc.executeInstruction(inst); err != nil {
				log.Printf("[TG:INSTR:%s] ticket=%s failed: %v", tc.accountID, inst.TicketID, err)
			}
		default:
			if count > 0 {
				log.Printf("[TG:INSTR:%s] processed %d instructions", tc.accountID, count)
			}
			return
		}
	}
}

func (tc *TelegramCollector) executeInstruction(inst *shared.AutomationInstruction) error {
	start := time.Now()
	log.Printf("[TG:EXEC:%s] start ticket=%s action=%s steps=%d",
		tc.accountID, inst.TicketID, inst.Action, len(inst.Steps))

	if err := tc.pauseCollection(); err != nil {
		log.Printf("[TG:EXEC:%s] pause before instruction (proceeding anyway): %v", tc.accountID, err)
	}
	defer tc.resumeCollection()

	timeout := inst.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var lastErr error
	for i, step := range inst.Steps {
		if ctx.Err() != nil {
			lastErr = fmt.Errorf("instruction timed out before step %d/%d: %w", i+1, len(inst.Steps), ctx.Err())
			break
		}
		log.Printf("[TG:EXEC:%s] step %d/%d type=%s", tc.accountID, i+1, len(inst.Steps), step.Type)

		maxAttempts := step.RetryCount
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		var stepErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			stepErr = tc.executeStep(ctx, step)
			if stepErr == nil {
				log.Printf("[TG:EXEC:%s] step %d/%d type=%s ✓", tc.accountID, i+1, len(inst.Steps), step.Type)
				break
			}
			log.Printf("[TG:EXEC:%s] step %d/%d type=%s attempt %d/%d error: %v",
				tc.accountID, i+1, len(inst.Steps), step.Type, attempt, maxAttempts, stepErr)
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if stepErr != nil {
			lastErr = stepErr
			if len(inst.FallbackSteps) > 0 {
				log.Printf("[TG:EXEC:%s] step %d/%d failed after %d attempt(s), running fallback steps",
					tc.accountID, i+1, len(inst.Steps), maxAttempts)
				lastErr = tc.runFallbackSteps(ctx, inst.FallbackSteps)
			}
			break
		}

		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	log.Printf("[TG:EXEC:%s] ✓ ticket=%s done in %v (err=%v)",
		tc.accountID, inst.TicketID, time.Since(start), lastErr)
	return lastErr
}

// runFallbackSteps runs an instruction's FallbackSteps after its main steps
// failed; its result replaces the main loop's error.
func (tc *TelegramCollector) runFallbackSteps(ctx context.Context, steps []shared.InstructionStep) error {
	var lastErr error
	for i, step := range steps {
		if err := tc.executeStep(ctx, step); err != nil {
			log.Printf("[TG:EXEC:%s] fallback step %d/%d error: %v", tc.accountID, i+1, len(steps), err)
			lastErr = err
		}
		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	return lastErr
}

func (tc *TelegramCollector) executeStep(ctx context.Context, step shared.InstructionStep) error {
	if step.DelayBefore > 0 {
		time.Sleep(time.Duration(step.DelayBefore) * time.Millisecond)
	}
	switch step.Type {
	case shared.StepTypeSendMessage:
		return tc.stepSendMessage(ctx, step)
	case shared.StepTypeReply:
		return tc.stepReply(ctx, step)
	case shared.StepTypeReact, shared.StepTypeLike:
		return tc.stepReact(ctx, step)
	case shared.StepTypeUpload:
		return tc.stepUpload(ctx, step)
	case shared.StepTypeDownload:
		return tc.stepDownload(ctx, step)
	case shared.StepTypeBlock:
		return tc.stepBlock(ctx, step, true)
	case shared.StepTypeUnfollow:
		return tc.stepBlock(ctx, step, false)
	case shared.StepTypeFollow:
		return tc.stepJoinChannel(ctx, step)
	case shared.StepTypeSearch:
		return tc.stepSearch(ctx, step)
	case shared.StepTypeSave:
		return tc.stepPinMessage(ctx, step)
	case shared.StepTypeShare:
		return tc.stepForward(ctx, step)
	case shared.StepTypeWait:
		return tc.stepWait(step)
	case shared.StepTypeLog:
		log.Printf("[TG:LOG:%s] %s", tc.accountID, step.Value)
		return nil
	case shared.StepTypeRateLimitCheck:
		log.Printf("[TG:RATELIMIT:%s] rate-limit check: OK (MTProto manages limits internally)", tc.accountID)
		return nil
	case shared.StepTypeAPICall:
		return tc.stepAPICall(ctx, step)
	case shared.StepTypeDBUpdate, shared.StepTypeDBRecord, shared.StepTypeAIGenerate:
		log.Printf("[TG:SKIP:%s] step=%s is orchestrator-side, skipping in collector", tc.accountID, step.Type)
		return nil
	case shared.StepTypeNavigate, shared.StepTypeClick, shared.StepTypeType,
		shared.StepTypeScroll, shared.StepTypeJavaScript, shared.StepTypePress:
		log.Printf("[TG:REJECT:%s] step=%s is browser-only, skipping", tc.accountID, step.Type)
		return nil
	default:
		return fmt.Errorf("unknown step type: %s", step.Type)
	}
}

func (tc *TelegramCollector) stepSendMessage(ctx context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepSendMessage: no message text")
	}
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepSendMessage: %w", err)
	}
	imageURL, _ := step.Options["image_url"].(string)
	if imageURL != "" {
		localPath, dlErr := tc.downloadRemoteImage(imageURL)
		if dlErr != nil {
			return fmt.Errorf("stepSendMessage: download image_url: %w", dlErr)
		}
		log.Printf("[TG:SEND:%s] sending image from url → %s with caption to %v", tc.accountID, localPath, peer)
		return tc.sendImage(ctx, peer, localPath, step.Value)
	}
	imagePath, _ := step.Options["image_path"].(string)
	if imagePath != "" {
		log.Printf("[TG:SEND:%s] sending image+caption to %v", tc.accountID, peer)
		return tc.sendImage(ctx, peer, imagePath, step.Value)
	}
	silent, _ := step.Options["silent"].(bool)
	log.Printf("[TG:SEND:%s] sending text (len=%d silent=%v) to %v", tc.accountID, len(step.Value), silent, peer)
	return tc.sendText(ctx, peer, step.Value, 0, silent)
}

func (tc *TelegramCollector) stepReply(ctx context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepReply: no reply text")
	}
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepReply: %w", err)
	}
	replyToID := tc.extractMsgID(step.Options, "message_id")
	log.Printf("[TG:REPLY:%s] replying to msg=%d in %v (len=%d)", tc.accountID, replyToID, peer, len(step.Value))
	return tc.sendText(ctx, peer, step.Value, replyToID, false)
}

func (tc *TelegramCollector) stepReact(ctx context.Context, step shared.InstructionStep) error {
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepReact: %w", err)
	}
	msgID := tc.extractMsgID(step.Options, "message_id")
	if msgID == 0 {
		return fmt.Errorf("stepReact: message_id required")
	}
	emoji, _ := step.Options["emoji"].(string)
	if emoji == "" {
		emoji = "👍"
	}
	log.Printf("[TG:REACT:%s] reacting to msg=%d emoji=%s in %v", tc.accountID, msgID, emoji, peer)
	return tc.sendReaction(ctx, peer, msgID, emoji)
}

func (tc *TelegramCollector) stepUpload(ctx context.Context, step shared.InstructionStep) error {
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepUpload: %w", err)
	}
	imageURL, _ := step.Options["image_url"].(string)
	if imageURL != "" {
		localPath, dlErr := tc.downloadRemoteImage(imageURL)
		if dlErr != nil {
			return fmt.Errorf("stepUpload: download image_url: %w", dlErr)
		}
		caption, _ := step.Options["caption"].(string)
		if caption == "" {
			caption = step.Value
		}
		log.Printf("[TG:UPLOAD:%s] uploading image from url → %s to %v", tc.accountID, localPath, peer)
		return tc.sendImage(ctx, peer, localPath, caption)
	}
	filePath, _ := step.Options["file_path"].(string)
	if filePath == "" {
		filePath = step.Value
	}
	if filePath == "" {
		return fmt.Errorf("stepUpload: file_path or image_url required")
	}
	caption, _ := step.Options["caption"].(string)
	mediaType, _ := step.Options["media_type"].(string)
	log.Printf("[TG:UPLOAD:%s] uploading file=%s type=%s to %v", tc.accountID, filePath, mediaType, peer)
	switch strings.ToLower(mediaType) {
	case "image", "photo":
		return tc.sendImage(ctx, peer, filePath, caption)
	case "video":
		return tc.sendVideo(ctx, peer, filePath, caption)
	case "audio":
		return tc.sendAudio(ctx, peer, filePath)
	default:
		return tc.sendDocument(ctx, peer, filePath, caption)
	}
}

func (tc *TelegramCollector) stepDownload(ctx context.Context, step shared.InstructionStep) error {
	url, _ := step.Options["url"].(string)
	if url == "" {
		url = step.Value
	}
	if url == "" {
		return fmt.Errorf("stepDownload: url required")
	}
	savePath, _ := step.Options["save_path"].(string)
	log.Printf("[TG:DOWNLOAD:%s] url=%s save_path=%q", tc.accountID, url, savePath)
	if savePath != "" {
		data, err := tc.fetchURL(url)
		if err != nil {
			return fmt.Errorf("stepDownload fetch: %w", err)
		}
		if err := os.WriteFile(savePath, data, 0o644); err != nil {
			return fmt.Errorf("stepDownload write: %w", err)
		}
		log.Printf("[TG:DOWNLOAD:%s] ✓ saved %s → %s", tc.accountID, url, savePath)
		return nil
	}
	localPath, err := tc.downloadRemoteImage(url)
	if err != nil {
		return fmt.Errorf("stepDownload: %w", err)
	}
	log.Printf("[TG:DOWNLOAD:%s] ✓ saved %s → %s", tc.accountID, url, localPath)
	return nil
}

func (tc *TelegramCollector) stepBlock(ctx context.Context, step shared.InstructionStep, block bool) error {
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepBlock: %w", err)
	}
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("stepBlock: gogram client not connected")
	}
	inputPeer, convErr := tc.toInputPeer(peer)
	if convErr != nil {
		return fmt.Errorf("stepBlock toInputPeer: %w", convErr)
	}
	action := "block"
	if !block {
		action = "unblock"
	}
	log.Printf("[TG:BLOCK:%s] %s %v", tc.accountID, action, peer)
	if block {
		if _, rawErr := client.ContactsBlock(false, inputPeer); rawErr != nil {
			return fmt.Errorf("stepBlock Block: %w", rawErr)
		}
	} else {
		if _, rawErr := client.ContactsUnblock(false, inputPeer); rawErr != nil {
			return fmt.Errorf("stepBlock Unblock: %w", rawErr)
		}
	}
	log.Printf("[TG:BLOCK:%s] ✓ %sed %v", tc.accountID, action, peer)
	return nil
}

func (tc *TelegramCollector) stepSearch(ctx context.Context, step shared.InstructionStep) error {
	query, _ := step.Options["query"].(string)
	if query == "" {
		query = step.Value
	}
	if query == "" {
		return fmt.Errorf("stepSearch: query required")
	}
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("stepSearch: gogram client not connected")
	}
	log.Printf("[TG:SEARCH:%s] searching contacts for %q", tc.accountID, query)
	contactsResult, err := client.ContactsGetContacts(0)
	if err != nil {
		return fmt.Errorf("stepSearch ContactsGetContacts: %w", err)
	}
	contactsObj, ok := contactsResult.(*telegram.ContactsContactsObj)
	if !ok {
		log.Printf("[TG:SEARCH:%s] contacts not modified or empty", tc.accountID)
		return nil
	}
	type matchResult struct {
		ID        int64  `json:"id"`
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Username  string `json:"username"`
		Phone     string `json:"phone"`
	}
	queryLower := strings.ToLower(query)
	var results []matchResult
	for _, u := range contactsObj.Users {
		if user, ok := u.(*telegram.UserObj); ok {
			name := strings.ToLower(fmt.Sprintf("%s %s %s", user.FirstName, user.LastName, user.Username))
			phone := user.Phone
			if strings.Contains(name, queryLower) || strings.Contains(phone, query) {
				results = append(results, matchResult{
					ID:        user.ID,
					FirstName: user.FirstName,
					LastName:  user.LastName,
					Username:  user.Username,
					Phone:     phone,
				})
			}
		}
	}
	log.Printf("[TG:SEARCH:%s] query=%q → %d match(es)", tc.accountID, query, len(results))
	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	if encoded, err := json.Marshal(results); err == nil {
		step.Options["result"] = string(encoded)
	}
	return nil
}

func (tc *TelegramCollector) stepJoinChannel(ctx context.Context, step shared.InstructionStep) error {
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepJoinChannel: %w", err)
	}
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("stepJoinChannel: gogram client not connected")
	}
	log.Printf("[TG:JOIN:%s] joining channel/group %v", tc.accountID, peer)
	if _, err := client.JoinChannel(peer); err != nil {
		return fmt.Errorf("stepJoinChannel: %w", err)
	}
	log.Printf("[TG:JOIN:%s] ✓ joined %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) stepPinMessage(ctx context.Context, step shared.InstructionStep) error {
	peer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepPinMessage: %w", err)
	}
	msgID := tc.extractMsgID(step.Options, "message_id")
	if msgID == 0 {
		return fmt.Errorf("stepPinMessage: message_id required")
	}
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("stepPinMessage: gogram client not connected")
	}
	log.Printf("[TG:PIN:%s] pinning msg=%d in %v", tc.accountID, msgID, peer)
	if _, err := client.PinMessage(peer, int32(msgID), &telegram.PinOptions{}); err != nil {
		return fmt.Errorf("stepPinMessage: %w", err)
	}
	log.Printf("[TG:PIN:%s] ✓ pinned msg=%d in %v", tc.accountID, msgID, peer)
	return nil
}

func (tc *TelegramCollector) stepForward(ctx context.Context, step shared.InstructionStep) error {
	fromPeer, err := tc.resolvePeer(step.Options)
	if err != nil {
		return fmt.Errorf("stepForward source: %w", err)
	}
	toPeerStr, _ := step.Options["to"].(string)
	if toPeerStr == "" {
		return fmt.Errorf("stepForward: 'to' required")
	}
	toPeer, err := tc.resolvePeerFromString(toPeerStr)
	if err != nil {
		return fmt.Errorf("stepForward target: %w", err)
	}
	msgID := tc.extractMsgID(step.Options, "message_id")
	if msgID == 0 {
		return fmt.Errorf("stepForward: message_id required")
	}
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("stepForward: gogram client not connected")
	}
	log.Printf("[TG:FORWARD:%s] forwarding msg=%d from %v to %v", tc.accountID, msgID, fromPeer, toPeer)
	if _, err := client.Forward(toPeer, fromPeer, []int32{int32(msgID)}); err != nil {
		return fmt.Errorf("stepForward: %w", err)
	}
	log.Printf("[TG:FORWARD:%s] ✓ forwarded msg=%d to %v", tc.accountID, msgID, toPeer)
	return nil
}

func (tc *TelegramCollector) stepWait(step shared.InstructionStep) error {
	ms := step.DelayAfter
	if ms <= 0 {
		ms = 2000
	}
	log.Printf("[TG:WAIT:%s] sleeping %dms", tc.accountID, ms)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return nil
}

func (tc *TelegramCollector) stepAPICall(ctx context.Context, step shared.InstructionStep) error {
	targetURL := step.Value
	if u, ok := step.Options["url"].(string); ok && u != "" {
		targetURL = u
	}
	if targetURL == "" {
		return fmt.Errorf("stepAPICall: url required (set step.Value or options.url)")
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
		return fmt.Errorf("stepAPICall: build request: %w", err)
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
		return fmt.Errorf("stepAPICall: request failed: %w", err)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("stepAPICall: read response: %w", err)
	}
	log.Printf("[TG:API:%s] %s %s → HTTP %d (%d bytes)",
		tc.accountID, method, targetURL, resp.StatusCode, len(respBytes))
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

func (tc *TelegramCollector) sendText(ctx context.Context, peer interface{}, text string, replyToID int, silent bool) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("sendText: gogram client not connected")
	}
	opts := &telegram.SendOptions{Silent: silent}
	if replyToID > 0 {
		opts.ReplyID = int32(replyToID)
	}
	if _, err := client.SendMessage(peer, text, opts); err != nil {
		return fmt.Errorf("sendText: %w", err)
	}
	log.Printf("[TG:SEND:%s] ✓ text sent to %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) sendImage(ctx context.Context, peer interface{}, filePath, caption string) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("sendImage: gogram client not connected")
	}
	if _, err := client.SendMedia(peer, filePath, &telegram.MediaOptions{Caption: caption}); err != nil {
		return fmt.Errorf("sendImage: %w", err)
	}
	log.Printf("[TG:SEND:%s] ✓ image sent to %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) sendVideo(ctx context.Context, peer interface{}, filePath, caption string) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("sendVideo: gogram client not connected")
	}
	if _, err := client.SendMedia(peer, filePath, &telegram.MediaOptions{Caption: caption}); err != nil {
		return fmt.Errorf("sendVideo: %w", err)
	}
	log.Printf("[TG:SEND:%s] ✓ video sent to %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) sendAudio(ctx context.Context, peer interface{}, filePath string) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("sendAudio: gogram client not connected")
	}
	if _, err := client.SendMedia(peer, filePath, &telegram.MediaOptions{
		Attributes: []telegram.DocumentAttribute{
			&telegram.DocumentAttributeAudio{Voice: strings.HasSuffix(filePath, ".ogg")},
		},
	}); err != nil {
		return fmt.Errorf("sendAudio: %w", err)
	}
	log.Printf("[TG:SEND:%s] ✓ audio sent to %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) sendDocument(ctx context.Context, peer interface{}, filePath, caption string) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("sendDocument: gogram client not connected")
	}
	if _, err := client.SendMedia(peer, filePath, &telegram.MediaOptions{
		Caption:       caption,
		ForceDocument: true,
	}); err != nil {
		return fmt.Errorf("sendDocument: %w", err)
	}
	log.Printf("[TG:SEND:%s] ✓ document sent to %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) sendReaction(ctx context.Context, peer interface{}, msgID int, emoji string) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("sendReaction: gogram client not connected")
	}
	reactions := []telegram.Reaction{&telegram.ReactionEmoji{Emoticon: emoji}}
	if err := client.SendReaction(peer, int32(msgID), reactions, false); err != nil {
		return fmt.Errorf("sendReaction: %w", err)
	}
	log.Printf("[TG:REACT:%s] ✓ reaction %q on msg=%d in %v", tc.accountID, emoji, msgID, peer)
	return nil
}

func (tc *TelegramCollector) PostToChannel(ctx context.Context, peer interface{}, text string) error {
	return tc.sendText(ctx, peer, text, 0, false)
}

func (tc *TelegramCollector) PostSilentToChannel(ctx context.Context, peer interface{}, text string) error {
	return tc.sendText(ctx, peer, text, 0, true)
}

func (tc *TelegramCollector) PostImageToChannel(ctx context.Context, peer interface{}, filePath, caption string) error {
	return tc.sendImage(ctx, peer, filePath, caption)
}

func (tc *TelegramCollector) PostAlbumToChannel(ctx context.Context, peer interface{}, filePaths []string, caption string) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("PostAlbumToChannel: gogram client not connected")
	}
	log.Printf("[TG:ALBUM:%s] posting album (%d files) to %v", tc.accountID, len(filePaths), peer)
	if _, err := client.SendAlbum(peer, filePaths, &telegram.MediaOptions{Caption: caption}); err != nil {
		return fmt.Errorf("PostAlbumToChannel: %w", err)
	}
	log.Printf("[TG:ALBUM:%s] ✓ album posted to %v", tc.accountID, peer)
	return nil
}

func (tc *TelegramCollector) DeleteMessage(ctx context.Context, peer interface{}, msgID int) error {
	client := tc.safeClient()
	if client == nil {
		return fmt.Errorf("DeleteMessage: gogram client not connected")
	}
	log.Printf("[TG:DELETE:%s] deleting msg=%d in %v", tc.accountID, msgID, peer)
	if _, err := client.DeleteMessages(peer, []int32{int32(msgID)}); err != nil {
		return fmt.Errorf("DeleteMessage: %w", err)
	}
	log.Printf("[TG:DELETE:%s] ✓ deleted msg=%d", tc.accountID, msgID)
	return nil
}

func (tc *TelegramCollector) GetChatMembers(ctx context.Context, peer interface{}) ([]UserInfo, error) {
	client := tc.safeClient()
	if client == nil {
		return nil, fmt.Errorf("GetChatMembers: gogram client not connected")
	}
	log.Printf("[TG:MEMBERS:%s] fetching members of %v", tc.accountID, peer)
	members, _, err := client.GetChatMembers(peer, &telegram.ParticipantOptions{
		Filter: &telegram.ChannelParticipantsRecent{},
	})
	if err != nil {
		return nil, fmt.Errorf("GetChatMembers: %w", err)
	}
	var result []UserInfo
	for _, m := range members {
		if m.User != nil {
			name := strings.TrimSpace(m.User.FirstName + " " + m.User.LastName)
			result = append(result, UserInfo{
				UserID:      tc.generateUserID(fmt.Sprintf("%d", m.User.ID)),
				Username:    m.User.Username,
				DisplayName: name,
			})
		}
	}
	log.Printf("[TG:MEMBERS:%s] ✓ got %d members from %v", tc.accountID, len(result), peer)
	return result, nil
}

func (tc *TelegramCollector) GetDialogs(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	client := tc.safeClient()
	if client == nil {
		return nil, fmt.Errorf("GetDialogs: gogram client not connected")
	}
	if limit <= 0 {
		limit = 50
	}
	log.Printf("[TG:DIALOGS:%s] fetching up to %d dialogs", tc.accountID, limit)
	dialogs, err := client.GetDialogs(&telegram.DialogOptions{Limit: int32(limit)})
	if err != nil {
		return nil, fmt.Errorf("GetDialogs: %w", err)
	}
	var result []map[string]interface{}
	for _, d := range dialogs {
		peer := d.Peer
		var id int64
		typ := "unknown"
		switch p := peer.(type) {
		case *telegram.PeerUser:
			id = p.UserID
			typ = "user"
		case *telegram.PeerChat:
			id = p.ChatID
			typ = "chat"
		case *telegram.PeerChannel:
			id = p.ChannelID
			typ = "channel"
		}
		result = append(result, map[string]interface{}{
			"id":    id,
			"type":  typ,
			"title": "",
		})
	}
	log.Printf("[TG:DIALOGS:%s] ✓ got %d dialogs", tc.accountID, len(result))
	return result, nil
}

func (tc *TelegramCollector) Close() {
	log.Printf("[TG:CLOSE:%s] shutting down, saving dedupe...", tc.accountID)
	tc.saveSeenMessages()
	select {
	case <-tc.shutdown:
	default:
		close(tc.shutdown)
	}
	tc.wg.Wait()
	log.Printf("[TG:CLOSE:%s] ✓ shutdown complete", tc.accountID)
}

func (tc *TelegramCollector) resolvePeer(opts map[string]interface{}) (interface{}, error) {
	var raw string
	for _, key := range []string{"to", "recipient", "channel", "group_id", "chat_id"} {
		v, ok := opts[key]
		if !ok {
			continue
		}
		switch val := v.(type) {
		case string:
			if val != "" {
				raw = val
				break
			}
		case float64:
			return int64(val), nil
		case int:
			return int64(val), nil
		case int64:
			return val, nil
		}
		if raw != "" {
			break
		}
	}
	if raw == "" {
		return nil, fmt.Errorf("no recipient in step options (to/recipient/channel/group_id/chat_id)")
	}
	return tc.resolvePeerFromString(raw)
}

func (tc *TelegramCollector) resolvePeerFromString(raw string) (interface{}, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "https://t.me/")
	raw = strings.TrimPrefix(raw, "t.me/")
	raw = strings.TrimPrefix(raw, "@")

	if id, err := strconv.ParseInt(raw, 10, 64); err == nil {
		tgDbg(tc.accountID, "peer resolved as numeric id=%d", id)
		return id, nil
	}

	client := tc.safeClient()
	if client == nil {
		return nil, fmt.Errorf("client not connected, cannot resolve username: %s", raw)
	}

	log.Printf("[TG:RESOLVE:%s] resolving username: %s", tc.accountID, raw)
	resolved, err := client.ResolveUsername(raw)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve username %q: %w", raw, err)
	}

	log.Printf("[TG:RESOLVE:%s] ✓ resolved @%s → %T", tc.accountID, raw, resolved)
	return resolved, nil
}

func (tc *TelegramCollector) toInputPeer(peer interface{}) (telegram.InputPeer, error) {
	switch v := peer.(type) {
	case telegram.InputPeer:
		return v, nil
	case int64:
		if v > 0 {
			return &telegram.InputPeerUser{UserID: v}, nil
		} else if v < 0 {
			if v < -1000000000 {
				return &telegram.InputPeerChannel{ChannelID: -v}, nil
			} else {
				return &telegram.InputPeerChat{ChatID: -v}, nil
			}
		}
		return nil, fmt.Errorf("invalid peer ID: %d", v)
	case string:
		resolved, err := tc.resolvePeerFromString(v)
		if err != nil {
			return nil, err
		}
		return tc.toInputPeer(resolved)
	default:
		return nil, fmt.Errorf("unsupported peer type %T", peer)
	}
}
