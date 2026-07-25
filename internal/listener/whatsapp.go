package listener

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"go.mau.fi/whatsmeow"
	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"sailstream/internal/config"
	"sailstream/internal/enviroment"
	"sailstream/internal/session"
	"sailstream/internal/shared"
)

var debugWA = os.Getenv("WA_DEBUG") == "1"

func waDbg(accountID, format string, args ...interface{}) {
	if !debugWA {
		return
	}
	log.Printf("[WA:DBG:%s] "+format, append([]interface{}{accountID}, args...)...)
}

var ErrUnauthorised = errors.New("whatsapp: 401 unauthorised – message not delivered")

const (
	notifBufferSize      = 500
	msgDedupeWindow      = 24 * time.Hour
	collectDrainWindow   = 100 * time.Millisecond
	maxBatchSize         = 200
	pauseAckTimeout      = 15 * time.Second
	mediaDirDefault      = "./media/whatsapp"
	chatCooldownDuration = 10 * time.Second
)

type WhatsAppClientSetter interface {
	SetClient(client *whatsmeow.Client) error
}

type WhatsAppCollector struct {
	platformID string
	subtypeID  string
	accountID  string
	subtype    string

	config     *ListenerConfig
	db         *sql.DB
	configMgr  *config.ConfigManager
	envMgr     *enviroment.Environment
	sessionMgr *session.Manager

	waClient *whatsmeow.Client

	notifBuffer      chan *Notification
	instructionQueue chan *shared.AutomationInstruction
	errorChan        chan *PlatformError

	connected      atomic.Bool
	collectRunning atomic.Bool
	// pauseCount is a reference count, not a bool: multiple instructions can be
	// mid-execution concurrently, and collection should only actually resume
	// once every one of them has called resumeCollection (fix for the earlier
	// single-bool version, where any resume would undo every other pause).
	pauseCount atomic.Int32

	drainPaused atomic.Bool
	pauseAck    chan struct{}
	resumeMu    sync.Mutex
	resumeReq   chan struct{}

	executionMu sync.Mutex

	seenMsgMu sync.Mutex
	seenMsgs  map[string]time.Time

	groupCacheMu sync.RWMutex
	groupCache   map[string]*waTypes.GroupInfo
	groupSF      singleflight.Group

	lastSentMu   sync.Mutex
	lastSentTime map[string]time.Time

	// pendingCursor holds the highest message timestamp seen in the current
	// batch. It is written by processMessage (called from the event handler)
	// and flushed atomically to global_cursors at the end of each Collect
	// drain, making cursor advancement batch-safe — the cursor only moves
	// past a message once that message has actually been handed to the
	// caller, not the instant it's observed by the event handler.
	pendingCursorMu   sync.Mutex
	pendingCursorTime time.Time
}

func NewWhatsAppCollector(
	platformID, subtypeID, accountID, subtype string,
	listenerConfig *ListenerConfig,
	db *sql.DB,
	configMgr *config.ConfigManager,
	envMgr *enviroment.Environment,
	sessionMgr *session.Manager,
) *WhatsAppCollector {
	log.Printf("[INIT] NewWhatsAppCollector for %s/%s (platform=%s subtype=%s)",
		subtypeID, accountID, platformID, subtype)

	wc := &WhatsAppCollector{
		platformID:       platformID,
		subtypeID:        subtypeID,
		accountID:        accountID,
		subtype:          subtype,
		config:           listenerConfig,
		db:               db,
		configMgr:        configMgr,
		envMgr:           envMgr,
		sessionMgr:       sessionMgr,
		notifBuffer:      make(chan *Notification, notifBufferSize),
		instructionQueue: make(chan *shared.AutomationInstruction, 100),
		errorChan:        make(chan *PlatformError, 50),
		pauseAck:         make(chan struct{}, 1),
		resumeReq:        make(chan struct{}),
		seenMsgs:         make(map[string]time.Time),
		groupCache:       make(map[string]*waTypes.GroupInfo),
		lastSentTime:     make(map[string]time.Time),
	}

	return wc
}

func (wc *WhatsAppCollector) getCursorSubtypeID() string {
	return "global"
}

func (wc *WhatsAppCollector) getCursorType() string {
	return "timestamp"
}

func (wc *WhatsAppCollector) getLastCollectionTimestamp() (time.Time, error) {
	var cursorValue string
	err := wc.db.QueryRow(`
		SELECT cursor_value FROM global_cursors
		WHERE platform = 'whatsapp' AND subtype = ? AND account_id = ? AND subtype_id = ? AND cursor_type = ?
	`, wc.subtype, wc.accountID, wc.getCursorSubtypeID(), wc.getCursorType()).Scan(&cursorValue)
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

func (wc *WhatsAppCollector) updateLastCollectionTimestamp(ts time.Time) error {
	_, err := wc.db.Exec(`
		INSERT INTO global_cursors (platform, subtype, account_id, subtype_id, cursor_type, cursor_value, updated_at)
		VALUES ('whatsapp', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(platform, subtype, account_id, subtype_id, cursor_type) DO UPDATE SET
			cursor_value = excluded.cursor_value,
			updated_at = CURRENT_TIMESTAMP
	`, wc.subtype, wc.accountID, wc.getCursorSubtypeID(), wc.getCursorType(), ts.UTC().Format(time.RFC3339Nano))
	return err
}

// stageCursorUpdate records ts as a candidate for the next cursor flush.
// It only advances the pending value, never moves it backward.
// Call this from processMessage; the cursor is not written to the DB until
// flushPendingCursor() is called at the end of a successful Collect drain,
// making advancement batch-safe.
func (wc *WhatsAppCollector) stageCursorUpdate(ts time.Time) {
	if ts.IsZero() {
		return
	}
	wc.pendingCursorMu.Lock()
	defer wc.pendingCursorMu.Unlock()
	if ts.After(wc.pendingCursorTime) {
		wc.pendingCursorTime = ts
	}
}

// flushPendingCursor writes the highest staged timestamp to global_cursors and
// resets the in-memory buffer. It is called once per Collect cycle, after the
// entire batch has been drained, so the cursor only advances when all messages
// in the batch are safely in the caller's hands.
func (wc *WhatsAppCollector) flushPendingCursor() {
	wc.pendingCursorMu.Lock()
	ts := wc.pendingCursorTime
	wc.pendingCursorTime = time.Time{}
	wc.pendingCursorMu.Unlock()

	if ts.IsZero() {
		return
	}
	if err := wc.updateLastCollectionTimestamp(ts); err != nil {
		log.Printf("[WA:CURSOR:%s] failed to flush cursor: %v", wc.accountID, err)
	} else {
		log.Printf("[WA:CURSOR:%s] cursor flushed → %s", wc.accountID, ts.UTC().Format(time.RFC3339Nano))
	}
}

func (wc *WhatsAppCollector) SetClient(client *whatsmeow.Client) error {
	if client == nil {
		return fmt.Errorf("SetClient: nil client")
	}
	wc.waClient = client
	client.AddEventHandler(wc.handleEvent)
	client.AddEventHandler(wc.handleUndecryptable)
	wc.connected.Store(true)
	log.Printf("[WA] Client injected for %s", wc.accountID)
	return nil
}

func (wc *WhatsAppCollector) handleUndecryptable(rawEvt interface{}) {
	evt, ok := rawEvt.(*events.UndecryptableMessage)
	if !ok {
		return
	}
	log.Printf("[WhatsApp:%s:%s] undecryptable message from %s in %s (id=%s) – will retry",
		wc.subtypeID, wc.accountID,
		evt.Info.Sender, evt.Info.Chat, evt.Info.ID)

	notif := &Notification{
		ID:         fmt.Sprintf("whatsapp_undecryptable_%s_%d", wc.accountID, time.Now().UnixNano()),
		PlatformID: wc.platformID,
		SubtypeID:  wc.subtypeID,
		AccountID:  wc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  evt.Info.Timestamp,
		Message: &MessageData{
			ConversationID: evt.Info.Chat.String(),
			MessageID:      evt.Info.ID,
			Text:           "[undecryptable – retrying]",
			Timestamp:      evt.Info.Timestamp,
		},
		RawData: map[string]interface{}{
			"undecryptable": true,
			"message_id":    evt.Info.ID,
		},
		CollectedAt: time.Now(),
	}
	select {
	case wc.notifBuffer <- notif:
	default:
		wc.reportError("BUFFER_FULL", "notification buffer full, undecryptable dropped", "warning")
	}
}

func (wc *WhatsAppCollector) handleEvent(rawEvt interface{}) {
	switch evt := rawEvt.(type) {
	case *events.Message:
		if evt.Info.IsFromMe {
			return
		}

		chat := evt.Info.Chat.String()
		if strings.HasSuffix(chat, "@broadcast") || strings.HasSuffix(chat, "@newsletter") {
			log.Printf("[WhatsApp:%s:%s] ignoring message from broadcast/newsletter: %s",
				wc.subtypeID, wc.accountID, chat)
			return
		}

		if evt.Info.Edit == waTypes.EditAttributeSenderRevoke ||
			evt.Info.Edit == waTypes.EditAttributeAdminRevoke {
			wc.handleRevokedMessage(evt)
			return
		}

		if evt.IsEdit {
			wc.handleEditedMessage(evt)
			return
		}

		if reaction := evt.Message.GetReactionMessage(); reaction != nil {
			wc.processReactionForStats(evt, reaction)
			return
		}

		if !wc.shouldCollectMessage(evt) {
			waDbg(wc.accountID, "skipping junk/empty message %s", evt.Info.ID)
			return
		}

		if wc.config != nil && !wc.config.ListenMessages {
			waDbg(wc.accountID, "ListenMessages disabled, dropping message")
			return
		}

		if evt.Info.IsGroup && wc.config != nil && !wc.config.ListenGroupMessages {
			waDbg(wc.accountID, "ListenGroupMessages disabled, dropping group message")
			return
		}

		if wc.isChatOnCooldown(chat) {
			log.Printf("[WhatsApp:%s:%s] cooldown active for chat %s – skipping incoming message (anti-loop)",
				wc.subtypeID, wc.accountID, chat)
			return
		}

		lastTimestamp, err := wc.getLastCollectionTimestamp()
		if err != nil {
			log.Printf("[WhatsApp:%s:%s] error reading cursor: %v", wc.subtypeID, wc.accountID, err)
		} else if !lastTimestamp.IsZero() && evt.Info.Timestamp.Before(lastTimestamp) {
			waDbg(wc.accountID, "skipping message %s with timestamp %v earlier than cursor %v",
				evt.Info.ID, evt.Info.Timestamp, lastTimestamp)
			return
		}

		notif, err := wc.processMessage(evt)
		if err != nil {
			log.Printf("[WhatsApp:%s:%s] processMessage error: %v", wc.subtypeID, wc.accountID, err)
			return
		}
		if notif != nil {
			// Stage, don't commit: the cursor must not advance past this
			// message until it has actually been drained out of notifBuffer
			// and handed to the Collect() caller. Writing it here, before
			// that handoff, risks losing the message if the process dies
			// (or Collect simply never gets to drain it) between this push
			// and the caller receiving it. flushPendingCursor() in Collect()
			// commits the highest staged value once the batch is safely out.
			//
			// pushNotification returns false if notifBuffer was full and the
			// message was dropped — in that case we must NOT stage the
			// cursor, or the dropped message's timestamp gets committed as
			// "processed" and is lost forever on the next run's cursor check.
			if wc.pushNotification(notif) {
				wc.stageCursorUpdate(evt.Info.Timestamp)
			}
		}

	case *events.Connected:
		wc.connected.Store(true)
		log.Printf("[WhatsApp:%s:%s] connected", wc.subtypeID, wc.accountID)

	case *events.Disconnected:
		wc.connected.Store(false)
		log.Printf("[WhatsApp:%s:%s] disconnected", wc.subtypeID, wc.accountID)
		wc.reportError("DISCONNECTED", "WhatsApp connection lost", "warning")

	case *events.LoggedOut:
		wc.connected.Store(false)
		wc.reportError("LOGGED_OUT",
			fmt.Sprintf("session logged out: reason=%d", evt.Reason), "critical")

	case *events.Receipt:
		waDbg(wc.accountID, "receipt: type=%s ids=%v", evt.Type, evt.MessageIDs)

	case *events.GroupInfo:
		wc.groupCacheMu.Lock()
		delete(wc.groupCache, evt.JID.String())
		wc.groupCacheMu.Unlock()
	}
}

func (wc *WhatsAppCollector) recordChatCooldown(jid waTypes.JID) {
	wc.lastSentMu.Lock()
	defer wc.lastSentMu.Unlock()
	wc.lastSentTime[jid.String()] = time.Now()
}

func (wc *WhatsAppCollector) isChatOnCooldown(chatJID string) bool {
	wc.lastSentMu.Lock()
	defer wc.lastSentMu.Unlock()
	last, ok := wc.lastSentTime[chatJID]
	return ok && time.Since(last) < chatCooldownDuration
}

func (wc *WhatsAppCollector) handleRevokedMessage(evt *events.Message) {
	msgID := evt.Info.ID
	log.Printf("[WhatsApp:%s:%s] message revoked: id=%s chat=%s",
		wc.subtypeID, wc.accountID, msgID, evt.Info.Chat)

	notif := &Notification{
		ID:         fmt.Sprintf("whatsapp_revoke_%s_%d", wc.accountID, time.Now().UnixNano()),
		PlatformID: wc.platformID,
		SubtypeID:  wc.subtypeID,
		AccountID:  wc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  evt.Info.Timestamp,
		Message: &MessageData{
			ConversationID: evt.Info.Chat.String(),
			MessageID:      msgID,
			Text:           "[message deleted]",
			Timestamp:      evt.Info.Timestamp,
		},
		RawData: map[string]interface{}{
			"revoked":    true,
			"message_id": msgID,
			"chat_jid":   evt.Info.Chat.String(),
			"sender_jid": evt.Info.Sender.String(),
		},
		CollectedAt: time.Now(),
	}
	wc.pushNotification(notif)
}

func (wc *WhatsAppCollector) handleEditedMessage(evt *events.Message) {
	msgID := evt.Info.ID
	newText := extractMessageText(evt.Message)
	log.Printf("[WhatsApp:%s:%s] message edited: id=%s chat=%s",
		wc.subtypeID, wc.accountID, msgID, evt.Info.Chat)

	notif := &Notification{
		ID:         fmt.Sprintf("whatsapp_edit_%s_%d", wc.accountID, time.Now().UnixNano()),
		PlatformID: wc.platformID,
		SubtypeID:  wc.subtypeID,
		AccountID:  wc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  evt.Info.Timestamp,
		Message: &MessageData{
			ConversationID: evt.Info.Chat.String(),
			MessageID:      msgID,
			Text:           newText,
			Timestamp:      evt.Info.Timestamp,
		},
		RawData: map[string]interface{}{
			"edited":     true,
			"message_id": msgID,
			"new_text":   newText,
			"chat_jid":   evt.Info.Chat.String(),
			"sender_jid": evt.Info.Sender.String(),
		},
		CollectedAt: time.Now(),
	}
	wc.pushNotification(notif)
}

func (wc *WhatsAppCollector) processReactionForStats(
	evt *events.Message,
	reaction *waProto.ReactionMessage,
) {
	dedupeKey := fmt.Sprintf("react_%s_%s_%s",
		evt.Info.Chat.String(),
		reaction.GetKey().GetID(),
		evt.Info.Sender.String(),
	)
	if !wc.checkAndMarkSeen(dedupeKey) {
		waDbg(wc.accountID, "duplicate reaction %s, skipping", dedupeKey)
		return
	}

	emoji := reaction.GetText()
	if emoji == "" {
		return
	}

	senderName := wc.resolveDisplayName(context.Background(), evt.Info.Sender, evt.Info.PushName)
	chatName := wc.resolveChatName(context.Background(), evt.Info.Chat, senderName, evt.Info.IsGroup)

	log.Printf("[WhatsApp:%s:%s] reaction: from %s in %s on msg %s (emoji:%s)",
		wc.subtypeID, wc.accountID, senderName, chatName,
		reaction.GetKey().GetID(), emoji)
}

func (wc *WhatsAppCollector) pushNotification(n *Notification) bool {
	select {
	case wc.notifBuffer <- n:
		return true
	default:
		wc.reportError("BUFFER_FULL", "notification buffer full, message dropped", "warning")
		return false
	}
}

func (wc *WhatsAppCollector) shouldCollectMessage(evt *events.Message) bool {
	msg := evt.Message
	if msg == nil {
		return false
	}
	if extractMessageText(msg) != "" {
		return true
	}
	if msg.GetImageMessage() != nil ||
		msg.GetVideoMessage() != nil ||
		msg.GetAudioMessage() != nil ||
		msg.GetDocumentMessage() != nil ||
		msg.GetStickerMessage() != nil {
		return true
	}
	if msg.GetLocationMessage() != nil || msg.GetContactMessage() != nil {
		return true
	}
	return false
}

func (wc *WhatsAppCollector) processMessage(evt *events.Message) (*Notification, error) {
	if !wc.checkAndMarkSeen(evt.Info.ID) {
		waDbg(wc.accountID, "duplicate message %s, skipping", evt.Info.ID)
		return nil, nil
	}

	ctx := context.Background()
	chatJID := evt.Info.Chat
	senderJID := evt.Info.Sender

	senderName := wc.resolveDisplayName(ctx, senderJID, evt.Info.PushName)
	chatName := wc.resolveChatName(ctx, chatJID, senderName, evt.Info.IsGroup)

	text := extractMessageText(evt.Message)

	remoteURLs, localPaths, attachments := wc.extractAndDownloadMedia(evt)

	productID := wc.extractProductID(text)
	var productData map[string]interface{}
	if productID != "" {
		if data, err := wc.getProductData(productID); err == nil {
			productData = data
		} else {
			log.Printf("[WhatsApp:%s:%s] product lookup %q: %v",
				wc.subtypeID, wc.accountID, productID, err)
		}
	}

	platformUserID := senderJID.String()
	ud, recent, isNew, err := wc.getUserInfoForNotification(ctx, platformUserID)
	if err != nil {
		log.Printf("[WhatsApp:%s:%s] user lookup error for %s: %v – dropping message for safety",
			wc.subtypeID, wc.accountID, platformUserID, err)
		return nil, nil
	}

	isNewUser := isNew
	userData := ud
	recentMsgs := recent

	if !isNewUser && userData != nil {
		if blocked, _ := userData["is_blocked"].(bool); blocked {
			log.Printf("[WhatsApp:%s:%s] user %s is blocked, dropping message",
				wc.subtypeID, wc.accountID, platformUserID)
			return nil, nil
		}
	}

	var groupMembers []UserInfo
	if evt.Info.IsGroup {
		groupMembers = wc.getGroupMembers(ctx, chatJID)
	}

	replyTo, replyText := extractReplyContext(evt.Message)
	userID := wc.generateUserID(senderJID.String())

	raw := map[string]interface{}{
		"chat_jid":          chatJID.String(),
		"sender_jid":        senderJID.String(),
		"message_id":        evt.Info.ID,
		"is_group":          evt.Info.IsGroup,
		"push_name":         evt.Info.PushName,
		"image_urls":        remoteURLs,
		"downloaded_images": localPaths,
		"platform":          "whatsapp",
		"subtype":           wc.subtype,
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

	return &Notification{
		ID:         fmt.Sprintf("whatsapp_msg_%s_%d", wc.accountID, time.Now().UnixNano()),
		PlatformID: wc.platformID,
		SubtypeID:  wc.subtypeID,
		AccountID:  wc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  evt.Info.Timestamp,
		Message: &MessageData{
			ConversationID:   chatJID.String(),
			ConversationName: chatName,
			IsGroup:          evt.Info.IsGroup,
			GroupMembers:     groupMembers,
			MessageID:        evt.Info.ID,
			Sender: UserInfo{
				UserID:      userID,
				Username:    wc.jidToUsername(senderJID),
				DisplayName: senderName,
			},
			Text:           text,
			Timestamp:      evt.Info.Timestamp,
			IsRead:         false,
			IsForwarded:    evt.Info.IsIncomingBroadcast(),
			DeliveryStatus: "delivered",
			ReplyTo:        replyTo,
			ReplyText:      replyText,
			MediaAttached:  attachments,
		},
		RawData:     raw,
		CollectedAt: time.Now(),
	}, nil
}

func (wc *WhatsAppCollector) extractAndDownloadMedia(
	evt *events.Message,
) (remoteURLs, localPaths []string, attachments []MediaAttachment) {
	msg := evt.Message

	type mediaItem struct {
		url      string
		msgType  string
		mimeType string
		fileName string
		dl       whatsmeow.DownloadableMessage
		sha256   []byte
	}

	var items []mediaItem

	if img := msg.GetImageMessage(); img != nil {
		items = append(items, mediaItem{
			url: img.GetURL(), msgType: "image", mimeType: img.GetMimetype(),
			dl: img, sha256: img.GetFileSHA256(),
		})
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		items = append(items, mediaItem{
			url: vid.GetURL(), msgType: "video", mimeType: vid.GetMimetype(),
			dl: vid, sha256: vid.GetFileSHA256(),
		})
	}
	if aud := msg.GetAudioMessage(); aud != nil {
		items = append(items, mediaItem{
			url: aud.GetURL(), msgType: "audio", mimeType: aud.GetMimetype(),
			dl: aud, sha256: aud.GetFileSHA256(),
		})
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		items = append(items, mediaItem{
			url: doc.GetURL(), msgType: "document", mimeType: doc.GetMimetype(),
			fileName: doc.GetFileName(), dl: doc, sha256: doc.GetFileSHA256(),
		})
	}
	if sticker := msg.GetStickerMessage(); sticker != nil {
		items = append(items, mediaItem{
			url: sticker.GetURL(), msgType: "sticker", mimeType: sticker.GetMimetype(),
			dl: sticker, sha256: sticker.GetFileSHA256(),
		})
	}

	for i, item := range items {
		remoteURLs = append(remoteURLs, item.url)

		var localPath string
		if wc.imageRecognitionEnabled() {
			downloaded, err := wc.downloadWAMedia(item.dl, item.msgType, item.mimeType, item.fileName, evt.Info.ID, i, item.sha256)
			if err != nil {
				log.Printf("[WhatsApp:%s:%s] media download (%s): %v",
					wc.subtypeID, wc.accountID, item.msgType, err)
			} else {
				localPath = downloaded
				localPaths = append(localPaths, localPath)
			}
		}

		attachments = append(attachments, MediaAttachment{
			ID:        fmt.Sprintf("%s_%d", evt.Info.ID, i),
			Type:      item.msgType,
			URL:       item.url,
			Thumbnail: localPath,
			Filename:  item.fileName,
		})
	}
	return
}

func (wc *WhatsAppCollector) downloadWAMedia(
	dl whatsmeow.DownloadableMessage,
	mediaType, mimeType, fileName, msgID string,
	idx int,
	expectedSHA256 []byte,
) (string, error) {
	if dl == nil {
		return "", fmt.Errorf("nil downloadable message")
	}

	ext := extensionFromMime(mimeType)
	if ext == "" {
		ext = ".bin"
	}
	dir := wc.getMediaDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	h := sha256.Sum256([]byte(fmt.Sprintf("%s_%d", msgID, idx)))
	name := hex.EncodeToString(h[:8]) + ext
	fullPath := filepath.Join(dir, name)

	if info, err := os.Stat(fullPath); err == nil && info.Size() > 0 {
		if len(expectedSHA256) > 0 {
			if ok, verifyErr := wc.verifyFileHash(fullPath, expectedSHA256); verifyErr != nil || !ok {
				log.Printf("[WhatsApp:%s:%s] cached file %s failed integrity check (%v), re-downloading",
					wc.subtypeID, wc.accountID, fullPath, verifyErr)
				os.Remove(fullPath)
			} else {
				return fullPath, nil
			}
		} else {
			return fullPath, nil
		}
	}

	data, err := wc.waClient.Download(context.Background(), dl)
	if err != nil {
		return "", fmt.Errorf("whatsmeow download: %w", err)
	}

	if len(expectedSHA256) > 0 {
		got := sha256.Sum256(data)
		if !bytes.Equal(got[:], expectedSHA256) {
			return "", fmt.Errorf("downloadWAMedia: SHA-256 mismatch for %s_%d (got %x, want %x)",
				msgID, idx, got[:], expectedSHA256)
		}
	}

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if os.IsExist(err) {
		if len(expectedSHA256) > 0 {
			if ok, _ := wc.verifyFileHash(fullPath, expectedSHA256); ok {
				return fullPath, nil
			}
			os.Remove(fullPath)
			return "", fmt.Errorf("concurrent write produced invalid file: %s", fullPath)
		}
		return fullPath, nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		os.Remove(fullPath)
		return "", err
	}
	return fullPath, nil
}

func (wc *WhatsAppCollector) verifyFileHash(path string, expected []byte) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return bytes.Equal(h.Sum(nil), expected), nil
}

func (wc *WhatsAppCollector) downloadRemoteImage(imageURL string) (string, error) {
	if imageURL == "" {
		return "", fmt.Errorf("empty URL")
	}
	dir := wc.getMediaDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(imageURL))
	fullPath := filepath.Join(dir, hex.EncodeToString(h[:])[:16]+".jpg")

	if info, err := os.Stat(fullPath); err == nil && info.Size() > 0 {
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
	return fullPath, nil
}

func (wc *WhatsAppCollector) checkAndMarkSeen(msgID string) bool {
	wc.seenMsgMu.Lock()
	defer wc.seenMsgMu.Unlock()
	wc.pruneSeenMsgs()
	wc.pruneLastSentTime()
	if _, seen := wc.seenMsgs[msgID]; seen {
		return false
	}
	wc.seenMsgs[msgID] = time.Now().Add(msgDedupeWindow)
	return true
}

// pruneLastSentTime removes cooldown entries well past their cooldown window.
// Piggybacks on the same cadence as pruneSeenMsgs (called per checkAndMarkSeen)
// rather than a dedicated ticker, since lastSentTime's growth is bounded by
// distinct chats ever messaged and doesn't need its own schedule.
func (wc *WhatsAppCollector) pruneLastSentTime() {
	wc.lastSentMu.Lock()
	defer wc.lastSentMu.Unlock()
	now := time.Now()
	for jid, ts := range wc.lastSentTime {
		if now.Sub(ts) > chatCooldownDuration*4 {
			delete(wc.lastSentTime, jid)
		}
	}
}

func (wc *WhatsAppCollector) pruneSeenMsgs() {
	now := time.Now()
	for id, exp := range wc.seenMsgs {
		if now.After(exp) {
			delete(wc.seenMsgs, id)
		}
	}
}

func (wc *WhatsAppCollector) pauseCollection() error {
	if wc.pauseCount.Add(1) > 1 {
		// Already paused on behalf of another in-flight instruction.
		return nil
	}
	wc.drainPaused.Store(true)
	log.Printf("[WhatsApp:%s:%s] pause requested", wc.subtypeID, wc.accountID)

	if !wc.collectRunning.Load() {
		return nil
	}

	select {
	case <-wc.pauseAck:
		return nil
	case <-time.After(pauseAckTimeout):
		log.Printf("[WhatsApp:%s:%s] pause ack timeout (proceeding, drainPaused is set)",
			wc.subtypeID, wc.accountID)
		return nil
	}
}

func (wc *WhatsAppCollector) resumeCollection() {
	if wc.pauseCount.Add(-1) > 0 {
		// Still paused on behalf of another in-flight instruction.
		return
	}
	newResume := make(chan struct{})

	wc.resumeMu.Lock()
	old := wc.resumeReq
	wc.resumeReq = newResume
	wc.resumeMu.Unlock()

	wc.drainPaused.Store(false)
	close(old)
	log.Printf("[WhatsApp:%s:%s] resumed", wc.subtypeID, wc.accountID)
}

func (wc *WhatsAppCollector) checkPause(ctx context.Context) bool {
	if !wc.drainPaused.Load() {
		return true
	}

	select {
	case wc.pauseAck <- struct{}{}:
	default:
	}

	wc.resumeMu.Lock()
	resumeCh := wc.resumeReq
	wc.resumeMu.Unlock()

	select {
	case <-resumeCh:
		return true
	case <-ctx.Done():
		return false
	}
}

func (wc *WhatsAppCollector) getGroupMembers(ctx context.Context, groupJID waTypes.JID) []UserInfo {
	info := wc.fetchGroupInfo(ctx, groupJID)
	if info == nil {
		return nil
	}
	var members []UserInfo
	for _, p := range info.Participants {
		name := wc.resolveDisplayName(ctx, p.JID, "")
		members = append(members, UserInfo{
			UserID:      wc.generateUserID(p.JID.String()),
			Username:    wc.jidToUsername(p.JID),
			DisplayName: name,
		})
	}
	return members
}

func (wc *WhatsAppCollector) fetchGroupInfo(ctx context.Context, groupJID waTypes.JID) *waTypes.GroupInfo {
	key := groupJID.String()

	wc.groupCacheMu.RLock()
	info, cached := wc.groupCache[key]
	wc.groupCacheMu.RUnlock()
	if cached {
		return info
	}

	v, err, _ := wc.groupSF.Do(key, func() (interface{}, error) {
		wc.groupCacheMu.RLock()
		if i, ok := wc.groupCache[key]; ok {
			wc.groupCacheMu.RUnlock()
			return i, nil
		}
		wc.groupCacheMu.RUnlock()

		fresh, err := wc.waClient.GetGroupInfo(ctx, groupJID)
		if err != nil {
			return nil, err
		}
		wc.groupCacheMu.Lock()
		wc.groupCache[key] = fresh
		wc.groupCacheMu.Unlock()
		return fresh, nil
	})
	if err != nil {
		log.Printf("[WhatsApp:%s:%s] fetchGroupInfo for %s: %v",
			wc.subtypeID, wc.accountID, key, err)
		return nil
	}
	if gi, ok := v.(*waTypes.GroupInfo); ok {
		return gi
	}
	return nil
}

func (wc *WhatsAppCollector) resolveDisplayName(ctx context.Context, jid waTypes.JID, pushName string) string {
	if wc.waClient != nil {
		if contact, err := wc.waClient.Store.Contacts.GetContact(ctx, jid); err == nil {
			if contact.FullName != "" {
				return contact.FullName
			}
			if contact.PushName != "" {
				return contact.PushName
			}
		}
	}
	if pushName != "" {
		return pushName
	}
	return jid.User
}

func (wc *WhatsAppCollector) resolveChatName(ctx context.Context, chatJID waTypes.JID, senderName string, isGroup bool) string {
	if !isGroup {
		return senderName
	}
	info := wc.fetchGroupInfo(ctx, chatJID)
	if info != nil && info.Name != "" {
		return info.Name
	}
	return chatJID.String()
}

func extractMessageText(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}
	if t := msg.GetConversation(); t != "" {
		return t
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	if img := msg.GetImageMessage(); img != nil && img.GetCaption() != "" {
		return img.GetCaption()
	}
	if vid := msg.GetVideoMessage(); vid != nil && vid.GetCaption() != "" {
		return vid.GetCaption()
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		return doc.GetFileName()
	}
	if msg.GetAudioMessage() != nil {
		return "[Audio Message]"
	}
	if msg.GetStickerMessage() != nil {
		return "[Sticker]"
	}
	if loc := msg.GetLocationMessage(); loc != nil {
		return fmt.Sprintf("[Location %.6f, %.6f]",
			loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
	}
	if contact := msg.GetContactMessage(); contact != nil {
		return fmt.Sprintf("[Contact: %s]", contact.GetDisplayName())
	}
	return ""
}

func extractReplyContext(msg *waProto.Message) (replyTo *string, replyText string) {
	if msg == nil {
		return nil, ""
	}

	var ctx *waProto.ContextInfo
	switch {
	case msg.GetExtendedTextMessage() != nil:
		ctx = msg.GetExtendedTextMessage().GetContextInfo()
	case msg.GetImageMessage() != nil:
		ctx = msg.GetImageMessage().GetContextInfo()
	case msg.GetVideoMessage() != nil:
		ctx = msg.GetVideoMessage().GetContextInfo()
	case msg.GetAudioMessage() != nil:
		ctx = msg.GetAudioMessage().GetContextInfo()
	case msg.GetDocumentMessage() != nil:
		ctx = msg.GetDocumentMessage().GetContextInfo()
	case msg.GetStickerMessage() != nil:
		ctx = msg.GetStickerMessage().GetContextInfo()
	}

	if ctx == nil {
		return nil, ""
	}
	id := ctx.GetStanzaID()
	if id == "" {
		return nil, ""
	}
	text := ""
	if q := ctx.GetQuotedMessage(); q != nil {
		text = extractMessageText(q)
	}
	return &id, text
}

var waProductPatterns = []*regexp.Regexp{
	regexp.MustCompile(`SKU\s*=\s*["']([^"']+)["']`),
	regexp.MustCompile(`SKU\s*=\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)product\s*id\s*:\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)sku\s*:\s*([^\s<>"']+)`),
}

func (wc *WhatsAppCollector) extractProductID(text string) string {
	for _, p := range waProductPatterns {
		if m := p.FindStringSubmatch(text); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

func (wc *WhatsAppCollector) getProductData(productID string) (map[string]interface{}, error) {
	if wc.db == nil || productID == "" {
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
	err := scanInto(wc.db.QueryRow(exactQ, productID))

	// Fall back to a fuzzy name match only when nothing exact was found and the
	// identifier is specific enough (>=4 chars) to keep the table scan rare and
	// avoid matching unrelated products on short/common substrings.
	if err == sql.ErrNoRows && len(productID) >= 4 {
		err = scanInto(wc.db.QueryRow(fuzzyQ, "%"+productID+"%"))
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

func (wc *WhatsAppCollector) getUserInfoForNotification(
	ctx context.Context,
	platformUserID string,
) (userData map[string]interface{}, recentMessages []string, isNew bool, err error) {
	if wc.db == nil {
		return nil, nil, true, nil
	}
	const q = `SELECT id, display_name, is_blocked, last_intent FROM platform_users WHERE platform = ? AND platform_user_id = ?`
	var id, displayName, lastIntent sql.NullString
	var blocked sql.NullBool
	err = wc.db.QueryRowContext(ctx, q, "whatsapp", platformUserID).Scan(&id, &displayName, &blocked, &lastIntent)
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
	if id.Valid {
		recentMessages, _ = wc.getRecentMessages(ctx, id.String, 3)
	}
	return userData, recentMessages, false, nil
}

func (wc *WhatsAppCollector) getRecentMessages(ctx context.Context, userID string, limit int) ([]string, error) {
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
	rows, err := wc.db.QueryContext(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var texts []string
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err == nil {
			texts = append(texts, text)
		}
	}
	return texts, rows.Err()
}

func (wc *WhatsAppCollector) generateUserID(jidStr string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(jidStr))))
	return "whatsapp_" + hex.EncodeToString(h[:8])
}

func (wc *WhatsAppCollector) jidToUsername(jid waTypes.JID) string {
	return strings.ToLower(strings.ReplaceAll(jid.User, ".", "_"))
}

func extensionFromMime(mimeType string) string {
	exts, err := mime.ExtensionsByType(mimeType)
	if err != nil || len(exts) == 0 {
		return ""
	}
	return exts[0]
}

func (wc *WhatsAppCollector) getMediaDir() string {
	if wc.configMgr != nil {
		cfg := wc.configMgr.GetConfig()
		if cfg != nil && cfg.Paths.PostImages != "" {
			return cfg.Paths.PostImages
		}
	}
	return mediaDirDefault
}

func (wc *WhatsAppCollector) imageRecognitionEnabled() bool {
	if wc.configMgr == nil {
		return false
	}
	cfg := wc.configMgr.GetConfig()
	if cfg == nil {
		return false
	}
	return cfg.ImageRecognition.Enabled
}

func (wc *WhatsAppCollector) reportError(code, msg, severity string) {
	log.Printf("[WA:ERROR:%s] [%s] %s (severity=%s)", wc.accountID, code, msg, severity)
	select {
	case wc.errorChan <- &PlatformError{
		PlatformID: wc.platformID,
		SubtypeID:  wc.subtypeID,
		AccountID:  wc.accountID,
		ErrorCode:  code,
		ErrorMsg:   msg,
		Timestamp:  time.Now(),
		Severity:   severity,
	}:
	default:
		log.Printf("[WhatsApp:%s:%s] error chan full, dropped: %s — %s",
			wc.subtypeID, wc.accountID, code, msg)
	}
}

func (wc *WhatsAppCollector) GetErrorChannel() <-chan *PlatformError {
	return wc.errorChan
}

func (wc *WhatsAppCollector) Collect(ctx context.Context, _ []*CookieData) ([]*Notification, error) {
	log.Printf("[COLLECT] Starting collection for %s", wc.accountID)
	if !wc.collectRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("collect already in progress for %s", wc.accountID)
	}
	defer wc.collectRunning.Store(false)

	if wc.config != nil && !wc.config.ListenMessages {
		waDbg(wc.accountID, "ListenMessages disabled, skipping collection")
		return []*Notification{}, nil
	}

	if wc.waClient == nil {
		return nil, fmt.Errorf("whatsapp client not set for %s", wc.accountID)
	}

	if !wc.connected.Load() {
		log.Printf("[COLLECT] Client not connected, skipping (will be reconnected by session manager)")
		return []*Notification{}, nil
	}

	if !wc.checkPause(ctx) {
		return nil, ctx.Err()
	}

	wc.ProcessPendingInstructions()

	var notifications []*Notification
	for len(notifications) < maxBatchSize {
		if !wc.checkPause(ctx) {
			break
		}
		select {
		case n := <-wc.notifBuffer:
			notifications = append(notifications, n)
		case <-ctx.Done():
			goto done
		default:
			select {
			case n := <-wc.notifBuffer:
				notifications = append(notifications, n)
			case <-time.After(collectDrainWindow):
				goto done
			case <-ctx.Done():
				goto done
			}
		}
	}

done:
	// Commit the batch-safe cursor now that every notification staged during
	// this drain is sitting in `notifications`, about to be handed back to
	// the caller. The cursor only advances once the whole batch has safely
	// left the buffer — if the process dies mid-drain, the cursor stays put
	// and the un-drained messages will be re-seen on the next run instead of
	// silently skipped.
	wc.flushPendingCursor()
	wc.ProcessPendingInstructions()
	log.Printf("[COLLECT] Returning %d notifications for %s", len(notifications), wc.accountID)
	return notifications, nil
}

func (wc *WhatsAppCollector) ReceiveInstructions(inst *shared.AutomationInstruction) error {
	if inst.Platform != "whatsapp" && inst.Platform != wc.platformID {
		return fmt.Errorf("wrong platform: %s", inst.Platform)
	}
	if inst.TicketID == "" {
		return fmt.Errorf("empty ticket ID")
	}
	if inst.CreatedAt.IsZero() {
		inst.CreatedAt = time.Now()
	}
	select {
	case wc.instructionQueue <- inst:
		log.Printf("[WhatsApp:%s:%s] instruction queued: %s (action:%s steps:%d)",
			wc.subtypeID, wc.accountID, inst.TicketID, inst.Action, len(inst.Steps))
		return nil
	case <-time.After(2 * time.Second):
		wc.reportError("QUEUE_FULL", "instruction queue full", "warning")
		return fmt.Errorf("instruction queue full")
	}
}

func (wc *WhatsAppCollector) ProcessPendingInstructions() {
	wc.executionMu.Lock()
	defer wc.executionMu.Unlock()
	count := 0
	for {
		select {
		case inst := <-wc.instructionQueue:
			count++
			log.Printf("[INSTR] Processing instruction %s (action:%s)", inst.TicketID, inst.Action)
			if err := wc.executeInstruction(inst); err != nil {
				log.Printf("[WhatsApp:%s:%s] instruction %s failed: %v",
					wc.subtypeID, wc.accountID, inst.TicketID, err)
			}
		default:
			if count > 0 {
				log.Printf("[INSTR] Processed %d instructions", count)
			}
			return
		}
	}
}

func (wc *WhatsAppCollector) executeInstruction(inst *shared.AutomationInstruction) error {
	start := time.Now()
	log.Printf("[WhatsApp:%s:%s] executing %s (action:%s)",
		wc.subtypeID, wc.accountID, inst.TicketID, inst.Action)

	if err := wc.pauseCollection(); err != nil {
		log.Printf("[WhatsApp:%s:%s] pause before instruction (proceeding): %v",
			wc.subtypeID, wc.accountID, err)
	}
	defer wc.resumeCollection()

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
		log.Printf("[WhatsApp:%s:%s] step %d/%d: %s",
			wc.subtypeID, wc.accountID, i+1, len(inst.Steps), step.Type)

		maxAttempts := step.RetryCount
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		var stepErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			stepErr = wc.executeStep(ctx, step)
			if stepErr == nil {
				break
			}
			log.Printf("[WhatsApp:%s:%s] step %d/%d attempt %d/%d error: %v",
				wc.subtypeID, wc.accountID, i+1, len(inst.Steps), attempt, maxAttempts, stepErr)
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if stepErr != nil {
			lastErr = stepErr
			if len(inst.FallbackSteps) > 0 {
				log.Printf("[WhatsApp:%s:%s] step %d/%d failed after %d attempt(s), running fallback steps",
					wc.subtypeID, wc.accountID, i+1, len(inst.Steps), maxAttempts)
				lastErr = wc.runFallbackSteps(ctx, inst.FallbackSteps)
			}
			break
		}

		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}

	log.Printf("[WhatsApp:%s:%s] instruction %s done in %v (err=%v)",
		wc.subtypeID, wc.accountID, inst.TicketID, time.Since(start), lastErr)
	return lastErr
}

// runFallbackSteps runs an instruction's FallbackSteps after its main steps
// failed. Its own result replaces the main loop's error either way, so the
// caller sees whether the fallback recovered or also failed.
func (wc *WhatsAppCollector) runFallbackSteps(ctx context.Context, steps []shared.InstructionStep) error {
	var lastErr error
	for i, step := range steps {
		if err := wc.executeStep(ctx, step); err != nil {
			log.Printf("[WhatsApp:%s:%s] fallback step %d/%d error: %v",
				wc.subtypeID, wc.accountID, i+1, len(steps), err)
			lastErr = err
		}
		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	return lastErr
}

func (wc *WhatsAppCollector) executeStep(ctx context.Context, step shared.InstructionStep) error {
	if step.DelayBefore > 0 {
		time.Sleep(time.Duration(step.DelayBefore) * time.Millisecond)
	}
	switch step.Type {
	case shared.StepTypeSendMessage:
		return wc.stepSendMessage(ctx, step)
	case shared.StepTypeReply:
		return wc.stepReply(ctx, step)
	case shared.StepTypeReact:
		return wc.stepReact(ctx, step)
	case shared.StepTypeUpload:
		return wc.stepUpload(ctx, step)
	case shared.StepTypeDownload:
		return wc.stepDownload(ctx, step)
	case shared.StepTypeBlock:
		return wc.stepBlock(ctx, step, true)
	case shared.StepTypeUnfollow:
		return wc.stepBlock(ctx, step, false)
	case shared.StepTypeSearch:
		return wc.stepSearch(ctx, step)
	case shared.StepTypeAPICall:
		return wc.stepAPICall(ctx, step)
	case shared.StepTypeRateLimitCheck:
		log.Printf("[WhatsApp:%s:%s] rate-limit check: OK", wc.subtypeID, wc.accountID)
		return nil
	case shared.StepTypeWait:
		return wc.stepWait(step)
	case shared.StepTypeLog:
		log.Printf("[WhatsApp:%s:%s] [LOG] %s", wc.subtypeID, wc.accountID, step.Value)
		return nil
	case shared.StepTypeDBUpdate, shared.StepTypeDBRecord, shared.StepTypeAIGenerate:
		log.Printf("[WhatsApp:%s:%s] step %s is orchestrator-side, skipping",
			wc.subtypeID, wc.accountID, step.Type)
		return nil
	case shared.StepTypeNavigate, shared.StepTypeClick, shared.StepTypeType,
		shared.StepTypeScroll, shared.StepTypeJavaScript, shared.StepTypePress:
		log.Printf("[WhatsApp:%s:%s] step %s not applicable (browser-only), skipping",
			wc.subtypeID, wc.accountID, step.Type)
		return nil
	case shared.StepTypeSave, shared.StepTypeShare, shared.StepTypeFollow,
		shared.StepTypeLike:
		log.Printf("[WhatsApp:%s:%s] step %s not implemented for WhatsApp",
			wc.subtypeID, wc.accountID, step.Type)
		return nil
	default:
		return fmt.Errorf("unknown step type: %s", step.Type)
	}
}

func (wc *WhatsAppCollector) stepSendMessage(ctx context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepSendMessage: no message text")
	}
	jid, err := wc.resolveJID(step.Options)
	if err != nil {
		return fmt.Errorf("stepSendMessage: %w", err)
	}

	imageURL, _ := step.Options["image_url"].(string)
	if imageURL != "" {
		localPath, dlErr := wc.downloadRemoteImage(imageURL)
		if dlErr != nil {
			return fmt.Errorf("stepSendMessage: download image_url: %w", dlErr)
		}
		if err := wc.sendImage(ctx, jid, localPath, step.Value); err != nil {
			return err
		}
		wc.recordChatCooldown(jid)
		return nil
	}

	imagePath, _ := step.Options["image_path"].(string)
	if imagePath != "" {
		if err := wc.sendImage(ctx, jid, imagePath, step.Value); err != nil {
			return err
		}
		wc.recordChatCooldown(jid)
		return nil
	}

	if err := wc.sendText(ctx, jid, step.Value); err != nil {
		return err
	}
	wc.recordChatCooldown(jid)
	return nil
}

func (wc *WhatsAppCollector) stepReply(ctx context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepReply: no reply text")
	}
	jid, err := wc.resolveJID(step.Options)
	if err != nil {
		return fmt.Errorf("stepReply: %w", err)
	}

	quotedID, _ := step.Options["message_id"].(string)
	quotedSender, _ := step.Options["quoted_sender"].(string)
	quotedText, _ := step.Options["quoted_text"].(string)

	msg := &waProto.Message{
		ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text: proto.String(step.Value),
			ContextInfo: &waProto.ContextInfo{
				StanzaID:    proto.String(quotedID),
				Participant: proto.String(quotedSender),
				QuotedMessage: &waProto.Message{
					Conversation: proto.String(quotedText),
				},
			},
		},
	}
	_, err = wc.waClient.SendMessage(ctx, jid, msg)
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – message not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("stepReply to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("stepReply send: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] reply sent to %s (quoted:%s)",
		wc.subtypeID, wc.accountID, jid, quotedID)

	wc.recordChatCooldown(jid)
	return nil
}

func (wc *WhatsAppCollector) stepReact(ctx context.Context, step shared.InstructionStep) error {
	jid, err := wc.resolveJID(step.Options)
	if err != nil {
		return fmt.Errorf("stepReact: %w", err)
	}
	msgID, _ := step.Options["message_id"].(string)
	if msgID == "" {
		return fmt.Errorf("stepReact: message_id required")
	}
	emoji, _ := step.Options["emoji"].(string)
	fromMe, _ := step.Options["from_me"].(bool)

	msg := &waProto.Message{
		ReactionMessage: &waProto.ReactionMessage{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(jid.String()),
				FromMe:    proto.Bool(fromMe),
				ID:        proto.String(msgID),
			},
			Text:              proto.String(emoji),
			SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
		},
	}
	_, err = wc.waClient.SendMessage(ctx, jid, msg)
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – react not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("stepReact to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("stepReact send: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] reaction %q sent to msg %s in %s",
		wc.subtypeID, wc.accountID, emoji, msgID, jid)

	wc.recordChatCooldown(jid)
	return nil
}

func (wc *WhatsAppCollector) stepUpload(ctx context.Context, step shared.InstructionStep) error {
	jid, err := wc.resolveJID(step.Options)
	if err != nil {
		return fmt.Errorf("stepUpload: %w", err)
	}

	imageURL, _ := step.Options["image_url"].(string)
	if imageURL != "" {
		localPath, dlErr := wc.downloadRemoteImage(imageURL)
		if dlErr != nil {
			return fmt.Errorf("stepUpload: download image_url: %w", dlErr)
		}
		caption, _ := step.Options["caption"].(string)
		if caption == "" {
			caption = step.Value
		}
		if err := wc.sendImage(ctx, jid, localPath, caption); err != nil {
			return err
		}
		wc.recordChatCooldown(jid)
		return nil
	}

	filePath, _ := step.Options["file_path"].(string)
	if filePath == "" {
		filePath = step.Value
	}
	if filePath == "" {
		return fmt.Errorf("stepUpload: file_path or image_url required")
	}
	mediaTypeStr, _ := step.Options["media_type"].(string)
	if mediaTypeStr == "" {
		mediaTypeStr = "image"
	}
	caption, _ := step.Options["caption"].(string)
	if caption == "" {
		caption = step.Value
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("stepUpload read file: %w", err)
	}
	mimeType := mime.TypeByExtension(filepath.Ext(filePath))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	switch strings.ToLower(mediaTypeStr) {
	case "image":
		if err := wc.sendImage(ctx, jid, filePath, caption); err != nil {
			return err
		}
	case "video":
		if err := wc.sendVideo(ctx, jid, data, mimeType, caption); err != nil {
			return err
		}
	case "audio":
		if err := wc.sendAudio(ctx, jid, data, mimeType); err != nil {
			return err
		}
	default:
		if err := wc.sendDocument(ctx, jid, data, mimeType, filepath.Base(filePath), caption); err != nil {
			return err
		}
	}

	wc.recordChatCooldown(jid)
	return nil
}

func (wc *WhatsAppCollector) stepDownload(ctx context.Context, step shared.InstructionStep) error {
	url, _ := step.Options["url"].(string)
	if url == "" {
		url = step.Value
	}
	if url == "" {
		return fmt.Errorf("stepDownload: url required")
	}
	savePath, _ := step.Options["save_path"].(string)
	if savePath != "" {
		data, err := wc.fetchURL(url)
		if err != nil {
			return fmt.Errorf("stepDownload fetch: %w", err)
		}
		if err := os.WriteFile(savePath, data, 0o644); err != nil {
			return fmt.Errorf("stepDownload write: %w", err)
		}
		log.Printf("[WhatsApp:%s:%s] downloaded %s → %s", wc.subtypeID, wc.accountID, url, savePath)
		return nil
	}
	localPath, err := wc.downloadRemoteImage(url)
	if err != nil {
		return fmt.Errorf("stepDownload: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] downloaded %s → %s", wc.subtypeID, wc.accountID, url, localPath)
	return nil
}

func (wc *WhatsAppCollector) stepBlock(ctx context.Context, step shared.InstructionStep, block bool) error {
	jid, err := wc.resolveJID(step.Options)
	if err != nil {
		return fmt.Errorf("stepBlock: %w", err)
	}
	action := events.BlocklistChangeActionBlock
	if !block {
		action = events.BlocklistChangeActionUnblock
	}
	if _, err := wc.waClient.UpdateBlocklist(ctx, jid, action); err != nil {
		return fmt.Errorf("stepBlock UpdateBlocklist: %w", err)
	}
	verb := "blocked"
	if !block {
		verb = "unblocked"
	}
	log.Printf("[WhatsApp:%s:%s] %s %s", wc.subtypeID, wc.accountID, verb, jid)
	return nil
}

func (wc *WhatsAppCollector) stepSearch(ctx context.Context, step shared.InstructionStep) error {
	query, _ := step.Options["query"].(string)
	if query == "" {
		query = step.Value
	}
	if query == "" {
		return fmt.Errorf("stepSearch: query required")
	}
	contacts, err := wc.waClient.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return fmt.Errorf("stepSearch GetAllContacts: %w", err)
	}
	queryLower := strings.ToLower(query)
	type matchResult struct {
		JID      string `json:"jid"`
		PushName string `json:"push_name"`
		FullName string `json:"full_name"`
		Phone    string `json:"phone"`
	}
	var results []matchResult
	for jid, info := range contacts {
		name := strings.ToLower(info.PushName + " " + info.FullName)
		phone := jid.User
		if strings.Contains(name, queryLower) || strings.Contains(phone, query) {
			results = append(results, matchResult{
				JID:      jid.String(),
				PushName: info.PushName,
				FullName: info.FullName,
				Phone:    phone,
			})
		}
	}
	log.Printf("[WhatsApp:%s:%s] search %q → %d matches",
		wc.subtypeID, wc.accountID, query, len(results))

	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	if encoded, err := json.Marshal(results); err == nil {
		step.Options["result"] = string(encoded)
	}
	return nil
}

func (wc *WhatsAppCollector) stepWait(step shared.InstructionStep) error {
	ms := step.DelayAfter
	if ms <= 0 {
		ms = 2000
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return nil
}

func (wc *WhatsAppCollector) stepAPICall(ctx context.Context, step shared.InstructionStep) error {
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

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("stepAPICall: request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("stepAPICall: read response: %w", err)
	}

	log.Printf("[WhatsApp:%s:%s] stepAPICall %s %s → HTTP %d (%d bytes)",
		wc.subtypeID, wc.accountID, method, targetURL, resp.StatusCode, len(respBytes))

	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	step.Options["result"] = string(respBytes)
	step.Options["status_code"] = resp.StatusCode

	if resp.StatusCode >= 400 {
		return fmt.Errorf("stepAPICall: server returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	return nil
}

func (wc *WhatsAppCollector) sendText(ctx context.Context, jid waTypes.JID, text string) error {
	_, err := wc.waClient.SendMessage(ctx, jid, &waProto.Message{
		Conversation: proto.String(text),
	})
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – message not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("sendText to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("sendText: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] text sent to %s", wc.subtypeID, wc.accountID, jid)
	return nil
}

func (wc *WhatsAppCollector) sendImage(ctx context.Context, jid waTypes.JID, imagePath, caption string) error {
	data, err := os.ReadFile(imagePath)
	if err != nil {
		return fmt.Errorf("sendImage read: %w", err)
	}
	uploaded, err := wc.waClient.Upload(ctx, data, whatsmeow.MediaImage)
	if err != nil {
		return fmt.Errorf("sendImage upload: %w", err)
	}
	mimeType := mime.TypeByExtension(filepath.Ext(imagePath))
	if mimeType == "" {
		mimeType = "image/jpeg"
	}
	_, err = wc.waClient.SendMessage(ctx, jid, &waProto.Message{
		ImageMessage: &waProto.ImageMessage{
			Caption:       proto.String(caption),
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – image not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("sendImage to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("sendImage send: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] image sent to %s", wc.subtypeID, wc.accountID, jid)
	return nil
}

func (wc *WhatsAppCollector) sendVideo(ctx context.Context, jid waTypes.JID, data []byte, mimeType, caption string) error {
	uploaded, err := wc.waClient.Upload(ctx, data, whatsmeow.MediaVideo)
	if err != nil {
		return fmt.Errorf("sendVideo upload: %w", err)
	}
	_, err = wc.waClient.SendMessage(ctx, jid, &waProto.Message{
		VideoMessage: &waProto.VideoMessage{
			Caption:       proto.String(caption),
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – video not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("sendVideo to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("sendVideo send: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] video sent to %s", wc.subtypeID, wc.accountID, jid)
	return nil
}

func (wc *WhatsAppCollector) sendAudio(ctx context.Context, jid waTypes.JID, data []byte, mimeType string) error {
	uploaded, err := wc.waClient.Upload(ctx, data, whatsmeow.MediaAudio)
	if err != nil {
		return fmt.Errorf("sendAudio upload: %w", err)
	}
	ptt := strings.HasPrefix(mimeType, "audio/ogg")
	_, err = wc.waClient.SendMessage(ctx, jid, &waProto.Message{
		AudioMessage: &waProto.AudioMessage{
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
			PTT:           proto.Bool(ptt),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – audio not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("sendAudio to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("sendAudio send: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] audio sent to %s", wc.subtypeID, wc.accountID, jid)
	return nil
}

func (wc *WhatsAppCollector) sendDocument(ctx context.Context, jid waTypes.JID, data []byte, mimeType, fileName, caption string) error {
	uploaded, err := wc.waClient.Upload(ctx, data, whatsmeow.MediaDocument)
	if err != nil {
		return fmt.Errorf("sendDocument upload: %w", err)
	}
	_, err = wc.waClient.SendMessage(ctx, jid, &waProto.Message{
		DocumentMessage: &waProto.DocumentMessage{
			Caption:       proto.String(caption),
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
			FileName:      proto.String(fileName),
			Title:         proto.String(fileName),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "401") {
			log.Printf("[WhatsApp:%s:%s] 401 from %s – document not delivered", wc.subtypeID, wc.accountID, jid)
			return fmt.Errorf("sendDocument to %s: %w", jid, ErrUnauthorised)
		}
		return fmt.Errorf("sendDocument send: %w", err)
	}
	log.Printf("[WhatsApp:%s:%s] document sent to %s", wc.subtypeID, wc.accountID, jid)
	return nil
}

func (wc *WhatsAppCollector) resolveJID(opts map[string]interface{}) (waTypes.JID, error) {
	raw := ""
	for _, key := range []string{"to", "recipient", "group_id"} {
		if v, ok := opts[key].(string); ok && v != "" {
			raw = v
			break
		}
	}
	if raw == "" {
		return waTypes.JID{}, fmt.Errorf("no recipient specified in step options (to/recipient/group_id)")
	}
	return wc.parseJID(raw)
}

func (wc *WhatsAppCollector) parseJID(raw string) (waTypes.JID, error) {
	if strings.ContainsRune(raw, '@') {
		return waTypes.ParseJID(raw)
	}
	return waTypes.ParseJID(raw + "@s.whatsapp.net")
}

func (wc *WhatsAppCollector) fetchURL(url string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
