package nnlp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"sailstream/internal/config"
	"sailstream/internal/enviroment"
	"sailstream/internal/listener"
	"sailstream/internal/wrapper"
)

var responseTemplates = map[string]map[string]string{
	"en": {
		"simple_ack":          "You're welcome! If you need anything else, just ask.",
		"confirmation":        "Great, that's noted!",
		"rejection":           "No problem – let me know if you'd like to try something else.",
		"price_haggle_reject": "I'm sorry, our prices are fixed and we can't offer a discount at this time.",
	},
	"ar": {
		"simple_ack":          "عفواً! إذا احتجت شيئاً آخر، فقط اسأل.",
		"confirmation":        "رائع، تم تدوين ذلك.",
		"rejection":           "لا مشكلة – أعلمني إذا أردت تجربة شيء آخر.",
		"price_haggle_reject": "آسف، أسعارنا ثابتة ولا يمكننا تقديم خصم حالياً.",
	},
	"ku": {
		"simple_ack":          "شایەنی نییە! ئەگەر شتێکی ترت پێویست بوو، تەنها بپرسە.",
		"confirmation":        "زۆر باشە، ئەوە تۆمار کرا.",
		"rejection":           "کێشە نییە – ئەگەر دەتەوێ شتێکی تر تاقی بکەیتەوە، پێم بڵێ.",
		"price_haggle_reject": "ببورە، نرخەکانمان جێگیرن و ناتوانین ئێستا داشکاندن بدەین.",
	},
}

const (
	greetingCooldown     = 5 * time.Minute
	queueMaxSize         = 1000
	numWorkers           = 3
	defaultDebounceDelay = 5 * time.Second
	maxPlausibleQuantity = 999
)

type QueueItem struct {
	Notification *listener.Notification
	ProcessTime  time.Time
	Priority     int
}

type notificationQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	items   []*QueueItem
	closed  bool
	maxSize int
}

func newNotificationQueue(maxSize int) *notificationQueue {
	q := &notificationQueue{
		items:   make([]*QueueItem, 0, 64),
		maxSize: maxSize,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *notificationQueue) enqueue(item *QueueItem) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.items) >= q.maxSize {
		return false
	}
	q.items = append(q.items, item)
	q.cond.Signal()
	return true
}

func (q *notificationQueue) dequeue() *QueueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return nil
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *notificationQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *notificationQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *notificationQueue) isEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) == 0
}

type Stats struct {
	sync.RWMutex
	Processed   int64
	Blocked     int64
	RateLimited int64
	AutoHearts  int64
	ImageHits   int64
	ImageMisses int64
	Queued      int64
	Skipped     int64
	QueueSize   int64
	AITickets   int64
}

type imageCache struct {
	sync.RWMutex
	path      string
	processed map[string]string
}

type ProcessResult struct {
	TicketID string                 `json:"ticket_id"`
	Action   string                 `json:"action"`
	Intent   string                 `json:"intent"`
	Data     map[string]interface{} `json:"data"`
	Error    string                 `json:"error,omitempty"`
}

type AITaskTicket struct {
	TicketID         string                 `json:"ticket_id"`
	UserID           string                 `json:"user_id"`
	PlatformID       string                 `json:"platform_id"`
	RawNotification  json.RawMessage        `json:"raw_notification"`
	DetectedLanguage string                 `json:"detected_language"`
	SessionContext   map[string]interface{} `json:"session_context"`
	VisionData       *VisionContext         `json:"vision_data,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
	Status           string                 `json:"status"`
	Priority         int                    `json:"priority"`
}

type VisionContext struct {
	ConfidenceLevel float64  `json:"confidence_level"`
	ImageURLs       []string `json:"image_urls"`
	MatchedProducts []string `json:"matched_products,omitempty"`
}

type scoredProduct struct {
	id      string
	score   int
	product map[string]interface{}
}

const maxDebounceWindow = 20 * time.Second

type debounceEntry struct {
	mu           sync.Mutex
	timer        *time.Timer
	notif        *listener.Notification
	mergeCount   int
	firstArrival time.Time
}

type Processor struct {
	db                 *sql.DB
	config             *config.ConfigManager
	stats              *Stats
	queue              chan *ProcessResult
	cache              *imageCache
	notifQueue         *notificationQueue
	wg                 sync.WaitGroup
	resultHandlerMu    sync.RWMutex
	resultHandler      func(*ProcessResult)
	greetingCooldownMu sync.Mutex
	greetingCooldowns  map[string]time.Time
	debouncers         sync.Map
	debounceDelay      time.Duration
	cnnWrapper         *wrapper.CNNWrapper
	skipNextMu         sync.Mutex
	skipNext           map[string]bool
}

func NewProcessor(db *sql.DB, cfg *config.ConfigManager) *Processor {
	cachePath := cfg.GetCachePath()
	if cachePath == "" {
		cachePath = "./cache/images"
	}
	os.MkdirAll(cachePath, 0o755)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	rawCfg := cfg.GetConfig()
	env := enviroment.NewEnvironment(rawCfg)
	cnnWrap := wrapper.NewCNNWrapper(rawCfg, env)

	return &Processor{
		db:                db,
		config:            cfg,
		stats:             &Stats{},
		queue:             make(chan *ProcessResult, 1000),
		cache:             &imageCache{path: cachePath, processed: make(map[string]string)},
		notifQueue:        newNotificationQueue(queueMaxSize),
		greetingCooldowns: make(map[string]time.Time),
		debounceDelay:     defaultDebounceDelay,
		cnnWrapper:        cnnWrap,
		skipNext:          make(map[string]bool),
	}
}

func (p *Processor) SetResultHandler(h func(*ProcessResult)) {
	p.resultHandlerMu.Lock()
	defer p.resultHandlerMu.Unlock()
	p.resultHandler = h
}

func (p *Processor) Start(ctx context.Context) {
	log.Printf("[Processor] Starting with %d workers", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
}

func (p *Processor) Stop() {
	log.Println("[Processor] Shutting down...")
	p.notifQueue.close()
	p.wg.Wait()
	close(p.queue)
	log.Println("[Processor] Stopped")
}

func (p *Processor) ProcessNotification(ctx context.Context, notification *listener.Notification) (*ProcessResult, error) {
	p.stats.Lock()
	p.stats.Processed++
	p.stats.Unlock()

	if notification.Type == listener.NotificationTypeMessage {
		p.debounceAndMerge(notification)
		return &ProcessResult{
			TicketID: fmt.Sprintf("debounced-%s-%d", notification.ID, time.Now().UnixNano()),
			Action:   "queued",
			Intent:   "awaiting_merge",
			Data: map[string]interface{}{
				"notification_id": notification.ID,
				"platform":        notification.PlatformID,
				"user_id":         p.getUserID(notification),
				"status":          "debouncing",
			},
		}, nil
	}

	if p.shouldProcessImmediately(notification) {
		log.Printf("[Processor] Immediate (non-message): %s", notification.ID)
		return p.processWithUserLifecycle(ctx, notification)
	}

	item := &QueueItem{Notification: notification, ProcessTime: time.Now()}
	if !p.notifQueue.enqueue(item) {
		if p.shouldSkipNotification(notification) {
			p.stats.Lock()
			p.stats.Skipped++
			p.stats.Unlock()
			return nil, fmt.Errorf("notification %s skipped (filter)", notification.ID)
		}
		if err := p.insertUrgentMessage(notification, "escalation", "queue full — dropped instead of processed"); err != nil {
			log.Printf("[Processor] failed to insert urgent message for dropped notification %s: %v", notification.ID, err)
		}
		return nil, fmt.Errorf("queue full — notification %s dropped", notification.ID)
	}

	p.stats.Lock()
	p.stats.Queued++
	p.stats.QueueSize = int64(p.notifQueue.size())
	p.stats.Unlock()

	return &ProcessResult{
		TicketID: fmt.Sprintf("queued-%s-%d", notification.ID, time.Now().UnixNano()),
		Action:   "queued",
		Intent:   "queued_for_processing",
		Data: map[string]interface{}{
			"notification_id": notification.ID,
			"platform":        notification.PlatformID,
			"queue_size":      p.notifQueue.size(),
			"queued_at":       time.Now(),
		},
	}, nil
}

func (p *Processor) debounceAndMerge(n *listener.Notification) {
	key := fmt.Sprintf("%s:%s", n.PlatformID, p.getUserID(n))

	newEntry := &debounceEntry{notif: n, mergeCount: 1, firstArrival: time.Now()}
	newEntry.mu.Lock()
	newEntry.timer = time.AfterFunc(p.debounceDelay, func() {
		p.finalizeDebounced(key, newEntry)
	})
	newEntry.mu.Unlock()

	actual, loaded := p.debouncers.LoadOrStore(key, newEntry)
	if !loaded {
		return
	}

	newEntry.mu.Lock()
	newEntry.timer.Stop()
	newEntry.mu.Unlock()

	existing := actual.(*debounceEntry)
	existing.mu.Lock()
	if existing.timer != nil {
		existing.timer.Stop()
	}
	existing.notif = mergeNotifications(existing.notif, n)
	existing.mergeCount++
	if time.Since(existing.firstArrival) >= maxDebounceWindow {
		existing.mu.Unlock()
		p.finalizeDebounced(key, existing)
		return
	}
	existing.timer = time.AfterFunc(p.debounceDelay, func() {
		p.finalizeDebounced(key, existing)
	})
	existing.mu.Unlock()
}

func (p *Processor) finalizeDebounced(key string, entry *debounceEntry) {
	p.debouncers.Delete(key)
	entry.mu.Lock()
	notif := entry.notif
	entry.mu.Unlock()

	item := &QueueItem{Notification: notif, ProcessTime: time.Now()}
	if !p.notifQueue.enqueue(item) {
		log.Printf("[Processor] Debounced queue full, dropped notification %s", notif.ID)
		if err := p.insertUrgentMessage(notif, "escalation", "debounced queue full — dropped instead of processed"); err != nil {
			log.Printf("[Processor] failed to insert urgent message for dropped notification %s: %v", notif.ID, err)
		}
		return
	}
	p.stats.Lock()
	p.stats.Queued++
	p.stats.QueueSize = int64(p.notifQueue.size())
	p.stats.Unlock()
	log.Printf("[Processor] Debounced merged notification queued for %s", key)
}

func mergeNotifications(a, b *listener.Notification) *listener.Notification {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	merged := *a

	if merged.Message == nil && b.Message != nil {
		msgCopy := *b.Message
		merged.Message = &msgCopy
	} else if merged.Message != nil && b.Message != nil {
		aText := merged.Message.Text
		bText := b.Message.Text
		if aText != "" && bText != "" {
			merged.Message.Text = aText + " " + bText
		} else if bText != "" {
			merged.Message.Text = bText
		}

		mediaMap := make(map[string]listener.MediaAttachment)
		for _, m := range merged.Message.MediaAttached {
			mediaMap[m.URL] = m
		}
		for _, m := range b.Message.MediaAttached {
			mediaMap[m.URL] = m
		}
		merged.Message.MediaAttached = make([]listener.MediaAttachment, 0, len(mediaMap))
		for _, m := range mediaMap {
			merged.Message.MediaAttached = append(merged.Message.MediaAttached, m)
		}
	}

	if b.Timestamp.After(merged.Timestamp) {
		merged.Timestamp = b.Timestamp
	}
	merged.CollectedAt = time.Now()
	merged.ID = fmt.Sprintf("merged_%s_%d", merged.PlatformID, time.Now().UnixNano())

	prevCount := 2
	if merged.RawData == nil {
		merged.RawData = make(map[string]interface{})
	} else {
		if c, ok := merged.RawData["merged_count"].(int); ok && c > 0 {
			prevCount = c + 1
		}
	}
	for k, v := range b.RawData {
		merged.RawData[k] = v
	}
	merged.RawData["merged_from"] = []string{a.ID, b.ID}
	merged.RawData["merged_count"] = prevCount

	return &merged
}

func (p *Processor) DrainQueue(ctx context.Context) {
	log.Println("[Processor] Draining queue...")
	for !p.notifQueue.isEmpty() {
		item := p.notifQueue.dequeue()
		if item == nil {
			break
		}
		result, err := p.processWithUserLifecycle(ctx, item.Notification)
		if err != nil {
			log.Printf("[Processor] Drain error for %s: %v", item.Notification.ID, err)
			continue
		}
		p.dispatchResult(ctx, result)
	}
	log.Println("[Processor] Queue drained")
}

func (p *Processor) GetQueueStats() map[string]interface{} {
	p.stats.RLock()
	defer p.stats.RUnlock()
	return map[string]interface{}{
		"processed":    p.stats.Processed,
		"queued":       p.stats.Queued,
		"skipped":      p.stats.Skipped,
		"queue_size":   p.stats.QueueSize,
		"blocked":      p.stats.Blocked,
		"rate_limited": p.stats.RateLimited,
		"auto_hearts":  p.stats.AutoHearts,
		"image_hits":   p.stats.ImageHits,
		"image_misses": p.stats.ImageMisses,
		"ai_tickets":   p.stats.AITickets,
	}
}

func (p *Processor) GetQueue() <-chan *ProcessResult { return p.queue }
func (p *Processor) GetStats() *Stats                { return p.stats }
func (p *Processor) Process(ctx context.Context, n *listener.Notification) (*ProcessResult, error) {
	return p.processWithUserLifecycle(ctx, n)
}

func (p *Processor) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	log.Printf("[Worker %d] Started", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker %d] Context cancelled — stopping", id)
			return
		default:
		}
		item := p.notifQueue.dequeue()
		if item == nil {
			log.Printf("[Worker %d] Queue closed — stopping", id)
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Worker %d] PANIC for notification %s: %v", id, item.Notification.ID, r)
				}
			}()
			p.stats.Lock()
			p.stats.QueueSize = int64(p.notifQueue.size())
			p.stats.Unlock()
			result, err := p.processWithUserLifecycle(ctx, item.Notification)
			if err != nil {
				log.Printf("[Worker %d] Error processing %s: %v", id, item.Notification.ID, err)
				return
			}
			p.dispatchResult(ctx, result)
		}()
	}
}

func (p *Processor) dispatchResult(ctx context.Context, result *ProcessResult) {
	p.resultHandlerMu.RLock()
	h := p.resultHandler
	p.resultHandlerMu.RUnlock()
	if h != nil {
		h(result)
	} else {
		select {
		case p.queue <- result:
			log.Printf("[Processor] Dispatched %s (action=%s intent=%s)", result.TicketID, result.Action, result.Intent)
		case <-ctx.Done():
			log.Printf("[Processor] Context cancelled — dropped result %s", result.TicketID)
		}
	}
	p.cleanupImagesAfterDispatch(result)
}

func (p *Processor) cleanupImagesAfterDispatch(result *ProcessResult) {
	if result == nil || result.Data == nil {
		return
	}
	if skip, _ := result.Data["skip_image_delete"].(bool); skip {
		return
	}
	notification, ok := result.Data["notification"].(*listener.Notification)
	if !ok || notification == nil {
		return
	}
	if p.hasImages(notification) {
		p.deleteImagesForNotification(notification)
	}
}

func (p *Processor) shouldProcessImmediately(notification *listener.Notification) bool {
	cfg := p.config.GetConfig()
	if cfg == nil {
		return false
	}
	pc, ok := cfg.Platforms[notification.PlatformID]
	if !ok || !pc.Enabled {
		return false
	}
	switch notification.Type {
	case listener.NotificationTypeMessage:
		return pc.Automation.AnswerDM.Enabled
	case listener.NotificationTypeComment:
		return pc.Automation.AnswerComments.Enabled
	default:
		return false
	}
}

func (p *Processor) shouldSkipNotification(notification *listener.Notification) bool {
	if p.isBlockedUser(notification) {
		return true
	}
	if p.isInQuietHours() {
		return true
	}
	if p.getNotificationText(notification) == "" && !p.hasImages(notification) {
		return true
	}
	return !p.shouldAutoReply(notification)
}

func (p *Processor) processWithUserLifecycle(ctx context.Context, notification *listener.Notification) (*ProcessResult, error) {
	userID, _, userWrite, err := p.ensureUser(ctx, notification)
	if err != nil {
		log.Printf("[Processor] ensureUser failed for %s: %v", notification.ID, err)
		if !errors.Is(err, errNoPlatformUserID) {
			if urgentErr := p.insertUrgentMessage(notification, "complex_query", "ensureUser failed: "+err.Error()); urgentErr != nil {
				log.Printf("[Processor] also failed to file urgent message for %s after ensureUser failure: %v", notification.ID, urgentErr)
			}
			return p.createResult(notification, "noop", "user_resolution_failed", map[string]interface{}{
				"notification_id": notification.ID,
				"error":           err.Error(),
			}), nil
		}
	}
	if userID != "" {
		if notification.RawData == nil {
			notification.RawData = make(map[string]interface{})
		}
		if _, exists := notification.RawData["user_data"]; !exists {
			notification.RawData["user_data"] = p.loadUserData(ctx, userID)
		}
	}

	result, err := p.processNotificationInternal(ctx, notification)
	if err != nil {
		return nil, err
	}

	if userWrite != nil {
		appendDBWrite(result, userWrite)
	}

	if userID != "" && result.Action != "queued" {
		if commitErr := p.commitTicketResult(ctx, userID, notification, result); commitErr != nil {
			log.Printf("[Processor] commitTicketResult failed for %s: %v", notification.ID, commitErr)
		}
	}
	if result.Action == "ai_ticket" && userID != "" {
		p.persistAITicket(ctx, userID, notification, result)
	}

	return result, nil
}

var errNoPlatformUserID = errors.New("no platform user ID in notification")

func (p *Processor) ensureUser(ctx context.Context, notification *listener.Notification) (string, bool, map[string]interface{}, error) {
	platformUserID := p.getUserID(notification)
	if platformUserID == "" {
		return "", false, nil, fmt.Errorf("%w: %s", errNoPlatformUserID, notification.ID)
	}
	username, displayName := p.getSenderMeta(notification)
	internalID := fmt.Sprintf("%s-%s", notification.PlatformID, platformUserID)

	// RowsAffected on the upsert below is always 1 whether it inserted or
	// updated, so it can't tell us which happened — check existence first.
	var existsFlag int
	existsErr := p.db.QueryRowContext(ctx, `
		SELECT 1 FROM platform_users WHERE platform = ? AND platform_user_id = ?
	`, notification.PlatformID, platformUserID).Scan(&existsFlag)
	if existsErr != nil && existsErr != sql.ErrNoRows {
		log.Printf("[Processor] ensureUser existence check failed for %s: %v", notification.ID, existsErr)
	}
	wasNewUser := existsErr == sql.ErrNoRows

	_, err := p.db.ExecContext(ctx, `
		INSERT INTO platform_users (id, platform, platform_user_id, username, display_name)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(platform, platform_user_id) DO UPDATE SET
			last_active  = CURRENT_TIMESTAMP,
			username     = COALESCE(excluded.username, username),
			display_name = COALESCE(excluded.display_name, display_name)
	`, internalID, notification.PlatformID, platformUserID, username, displayName)
	if err != nil {
		return "", false, nil, fmt.Errorf("ensureUser: %w", err)
	}
	if wasNewUser {
		log.Printf("[Processor] New user created: platform=%s userID=%s", notification.PlatformID, platformUserID)
	}
	write := map[string]interface{}{
		"table":        "platform_users",
		"op":           map[bool]string{true: "insert", false: "update"}[wasNewUser],
		"user_id":      internalID,
		"platform":     notification.PlatformID,
		"username":     username,
		"display_name": displayName,
		"is_new_user":  wasNewUser,
	}
	return internalID, wasNewUser, write, nil
}

func (p *Processor) loadUserData(ctx context.Context, userID string) map[string]interface{} {
	var (
		lastIntent    sql.NullString
		convState     sql.NullString
		isBlocked     sql.NullInt64
		totalOrders   sql.NullInt64
		totalSpent    sql.NullFloat64
		lastProductID sql.NullString
		pendingData   sql.NullString
	)
	err := p.db.QueryRowContext(ctx, `
		SELECT last_intent, conversation_state, is_blocked, total_orders, total_spent, last_product_sku, pending_data
		FROM platform_users WHERE id = ?
	`, userID).Scan(&lastIntent, &convState, &isBlocked, &totalOrders, &totalSpent, &lastProductID, &pendingData)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("[Processor] loadUserData error for %s: %v", userID, err)
		}
		return nil
	}
	return map[string]interface{}{
		"last_intent":        lastIntent.String,
		"conversation_state": convState.String,
		"is_blocked":         isBlocked.Int64 == 1,
		"total_orders":       totalOrders.Int64,
		"total_spent":        totalSpent.Float64,
		"last_product_sku":   lastProductID.String,
		"pending_data":       pendingData.String,
	}
}

func (p *Processor) commitTicketResult(ctx context.Context, userID string, notification *listener.Notification, result *ProcessResult) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	msgID := fmt.Sprintf("msg-%s-%d", notification.ID, time.Now().UnixNano())

	text := p.getNotificationText(notification)
	messageType := "text"
	mediaURL := ""
	if p.hasImages(notification) {
		messageType = "image"
		if urls := p.collectImageURLs(notification); len(urls) > 0 {
			mediaURL = urls[0]
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO messages
			(id, platform, user_id, direction, message_text, message_type, media_url, intent, processed, received_at)
		VALUES (?, ?, ?, 'incoming', ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
	`, msgID, notification.PlatformID, userID, text, messageType, mediaURL, result.Intent); err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	lastProductSKU := ""
	if product, ok := result.Data["product"].(map[string]interface{}); ok {
		if sku, ok := product["sku"].(string); ok {
			lastProductSKU = sku
		}
	}
	setPending := false
	pendingData := ""
	if pd, ok := result.Data["pending_data"]; ok {
		if pdJSON, err := json.Marshal(pd); err == nil {
			pendingData = string(pdJSON)
			setPending = true
		}
	} else if clear, ok := result.Data["clear_pending_data"].(bool); ok && clear {
		setPending = true
	}
	newConvState := intentToConversationState(result.Intent)
	if _, err := tx.ExecContext(ctx, `
		UPDATE platform_users
		SET last_intent        = ?,
		    conversation_state = ?,
		    last_product_sku    = COALESCE(NULLIF(?, ''), last_product_sku),
		    pending_data       = CASE WHEN ? THEN ? ELSE pending_data END,
		    last_active        = CURRENT_TIMESTAMP
		WHERE id = ?
	`, result.Intent, newConvState, lastProductSKU, setPending, pendingData, userID); err != nil {
		return fmt.Errorf("update user state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	appendDBWrite(result, map[string]interface{}{
		"table":        "messages",
		"op":           "insert",
		"message_id":   msgID,
		"user_id":      userID,
		"message_type": messageType,
		"intent":       result.Intent,
	})
	appendDBWrite(result, map[string]interface{}{
		"table":              "platform_users",
		"op":                 "update",
		"user_id":            userID,
		"last_intent":        result.Intent,
		"conversation_state": newConvState,
		"last_product_sku":   lastProductSKU,
		"pending_data_set":   setPending,
	})
	return nil
}

func (p *Processor) persistAITicket(ctx context.Context, userID string, notification *listener.Notification, result *ProcessResult) {
	if ticket, ok := result.Data["ai_ticket"].(AITaskTicket); ok {
		ticketJSON, err := json.Marshal(ticket)
		if err != nil {
			log.Printf("[Processor] Failed to marshal AI ticket for %s: %v", notification.ID, err)
			return
		}
		_, err = p.db.ExecContext(ctx, `
			INSERT INTO ai_tickets (ticket_id, user_id, platform_id, ticket_data, status, created_at)
			VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
		`, ticket.TicketID, userID, notification.PlatformID, string(ticketJSON))
		if err != nil {
			log.Printf("[Processor] Failed to persist AI ticket for %s: %v", notification.ID, err)
			return
		}
		appendDBWrite(result, map[string]interface{}{
			"table":     "ai_tickets",
			"op":        "insert",
			"ticket_id": ticket.TicketID,
			"user_id":   userID,
		})
	}
}

// appendDBWrite records a lightweight "what got written" entry into the
// ticket's Data map under "db_writes", so anything downstream that inspects
// ProcessResult.Data (namely the sandbox tap) can see actual DB mutations —
// account creation, message inserts, conversation-state overwrites, AI
// ticket inserts — none of which otherwise leave a trace outside the DB
// itself. Only ever called after a write is confirmed committed.
func appendDBWrite(result *ProcessResult, entry map[string]interface{}) {
	if result == nil {
		return
	}
	if result.Data == nil {
		result.Data = make(map[string]interface{})
	}
	writes, _ := result.Data["db_writes"].([]map[string]interface{})
	writes = append(writes, entry)
	result.Data["db_writes"] = writes
}

func intentToConversationState(intent string) string {
	switch intent {
	case "order_intent", "order_intent_detected", "order_details_received",
		"order_intent_confirmed", "insufficient_stock":
		return "ordering"
	case "product_confirmation", "product_price_query", "product_availability",
		"alias_search", "image_recognition", "pack_inquiry", "multiple_products_found", "product_unknown":
		return "browsing"
	case "complex_query", "manual_answer", "escalation", "requires_ai":
		return "support"
	default:
		return "idle"
	}
}

func (p *Processor) getSenderMeta(notification *listener.Notification) (username, displayName string) {
	if notification.Message != nil {
		return notification.Message.Sender.Username, notification.Message.Sender.DisplayName
	}
	if notification.Comment != nil {
		return notification.Comment.CommentAuthor.Username, notification.Comment.CommentAuthor.DisplayName
	}
	return "", ""
}

func (p *Processor) processNotificationInternal(ctx context.Context, notification *listener.Notification) (*ProcessResult, error) {
	log.Printf("[Processor] Processing notification=%s platform=%s type=%s",
		notification.ID, notification.PlatformID, notification.Type)

	switch notification.Type {
	case listener.NotificationTypeMessage, listener.NotificationTypeComment:
	default:
		log.Printf("[Processor] Unsupported notification type %q for %s — skipping", notification.Type, notification.ID)
		return p.createResult(notification, "noop", "unsupported_type", map[string]interface{}{
			"type": string(notification.Type),
		}), nil
	}

	userID := p.getUserID(notification)

	p.skipNextMu.Lock()
	if p.skipNext[userID] {
		delete(p.skipNext, userID)
		p.skipNextMu.Unlock()
		log.Printf("[Processor] Skipping message after fallback for user %s", userID)
		return p.createResult(notification, "noop", "skipped_after_fallback", map[string]interface{}{
			"reason": "previous_message_was_fallback",
		}), nil
	}
	p.skipNextMu.Unlock()

	if p.isInQuietHours() {
		return p.createResultNoImageDelete(notification, "block", "quiet_hours", map[string]interface{}{"reason": "quiet_hours"}), nil
	}
	if p.isBlockedUser(notification) {
		p.stats.Lock()
		p.stats.Blocked++
		p.stats.Unlock()
		return p.createResultNoImageDelete(notification, "block", "blocked_user", map[string]interface{}{
			"user_id": p.getUserID(notification),
		}), nil
	}

	if notification.Type == listener.NotificationTypeComment && p.shouldAutoHeart(notification) {
		p.stats.Lock()
		p.stats.AutoHearts++
		p.stats.Unlock()
		return p.createResult(notification, "auto_heart", "auto_engagement", map[string]interface{}{
			"platform":  notification.PlatformID,
			"immediate": true,
		}), nil
	}

	if !p.shouldAutoReply(notification) {
		return p.createResult(notification, "noop", "auto_reply_disabled", nil), nil
	}

	text := p.getNotificationText(notification)
	hasImages := p.hasImages(notification)
	if text == "" && !hasImages {
		return p.createResult(notification, "noop", "empty_content", nil), nil
	}

	userData := p.getUserData(notification)
	if userData != nil {
		if result := p.handlePreviousIntent(ctx, notification, userData, text); result != nil {
			return result, nil
		}
	}

	if text != "" && p.isSimpleAcknowledgement(text) {
		return p.createSimpleAck(notification, text), nil
	}

	if text != "" {
		if p.isPureGreeting(text) {
			return p.createGreetingTicket(notification), nil
		}
		if p.isStoreInfoQuery(text) {
			return p.createStoreInfoTicket(notification), nil
		}
	}

	if text != "" {
		if p.isCancellationIntent(text) {
			return p.createResult(notification, "send_cancellation", "cancellation", map[string]interface{}{
				"language": p.detectLanguage(text),
			}), nil
		}
		if intent, matched := p.classifyEscalationIntent(text); matched {
			return p.createIntentEscalationTicket(ctx, notification, intent), nil
		}
	}

	product, productSource, needsClarification, matches := p.resolveProduct(ctx, notification, text, userData)

	if needsClarification {
		return p.createClarificationTicket(notification, matches, text), nil
	}

	if product == nil && productSource == "image_no_match" {
		return p.askForProductName(notification), nil
	}

	if product == nil && text != "" && p.isPriceHaggle(text) {
		return p.createPriceHaggleRejection(notification, text), nil
	}

	intentNeedsProduct := text != "" && (p.isPriceQuery(text) || p.isOrderIntent(text) || p.isPackIntent(text) || p.isAvailabilityQuery(text))

	if intentNeedsProduct && product == nil {
		return p.createProductRequestTicket(notification), nil
	}

	if product != nil {
		if p.isPackIntent(text) {
			return p.handlePackIntent(notification, product, text), nil
		}
		if p.isOrderIntent(text) {
			return p.createOrderIntentTicket(notification, product, text), nil
		}
		if p.isPriceQuery(text) {
			return p.createProductPriceTicket(notification, product), nil
		}
		if p.isAvailabilityQuery(text) {
			return p.createProductAvailabilityTicket(notification, product), nil
		}
		return p.createProductConfirmation(notification, []map[string]interface{}{product}, "resolved"), nil
	}

	if p.shouldUseAI(notification) {
		return p.createAITicket(ctx, notification), nil
	}
	return p.createFallbackTicket(notification), nil
}

func (p *Processor) handlePreviousIntent(ctx context.Context, notification *listener.Notification, userData map[string]interface{}, text string) *ProcessResult {
	lastIntent, _ := userData["last_intent"].(string)
	if lastIntent == "" {
		return nil
	}
	if lastIntent == "greeting" && p.isPureGreeting(text) {
		userID := p.getUserID(notification)
		p.greetingCooldownMu.Lock()
		lastGreeted, exists := p.greetingCooldowns[userID]
		now := time.Now()
		for uid, t := range p.greetingCooldowns {
			if now.Sub(t) > greetingCooldown {
				delete(p.greetingCooldowns, uid)
			}
		}
		p.greetingCooldownMu.Unlock()
		if exists && time.Since(lastGreeted) < greetingCooldown {
			log.Printf("[Processor] Suppressing duplicate greeting from user %s", userID)
			return p.createResult(notification, "noop", "duplicate_greeting", nil)
		}
	}
	if p.isSimpleAcknowledgement(text) && p.isAckAppropriateState(lastIntent) {
		return p.createSimpleAck(notification, text)
	}
	if lastIntent == "order_intent" {
		deliveryDetails := p.extractDeliveryDetails(text)
		quantity := p.extractOrderQuantity(text)
		if len(deliveryDetails) > 0 || quantity > 1 {
			productID, _ := userData["last_product_sku"].(string)
			if product := p.getProductBySKU(ctx, productID); product != nil {
				return p.createResult(notification, "ask_order_confirmation", "order_details_received", map[string]interface{}{
					"product":          product,
					"delivery_details": deliveryDetails,
					"quantity":         quantity,
					"language":         p.detectLanguage(text),
				})
			}
		}
		if p.isConfirmationResponse(text) {
			productID, _ := userData["last_product_sku"].(string)
			if product := p.getProductBySKU(ctx, productID); product != nil {
				return p.createResult(notification, "send_order_template", "order_intent_confirmed", map[string]interface{}{
					"product":  product,
					"language": p.detectLanguage(text),
				})
			}
		}
		if p.isRejectionResponse(text) {
			return p.createResult(notification, "send_cancellation", "order_rejected", map[string]interface{}{
				"language": p.detectLanguage(text),
			})
		}
	}
	if lastIntent == "multiple_products_found" {
		if matches := p.searchProductsByText(ctx, text, notification.PlatformID); len(matches) == 1 {
			product := matches[0]
			originalText := text
			if pd, ok := userData["pending_data"].(string); ok && pd != "" {
				var stash struct {
					OriginalText string `json:"original_text"`
				}
				if err := json.Unmarshal([]byte(pd), &stash); err == nil && stash.OriginalText != "" {
					originalText = stash.OriginalText
				}
			}
			switch {
			case p.isPackIntent(originalText):
				return p.handlePackIntent(notification, product, originalText)
			case p.isOrderIntent(originalText):
				return p.createOrderIntentTicket(notification, product, originalText)
			case p.isPriceQuery(originalText):
				return p.createProductPriceTicket(notification, product)
			case p.isAvailabilityQuery(originalText):
				return p.createProductAvailabilityTicket(notification, product)
			default:
				return p.createProductConfirmation(notification, []map[string]interface{}{product}, "resolved")
			}
		}
	}
	if lastIntent == "awaiting_confirmation" {
		if p.isConfirmationResponse(text) {
			productID, _ := userData["last_product_sku"].(string)
			if product := p.getProductBySKU(ctx, productID); product != nil {
				return p.createResult(notification, "ask_for_order", "product_confirmed", map[string]interface{}{
					"product":  product,
					"language": p.detectLanguage(text),
				})
			}
		}
		if p.isRejectionResponse(text) {
			lang := p.detectLanguage(text)
			return p.createResult(notification, "send_message", "product_rejected", map[string]interface{}{
				"reply_text": p.getTemplate(lang, "rejection"),
				"language":   lang,
			})
		}
	}
	return nil
}

func (p *Processor) isAckAppropriateState(lastIntent string) bool {
	switch lastIntent {
	case "greeting", "order_intent", "awaiting_confirmation",
		"store_info", "product_price_query", "product_availability", "order_completed":
		return true
	}
	return false
}

func (p *Processor) recordGreetingCooldown(userID string) {
	p.greetingCooldownMu.Lock()
	p.greetingCooldowns[userID] = time.Now()
	p.greetingCooldownMu.Unlock()
}

func (p *Processor) buildResult(notification *listener.Notification, action, intent string, data map[string]interface{}, skipImageDelete bool) *ProcessResult {
	if data == nil {
		data = make(map[string]interface{})
	}
	data["notification_id"] = notification.ID
	data["notification"] = notification
	data["platform"] = notification.PlatformID
	data["notification_type"] = notification.Type
	data["raw_text"] = p.getNotificationText(notification)
	data["user_id"] = p.getUserID(notification)
	data["skip_image_delete"] = skipImageDelete

	if orig, ok := notification.RawData["_sandbox_original_id"].(string); ok {
		data["_sandbox_original_id"] = orig
		data["notification_id"] = orig
	}

	if ud := p.getUserData(notification); ud != nil {
		data["user_data"] = ud
	}
	if cfg := p.config.GetConfig(); cfg != nil {
		data["store_info"] = map[string]interface{}{
			"name":           cfg.Store.Name,
			"address":        cfg.Store.Address,
			"contact":        cfg.Store.Contact,
			"business_hours": cfg.Store.BusinessHours,
		}
	}
	return &ProcessResult{
		TicketID: fmt.Sprintf("%s-%s-%d", action, notification.ID, time.Now().UnixNano()),
		Action:   action,
		Intent:   intent,
		Data:     data,
	}
}

func (p *Processor) createResultNoImageDelete(notification *listener.Notification, action, intent string, data map[string]interface{}) *ProcessResult {
	return p.buildResult(notification, action, intent, data, true)
}

func (p *Processor) createResult(notification *listener.Notification, action, intent string, data map[string]interface{}) *ProcessResult {
	return p.buildResult(notification, action, intent, data, false)
}

func (p *Processor) createGreetingTicket(notification *listener.Notification) *ProcessResult {
	p.recordGreetingCooldown(p.getUserID(notification))
	return p.createResult(notification, "send_greeting", "greeting", map[string]interface{}{
		"language": p.detectLanguage(p.getNotificationText(notification)),
	})
}

func (p *Processor) createStoreInfoTicket(notification *listener.Notification) *ProcessResult {
	return p.createResult(notification, "send_store_info", "store_info", nil)
}

func (p *Processor) createProductPriceTicket(notification *listener.Notification, product map[string]interface{}) *ProcessResult {
	return p.createResult(notification, "send_product", "product_price_query", map[string]interface{}{"product": product})
}

func (p *Processor) createProductAvailabilityTicket(notification *listener.Notification, product map[string]interface{}) *ProcessResult {
	return p.createResult(notification, "send_product", "product_availability", map[string]interface{}{"product": product})
}

func (p *Processor) createOrderIntentTicket(notification *listener.Notification, product map[string]interface{}, text string) *ProcessResult {
	quantity := p.extractOrderQuantity(text)
	lang := p.detectLanguage(text)
	var stock int64
	switch v := product["stock"].(type) {
	case int64:
		stock = v
	case int:
		stock = int64(v)
	case float64:
		stock = int64(v)
	}
	if stock < int64(quantity) {
		return p.createResult(notification, "send_stock_warning", "insufficient_stock", map[string]interface{}{
			"product":            product,
			"requested_quantity": quantity,
			"available_stock":    stock,
			"language":           lang,
		})
	}
	return p.createResult(notification, "send_order_template", "order_intent", map[string]interface{}{
		"product":            product,
		"suggested_quantity": quantity,
		"language":           lang,
	})
}

func (p *Processor) createProductConfirmation(notification *listener.Notification, products []map[string]interface{}, source string) *ProcessResult {
	return p.createResult(notification, "ask_product", "product_confirmation", map[string]interface{}{
		"products": products,
		"source":   source,
	})
}

func (p *Processor) askForProductName(notification *listener.Notification) *ProcessResult {
	return p.createResult(notification, "ask_product", "product_name_request", map[string]interface{}{
		"reason": "image_no_match",
	})
}

func confidentTopMatch(matches []map[string]interface{}) (map[string]interface{}, bool) {
	if len(matches) < 2 {
		return nil, false
	}
	topScore, _ := matches[0]["_search_score"].(int)
	secondScore, _ := matches[1]["_search_score"].(int)
	if topScore >= 3 && topScore >= secondScore+2 {
		return matches[0], true
	}
	return nil, false
}

func (p *Processor) resolveProduct(ctx context.Context, notification *listener.Notification, text string, userData map[string]interface{}) (map[string]interface{}, string, bool, []map[string]interface{}) {
	if hasProduct, pd := p.hasProductData(notification); hasProduct {
		return pd, "attached", false, nil
	}

	if text != "" {
		matches := p.searchProductsByText(ctx, text, notification.PlatformID)
		if len(matches) == 1 {
			return matches[0], "text_search", false, nil
		}
		if len(matches) > 1 {
			if top, ok := confidentTopMatch(matches); ok {
				return top, "text_search", false, nil
			}
			return nil, "", true, matches
		}
	}

	if p.isPlatformImageRecognitionEnabled(notification.PlatformID) && p.hasImages(notification) {
		imageMatches := p.processImages(ctx, notification)
		if len(imageMatches) == 1 {
			p.stats.Lock()
			p.stats.ImageHits++
			p.stats.Unlock()
			return imageMatches[0], "image_recognition", false, nil
		}
		if len(imageMatches) > 1 {
			p.stats.Lock()
			p.stats.ImageHits++
			p.stats.Unlock()
			return nil, "", true, imageMatches
		}
		p.stats.Lock()
		p.stats.ImageMisses++
		p.stats.Unlock()
		return nil, "image_no_match", false, nil
	}

	if lastProductID, ok := userData["last_product_sku"].(string); ok && lastProductID != "" {
		if product := p.getProductBySKU(ctx, lastProductID); product != nil {
			return product, "previous_conversation", false, nil
		}
	}

	return nil, "", false, nil
}

func (p *Processor) isPackIntent(text string) bool {
	if text == "" {
		return false
	}
	t := strings.ToLower(text)
	packWords := []string{
		"box", "boxes", "carton", "cartons", "pack", "packs", "case", "cases",
		"كرتون", "علبة", "علب", "صندوق", "صناديق",
		"کارتن", "بۆکس", "پاکەت",
	}
	for _, w := range packWords {
		if containsWholeWordPhrase(t, w) {
			return true
		}
	}
	return false
}

func (p *Processor) extractPackQuantity(text string) int {
	return p.extractOrderQuantity(text)
}

func (p *Processor) handlePackIntent(notification *listener.Notification, product map[string]interface{}, text string) *ProcessResult {
	var qtyPerPack int64
	switch v := product["quantity_per_pack"].(type) {
	case int64:
		qtyPerPack = v
	case int:
		qtyPerPack = int64(v)
	case float64:
		qtyPerPack = int64(v)
	}

	if qtyPerPack <= 1 {
		return p.createOrderIntentTicket(notification, product, text)
	}

	var pricePerPack float64
	switch v := product["price_per_pack"].(type) {
	case float64:
		pricePerPack = v
	case int64:
		pricePerPack = float64(v)
	}

	currency, _ := product["currency"].(string)
	packCount := p.extractPackQuantity(text)
	totalPrice := float64(packCount) * pricePerPack

	return p.createResultNoImageDelete(notification, "pack_price", "pack_inquiry", map[string]interface{}{
		"product":        product,
		"pack_count":     packCount,
		"items_per_pack": qtyPerPack,
		"price_per_pack": pricePerPack,
		"total_price":    totalPrice,
		"currency":       currency,
		"language":       p.detectLanguage(text),
	})
}

func (p *Processor) createClarificationTicket(notification *listener.Notification, matches []map[string]interface{}, originalText string) *ProcessResult {
	return p.createResultNoImageDelete(notification, "ask_clarify_product", "multiple_products_found", map[string]interface{}{
		"products":        matches,
		"original_text":   originalText,
		"original_images": p.collectImageURLs(notification),
		"pending_data":    map[string]interface{}{"original_text": originalText},
	})
}

func (p *Processor) createProductRequestTicket(notification *listener.Notification) *ProcessResult {
	imageEnabled := p.isPlatformImageRecognitionEnabled(notification.PlatformID)
	action := "ask_product_name"
	prompt := "Which product are you asking about? Please tell me the name."
	if imageEnabled {
		action = "ask_product_name_or_image"
		prompt = "Which product are you asking about? Please send me the name or a picture of the product."
	}
	return p.createResultNoImageDelete(notification, action, "product_unknown", map[string]interface{}{
		"prompt":          prompt,
		"image_supported": imageEnabled,
		"language":        p.detectLanguage(p.getNotificationText(notification)),
	})
}

func (p *Processor) createAITicket(ctx context.Context, notification *listener.Notification) *ProcessResult {
	cfg := p.config.GetConfig()
	if cfg == nil || cfg.AI.APIKey == "" {
		return p.createFallbackTicket(notification)
	}
	p.stats.Lock()
	p.stats.AITickets++
	p.stats.Unlock()
	text := p.getNotificationText(notification)
	lang := p.detectLanguage(text)
	userID := p.getUserID(notification)
	var visionCtx *VisionContext
	if p.hasImages(notification) {
		visionCtx = &VisionContext{
			ConfidenceLevel: cfg.ImageRecognition.ConfidenceThreshold,
			ImageURLs:       p.collectImageURLs(notification),
		}
		imageResults := p.processImages(ctx, notification)
		if len(imageResults) > 0 {
			for _, product := range imageResults {
				if pid, ok := product["id"].(string); ok {
					visionCtx.MatchedProducts = append(visionCtx.MatchedProducts, pid)
				}
			}
		}
	}

	userData := p.getUserData(notification)

	var productCtx map[string]interface{}
	if resolved, _, ambiguous, _ := p.resolveProduct(ctx, notification, text, userData); resolved != nil && !ambiguous {
		productCtx = resolved
	}

	rawNotifBytes, err := json.Marshal(notification)
	if err != nil {
		log.Printf("[Processor] Error serializing notification for AI ticket %s: %v", notification.ID, err)
		return p.createFallbackTicket(notification)
	}
	sessionCtx := make(map[string]interface{})
	if userData != nil {
		sessionCtx["last_intent"] = userData["last_intent"]
		sessionCtx["conversation_state"] = userData["conversation_state"]
		sessionCtx["total_orders"] = userData["total_orders"]
	}
	if recentMessages, ok := notification.RawData["recent_messages"]; ok {
		sessionCtx["recent_messages"] = recentMessages
	}
	sessionCtx["current_text"] = text
	sessionCtx["detected_language"] = lang
	sessionCtx["platform"] = notification.PlatformID
	sessionCtx["product_context"] = productCtx
	aiTicket := AITaskTicket{
		TicketID:         uuid.New().String(),
		UserID:           userID,
		PlatformID:       notification.PlatformID,
		RawNotification:  rawNotifBytes,
		DetectedLanguage: lang,
		SessionContext:   sessionCtx,
		VisionData:       visionCtx,
		CreatedAt:        time.Now(),
		Status:           "pending",
		Priority:         1,
	}
	log.Printf("[Processor] Generated AI ticket %s for notification %s (language=%s, has_vision=%v, has_product=%v)",
		aiTicket.TicketID, notification.ID, lang, visionCtx != nil, productCtx != nil)
	return p.createResult(notification, "ai_ticket", "requires_ai", map[string]interface{}{
		"ai_ticket":       aiTicket,
		"text":            text,
		"language":        lang,
		"platform":        notification.PlatformID,
		"product_context": productCtx,
		"fallback_text":   "I'm processing your request. One moment please.",
	})
}

func (p *Processor) collectImageURLs(notification *listener.Notification) []string {
	var urls []string
	if notification.Message != nil {
		for _, m := range notification.Message.MediaAttached {
			if m.URL != "" {
				urls = append(urls, m.URL)
			}
		}
	}
	if notification.Comment != nil {
		for _, m := range notification.Comment.MediaAttached {
			if m.URL != "" {
				urls = append(urls, m.URL)
			}
		}
	}
	return urls
}

func (p *Processor) createFallbackTicket(notification *listener.Notification) *ProcessResult {
	cfg := p.config.GetConfig()
	userID := p.getUserID(notification)

	if err := p.insertUrgentMessage(notification, "complex_query", "No matching product/rule and AI not used"); err != nil {
		log.Printf("[Processor] failed to insert urgent message for %s: %v", notification.ID, err)
	}

	if userID != "" {
		p.skipNextMu.Lock()
		p.skipNext[userID] = true
		p.skipNextMu.Unlock()
	}

	if cfg != nil {
		if pc, ok := cfg.Platforms[notification.PlatformID]; ok && len(pc.Messages.Fallback) > 0 {
			return p.createResult(notification, "send_fallback", "manual_answer", nil)
		}
	}
	return p.createResult(notification, "noop", "no_response_available", nil)
}

func (p *Processor) insertUrgentMessage(notification *listener.Notification, msgType, reason string) error {
	platformUserID := p.getUserID(notification)
	username, displayName := p.getSenderMeta(notification)

	internalID := ""
	if platformUserID != "" {
		var id string
		err := p.db.QueryRow(`
			SELECT id FROM platform_users WHERE platform = ? AND platform_user_id = ?
		`, notification.PlatformID, platformUserID).Scan(&id)
		switch {
		case err == nil:
			internalID = id
		case err == sql.ErrNoRows:
			log.Printf("[Processor] insertUrgentMessage: no platform_users row yet for platform=%s userID=%s (notification=%s); self-healing via ensureUser",
				notification.PlatformID, platformUserID, notification.ID)
			if healedID, _, _, healErr := p.ensureUser(context.Background(), notification); healErr == nil {
				internalID = healedID
			} else {
				log.Printf("[Processor] insertUrgentMessage: self-heal failed for notification %s: %v", notification.ID, healErr)
			}
		default:
			log.Printf("[Processor] insertUrgentMessage: lookup failed for platform=%s userID=%s (notification=%s): %v",
				notification.PlatformID, platformUserID, notification.ID, err)
		}
	} else {
		log.Printf("[Processor] insertUrgentMessage: notification %s has no platform user ID", notification.ID)
	}

	ticketID := uuid.New().String()
	text := p.getNotificationText(notification)
	imagePath := ""
	if p.hasImages(notification) {
		paths := p.collectImagePaths(notification)
		if len(paths) > 0 {
			imagePath = paths[0]
		}
	}

	if internalID == "" {
		identifiers := []string{}
		if platformUserID != "" {
			identifiers = append(identifiers, fmt.Sprintf("platform_user_id=%s", platformUserID))
		}
		if username != "" {
			identifiers = append(identifiers, fmt.Sprintf("username=%s", username))
		}
		if displayName != "" {
			identifiers = append(identifiers, fmt.Sprintf("display_name=%s", displayName))
		}
		identifiers = append(identifiers, fmt.Sprintf("notification_id=%s", notification.ID))
		log.Printf("[Processor] COULD NOT RESOLVE USER for urgent message, needs manual follow-up: %s | text=%q",
			strings.Join(identifiers, ", "), text)
		return fmt.Errorf("could not resolve internal user for urgent message (notification=%s, identifiers=%s)",
			notification.ID, strings.Join(identifiers, ", "))
	}

	_, err := p.db.Exec(`
		INSERT INTO urgent_messages (
			id, user_id, platform, message_type, original_text, image_path,
			cnn_results, confidence, status, priority, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, ticketID, internalID, notification.PlatformID, msgType, text, imagePath,
		nil, nil, "pending", 50)
	if err != nil {
		return fmt.Errorf("insert urgent message: %w", err)
	}

	log.Printf("[Processor] Inserted urgent message %s for user %s (reason: %s)", ticketID, internalID, reason)
	return nil
}

func (p *Processor) isSimpleAcknowledgement(text string) bool {
	t := strings.TrimSpace(strings.ToLower(text))
	words := []string{
		"ok", "okay", "thanks", "thank you", "thx", "cool", "great", "yes", "sure",
		"appreciate", "nice", "perfect", "good",
		"شكرا", "شكراً", "شكرا جزيلا", "تمام", "ممتاز", "كفو", "يعطيك العافية", "حلو", "طيب", "منيح",
		"سوپاس", "زۆر سوپاس", "دەستتان خۆش", "باشە", "زۆر باش", "شایستە", "جوان", "پەسەندم", "ڕازیم",
	}
	for _, w := range words {
		if t == w || strings.HasPrefix(t, w+" ") {
			return true
		}
	}
	return false
}

func (p *Processor) createSimpleAck(notification *listener.Notification, text string) *ProcessResult {
	lang := p.detectLanguage(text)
	return p.createResult(notification, "send_message", "simple_acknowledgement", map[string]interface{}{
		"reply_text": p.getTemplate(lang, "simple_ack"),
		"language":   lang,
	})
}

func (p *Processor) isPriceHaggle(text string) bool {
	t := strings.ToLower(text)
	for _, w := range []string{
		"cheaper", "discount", "reduce price", "lower price", "can you lower",
		"less", "too expensive", "any discount", "special price",
		"تخفيض", "خصم", "سعر أقل", "غالي", "رخص",
		"داشکاندن", "نرخ کەمتر", "هەرزانتر", "گرانە",
	} {
		if containsWholeWordPhrase(t, w) {
			return true
		}
	}
	return false
}

func (p *Processor) createPriceHaggleRejection(notification *listener.Notification, text string) *ProcessResult {
	lang := p.detectLanguage(text)
	return p.createResult(notification, "send_message", "price_haggle_rejected", map[string]interface{}{
		"reply_text": p.getTemplate(lang, "price_haggle_reject"),
		"language":   lang,
	})
}

func (p *Processor) getTemplate(lang, key string) string {
	if m, ok := responseTemplates[lang]; ok {
		if t, ok := m[key]; ok {
			return t
		}
	}
	return responseTemplates["en"][key]
}

func (p *Processor) isBlockedUser(notification *listener.Notification) bool {
	if ud := p.getUserData(notification); ud != nil {
		switch v := ud["is_blocked"].(type) {
		case bool:
			if v {
				return true
			}
		case float64:
			if v == 1 {
				return true
			}
		case int64:
			if v == 1 {
				return true
			}
		}
	}
	cfg := p.config.GetConfig()
	if cfg == nil {
		return false
	}
	if pc, ok := cfg.Platforms[notification.PlatformID]; ok {
		uid := strings.ToLower(p.getUserID(notification))
		for _, kw := range pc.Automation.Filters.BlockKeywords {
			if strings.Contains(uid, strings.ToLower(kw)) {
				return true
			}
		}
	}
	return false
}

func (p *Processor) isInQuietHours() bool {
	cfg := p.config.GetConfig()
	if cfg == nil {
		return false
	}
	qh := cfg.Scheduler.QuietHours
	if !qh.Enabled {
		return false
	}
	parse := func(s string) (int, bool) {
		parts := strings.SplitN(s, ":", 2)
		if len(parts) != 2 {
			return 0, false
		}
		h, e1 := strconv.Atoi(parts[0])
		m, e2 := strconv.Atoi(parts[1])
		if e1 != nil || e2 != nil {
			return 0, false
		}
		return h*60 + m, true
	}
	from, ok1 := parse(qh.From)
	to, ok2 := parse(qh.To)
	if !ok1 || !ok2 {
		return false
	}
	now := time.Now()
	cur := now.Hour()*60 + now.Minute()
	if from > to {
		return cur >= from || cur <= to
	}
	return cur >= from && cur <= to
}

func (p *Processor) shouldAutoHeart(notification *listener.Notification) bool {
	cfg := p.config.GetConfig()
	if cfg == nil {
		return false
	}
	pc, ok := cfg.Platforms[notification.PlatformID]
	if !ok || !pc.Automation.AutoHeart.Enabled {
		return false
	}
	t := strings.ToLower(p.getNotificationText(notification))
	for _, w := range []string{
		"thanks", "thank you", "great", "awesome", "love", "perfect", "good", "nice", "amazing", "excellent",
		"شكرا", "رائع", "ممتاز", "جميل", "احسنت", "بارك الله",
		"سوپاس", "دەستتان خۆش", "جوان", "زۆر باش", "شایستە",
	} {
		if containsWholeWordPhrase(t, w) {
			return true
		}
	}
	return false
}

func (p *Processor) shouldAutoReply(notification *listener.Notification) bool {
	cfg := p.config.GetConfig()
	if cfg == nil {
		return false
	}
	pc, ok := cfg.Platforms[notification.PlatformID]
	if !ok {
		return false
	}
	switch notification.Type {
	case listener.NotificationTypeMessage:
		return pc.Automation.AnswerDM.Enabled
	case listener.NotificationTypeComment:
		return pc.Automation.AnswerComments.Enabled
	default:
		return false
	}
}

func (p *Processor) shouldUseAI(notification *listener.Notification) bool {
	cfg := p.config.GetConfig()
	return cfg != nil && cfg.AI.APIKey != ""
}

func (p *Processor) detectLanguage(text string) string {
	if text == "" {
		return "en"
	}
	for _, r := range text {
		if r >= '\u0600' && r <= '\u06FF' {
			if strings.ContainsAny(text, "ەێۆڕ") {
				return "ku"
			}
			return "ar"
		}
	}
	return "en"
}

func (p *Processor) isPureGreeting(text string) bool {
	t := strings.TrimSpace(strings.ToLower(text))
	greetings := []string{
		"hello", "hi", "hey", "greetings", "good morning", "good afternoon", "good evening",
		"مرحبا", "السلام عليكم", "اهلا", "صباح الخير", "مساء الخير",
		"سڵاو", "چۆنی", "بەخێربێی", "بەیانیت باش", "ئێوارەت باش",
	}
	for _, g := range greetings {
		if t == g {
			return true
		}
	}
	return false
}

func wordTokens(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return !unicode.IsLetter(r) })
}

func containsWholeWordPhrase(t, kw string) bool {
	tTokens := wordTokens(t)
	kwTokens := wordTokens(kw)
	if len(kwTokens) == 0 || len(tTokens) < len(kwTokens) {
		return false
	}
	for i := 0; i+len(kwTokens) <= len(tTokens); i++ {
		match := true
		for j, kwt := range kwTokens {
			if tTokens[i+j] != kwt {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func matchesLangWords(t, lang string, words map[string][]string) bool {
	for _, w := range words[lang] {
		if containsWholeWordPhrase(t, w) {
			return true
		}
	}
	if lang != "en" {
		for _, w := range words["en"] {
			if containsWholeWordPhrase(t, w) {
				return true
			}
		}
	}
	return false
}

func (p *Processor) isStoreInfoQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	storeWords := map[string][]string{
		"en": {"address", "location", "where", "store", "shop", "open", "close", "hour", "contact"},
		"ar": {"عنوان", "موقع", "أين", "متجر", "محل", "مفتوح", "مغلق", "ساعة", "اتصال"},
		"ku": {"ناونیشان", "شوێن", "لەکوێ", "دوکان", "فرۆشگا", "کراوە", "داخراو", "کاتژمێر", "پەیوەندی"},
	}
	return matchesLangWords(t, lang, storeWords)
}

func (p *Processor) isOrderIntent(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	orderWords := map[string][]string{
		"en": {"order", "buy", "purchase", "want", "need", "get", "take", "give me", "send me", "ship", "deliver"},
		"ar": {"طلب", "اشتري", "شراء", "أريد", "أحتاج", "أعطني", "أرسل لي", "شحن", "توصيل"},
		"ku": {"داواکاری", "بکڕە", "کڕین", "دەمەوێت", "پێویستمە", "بیدەمێ", "بنێرمێ", "ناردن", "گەیاندن"},
	}
	return matchesLangWords(t, lang, orderWords)
}

func (p *Processor) isPriceQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	priceWords := map[string][]string{
		"en": {"price", "cost", "how much", "worth"},
		"ar": {"سعر", "ثمن", "كم سعر", "تكلفة"},
		"ku": {"نرخ", "بها", "چەند", "نرخی"},
	}
	return matchesLangWords(t, lang, priceWords)
}

func (p *Processor) isAvailabilityQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"available", "in stock", "have", "stock", "exist"},
		"ar": {"متوفر", "في المخزون", "يوجد", "مخزون", "موجود"},
		"ku": {"بەردەستە", "لە کۆگادا", "هەیە", "کۆگا", "موجودە"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isCancellationIntent(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"cancel my order", "cancel order", "cancel it", "cancel the order"},
		"ar": {"الغاء الطلب", "إلغاء طلبي", "الغي طلبي", "الغي الطلب"},
		"ku": {"هەڵوەشاندنەوەی داواکاری", "داواکاریەکەم هەڵبوەشێنەوە"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isComplaint(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"broken", "damaged", "defective", "not working", "bad quality", "disappointed", "terrible", "worst", "complaint"},
		"ar": {"مكسور", "تالف", "معطل", "لا يعمل", "سيء", "خايب", "شكوى"},
		"ku": {"شکاوە", "تێکچووە", "کارناکات", "خراپە", "سکاڵا"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isReturnRequest(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"return", "refund", "give back", "send back", "exchange"},
		"ar": {"استرجاع", "استرداد", "ارجاع", "استبدال"},
		"ku": {"گەڕاندنەوە", "پارە گەڕاندنەوە", "گۆڕینەوە"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isShippingQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"where is my order", "tracking", "shipment", "when will it arrive", "delivery time", "hasn't arrived"},
		"ar": {"وين طلبي", "تتبع الشحنة", "متى يوصل", "وقت التوصيل", "لم يصل"},
		"ku": {"داواکاریەکەم لەکوێیە", "شوێنکەوتنی گەیاندن", "کەی دەگات"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isPaymentIssue(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"payment failed", "charged twice", "double charged", "payment issue", "didn't go through", "transaction failed"},
		"ar": {"فشل الدفع", "خصم مرتين", "مشكلة دفع", "لم تتم العملية"},
		"ku": {"پارەدان سەرکەوتوو نەبوو", "دوو جار پارە کێشراوە", "کێشەی پارەدان"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isTechnicalIssue(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"app crash", "website error", "can't log in", "login issue", "app not working", "bug"},
		"ar": {"تحطم التطبيق", "خطأ بالموقع", "ما بقدر ادخل", "مشكلة تسجيل الدخول"},
		"ku": {"ئەپەکە دادەخات", "هەڵەی ماڵپەڕ", "ناتوانم بچمە ژوورەوە"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) isFeedback(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {"feedback", "suggestion", "you should add", "would be nice if"},
		"ar": {"اقتراح", "ملاحظة", "لازم تضيفوا"},
		"ku": {"پێشنیار", "تێبینی"},
	}
	return matchesLangWords(t, lang, words)
}

func (p *Processor) classifyEscalationIntent(text string) (string, bool) {
	switch {
	case p.isComplaint(text):
		return "complaint", true
	case p.isReturnRequest(text):
		return "return", true
	case p.isPaymentIssue(text):
		return "payment_issue", true
	case p.isShippingQuery(text):
		return "shipping_query", true
	case p.isTechnicalIssue(text):
		return "technical_issue", true
	case p.isFeedback(text):
		return "feedback", true
	default:
		return "", false
	}
}

func (p *Processor) createIntentEscalationTicket(ctx context.Context, notification *listener.Notification, intent string) *ProcessResult {
	if err := p.insertUrgentMessage(notification, "support", "classified as "+intent); err != nil {
		log.Printf("[Processor] failed to insert urgent message for %s: %v", notification.ID, err)
	}
	if p.shouldUseAI(notification) {
		result := p.createAITicket(ctx, notification)
		result.Intent = intent
		return result
	}
	userID := p.getUserID(notification)
	if userID != "" {
		p.skipNextMu.Lock()
		p.skipNext[userID] = true
		p.skipNextMu.Unlock()
	}
	if cfg := p.config.GetConfig(); cfg != nil {
		if pc, ok := cfg.Platforms[notification.PlatformID]; ok && len(pc.Messages.Fallback) > 0 {
			return p.createResult(notification, "send_fallback", intent, nil)
		}
	}
	return p.createResult(notification, "noop", intent, nil)
}

func (p *Processor) isConfirmationResponse(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	for _, w := range []string{
		"yes", "ok", "okay", "sure", "confirm", "agree", "yep", "yeah", "absolutely", "alright",
		"نعم", "أكيد", "طبعا", "حاضر", "تمام", "أوكي", "ابشر", "ماشي", "خلاص", "موافق",
		"بەڵێ", "ئەرێ", "باشە", "زۆر باش", "باشتر", "ڕازی",
	} {
		if t == w || strings.HasPrefix(t, w) {
			return true
		}
	}
	return false
}

func (p *Processor) isRejectionResponse(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	for _, w := range []string{
		"no", "cancel", "stop", "wrong", "nope", "nah", "not interested", "no thanks",
		"لا", "لأ", "لا شكرا", "مش عايز", "مش عاوزه", "مش مهتم", "بلاش", "خلاص مش عايز",
		"نەخێر", "نا", "ناخوازم", "بێزار", "پێویست نیم", "دەستم لێ نەکەوت",
	} {
		if t == w || strings.HasPrefix(t, w) {
			return true
		}
	}
	return false
}

func (p *Processor) extractKeywords(text string) []string {
	t := strings.ToLower(strings.TrimSpace(text))
	stop := map[string]bool{
		"the": true, "a": true, "an": true, "and": true, "or": true,
		"for": true, "to": true, "in": true, "on": true, "at": true,
		"price": true, "cost": true,
	}
	var kws []string
	seen := map[string]bool{}
	for _, w := range strings.Fields(t) {
		if !stop[w] && len([]rune(w)) > 2 && !seen[w] {
			kws = append(kws, w)
			seen[w] = true
		}
	}
	return kws
}

func (p *Processor) searchProductsByText(ctx context.Context, text, _ string) []map[string]interface{} {
	keywords := p.extractKeywords(text)
	if len(keywords) == 0 {
		return nil
	}
	lang := p.detectLanguage(text)
	var aliasCol, usesCol string
	switch lang {
	case "ar":
		aliasCol, usesCol = "aliases_ar", "uses_ar"
	case "ku":
		aliasCol, usesCol = "aliases_ku", "uses_ku"
	default:
		aliasCol, usesCol = "aliases_en", "uses_en"
	}
	query := fmt.Sprintf(`
		SELECT
			id, sku, name, description, category, subcategory, tags,
			price, price_per_pack, quantity_per_pack, currency,
			stock, reserved_stock, low_stock_threshold,
			image_url, thumbnail_url,
			weight_kg, dimensions,
			is_active, is_featured,
			metadata, created_at, updated_at
		FROM products
		WHERE is_active = 1
		  AND (
		        name        LIKE ? OR
		        description LIKE ? OR
		        sku         LIKE ? OR
		        aliases_en  LIKE ? OR
		        aliases_ar  LIKE ? OR
		        aliases_ku  LIKE ? OR
		        uses_en     LIKE ? OR
		        uses_ar     LIKE ? OR
		        uses_ku     LIKE ? OR
		        %s          LIKE ? OR
		        %s          LIKE ?
		      )
		LIMIT 20
	`, aliasCol, usesCol)
	scores := make(map[string]int)
	bucket := make(map[string]map[string]interface{})
	for _, kw := range keywords {
		term := "%" + kw + "%"
		rows, err := p.db.QueryContext(ctx, query,
			term, term, term,
			term, term, term,
			term, term, term,
			term, term,
		)
		if err != nil {
			log.Printf("[Processor] Product search error (kw=%q): %v", kw, err)
			continue
		}
		for rows.Next() {
			prod, err := p.scanProduct(rows)
			if err != nil {
				continue
			}
			id, _ := prod["id"].(string)
			if id == "" {
				continue
			}
			name, _ := prod["name"].(string)
			sku, _ := prod["sku"].(string)
			weight := 1
			if containsWholeWordPhrase(strings.ToLower(name), kw) || strings.Contains(strings.ToLower(sku), kw) {
				weight = 3
			}
			scores[id] += weight
			if _, exists := bucket[id]; !exists {
				bucket[id] = prod
			}
		}
		rows.Close()
	}
	if len(bucket) == 0 {
		return nil
	}
	ranked := make([]scoredProduct, 0, len(scores))
	for id, score := range scores {
		ranked = append(ranked, scoredProduct{id: id, score: score, product: bucket[id]})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].score > ranked[j].score })
	out := make([]map[string]interface{}, len(ranked))
	for i, r := range ranked {
		r.product["_search_score"] = r.score
		out[i] = r.product
	}
	return out
}

func (p *Processor) getProductBySKU(ctx context.Context, sku string) map[string]interface{} {
	if sku == "" {
		return nil
	}
	rows, err := p.db.QueryContext(ctx, `
		SELECT
			id, sku, name, description, category, subcategory, tags,
			price, price_per_pack, quantity_per_pack, currency,
			stock, reserved_stock, low_stock_threshold,
			image_url, thumbnail_url,
			weight_kg, dimensions,
			is_active, is_featured,
			metadata, created_at, updated_at
		FROM products
		WHERE sku = ? AND is_active = 1
		LIMIT 1
	`, sku)
	if err != nil {
		log.Printf("[Processor] getProductBySKU error (%s): %v", sku, err)
		return nil
	}
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	prod, err := p.scanProduct(rows)
	if err != nil {
		log.Printf("[Processor] scanProduct error (%s): %v", sku, err)
		return nil
	}
	return prod
}

func (p *Processor) getProductByInternalID(ctx context.Context, id string) map[string]interface{} {
	if id == "" {
		return nil
	}
	rows, err := p.db.QueryContext(ctx, `
		SELECT
			id, sku, name, description, category, subcategory, tags,
			price, price_per_pack, quantity_per_pack, currency,
			stock, reserved_stock, low_stock_threshold,
			image_url, thumbnail_url,
			weight_kg, dimensions,
			is_active, is_featured,
			metadata, created_at, updated_at
		FROM products
		WHERE id = ? AND is_active = 1
		LIMIT 1
	`, id)
	if err != nil {
		log.Printf("[Processor] getProductByInternalID error (%s): %v", id, err)
		return nil
	}
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	prod, err := p.scanProduct(rows)
	if err != nil {
		log.Printf("[Processor] scanProduct error (%s): %v", id, err)
		return nil
	}
	return prod
}

func (p *Processor) scanProduct(rows *sql.Rows) (map[string]interface{}, error) {
	var (
		id, sku, name, description, category, subcategory, tags sql.NullString
		price, pricePerPack, weightKg                           sql.NullFloat64
		quantityPerPack                                         sql.NullInt64
		currency                                                sql.NullString
		stock, reservedStock, lowStockThreshold                 sql.NullInt64
		imageURL, thumbnailURL, dimensions                      sql.NullString
		isActive, isFeatured                                    sql.NullBool
		metadata                                                sql.NullString
		createdAt, updatedAt                                    sql.NullTime
	)
	if err := rows.Scan(
		&id, &sku, &name, &description, &category, &subcategory, &tags,
		&price, &pricePerPack, &quantityPerPack, &currency,
		&stock, &reservedStock, &lowStockThreshold,
		&imageURL, &thumbnailURL,
		&weightKg, &dimensions,
		&isActive, &isFeatured,
		&metadata, &createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}
	parseJSON := func(s sql.NullString) interface{} {
		if !s.Valid || s.String == "" {
			return nil
		}
		var v interface{}
		_ = json.Unmarshal([]byte(s.String), &v)
		return v
	}
	prod := map[string]interface{}{
		"id":                  id.String,
		"sku":                 sku.String,
		"name":                name.String,
		"description":         description.String,
		"category":            category.String,
		"subcategory":         subcategory.String,
		"tags":                parseJSON(tags),
		"price":               price.Float64,
		"price_per_pack":      pricePerPack.Float64,
		"quantity_per_pack":   quantityPerPack.Int64,
		"currency":            currency.String,
		"stock":               stock.Int64,
		"reserved_stock":      reservedStock.Int64,
		"low_stock_threshold": lowStockThreshold.Int64,
		"image_url":           imageURL.String,
		"thumbnail_url":       thumbnailURL.String,
		"weight_kg":           weightKg.Float64,
		"dimensions":          dimensions.String,
		"is_active":           isActive.Bool,
		"is_featured":         isFeatured.Bool,
		"metadata":            parseJSON(metadata),
	}
	if createdAt.Valid {
		prod["created_at"] = createdAt.Time
	}
	if updatedAt.Valid {
		prod["updated_at"] = updatedAt.Time
	}
	return prod, nil
}

func (p *Processor) extractOrderQuantity(text string) int {
	nums := p.extractNumbers(text)
	for _, n := range nums {
		if n >= 1 && n <= maxPlausibleQuantity {
			return n
		}
	}
	return 1
}

func (p *Processor) extractDeliveryDetails(text string) map[string]string {
	t := strings.ToLower(text)
	details := make(map[string]string)
	for _, w := range []string{"address", "location", "street", "house", "عنوان", "شارع", "ناونیشان"} {
		if strings.Contains(t, w) {
			details["has_address"] = "true"
			break
		}
	}
	for _, w := range []string{"phone", "number", "mobile", "contact", "هاتف", "رقم", "مۆبایل", "ژمارە"} {
		if strings.Contains(t, w) {
			details["has_phone"] = "true"
			break
		}
	}
	return details
}

func (p *Processor) extractNumbers(text string) []int {
	re := regexp.MustCompile(`\d+`)
	var nums []int
	for _, m := range re.FindAllString(text, -1) {
		if n, err := strconv.Atoi(m); err == nil {
			nums = append(nums, n)
		}
	}
	return nums
}

func (p *Processor) hasImages(notification *listener.Notification) bool {
	if notification.Message != nil && len(notification.Message.MediaAttached) > 0 {
		return true
	}
	if notification.Comment != nil && len(notification.Comment.MediaAttached) > 0 {
		return true
	}
	if notification.RawData != nil {
		if imgs, ok := notification.RawData["images"].([]interface{}); ok && len(imgs) > 0 {
			return true
		}
	}
	return false
}

func (p *Processor) processImages(ctx context.Context, notification *listener.Notification) []map[string]interface{} {
	paths := p.collectImagePaths(notification)
	if len(paths) == 0 {
		return nil
	}
	cfg := p.config.GetConfig()
	if cfg == nil {
		return nil
	}
	modelPath := p.findLatestModel(cfg.Paths.Models)
	if modelPath == "" {
		log.Println("[Processor] No CNN model found — skipping image recognition")
		return nil
	}
	var products []map[string]interface{}
	for _, imgPath := range paths {
		if _, err := os.Stat(imgPath); os.IsNotExist(err) {
			continue
		}
		predictStart := time.Now()
		predResult, err := p.cnnWrapper.Predict(wrapper.CNNPredictParams{
			ModelPath: modelPath,
			ImagePath: imgPath,
			UseTTA:    true,
			TopK:      1,
		})
		procMs := time.Since(predictStart).Milliseconds()
		if err != nil {
			log.Printf("[Processor] CNN prediction failed (%s): %v", imgPath, err)
			continue
		}
		if predResult == nil || len(predResult.Predictions) == 0 {
			log.Printf("[Processor] No prediction for %s", imgPath)
			continue
		}
		top := predResult.Predictions[0]
		if top.Confidence < cfg.ImageRecognition.ConfidenceThreshold {
			log.Printf("[Processor] Low confidence %.2f < %.2f for %s",
				top.Confidence, cfg.ImageRecognition.ConfidenceThreshold, imgPath)
			continue
		}
		product := p.getProductByInternalID(ctx, top.PID)
		if product == nil {
			log.Printf("[Processor] Product not found for PID %s", top.PID)
			continue
		}
		p.logImageProcessing(ctx, notification, top.PID, top.Confidence, procMs)
		product["detected_confidence"] = top.Confidence
		product["detected_in_image"] = filepath.Base(imgPath)
		product["detected_image_path"] = imgPath
		if url := p.getOriginalURLFromNotification(notification, imgPath); url != "" {
			product["original_image_url"] = url
		}
		products = append(products, product)
	}
	return products
}

func (p *Processor) logImageProcessing(ctx context.Context, notification *listener.Notification, matchedProductID string, confidence float64, procMs int64) {
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO image_processing_log (id, user_id, confidence, match_product_id, processing_time_ms, created_at)
		VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, uuid.New().String(), p.getUserID(notification), confidence, matchedProductID, procMs)
	if err != nil {
		log.Printf("[Processor] failed to log image processing for %s: %v", notification.ID, err)
	}
}

func (p *Processor) collectImagePaths(notification *listener.Notification) []string {
	var paths []string
	if notification.Message != nil {
		for _, m := range notification.Message.MediaAttached {
			if m.Thumbnail != "" {
				paths = append(paths, m.Thumbnail)
			}
		}
	}
	if notification.Comment != nil {
		for _, m := range notification.Comment.MediaAttached {
			if m.Thumbnail != "" {
				paths = append(paths, m.Thumbnail)
			}
		}
	}
	return paths
}

func (p *Processor) getOriginalURLFromNotification(notification *listener.Notification, localPath string) string {
	if notification.Message != nil {
		for _, m := range notification.Message.MediaAttached {
			if m.Thumbnail == localPath {
				return m.URL
			}
		}
	}
	if notification.Comment != nil {
		for _, m := range notification.Comment.MediaAttached {
			if m.Thumbnail == localPath {
				return m.URL
			}
		}
	}
	return ""
}

func (p *Processor) findLatestModel(modelsDir string) string {
	if modelsDir == "" {
		return ""
	}
	if _, err := os.Stat(modelsDir); os.IsNotExist(err) {
		return ""
	}
	matches, err := filepath.Glob(filepath.Join(modelsDir, "*.h5"))
	if err != nil || len(matches) == 0 {
		return ""
	}
	var latest string
	var latestMod time.Time
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil || info.IsDir() {
			continue
		}
		if info.ModTime().After(latestMod) {
			latestMod = info.ModTime()
			latest = m
		}
	}
	return latest
}

func (p *Processor) isPlatformImageRecognitionEnabled(platformID string) bool {
	cfg := p.config.GetConfig()
	if cfg == nil || !cfg.ImageRecognition.Enabled {
		return false
	}
	if pc, ok := cfg.Platforms[platformID]; ok {
		if pc.Automation.AnswerDM.Enabled || pc.Automation.AnswerComments.Enabled {
			return true
		}
	}
	return false
}

func (p *Processor) hasProductData(notification *listener.Notification) (bool, map[string]interface{}) {
	if notification.RawData != nil {
		if pd, ok := notification.RawData["product_data"].(map[string]interface{}); ok {
			return true, pd
		}
	}
	return false, nil
}

func (p *Processor) deleteImagesForNotification(notification *listener.Notification) {
	paths := p.collectImagePaths(notification)
	for _, path := range paths {
		p.safeDeleteImage(path)
	}
}

func (p *Processor) safeDeleteImage(imgPath string) {
	if imgPath == "" {
		return
	}
	if err := os.Remove(imgPath); err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("[Processor] Failed to delete cached image %s: %v", imgPath, err)
	}
}

func (p *Processor) getNotificationText(notification *listener.Notification) string {
	if notification.Message != nil && notification.Message.Text != "" {
		return notification.Message.Text
	}
	if notification.Comment != nil && notification.Comment.CommentText != "" {
		return notification.Comment.CommentText
	}
	return ""
}

func (p *Processor) getUserID(notification *listener.Notification) string {
	if notification.Message != nil {
		return notification.Message.Sender.UserID
	}
	if notification.Comment != nil {
		return notification.Comment.CommentAuthor.UserID
	}
	return ""
}

func (p *Processor) getUserData(notification *listener.Notification) map[string]interface{} {
	if notification.RawData == nil {
		return nil
	}
	if ud, ok := notification.RawData["user_data"].(map[string]interface{}); ok {
		return ud
	}
	return nil
}
