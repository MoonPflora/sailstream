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

	// Check subtype-level automation first (dashboard saves per-subtype).
	// Falls back to platform-level automation if no subtype match.
	if notification.SubtypeID != "" {
		for _, sub := range pc.Subtypes {
			if sub.ID == notification.SubtypeID {
				switch notification.Type {
				case listener.NotificationTypeMessage:
					return sub.Automation.AnswerDM.Enabled
				case listener.NotificationTypeComment:
					return sub.Automation.AnswerComments.Enabled
				}
				return false
			}
		}
	}

	// Fallback to platform-level automation
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

	// preserve_prior_state means this ticket only answered a side question
	// (e.g. "how much is delivery?" asked mid-checkout) and must NOT disturb
	// whatever order-flow state the user was already in. Without this, the
	// unconditional last_intent/conversation_state update below would silently
	// bump the user out of their in-progress order (losing collected delivery
	// details, an order about to be confirmed, etc.) just because they asked
	// an unrelated question along the way.
	if preserve, _ := result.Data["preserve_prior_state"].(bool); preserve {
		if _, err := tx.ExecContext(ctx, `UPDATE platform_users SET last_active = CURRENT_TIMESTAMP WHERE id = ?`, userID); err != nil {
			return fmt.Errorf("update user last_active: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		appendDBWrite(result, map[string]interface{}{
			"table":      "messages",
			"op":         "insert",
			"message_id": msgID,
			"user_id":    userID,
		})
		appendDBWrite(result, map[string]interface{}{
			"table":   "platform_users",
			"op":      "update",
			"user_id": userID,
			"note":    "state_preserved (side question during active flow)",
		})
		return nil
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
	} else if !setPending && isProductBrowsingIntent(result.Intent) {
		// New product browsing intent — clear any stale pending_data from prior order flow
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
	// The UPDATE above uses COALESCE(NULLIF(?, ''), last_product_sku) — an
	// empty lastProductSKU leaves the existing column value untouched. The
	// trace log used to print the raw (often empty) Go variable here, which
	// made it look like last_product_sku had been cleared when it hadn't.
	skuLogValue := interface{}(lastProductSKU)
	if lastProductSKU == "" {
		skuLogValue = "(unchanged)"
	}
	appendDBWrite(result, map[string]interface{}{
		"table":              "platform_users",
		"op":                 "update",
		"user_id":            userID,
		"last_intent":        result.Intent,
		"conversation_state": newConvState,
		"last_product_sku":   skuLogValue,
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

// intentToConversationState maps an intent to a conversation state.
// Package-level function shared by both Processor and Compiler.
func intentToConversationState(intent string) string {
	switch intent {
	case "order_intent", "order_intent_detected", "order_intent_confirmed",
		"insufficient_stock", "awaiting_quantity", "order_details_received",
		"awaiting_order_details":
		// awaiting_quantity/awaiting_order_details used to be returned
		// verbatim as the conversation_state value, but the platform_users
		// table's CHECK constraint only allows
		// ('idle', 'browsing', 'ordering', 'support') — neither of those
		// strings is in that list. Every commit for a ticket with one of
		// these intents was silently failing the UPDATE (constraint
		// violation), which rolled back the ENTIRE state write for that
		// turn — last_intent, conversation_state, last_product_sku, and
		// pending_data all stayed stuck at their previous values even
		// though a reply implying a new state had already been sent. This
		// is the actual cause of last_product_sku/pending_data appearing to
		// never clear. All of these are mid-order sub-steps, so they map to
		// the same "ordering" bucket as order_intent/insufficient_stock.
		return "ordering"
	case "order_completed":
		// The order is done, not "in progress" — leaving this in the same
		// bucket as order_intent/insufficient_stock caused conversation_state
		// to stay stuck on "ordering" indefinitely after a successful
		// checkout, until some unrelated later message happened to overwrite
		// it with a different intent.
		return "idle"
	case "product_confirmation", "product_price_query", "product_availability",
		"alias_search", "image_recognition", "pack_inquiry", "multiple_products_found", "product_unknown":
		return "browsing"
	case "complex_query", "manual_answer", "escalation", "requires_ai":
		return "support"
	default:
		return "idle"
	}
}

func isProductBrowsingIntent(intent string) bool {
	switch intent {
	case "product_availability", "product_price_query", "product_confirmation",
		"alias_search", "image_recognition", "pack_inquiry", "multiple_products_found":
		return true
	}
	return false
}

// errInsufficientStock signals that stock ran out between the time the order
// was first quoted to the customer and the moment they actually confirmed it,
// so the caller can send a proper stock-warning instead of a generic
// "something went wrong" message.
var errInsufficientStock = errors.New("insufficient stock")

// createOrderInDB finalizes an order. shippingCity/shippingCost (from
// matchShippingCity, resolved earlier in the flow and carried through
// pending_data) are added on top of the product subtotal to make the DB total
// match what the customer was shown and asked to confirm.
func (p *Processor) createOrderInDB(ctx context.Context, notification *listener.Notification, userData map[string]interface{}, product map[string]interface{}, quantity int, shippingCity string, shippingCost float64) (string, error) {
	userID := p.getUserID(notification)

	var internalID string
	err := p.db.QueryRowContext(ctx, `SELECT id FROM platform_users WHERE platform = ? AND platform_user_id = ?`,
		notification.PlatformID, userID).Scan(&internalID)
	if err != nil {
		return "", fmt.Errorf("lookup internal user: %w", err)
	}

	orderID := "ORD-" + uuid.New().String()[:8]

	var price float64
	switch v := product["price"].(type) {
	case float64:
		price = v
	case int64:
		price = float64(v)
	case int:
		price = float64(v)
	}

	subtotal := price * float64(quantity)
	total := subtotal + shippingCost
	productName, _ := product["name"].(string)
	productSKU, _ := product["sku"].(string)
	productImageURL, _ := product["image_url"].(string)
	productID, _ := product["id"].(string)

	// Capture delivery details (name/phone/address) supplied during ordering
	// so the created order carries them in shipping_address. The entire raw
	// paragraph the user sent is kept verbatim as the shipping address, and
	// also stored as customer_notes.
	shippingAddr := ""
	customerNotes := ""
	if pd, ok := userData["pending_data"].(string); ok && pd != "" {
		var pdMap map[string]interface{}
		if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
			if raw, ok := pdMap["raw_text"].(string); ok && raw != "" {
				shippingAddr = raw
				customerNotes = raw
			} else if dd, ok := pdMap["delivery_details"].(map[string]interface{}); ok {
				var parts []string
				for k, v := range dd {
					if k == "has_name" || k == "has_phone" || k == "has_address" {
						continue
					}
					parts = append(parts, fmt.Sprintf("%s=%v", k, v))
				}
				if len(parts) > 0 {
					shippingAddr = strings.Join(parts, ", ")
					customerNotes = shippingAddr
				}
			}
		}
	}

	// Populate the order columns that would otherwise be left NULL:
	// tracking_number, customer_notes, internal_notes, platform_conversation_id.
	trackingNumber := "TRK-" + strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", ""))[:12]
	internalNotes := "Created via automated chat flow"
	if shippingCity != "" {
		internalNotes = fmt.Sprintf("Created via automated chat flow. Shipping: %s (%.2f).", shippingCity, shippingCost)
	}
	platformConvID := ""
	if notification.Message != nil {
		platformConvID = notification.Message.ConversationID
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin order tx: %w", err)
	}
	defer tx.Rollback()

	// Re-check stock inside the transaction: the product data the customer was
	// quoted may be minutes or hours old by the time they actually confirm, and
	// other orders may have consumed the stock in the meantime. Without this,
	// an order could be "created" for something that's no longer available,
	// with nothing in the flow ever telling the customer or reserving stock.
	if productID != "" {
		var currentStock, reserved int64
		if err := tx.QueryRowContext(ctx, `SELECT stock, reserved_stock FROM products WHERE id = ?`, productID).Scan(&currentStock, &reserved); err != nil {
			return "", fmt.Errorf("recheck stock: %w", err)
		}
		if currentStock-reserved < int64(quantity) {
			return "", errInsufficientStock
		}
		if _, err := tx.ExecContext(ctx, `UPDATE products SET reserved_stock = reserved_stock + ? WHERE id = ?`, quantity, productID); err != nil {
			return "", fmt.Errorf("reserve stock: %w", err)
		}
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO orders (id, user_id, platform, status, total, subtotal, shipping_address, tracking_number, customer_notes, internal_notes, platform_conversation_id, created_at, updated_at)
		VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, orderID, internalID, notification.PlatformID, total, subtotal, shippingAddr, trackingNumber, customerNotes, internalNotes, platformConvID)
	if err != nil {
		return "", fmt.Errorf("insert order: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price, product_name, product_sku, product_image_url)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, orderID, productID, quantity, price, subtotal, productName, productSKU, productImageURL)
	if err != nil {
		return "", fmt.Errorf("insert order item: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE platform_users SET total_orders = total_orders + 1, total_spent = total_spent + ?, last_active = CURRENT_TIMESTAMP WHERE id = ?
	`, total, internalID)
	if err != nil {
		return "", fmt.Errorf("update user stats: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit order tx: %w", err)
	}

	log.Printf("[Processor] Order %s created for user %s (product=%s, qty=%d, subtotal=%.2f, shipping=%.2f, total=%.2f)", orderID, internalID, productSKU, quantity, subtotal, shippingCost, total)
	return orderID, nil
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

	if text != "" {
		if p.isPureGreeting(text) {
			return p.createGreetingTicket(notification), nil
		}
		if p.isStoreInfoQuery(text) {
			return p.createStoreInfoTicket(notification), nil
		}
		// Standalone "how much is delivery?" question — answered directly from
		// the shipping table, independent of any product/order in progress.
		if p.isDeliveryCostQuery(text) {
			return p.createDeliveryCostTicket(ctx, notification, text), nil
		}
	}

	if text != "" {
		if p.isCancellationIntent(text) {
			return p.createResult(notification, "send_cancellation", "cancellation", map[string]interface{}{
				"language": p.detectLanguage(text),
			}), nil
		}
		if p.isOrderStatusQuery(text) {
			return p.createOrderStatusTicket(ctx, notification), nil
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

	intentNeedsProduct := text != "" && (p.isPriceQuery(text) || p.isOrderIntent(text) || p.isPackIntent(text) || p.isAvailabilityQuery(text) || p.isCompatibilityQuery(text))

	// Fallback: if product not found but user has last_product_sku, try that before asking "what product?"
	if intentNeedsProduct && product == nil && userData != nil {
		if lastSKU, ok := userData["last_product_sku"].(string); ok && lastSKU != "" {
			if lastProduct := p.getProductBySKU(ctx, lastSKU); lastProduct != nil {
				product = lastProduct
				productSource = "previous_conversation"
			}
		}
	}

	if intentNeedsProduct && product == nil {
		return p.createProductRequestTicket(notification), nil
	}

	if product != nil {
		if p.isCompatibilityQuery(text) {
			return p.createProductCompatibilityTicket(ctx, notification, product, text), nil
		}
		if p.isPackIntent(text) {
			return p.handlePackIntent(ctx, notification, product, text), nil
		}
		if p.isOrderIntent(text) {
			if productSource == "text_search" {
				// The product itself was just matched from this same message's
				// free text (as opposed to continuing a conversation about a
				// product already discussed) — confirm it's the right one
				// before starting an order, rather than trusting a fuzzy text
				// match to silently become a purchase.
				return p.createOrderProductConfirmation(notification, product, text), nil
			}
			return p.createOrderIntentTicket(ctx, notification, product, text), nil
		}
		if p.isPriceQuery(text) {
			return p.createProductPriceTicket(notification, product), nil
		}
		if p.isAvailabilityQuery(text) {
			return p.createProductAvailabilityTicket(notification, product), nil
		}
		return p.createProductConfirmation(notification, []map[string]interface{}{product}, "resolved", text), nil
	}

	// Simple acknowledgement check — placed after product/resolution logic
	// so that phrases like "yes, i want 2 please" are caught as order intent
	// rather than short-circuiting to a generic "You're welcome!" reply.
	if text != "" && p.isSimpleAcknowledgement(text) {
		return p.createSimpleAck(notification, text), nil
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
	if lastIntent == "product_availability" || lastIntent == "product_price_query" || lastIntent == "product_confirmation" {
		// A brand-new product query ("do you have turbo wick?") must NOT reuse the
		// previous product's sku. Re-enter the fresh pipeline instead of hijacking
		// the last shown product into an order.
		if p.isAvailabilityQuery(text) {
			return nil
		}
		// User had a product result shown to them — "yes", "i want it", "give me 2"
		// means they want to proceed to order, not just acknowledge.
		if p.isRejectionResponse(text) {
			// "product_confirmation" specifically means we just asked "is
			// this the product you meant?" — a rejection here means the
			// search matched the wrong item, so try the next-ranked match
			// instead of just apologizing. product_availability/
			// product_price_query rejections are different ("no thanks, not
			// buying it") and keep the plain reply.
			if lastIntent == "product_confirmation" {
				return p.browseNextProductMatch(ctx, notification, userData, text)
			}
			lang := p.detectLanguage(text)
			return p.createResult(notification, "send_message", "product_rejected", map[string]interface{}{
				"reply_text": p.getTemplate(lang, "rejection"),
				"language":   lang,
			})
		}
		productID, _ := userData["last_product_sku"].(string)
		product := p.getProductBySKU(ctx, productID)
		if product == nil {
			return nil
		}
		if p.isCompatibilityQuery(text) {
			return p.createProductCompatibilityTicket(ctx, notification, product, text)
		}
		if p.isOrderStatusQuery(text) {
			return p.createOrderStatusTicket(ctx, notification)
		}
		if p.isDeliveryCostQuery(text) {
			return p.createDeliveryCostTicket(ctx, notification, text)
		}
		// If this product_confirmation was raised specifically to confirm an
		// order that was expressed in one compound message ("hello, I want 10
		// wicks, address is...") — see createOrderProductConfirmation — a
		// "yes" here means "yes that's my product", not a fresh order phrase,
		// and any quantity was already captured separately. Route through
		// confirmOrderProductConfirmation instead of createOrderIntentTicket,
		// which would otherwise re-run extractExplicitQuantity against "yes"
		// and lose the 10, and would re-run salvage against a message that
		// was never sent as this reply.
		if mode, pendingQty, hasPendingQty := p.readOrderConfirmPending(userData); mode == "order_confirm_product" {
			if p.isRejectionResponse(text) {
				return p.createResult(notification, "send_message", "product_rejected", map[string]interface{}{
					"reply_text": p.getTemplate(p.detectLanguage(text), "rejection"),
					"language":   p.detectLanguage(text),
				})
			}
			if p.isConfirmationResponse(text) || p.isSimpleAcknowledgement(text) || p.isOrderIntent(text) {
				return p.confirmOrderProductConfirmation(ctx, notification, product, pendingQty, hasPendingQty, text)
			}
		}
		// Any of: an explicit order phrase, a plain affirmative ("yes"/"ok"), or a
		// simple acknowledgement all mean "proceed towards ordering this product".
		// createOrderIntentTicket decides whether a quantity was actually stated;
		// if it wasn't (e.g. the reply was just "yes"), it asks instead of
		// silently assuming 1 — that silent assumption was the original bug.
		//
		// A bare quantity reply ("2", "two please") also counts — it has no
		// order word in it at all, so it's caught separately via
		// extractExplicitQuantity rather than isOrderIntent.
		if _, hasQty := p.extractExplicitQuantity(text); p.isOrderIntent(text) || p.isConfirmationResponse(text) || p.isSimpleAcknowledgement(text) || hasQty {
			return p.createOrderIntentTicket(ctx, notification, product, text)
		}
	}

	if lastIntent == "order_intent" {
		lang := p.detectLanguage(text)
		// Let the user bail out ("cancel", "no") at any point instead of forcing
		// them through the rest of the flow.
		if r := p.handleOrderFlowCancellation(notification, text, lang); r != nil {
			return r
		}
		// A shipping-cost question mid-checkout gets answered without losing the
		// customer's place in the order (pending_data/last_intent untouched).
		if r := p.handleOrderFlowSideQuery(ctx, notification, text, lang); r != nil {
			return r
		}

		// Preserve quantity already captured during order intent (e.g. "i want two").
		// IMPORTANT: do NOT re-extract quantity from delivery text — a phone number
		// contains digits that would corrupt the order quantity.
		quantity := 1
		var existingDelivery map[string]string
		if pd, ok := userData["pending_data"].(string); ok && pd != "" {
			var pdMap map[string]interface{}
			if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
				if q, ok := pdMap["quantity"].(float64); ok && q > 0 {
					quantity = int(q)
				}
				if prevDD, ok := pdMap["delivery_details"].(map[string]interface{}); ok {
					existingDelivery = make(map[string]string)
					for k, v := range prevDD {
						if vs, ok := v.(string); ok && vs != "" {
							existingDelivery[k] = vs
						}
					}
				}
			}
		}

		productID, _ := userData["last_product_sku"].(string)
		product := p.getProductBySKU(ctx, productID)

		// This is the state the system explicitly asked for delivery details in
		// (the order template was just sent) — so, and only here, a reply is
		// checked for a valid phone number. If found, the completeness gate
		// passes and the *entire raw text* becomes shipping_address (below);
		// there's no separate name/address parsing.
		deliveryDetails := p.mergeAllDeliveryFields(text, existingDelivery)

		if r := p.tryFinalizeDelivery(ctx, notification, product, quantity, deliveryDetails, text, lang); r != nil {
			return r
		}
		// No valid phone yet — ask for delivery details (phone + address).
		// Partial fields already found are stashed so the next reply can be merged.
		if product != nil {
			pendingData := map[string]interface{}{
				"quantity": float64(quantity),
			}
			if len(deliveryDetails) > 0 {
				pendingData["delivery_details"] = deliveryDetails
			}
			return p.createResult(notification, "ask_delivery_details", "awaiting_order_details", map[string]interface{}{
				"product":          product,
				"quantity":         quantity,
				"pending_data":     pendingData,
				"delivery_details": deliveryDetails,
				"language":         lang,
			})
		}
	}
	if lastIntent == "awaiting_quantity" {
		lang := p.detectLanguage(text)
		if r := p.handleOrderFlowCancellation(notification, text, lang); r != nil {
			return r
		}
		if r := p.handleOrderFlowSideQuery(ctx, notification, text, lang); r != nil {
			return r
		}

		mode := "order"
		var existingDelivery map[string]string
		if pd, ok := userData["pending_data"].(string); ok && pd != "" {
			var pdMap map[string]interface{}
			if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
				if m, ok := pdMap["mode"].(string); ok && m != "" {
					mode = m
				}
				if prevDD, ok := pdMap["delivery_details"].(map[string]interface{}); ok {
					existingDelivery = make(map[string]string)
					for k, v := range prevDD {
						if vs, ok := v.(string); ok && vs != "" {
							existingDelivery[k] = vs
						}
					}
				}
			}
		}

		productID, _ := userData["last_product_sku"].(string)
		product := p.getProductBySKU(ctx, productID)
		if product == nil {
			return p.createResult(notification, "ask_product_name", "product_unknown", map[string]interface{}{
				"language": lang,
			})
		}

		quantity, explicit := p.extractExplicitQuantity(text)
		if !explicit {
			// Still no usable number — re-ask rather than guessing, keeping
			// whatever delivery details were already volunteered.
			pendingData := map[string]interface{}{"mode": mode}
			if len(existingDelivery) > 0 {
				pendingData["delivery_details"] = existingDelivery
			}
			return p.createResult(notification, "ask_quantity", "awaiting_quantity", map[string]interface{}{
				"product":      product,
				"pending_data": pendingData,
				"language":     lang,
				"retry":        true,
			})
		}

		if mode == "pack" {
			return p.buildPackResult(notification, product, quantity, lang)
		}

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

		deliveryDetails := p.mergeAllDeliveryFields(text, existingDelivery)
		if r := p.tryFinalizeDelivery(ctx, notification, product, quantity, deliveryDetails, text, lang); r != nil {
			return r
		}
		pendingData := map[string]interface{}{"quantity": float64(quantity)}
		if len(deliveryDetails) > 0 {
			pendingData["delivery_details"] = deliveryDetails
		}
		return p.createResult(notification, "send_order_template", "order_intent", map[string]interface{}{
			"product":            product,
			"suggested_quantity": quantity,
			"pending_data":       pendingData,
			"language":           lang,
			"shipping_options":   p.getShippingOptions(ctx),
		})
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
				return p.handlePackIntent(ctx, notification, product, originalText)
			case p.isOrderIntent(originalText):
				return p.createOrderIntentTicket(ctx, notification, product, originalText)
			case p.isPriceQuery(originalText):
				return p.createProductPriceTicket(notification, product)
			case p.isAvailabilityQuery(originalText):
				return p.createProductAvailabilityTicket(notification, product)
			default:
				return p.createProductConfirmation(notification, []map[string]interface{}{product}, "resolved", originalText)
			}
		}
	}
	if lastIntent == "awaiting_order_details" {
		lang := p.detectLanguage(text)
		// This state previously had NO cancellation check at all — a user typing
		// "actually cancel this" while providing delivery details would have that
		// message run through address-salvaging (finding nothing usable) and just
		// get re-asked for the same missing fields, with their cancellation
		// silently ignored. Fixed by checking first, like every other order state.
		if r := p.handleOrderFlowCancellation(notification, text, lang); r != nil {
			return r
		}
		if r := p.handleOrderFlowSideQuery(ctx, notification, text, lang); r != nil {
			return r
		}

		// User sent delivery details — accumulate partial info from pending_data,
		// then only proceed to final confirmation once name + phone + address are complete.
		quantity := 1
		loadedPendingQuantity := false
		var existing map[string]string

		// Load any previously-stashed partial delivery details
		if pd, ok := userData["pending_data"].(string); ok && pd != "" {
			var pdMap map[string]interface{}
			if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
				if q, ok := pdMap["quantity"].(float64); ok && q > 0 {
					quantity = int(q)
					loadedPendingQuantity = true
				}
				if prevDD, ok := pdMap["delivery_details"].(map[string]interface{}); ok {
					existing = make(map[string]string)
					for k, v := range prevDD {
						if vs, ok := v.(string); ok && vs != "" {
							existing[k] = vs
						}
					}
				}
			}
		}
		// Also the explicit "waiting for delivery details" state (a follow-up
		// after the order template) — check this reply for a phone number and
		// merge with whatever was already found across earlier partial replies.
		deliveryDetails := p.mergeAllDeliveryFields(text, existing)

		// Accumulate the full raw paragraph the user sent across partial
		// messages ("Ahmad", then "07343434", then "Baghdad") so the final
		// shipping_address / customer_notes capture everything, not just the
		// last message.
		rawText := text
		if pd, ok := userData["pending_data"].(string); ok && pd != "" {
			var pdMap map[string]interface{}
			if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
				if prevRaw, ok := pdMap["raw_text"].(string); ok && prevRaw != "" {
					rawText = prevRaw + " " + text
				}
			}
		}

		// IMPORTANT: do not re-extract quantity from delivery text. A phone
		// number or house number would corrupt the order quantity. Only update
		// quantity if no phone was detected in this reply AND the user clearly
		// stated a new quantity (e.g. "actually make it 5").
		if !loadedPendingQuantity {
			hasDeliveryInfo := deliveryDetails["phone"] != ""
			if !hasDeliveryInfo {
				if extractedQuantity, explicit := p.extractExplicitQuantity(text); explicit {
					quantity = extractedQuantity
				}
			}
		}

		productID, _ := userData["last_product_sku"].(string)
		product := p.getProductBySKU(ctx, productID)
		if product == nil {
			return p.createResult(notification, "ask_product_name", "product_unknown", map[string]interface{}{
				"language": lang,
			})
		}

		if r := p.tryFinalizeDelivery(ctx, notification, product, quantity, deliveryDetails, rawText, lang); r != nil {
			return r
		}

		// Missing one or more fields — re-ask for only what's missing.
		missing := map[string]bool{}
		if deliveryDetails["phone"] == "" {
			missing["phone"] = true
		}
		if deliveryDetails["name"] == "" {
			missing["name"] = true
		}
		if deliveryDetails["address"] == "" {
			missing["address"] = true
		}
		reason := p.deliveryDetailsPrompt(deliveryDetails, lang, missing)
		pendingData := map[string]interface{}{
			"quantity":         float64(quantity),
			"delivery_details": deliveryDetails,
			"raw_text":         rawText,
		}
		return p.createResult(notification, "ask_delivery_details", "awaiting_order_details", map[string]interface{}{
			"product":          product,
			"quantity":         quantity,
			"pending_data":     pendingData,
			"delivery_details": deliveryDetails,
			"prompt":           reason,
			"language":         lang,
		})
	}
	if lastIntent == "awaiting_confirmation" {
		lang := p.detectLanguage(text)

		// A shipping-cost question asked right at the confirmation step gets
		// answered without discarding the pending order.
		if r := p.handleOrderFlowSideQuery(ctx, notification, text, lang); r != nil {
			return r
		}

		if p.isChangeDetailsRequest(text) {
			// User wants to change the shipping address before confirming —
			// send them back to delivery-details collection with a clean slate
			// (old partials are cleared so the new address replaces them).
			productID, _ := userData["last_product_sku"].(string)
			product := p.getProductBySKU(ctx, productID)
			if product != nil {
				quantity := 1
				if pd, ok := userData["pending_data"].(string); ok && pd != "" {
					var pdMap map[string]interface{}
					if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
						if q, ok := pdMap["quantity"].(float64); ok && q > 0 {
							quantity = int(q)
						}
					}
				}
				return p.createResult(notification, "ask_delivery_details", "awaiting_order_details", map[string]interface{}{
					"product":            product,
					"quantity":           quantity,
					"clear_pending_data": true,
					"language":           lang,
				})
			}
		}
		if p.isConfirmationResponse(text) {
			productID, _ := userData["last_product_sku"].(string)
			quantity := 1
			var shippingCity string
			var shippingCost float64
			if pd, ok := userData["pending_data"].(string); ok && pd != "" {
				var pdMap map[string]interface{}
				if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
					if q, ok := pdMap["quantity"].(float64); ok {
						quantity = int(q)
					}
					if sc, ok := pdMap["shipping_city"].(string); ok {
						shippingCity = sc
					}
					if sc, ok := pdMap["shipping_cost"].(float64); ok {
						shippingCost = sc
					}
				}
			}
			if product := p.getProductBySKU(ctx, productID); product != nil {
				// Create order in DB — total includes the shipping cost that was
				// already shown to the customer at confirmation time, so the
				// charged amount and the quoted amount always match.
				orderID, err := p.createOrderInDB(ctx, notification, userData, product, quantity, shippingCity, shippingCost)
				if err != nil {
					if errors.Is(err, errInsufficientStock) {
						// Stock ran out between the quote and the confirmation —
						// tell the customer plainly instead of a generic failure.
						var stock int64
						switch v := product["stock"].(type) {
						case int64:
							stock = v
						case int:
							stock = int64(v)
						case float64:
							stock = int64(v)
						}
						return p.createResult(notification, "send_stock_warning", "insufficient_stock", map[string]interface{}{
							"product":            product,
							"requested_quantity": quantity,
							"available_stock":    stock,
							"clear_pending_data": true,
							"language":           lang,
						})
					}
					log.Printf("[Processor] Failed to create order in DB: %v", err)
					return p.createResult(notification, "send_message", "order_failed", map[string]interface{}{
						"reply_text": "Sorry, there was an issue creating your order. Please try again.",
						"language":   lang,
					})
				}
				data := map[string]interface{}{
					"order_id":           orderID,
					"product":            product,
					"quantity":           quantity,
					"clear_pending_data": true,
					"language":           lang,
				}
				if shippingCity != "" {
					data["shipping_city"] = shippingCity
					data["shipping_cost"] = shippingCost
				}
				return p.createResult(notification, "order_created", "order_completed", data)
			}
		}
		if p.isCancellationIntent(text) || p.isRejectionResponse(text) {
			// Cancel the most recent order in DB (any status — we need to
			// see 'shipped'/'delivered' too, specifically to refuse those)
			// and release any stock that was reserved for it.
			userID := p.getUserID(notification)
			if userID != "" {
				var internalID string
				err := p.db.QueryRowContext(ctx, `SELECT id FROM platform_users WHERE platform = ? AND platform_user_id = ?`,
					notification.PlatformID, userID).Scan(&internalID)
				if err == nil && internalID != "" {
					var orderID, status string
					var total float64
					err = p.db.QueryRowContext(ctx,
						`SELECT id, status, total FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 1`,
						internalID).Scan(&orderID, &status, &total)
					if err == nil && orderID != "" {
						// Rule: an order that has already shipped is
						// uncancelable — leave it untouched and tell the
						// customer honestly instead of confirming a
						// cancellation that didn't happen.
						if status == "shipped" || status == "delivered" {
							return p.createResult(notification, "send_message", "order_uncancelable", map[string]interface{}{
								"reply_text":         "This order has already shipped and can no longer be cancelled. Please contact us about a return instead.",
								"order_id":           orderID,
								"clear_pending_data": true,
								"language":           lang,
							})
						}
						if status == "pending" || status == "confirmed" || status == "processing" {
							var productID string
							var qty int
							_ = p.db.QueryRowContext(ctx, `SELECT product_id, quantity FROM order_items WHERE order_id = ? LIMIT 1`, orderID).Scan(&productID, &qty)
							_, _ = p.db.ExecContext(ctx, `UPDATE orders SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = ?`, orderID)
							if productID != "" {
								_, _ = p.db.ExecContext(ctx, `UPDATE products SET reserved_stock = MAX(0, reserved_stock - ?) WHERE id = ?`, qty, productID)
							}
							_, _ = p.db.ExecContext(ctx, `UPDATE platform_users SET total_orders = MAX(0, total_orders - 1), total_spent = MAX(0, total_spent - ?), last_active = CURRENT_TIMESTAMP WHERE id = ?`, total, internalID)
							log.Printf("[Processor] Order %s cancelled for user %s", orderID, internalID)
							return p.createResult(notification, "send_message", "order_cancelled", map[string]interface{}{
								"reply_text":         "Your order has been cancelled.",
								"order_id":           orderID,
								"clear_pending_data": true,
								"language":           lang,
							})
						}
						// already cancelled/refunded — fall through to the
						// generic reply below rather than re-cancelling.
					}
				}
			}
			return p.createResult(notification, "send_message", "product_rejected", map[string]interface{}{
				"reply_text":         p.getTemplate(lang, "rejection"),
				"clear_pending_data": true,
				"language":           lang,
			})
		}

		// Nothing recognized (change/confirm/cancel/side question) — re-show
		// the same confirmation instead of silently falling through to the
		// generic message pipeline. Falling through used to overwrite
		// last_intent/conversation_state with whatever the fresh classifier
		// guessed, which could drop the customer out of a fully-collected,
		// one-reply-from-done order with no warning.
		if r := p.rebuildConfirmationPrompt(ctx, notification, userData, lang); r != nil {
			return r
		}
	}
	return nil
}

// rebuildConfirmationPrompt re-issues the same order-confirmation summary
// when a reply during awaiting_confirmation didn't match CONFIRM/CHANGE/
// CANCEL or a recognized side question. It reconstructs the summary from
// pending_data (already persisted from the original prompt) rather than
// guessing, so the customer sees exactly what they saw before along with a
// clarifying nudge, instead of the conversation silently reverting to a
// fresh, contextless state.
func (p *Processor) rebuildConfirmationPrompt(ctx context.Context, notification *listener.Notification, userData map[string]interface{}, lang string) *ProcessResult {
	productID, _ := userData["last_product_sku"].(string)
	product := p.getProductBySKU(ctx, productID)
	if product == nil {
		return nil
	}
	quantity := 1
	var deliveryDetails map[string]string
	var rawText string
	var shippingCity string
	var shippingCost float64
	if pd, ok := userData["pending_data"].(string); ok && pd != "" {
		var pdMap map[string]interface{}
		if err := json.Unmarshal([]byte(pd), &pdMap); err == nil {
			if q, ok := pdMap["quantity"].(float64); ok && q > 0 {
				quantity = int(q)
			}
			if dd, ok := pdMap["delivery_details"].(map[string]interface{}); ok {
				deliveryDetails = map[string]string{}
				for k, v := range dd {
					if vs, ok := v.(string); ok {
						deliveryDetails[k] = vs
					}
				}
			}
			if rt, ok := pdMap["raw_text"].(string); ok {
				rawText = rt
			}
			if sc, ok := pdMap["shipping_city"].(string); ok {
				shippingCity = sc
			}
			if sc, ok := pdMap["shipping_cost"].(float64); ok {
				shippingCost = sc
			}
		}
	}
	data := map[string]interface{}{
		"product":          product,
		"delivery_details": deliveryDetails,
		"shipping_address": rawText,
		"quantity":         quantity,
		"language":         lang,
		"clarify_retry":    true,
	}
	if shippingCity != "" {
		data["shipping_city"] = shippingCity
		data["shipping_cost"] = shippingCost
	}
	// Deliberately no "pending_data" key here: commitTicketResult leaves the
	// pending_data column untouched when it's absent (and this intent isn't a
	// browsing intent), so the already-collected order details persist as-is.
	return p.createResult(notification, "ask_order_confirmation", "order_details_received", data)
}

func (p *Processor) isAckAppropriateState(lastIntent string) bool {
	switch lastIntent {
	case "greeting", "store_info":
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

// getShippingOptions returns every city+cost pair from the shipping table.
// Centralized here because the same query used to be copy-pasted inline in
// two separate places in handlePreviousIntent (each slightly out of sync with
// the other), and its result was never actually read by the compiler either.
func (p *Processor) getShippingOptions(ctx context.Context) []map[string]interface{} {
	opts := []map[string]interface{}{}
	if p.db == nil {
		return opts
	}
	rows, err := p.db.QueryContext(ctx, "SELECT city, cost FROM shipping ORDER BY city")
	if err != nil {
		log.Printf("[Processor] getShippingOptions query error: %v", err)
		return opts
	}
	defer rows.Close()
	for rows.Next() {
		var city string
		var cost float64
		if err := rows.Scan(&city, &cost); err == nil {
			opts = append(opts, map[string]interface{}{"city": city, "cost": cost})
		}
	}
	return opts
}

// matchShippingCity looks for a shipping-table city named somewhere in the
// given text (typically the customer's delivery address) and returns its
// cost. This is how the order total, the order-confirmation message, and the
// final receipt all agree on the delivery charge.
func (p *Processor) matchShippingCity(ctx context.Context, text string) (city string, cost float64, found bool) {
	if strings.TrimSpace(text) == "" {
		return "", 0, false
	}
	lower := strings.ToLower(text)
	for _, o := range p.getShippingOptions(ctx) {
		c, _ := o["city"].(string)
		if c == "" {
			continue
		}
		if containsWholeWordPhrase(lower, strings.ToLower(c)) {
			cst, _ := o["cost"].(float64)
			return c, cst, true
		}
	}
	return "", 0, false
}

// createDeliveryCostTicket answers a standalone "how much is delivery?"
// question directly from the shipping table — the specific city's cost if
// one was mentioned, or the full rate list otherwise.
func (p *Processor) createDeliveryCostTicket(ctx context.Context, notification *listener.Notification, text string) *ProcessResult {
	lang := p.detectLanguage(text)
	city, cost, found := p.matchShippingCity(ctx, text)
	return p.createResult(notification, "send_delivery_cost", "delivery_cost_query", map[string]interface{}{
		"matched_city":     city,
		"matched_cost":     cost,
		"matched":          found,
		"shipping_options": p.getShippingOptions(ctx),
		"language":         lang,
	})
}

// mergeAllDeliveryFields merges a phone number found in this reply with
// whatever was already collected ("new reply wins" for phone). Only the
// phone-completeness check matters here — there's no separate name/address
// extraction. The full accumulated raw text (handled separately by callers
// via pending_data["raw_text"]) is what actually gets stored as
// shipping_address; this function only decides whether we've seen a valid
// phone number yet.
func (p *Processor) mergeAllDeliveryFields(text string, existing map[string]string) map[string]string {
	return p.mergeDeliveryText(text, existing)
}

// tryFinalizeDelivery returns a ready "ask_order_confirmation" ticket once
// delivery details are complete (per hasCompleteDelivery), including a
// shipping cost looked up from the shipping table when the address matches a
// known city. Returns nil if delivery info is still incomplete, so the caller
// decides how to ask for what's missing.
func (p *Processor) tryFinalizeDelivery(ctx context.Context, notification *listener.Notification, product map[string]interface{}, quantity int, deliveryDetails map[string]string, rawText, lang string) *ProcessResult {
	if product == nil || !hasCompleteDelivery(deliveryDetails) {
		return nil
	}
	city, cost, found := p.matchShippingCity(ctx, rawText)
	fullDetails := map[string]interface{}{
		"delivery_details": deliveryDetails,
		"quantity":         float64(quantity),
		"raw_text":         rawText,
	}
	data := map[string]interface{}{
		"product":          product,
		"pending_data":     fullDetails,
		"delivery_details": deliveryDetails,
		"shipping_address": rawText,
		"quantity":         quantity,
		"language":         lang,
		"shipping_options": p.getShippingOptions(ctx),
	}
	if found {
		fullDetails["shipping_city"] = city
		fullDetails["shipping_cost"] = cost
		data["shipping_city"] = city
		data["shipping_cost"] = cost
	}
	return p.createResult(notification, "ask_order_confirmation", "order_details_received", data)
}

// handleOrderFlowCancellation lets the user bail out of an in-progress order
// at any step ("cancel", "no", "stop") instead of forcing them to either
// finish the flow or send something that happens to parse as a field. This is
// checked at the top of every order-related state below.
func (p *Processor) handleOrderFlowCancellation(notification *listener.Notification, text, lang string) *ProcessResult {
	if p.isCancellationIntent(text) || p.isRejectionResponse(text) {
		return p.createResult(notification, "send_cancellation", "order_rejected", map[string]interface{}{
			"language":           lang,
			"clear_pending_data": true,
		})
	}
	return nil
}

// handleOrderFlowSideQuery answers a shipping-cost question asked in the
// middle of checkout (e.g. right after being asked for an address) WITHOUT
// losing the customer's place in the order. The preserve_prior_state flag
// tells commitTicketResult to leave last_intent/conversation_state/pending_data
// untouched, so the very next message is still handled by the same
// in-progress state as before this question was asked.
func (p *Processor) handleOrderFlowSideQuery(ctx context.Context, notification *listener.Notification, text, lang string) *ProcessResult {
	if p.isDeliveryCostQuery(text) {
		result := p.createDeliveryCostTicket(ctx, notification, text)
		result.Data["preserve_prior_state"] = true
		return result
	}
	if p.isOrderStatusQuery(text) {
		result := p.createOrderStatusTicket(ctx, notification)
		result.Data["preserve_prior_state"] = true
		return result
	}
	return nil
}

// createOrderProductConfirmation asks "is this your product?" before
// starting an order that was expressed in the same message where the
// product itself was matched from free text (e.g. "hello, I want 10 wicks,
// address is..."). A fuzzy text match shouldn't silently become an order.
// The quantity, if explicitly stated, is preserved so the customer isn't
// asked to repeat it after confirming — but any delivery details bundled
// into that same message are deliberately NOT carried forward. Once
// confirmed, confirmOrderProductConfirmation always sends the standard
// "send your delivery details" prompt and waits for the customer's actual
// reply to it, rather than trusting a parsed guess at what they meant by
// "address" or a phone-looking number in free text.
func (p *Processor) createOrderProductConfirmation(notification *listener.Notification, product map[string]interface{}, text string) *ProcessResult {
	pending := map[string]interface{}{"mode": "order_confirm_product"}
	if qty, ok := p.extractExplicitQuantity(text); ok {
		pending["quantity"] = float64(qty)
	}
	return p.createResult(notification, "ask_product_confirmation", "product_confirmation", map[string]interface{}{
		"product":      product,
		"source":       "resolved",
		"pending_data": pending,
	})
}

// readOrderConfirmPending decodes the pending_data JSON left by
// createOrderProductConfirmation, so a follow-up "yes" can resume the order
// using the quantity that was already stated, instead of re-parsing it out
// of a bare confirmation reply like "yes" (which has no number in it at all).
func (p *Processor) readOrderConfirmPending(userData map[string]interface{}) (mode string, quantity int, hasQuantity bool) {
	pd, ok := userData["pending_data"].(string)
	if !ok || pd == "" {
		return "", 0, false
	}
	var pdMap map[string]interface{}
	if err := json.Unmarshal([]byte(pd), &pdMap); err != nil {
		return "", 0, false
	}
	mode, _ = pdMap["mode"].(string)
	if q, ok := pdMap["quantity"].(float64); ok && q > 0 {
		return mode, int(q), true
	}
	return mode, 0, false
}

// confirmOrderProductConfirmation resumes an order after the customer
// confirmed "yes, that's my product" following createOrderProductConfirmation.
// It deliberately does NOT reuse createOrderIntentTicket's salvage/
// tryFinalizeDelivery shortcut — even if the original compound message
// looked like it contained an address/phone/name, this always sends the
// customer the standard delivery-details prompt and waits for a real reply
// to it before anything is treated as confirmed.
func (p *Processor) confirmOrderProductConfirmation(ctx context.Context, notification *listener.Notification, product map[string]interface{}, quantity int, hasQuantity bool, text string) *ProcessResult {
	lang := p.detectLanguage(text)
	if !hasQuantity {
		return p.createResult(notification, "ask_quantity", "awaiting_quantity", map[string]interface{}{
			"product":      product,
			"pending_data": map[string]interface{}{"mode": "order"},
			"language":     lang,
		})
	}
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
		"pending_data":       map[string]interface{}{"quantity": float64(quantity)},
		"language":           lang,
		"shipping_options":   p.getShippingOptions(ctx),
	})
}

func (p *Processor) createOrderIntentTicket(ctx context.Context, notification *listener.Notification, product map[string]interface{}, text string) *ProcessResult {
	lang := p.detectLanguage(text)

	quantity, explicitQty := p.extractExplicitQuantity(text)

	if !explicitQty {
		// The user never actually stated a quantity ("I want it", "give me the
		// shampoo"). Ask instead of silently ordering 1 — that silent default
		// was the original bug: a full order template (with a made-up quantity
		// and total) used to be sent before the user had said how many they
		// wanted.
		return p.createResult(notification, "ask_quantity", "awaiting_quantity", map[string]interface{}{
			"product":      product,
			"pending_data": map[string]interface{}{"mode": "order"},
			"language":     lang,
		})
	}

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

	// Deliberately do NOT try to parse delivery details out of this message,
	// even if it also contains something that looks like an address/phone
	// ("I want 10 wicks for [address+number+name]"). Delivery-detail
	// extraction only ever runs once the system has explicitly asked for it
	// (the order_intent / awaiting_order_details states below) — not here,
	// on the message that merely expressed the order intent itself.
	return p.createResult(notification, "send_order_template", "order_intent", map[string]interface{}{
		"product":            product,
		"suggested_quantity": quantity,
		"pending_data":       map[string]interface{}{"quantity": float64(quantity)},
		"language":           lang,
		"shipping_options":   p.getShippingOptions(ctx),
	})
}

// createProductConfirmation asks "is this your product?" for a single
// confident match. originalText is the query that produced the match — it's
// stashed in pending_data (mode "product_browse") so that if the customer
// rejects this match ("not this", "wrong one"), browseNextProductMatch can
// re-run the same search and offer the next-ranked result instead of just
// apologizing. attempt starts at 1 (this is the first product shown for this
// query).
func (p *Processor) createProductConfirmation(notification *listener.Notification, products []map[string]interface{}, source string, originalText string) *ProcessResult {
	if len(products) == 0 {
		return p.askForProductName(notification)
	}
	if len(products) > 1 {
		return p.createClarificationTicket(notification, products, p.getNotificationText(notification))
	}
	// Exactly one resolved product: confirm it by name/price instead of
	// asking "what product?" again — action "ask_product" ignores the
	// resolved product entirely and re-prompts for a name, which is wrong
	// when we already have a confident single match.
	pending := map[string]interface{}{"mode": "product_browse", "attempt": 1}
	if originalText != "" {
		pending["original_text"] = originalText
	}
	return p.createResult(notification, "ask_product_confirmation", "product_confirmation", map[string]interface{}{
		"product":      products[0],
		"source":       source,
		"pending_data": pending,
	})
}

// readBrowsePending decodes the pending_data left by createProductConfirmation
// (mode "product_browse"), returning the original search query and how many
// products have been shown for it so far. Returns ok=false if the pending
// state isn't a browse state (e.g. it belongs to a different flow like
// order_confirm_product, or there's nothing stashed at all), so callers know
// to fall back to a plain rejection reply instead of trying to re-search.
func (p *Processor) readBrowsePending(userData map[string]interface{}) (originalText string, attempt int, ok bool) {
	pd, has := userData["pending_data"].(string)
	if !has || pd == "" {
		return "", 0, false
	}
	var pdMap map[string]interface{}
	if err := json.Unmarshal([]byte(pd), &pdMap); err != nil {
		return "", 0, false
	}
	if mode, _ := pdMap["mode"].(string); mode != "product_browse" {
		return "", 0, false
	}
	originalText, _ = pdMap["original_text"].(string)
	if originalText == "" {
		return "", 0, false
	}
	attempt = 1
	if a, isNum := pdMap["attempt"].(float64); isNum && a > 0 {
		attempt = int(a)
	}
	return originalText, attempt, true
}

// maxProductBrowseAttempts caps how many candidate products we'll offer in a
// row for the same query before giving up and escalating to a human. The
// first match is attempt 1; a rejection there tries the 2nd-ranked result
// (attempt 2), a second rejection tries the 3rd-ranked result (attempt 3). A
// rejection at attempt 3 (or running out of matches earlier) escalates.
const maxProductBrowseAttempts = 3

// browseNextProductMatch handles a rejection ("not this", "wrong one") of a
// product shown via createProductConfirmation. It re-runs the original
// search and offers the next-ranked candidate. Once maxProductBrowseAttempts
// candidates have all been rejected — or the search simply doesn't have
// another candidate to offer — it gives up and escalates to a human via
// escalateFailedBrowse instead of continuing to guess.
func (p *Processor) browseNextProductMatch(ctx context.Context, notification *listener.Notification, userData map[string]interface{}, text string) *ProcessResult {
	lang := p.detectLanguage(text)
	originalText, attempt, ok := p.readBrowsePending(userData)
	if !ok {
		// Nothing to retry against (product came from an image match, SKU
		// continuation, etc.) — fall back to the old plain reply.
		return p.createResult(notification, "send_message", "product_rejected", map[string]interface{}{
			"reply_text": p.getTemplate(lang, "rejection"),
			"language":   lang,
		})
	}
	if attempt >= maxProductBrowseAttempts {
		return p.escalateFailedBrowse(notification, originalText, lang)
	}
	matches := p.searchProductsByText(ctx, originalText, notification.PlatformID)
	nextIndex := attempt // matches[0..attempt-1] have already been shown/rejected
	if nextIndex >= len(matches) {
		return p.escalateFailedBrowse(notification, originalText, lang)
	}
	next := matches[nextIndex]
	return p.createResult(notification, "ask_product_confirmation", "product_confirmation", map[string]interface{}{
		"product": next,
		"source":  "browse_retry",
		"pending_data": map[string]interface{}{
			"mode":          "product_browse",
			"original_text": originalText,
			"attempt":       attempt + 1,
		},
	})
}

// escalateFailedBrowse is reached once the customer has rejected every
// candidate we could find for their query. Rather than keep guessing (or
// leaving them with just a generic "no problem"), it sends the standard
// fallback message and files an urgent_messages row so a human follows up —
// three misses in a row on the same query usually means the catalog doesn't
// have a clean match, or the search just isn't finding what they mean.
func (p *Processor) escalateFailedBrowse(notification *listener.Notification, originalText, lang string) *ProcessResult {
	if err := p.insertUrgentMessage(notification, "product_browse_exhausted",
		fmt.Sprintf("Customer rejected every match found for query %q — needs manual product help.", originalText)); err != nil {
		log.Printf("[Processor] failed to insert urgent message for exhausted browse (notification=%s): %v", notification.ID, err)
	}
	return p.createResult(notification, "send_fallback", "product_browse_exhausted", map[string]interface{}{
		"language":           lang,
		"original_text":      originalText,
		"clear_pending_data": true,
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

// buildPackResult creates the pack-price ticket once the pack count is known
// — factored out so both handlePackIntent (count stated immediately) and the
// awaiting_quantity follow-up (count given in a later reply) share the exact
// same math.
func (p *Processor) buildPackResult(notification *listener.Notification, product map[string]interface{}, packCount int, lang string) *ProcessResult {
	var qtyPerPack int64
	switch v := product["quantity_per_pack"].(type) {
	case int64:
		qtyPerPack = v
	case int:
		qtyPerPack = int64(v)
	case float64:
		qtyPerPack = int64(v)
	}
	var pricePerPack float64
	switch v := product["price_per_pack"].(type) {
	case float64:
		pricePerPack = v
	case int64:
		pricePerPack = float64(v)
	}
	currency, _ := product["currency"].(string)
	totalPrice := float64(packCount) * pricePerPack
	return p.createResultNoImageDelete(notification, "pack_price", "pack_inquiry", map[string]interface{}{
		"product":        product,
		"pack_count":     packCount,
		"items_per_pack": qtyPerPack,
		"price_per_pack": pricePerPack,
		"total_price":    totalPrice,
		"currency":       currency,
		"language":       lang,
	})
}

func (p *Processor) handlePackIntent(ctx context.Context, notification *listener.Notification, product map[string]interface{}, text string) *ProcessResult {
	lang := p.detectLanguage(text)
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
		return p.createOrderIntentTicket(ctx, notification, product, text)
	}

	packCount, explicit := p.extractExplicitQuantity(text)
	if !explicit {
		// Same fix as the single-item flow: don't silently assume "1 box" —
		// ask how many, the same way an unstated single-item quantity is asked.
		return p.createResult(notification, "ask_quantity", "awaiting_quantity", map[string]interface{}{
			"product":      product,
			"pending_data": map[string]interface{}{"mode": "pack"},
			"language":     lang,
		})
	}
	return p.buildPackResult(notification, product, packCount, lang)
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
	// If text contains order-related keywords, it's NOT a simple ack —
	// it may be an order intent that needs further processing.
	orderKeywords := []string{
		"want", "buy", "purchase", "order", "get",
		"i need", "i'll take", "gimme", "give me",
		"أريد", "اشتري", "طلب", "شراء",
		"دەمەوێت", "بکڕم", "داوا", "کڕین",
	}
	for _, kw := range orderKeywords {
		if strings.Contains(t, kw) {
			return false
		}
	}
	words := []string{
		"ok", "okay", "thanks", "thank you", "thx", "cool", "great", "yes", "sure",
		"appreciate", "nice", "perfect", "good",
		"شكرا", "شكراً", "شكرا جزيلا", "تمام", "ممتاز", "كفو", "يعطيك العافية", "حلو", "طيب", "منيح",
		"سوپاس", "زۆر سوپاس", "دەستتان خۆش", "باشە", "زۆر باش", "شایستە", "جوان", "پەسەندم", "ڕازیم",
	}
	for _, w := range words {
		if t == w || strings.HasPrefix(t, w+" ") {
			// Also reject if text includes a quantity — either digits ("2") or a
			// spelled-out number word ("two", "اثنين", "دوو") — since that means
			// the user is answering a quantity question, not just acknowledging.
			if _, explicit := p.extractExplicitQuantity(t); explicit {
				return false
			}
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
	// Sandbox notifications always auto-reply regardless of config.
	if notification.RawData != nil {
		if sandbox, _ := notification.RawData["sandbox"].(bool); sandbox {
			return true
		}
	}

	cfg := p.config.GetConfig()
	if cfg == nil {
		return false
	}
	pc, ok := cfg.Platforms[notification.PlatformID]
	if !ok {
		return false
	}

	// Check subtype-level automation first (dashboard saves per-subtype).
	// Falls back to platform-level automation if no subtype match.
	if notification.SubtypeID != "" {
		for _, sub := range pc.Subtypes {
			if sub.ID == notification.SubtypeID {
				switch notification.Type {
				case listener.NotificationTypeMessage:
					return sub.Automation.AnswerDM.Enabled
				case listener.NotificationTypeComment:
					return sub.Automation.AnswerComments.Enabled
				}
				return false
			}
		}
	}

	// Fallback to platform-level automation
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
	// NOTE: "ship"/"deliver" (and Arabic/Kurdish equivalents) were deliberately
	// removed from this list. They used to sit alongside "want"/"buy"/"need",
	// which meant a plain question like "how much is delivery to Erbil?" was
	// classified as an order attempt (and then dumped into the quantity/order
	// pipeline) instead of being answered as the shipping-cost question it is.
	// See isDeliveryCostQuery below for the dedicated handler.
	orderWords := map[string][]string{
		"en": {"order", "buy", "purchase", "want", "need", "get", "take", "give me", "send me"},
		"ar": {"طلب", "اشتري", "شراء", "أريد", "أحتاج", "أعطني", "أرسل لي"},
		"ku": {"داواکاری", "بکڕە", "کڕین", "دەمەوێت", "پێویستمە", "بیدەمێ", "بنێرمێ"},
	}
	// Availability-phrase questions ("do you have X", "is X available?") are
	// NOT order intents — they must not hijack the order flow.
	availWords := map[string][]string{
		"en": {"available", "in stock", "have", "stock", "exist", "do you have"},
		"ar": {"متوفر", "في المخزون", "يوجد", "مخزون", "موجود", "هل لديك"},
		"ku": {"بەردەستە", "لە کۆگادا", "هەیە", "کۆگا", "موجودە", "تۆ لەگەڵت هەیە"},
	}
	if matchesLangWords(t, lang, availWords) {
		return false
	}
	// "order" used as a noun referring to something already placed ("my
	// order", "show me my order", "order status") is NOT an order intent —
	// without this, isOrderStatusQuery-style phrases get misread as "place
	// a new order" purely because they contain the word "order".
	if p.isOrderStatusQuery(t) {
		return false
	}
	return matchesLangWords(t, lang, orderWords)
}

// isDeliveryCostQuery detects a standalone question about shipping/delivery
// price ("how much is delivery to Erbil?", "شحن كم؟") as distinct from actually
// placing an order. Checked independently of isOrderIntent so it can be
// answered directly from the shipping table without hijacking (or being
// hijacked by) the order flow.
func (p *Processor) isDeliveryCostQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	words := map[string][]string{
		"en": {
			"delivery cost", "delivery price", "delivery fee", "delivery charge",
			"shipping cost", "shipping price", "shipping fee", "shipping charge",
			"how much is delivery", "how much for delivery", "how much to deliver",
			"how much is shipping", "how much for shipping", "cost of delivery",
			"cost to deliver", "delivery to",
		},
		"ar": {
			"سعر التوصيل", "تكلفة التوصيل", "أجرة التوصيل", "اجرة التوصيل",
			"كم التوصيل", "كم سعر التوصيل", "كم يكلف التوصيل", "كم أجرة التوصيل",
			"سعر الشحن", "تكلفة الشحن", "كم الشحن",
		},
		"ku": {
			"نرخی گەیاندن", "کرێی گەیاندن", "گەیاندن چەندە", "بۆ گەیاندن چەندە",
			"نرخی ناردن", "چەند بۆ گەیاندن",
		},
	}
	return matchesLangWords(t, lang, words)
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

// isCompatibilityQuery detects "does this work for/with X", "is this
// compatible with X", "will it fit my X" style questions — the customer is
// asking whether the currently-discussed product suits a specific
// device/use-case, not asking about stock or price.
func (p *Processor) isCompatibilityQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	phrases := map[string][]string{
		"en": {
			"work for", "work with", "works for", "works with", "will it work",
			"compatible with", "compatible for", "fit my", "fit for", "fits my",
			"fits for", "suitable for", "suit my", "use it for", "use this for",
			"usable for", "good for my", "good for a",
		},
		"ar": {"يشتغل مع", "يعمل مع", "متوافق مع", "يناسب", "يصلح ل", "يصلح مع"},
		"ku": {"لەگەڵ کار دەکات", "گونجاوە بۆ", "لەبار دەکات", "بۆ باشە"},
	}
	return matchesLangWords(t, lang, phrases)
}

// extractCompatibilityTarget pulls the device/use-case out of a
// compatibility question by cutting everything up to and including the
// trigger phrase, then dropping possessives/filler from what's left.
// "does this work for my turbo heater?" -> "turbo heater"
// "هل يعمل مع سخان التوربو؟" -> "سخان التوربو"
func (p *Processor) extractCompatibilityTarget(text string) string {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	triggersByLang := map[string][]string{
		"en": {
			"works with", "work with", "works for", "work for", "will it work with",
			"will it work for", "will it work",
			"compatible with", "compatible for",
			"fits with", "fits for", "fits my", "fit with", "fit for", "fit my",
			"suitable for", "suit my", "use it for", "use this for", "usable for",
			"good for my", "good for a", "good for",
		},
		"ar": {"يشتغل مع", "يعمل مع", "متوافق مع", "يناسب", "يصلح مع", "يصلح ل"},
		"ku": {"لەگەڵ کار دەکات", "گونجاوە بۆ", "لەبار دەکات", "بۆ باشە"},
	}
	// The classifier (isCompatibilityQuery) already detected the language via
	// matchesLangWords' en-fallback, so try the detected language's triggers
	// first, then fall back to checking all of them — a message can be
	// mostly Arabic/Kurdish with an English trigger phrase mixed in, or vice
	// versa, and we'd rather find *something* than return empty.
	ordered := append([]string{}, triggersByLang[lang]...)
	for l, trigs := range triggersByLang {
		if l != lang {
			ordered = append(ordered, trigs...)
		}
	}

	rest := ""
	for _, trig := range ordered {
		if idx := strings.Index(t, trig); idx != -1 {
			rest = t[idx+len(trig):]
			break
		}
	}
	if rest == "" {
		return ""
	}
	stop := map[string]bool{
		// English
		"my": true, "the": true, "a": true, "an": true, "your": true,
		"our": true, "this": true, "that": true, "please": true, "it": true,
		// Arabic / Kurdish possessives and fillers ("my", "for", "please")
		"من": true, "الخاص": true, "لو": true, "سمحت": true, "ال": true,
		"بۆ": true, "ئەم": true, "ئەو": true,
	}
	var words []string
	for _, w := range strings.FieldsFunc(rest, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if !stop[w] && len([]rune(w)) > 1 {
			words = append(words, w)
		}
	}
	return strings.Join(words, " ")
}

// productMatchesUse checks whether the given product's "uses" text mentions
// the target device/use-case the customer asked about. This is a plain
// keyword/substring match against uses_en/uses_ar/uses_ku (plus the
// description as a fallback) — it will not catch typos ("hater" vs
// "heater") or synonyms not present in the uses text, but it's a solid
// first pass without needing a separate compatibility/fitment table.
func (p *Processor) productMatchesUse(ctx context.Context, sku, target, lang string) (bool, error) {
	if target == "" || sku == "" {
		return false, nil
	}
	row := p.db.QueryRowContext(ctx, `
		SELECT uses_en, uses_ar, uses_ku, description
		FROM products
		WHERE sku = ? AND is_active = 1
		LIMIT 1
	`, sku)
	var usesEn, usesAr, usesKu, desc sql.NullString
	if err := row.Scan(&usesEn, &usesAr, &usesKu, &desc); err != nil {
		return false, err
	}
	// Search the language-appropriate uses column first, then fall back to
	// English uses and the description, mirroring the en-fallback pattern
	// used elsewhere (matchesLangWords, searchProductsByText).
	var primary string
	switch lang {
	case "ar":
		primary = usesAr.String
	case "ku":
		primary = usesKu.String
	default:
		primary = usesEn.String
	}
	haystack := strings.ToLower(primary + " " + usesEn.String + " " + desc.String)
	for _, kw := range strings.Fields(target) {
		if len([]rune(kw)) > 2 && strings.Contains(haystack, kw) {
			return true, nil
		}
	}
	return false, nil
}

// createProductCompatibilityTicket answers a "does this work for X?"
// question by checking the product's uses_en/uses_ar/uses_ku text for the
// extracted target. Three outcomes: a confident yes, a confident "not
// listed" no, or — if we couldn't extract a target at all — a request to
// rephrase, since guessing here risks giving a wrong yes/no.
func (p *Processor) createProductCompatibilityTicket(ctx context.Context, notification *listener.Notification, product map[string]interface{}, text string) *ProcessResult {
	lang := p.detectLanguage(text)
	sku, _ := product["sku"].(string)
	target := p.extractCompatibilityTarget(text)

	if target == "" {
		return p.createResult(notification, "send_compatibility_answer", "product_compatibility_unknown", map[string]interface{}{
			"product":  product,
			"language": lang,
		})
	}

	match, err := p.productMatchesUse(ctx, sku, target, lang)
	if err != nil {
		log.Printf("[Processor] compatibility check error (sku=%s): %v", sku, err)
	}
	intent := "product_compatibility_no"
	if match {
		intent = "product_compatibility_yes"
	}
	return p.createResult(notification, "send_compatibility_answer", intent, map[string]interface{}{
		"product":  product,
		"target":   target,
		"language": lang,
	})
}

// createOrderStatusTicket looks up the customer's most recent order(s) and
// returns them directly — this is a self-serve DB read, not an escalation,
// since we already have everything needed (status/total/items) without
// involving a human.
func (p *Processor) createOrderStatusTicket(ctx context.Context, notification *listener.Notification) *ProcessResult {
	lang := p.detectLanguage(p.getNotificationText(notification))

	userID := p.getUserID(notification)
	var internalID string
	if userID != "" {
		if err := p.db.QueryRowContext(ctx,
			`SELECT id FROM platform_users WHERE platform = ? AND platform_user_id = ?`,
			notification.PlatformID, userID).Scan(&internalID); err != nil && err != sql.ErrNoRows {
			log.Printf("[Processor] order status: platform_users lookup error for %s: %v", userID, err)
		}
	}
	if internalID == "" {
		return p.createResult(notification, "send_order_status", "order_status_none", map[string]interface{}{
			"language": lang,
		})
	}

	rows, err := p.db.QueryContext(ctx, `
		SELECT id, status, total, created_at
		FROM orders
		WHERE user_id = ?
		ORDER BY created_at DESC
		LIMIT 3
	`, internalID)
	if err != nil {
		log.Printf("[Processor] order status: orders query error for %s: %v", internalID, err)
		return p.createResult(notification, "send_order_status", "order_status_none", map[string]interface{}{
			"language": lang,
		})
	}
	defer rows.Close()

	var orders []map[string]interface{}
	for rows.Next() {
		var id, status, createdAt string
		var total float64
		if err := rows.Scan(&id, &status, &total, &createdAt); err != nil {
			continue
		}
		orders = append(orders, map[string]interface{}{
			"id":         id,
			"status":     status,
			"total":      total,
			"created_at": createdAt,
		})
	}

	if len(orders) == 0 {
		return p.createResult(notification, "send_order_status", "order_status_none", map[string]interface{}{
			"language": lang,
		})
	}

	// Attach line items for the most recent order only — that's the one
	// customers almost always mean, and it keeps the query cheap.
	if id, ok := orders[0]["id"].(string); ok {
		itemRows, err := p.db.QueryContext(ctx, `
			SELECT product_name, quantity FROM order_items WHERE order_id = ?
		`, id)
		if err == nil {
			var items []map[string]interface{}
			for itemRows.Next() {
				var name string
				var qty int
				if err := itemRows.Scan(&name, &qty); err == nil {
					items = append(items, map[string]interface{}{"name": name, "quantity": qty})
				}
			}
			itemRows.Close()
			orders[0]["items"] = items
		}
	}

	return p.createResult(notification, "send_order_status", "order_status_found", map[string]interface{}{
		"orders":   orders,
		"language": lang,
	})
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

// isOrderStatusQuery detects a self-serve "show/check my order(s)" request —
// the customer wants to see an order they already placed, as distinct from
// isOrderIntent ("order the wick") or isShippingQuery ("where is my order" /
// "tracking", which escalates to support since we don't have carrier data).
func (p *Processor) isOrderStatusQuery(text string) bool {
	t := strings.ToLower(text)
	lang := p.detectLanguage(t)
	phrases := map[string][]string{
		"en": {
			"my order", "my orders", "show me my order", "show my order",
			"check my order", "view my order", "see my order", "order status",
			"my order status", "my past order", "my previous order", "my order history",
		},
		"ar": {
			"طلبي", "طلباتي", "اعرض طلبي", "تحقق من طلبي", "حالة الطلب", "حالة طلبي",
		},
		"ku": {
			"داواکاریەکەم", "داواکاریەکانم", "داواکاریەکەم پیشان بدە", "باری داواکاری",
		},
	}
	return matchesLangWords(t, lang, phrases)
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
		// Word-boundary prefix match: bare HasPrefix(t, w) would match "nokia" on
		// "no", "oklahoma" on "ok", etc. Require the match to end the string or be
		// followed by a space/punctuation.
		if t == w || strings.HasPrefix(t, w+" ") {
			return true
		}
	}
	return false
}

func (p *Processor) isRejectionResponse(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	for _, w := range []string{
		"no", "cancel", "stop", "wrong", "nope", "nah", "not interested", "no thanks",
		"not this", "not this one", "not it", "that's not it", "thats not it",
		"it's not this", "its not this", "it's not this one", "its not this one",
		"wrong one", "wrong product", "different product", "not the right one",
		"لا", "لأ", "لا شكرا", "مش عايز", "مش عاوزه", "مش مهتم", "بلاش", "خلاص مش عايز",
		"مش هذا", "مو هذا", "مب هذا", "غلط",
		"نەخێر", "نا", "ناخوازم", "بێزار", "پێویست نیم", "دەستم لێ نەکەوت", "ئەمە نییە",
	} {
		// Word-boundary prefix match — bare HasPrefix(t, "no") would fire on
		// "nothing else", "november", "nokia", silently cancelling an order the
		// user never rejected.
		if t == w || strings.HasPrefix(t, w+" ") {
			return true
		}
	}
	return false
}

func (p *Processor) isChangeDetailsRequest(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	for _, w := range []string{
		"change", "change address", "change details", "edit", "update address", "new address", "different address",
		"غيّر", "غيار", "تغيير", "تغيير العنوان", "عدّل", "تعديل", "تعديل العنوان",
		"گۆڕین", "بیگۆڕە", "ناونیشان بگۆڕە", "گۆڕانکاری",
	} {
		if t == w || strings.HasPrefix(t, w+" ") {
			return true
		}
	}
	return false
}

func (p *Processor) extractKeywords(text string) []string {
	t := strings.ToLower(strings.TrimSpace(text))
	// Strip conversational filler so product nouns become the keywords:
	// "hello, do you have wick?" -> "wick", "do you have brush" -> "brush".
	for _, prefix := range []string{
		"hello ", "hi ", "hey ", "greetings ", "good morning ", "good afternoon ", "good evening ",
	} {
		t = strings.TrimPrefix(t, prefix)
	}
	stop := map[string]bool{
		"the": true, "a": true, "an": true, "and": true, "or": true,
		"for": true, "to": true, "in": true, "on": true, "at": true,
		"price": true, "cost": true,
		"hello": true, "hi": true, "hey": true, "greetings": true,
		"good": true, "morning": true, "afternoon": true, "evening": true,
		"do": true, "you": true, "have": true, "please": true, "can": true,
		"i": true, "want": true, "need": true, "get": true, "is": true,
		"are": true, "there": true, "available": true, "some": true,
		"مرحبا": true, "السلام": true, "اهلا": true, "يوجد": true, "متوفر": true,
		"سڵاو": true, "هەیە": true, "بەردەستە": true,
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

// numberWords maps spelled-out quantity words (English, Arabic, Kurdish) to
// their integer value, so "I want four" / "أربعة" / "چوار" are recognized as
// quantities exactly like digit input ("4"). Without this, extractOrderQuantity
// silently fell back to a hardcoded 1 for any non-digit reply.
var numberWords = map[string]map[string]int{
	"en": {
		// Deliberately NOT including "a"/"an"/"single"/"couple"/"dozen" — those
		// show up as ordinary articles in unrelated sentences ("a discount", "a
		// person") far more often than as a real quantity, so treating them as
		// numbers would defeat the whole point of only acting on an EXPLICIT
		// quantity. Stick to unambiguous cardinal number words.
		"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
		"eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13,
		"fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
		"nineteen": 19, "twenty": 20,
	},
	"ar": {
		"واحد": 1, "واحدة": 1, "اثنين": 2, "اثنان": 2, "ثنين": 2, "ثلاثة": 3, "ثلاث": 3,
		"اربعة": 4, "أربعة": 4, "اربع": 4, "أربع": 4, "خمسة": 5, "خمس": 5, "ستة": 6, "ست": 6,
		"سبعة": 7, "سبع": 7, "ثمانية": 8, "ثماني": 8, "تسعة": 9, "تسع": 9, "عشرة": 10, "عشر": 10,
	},
	"ku": {
		"یەک": 1, "دوو": 2, "سێ": 3, "چوار": 4, "پێنج": 5, "شەش": 6, "حەوت": 7,
		"هەشت": 8, "نۆ": 9, "دە": 10,
	},
}

// extractExplicitQuantity looks for a quantity the user actually stated —
// either as a digit ("4") or a spelled-out number word in any supported
// language ("four", "أربعة", "چوار"). It returns (quantity, true) only when
// something explicit was found. Callers MUST check the bool: silently
// defaulting to 1 when the user never gave a number is exactly what causes
// an order to be created for a quantity nobody asked for.
func (p *Processor) extractExplicitQuantity(text string) (int, bool) {
	nums := p.extractNumbers(text)
	for _, n := range nums {
		if n >= 1 && n <= maxPlausibleQuantity {
			return n, true
		}
	}
	tokens := wordTokens(strings.ToLower(text))
	for _, tok := range tokens {
		for _, lang := range [...]string{"en", "ar", "ku"} {
			if n, ok := numberWords[lang][tok]; ok && n >= 1 {
				return n, true
			}
		}
	}
	return 1, false
}

// extractOrderQuantity preserves the historical "default to 1" behavior for
// callers that already have their own way of asking the user when a quantity
// is genuinely required (see extractExplicitQuantity for that check).
func (p *Processor) extractOrderQuantity(text string) int {
	n, _ := p.extractExplicitQuantity(text)
	return n
}

func looksLikePhone(s string) bool {
	// A valid number must start with 07 and be longer than 6 digits total,
	// so "blalalalala 07343434" is a correct entry.
	t := strings.TrimSpace(s)
	if !strings.HasPrefix(t, "07") {
		return false
	}
	digits := 0
	for _, r := range t {
		if unicode.IsDigit(r) {
			digits++
		}
	}
	return digits > 6 && digits <= 15
}

// hasCompleteDelivery checks whether delivery details contain a valid phone number.
// That's the ONLY requirement — everything else is optional.
func hasCompleteDelivery(d map[string]string) bool {
	return d["phone"] != ""
}

func (p *Processor) extractDeliveryDetails(text string) map[string]string {
	// Capture any valid phone number (7+ digits, optionally starting with + or 00).
	// The entire text will be saved as shipping_address separately.
	details := make(map[string]string)

	// Match international format (+XXX), 00XXX format, or local (7+ digits)
	phoneRe := regexp.MustCompile(`(\+?[0-9]{7,15}|00[0-9]{7,15}|[0-9]{7,15})`)
	allMatches := phoneRe.FindAllString(text, -1)
	if len(allMatches) > 0 {
		details["phone"] = allMatches[0]
		details["has_phone"] = "true"
	}
	return details
}

// mergeDeliveryText parses the user's reply for a valid phone number.
// The ENTIRE text will be used as shipping_address — we don't extract
// name or address separately anymore.
func (p *Processor) mergeDeliveryText(text string, existing map[string]string) map[string]string {
	merged := p.extractDeliveryDetails(text)

	// Merge any previously found phone number (new replies win).
	for k, v := range existing {
		if v == "" {
			continue
		}
		if _, already := merged[k]; !already {
			merged[k] = v
		}
	}
	return merged
}

// deliveryDetailsPrompt asks for phone number only if missing.
func (p *Processor) deliveryDetailsPrompt(current map[string]string, lang string, missing map[string]bool) string {
	if !missing["phone"] {
		return ""
	}
	switch lang {
	case "ar":
		return "من فضلك أرسل رقم الهاتف وعنوان التوصيل (مثال: رواندوز 0770232323)"
	case "ku":
		return "تکایە ژمارەی تەلەفۆن و ناونیشانی گەیاندن بنێرە (نموونە: رواندوز 0770232323)"
	default:
		return "Please send your phone number and delivery address (example: Rawa Slemani 0770232323)"
	}
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
	pc, ok := cfg.Platforms[platformID]
	if !ok {
		return false
	}

	// Check any subtype automation first
	for _, sub := range pc.Subtypes {
		if sub.Automation.AnswerDM.Enabled || sub.Automation.AnswerComments.Enabled {
			return true
		}
	}

	// Fallback to platform-level
	return pc.Automation.AnswerDM.Enabled || pc.Automation.AnswerComments.Enabled
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
