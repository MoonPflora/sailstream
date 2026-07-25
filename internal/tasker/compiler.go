package tasker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"sailstream/internal/comms"
	"sailstream/internal/config"
	"sailstream/internal/listener"
	"sailstream/internal/nnlp"
	"sailstream/internal/shared"
)

type RateLimiter interface {
	CanProceed(platform, subtypeID, action string) (bool, time.Duration)
	RecordUsage(platform, subtypeID, action string)
}

type Compiler struct {
	db              *sql.DB
	configMgr       *config.ConfigManager
	llmClient       *comms.Client
	rateLimiter     RateLimiter
	queue           *InstructionQueue
	errorChan       chan *CompilerError
	statusChan      chan *CompilerStatus
	instructionChan chan *CompiledInstruction
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

type InstructionQueue struct {
	mu           sync.Mutex
	queue        []*QueuedInstruction
	maxSize      int
	processedIDs map[string]time.Time
}

type QueuedInstruction struct {
	Instruction  *shared.AutomationInstruction
	Ticket       *nnlp.ProcessResult
	Notification *listener.Notification
	Priority     int
	QueuedAt     time.Time
	Attempts     int
	MaxAttempts  int
	Status       string
	Error        string
	LastAttempt  time.Time
}

type CompilerError struct {
	TicketID       string    `json:"ticket_id"`
	NotificationID string    `json:"notification_id"`
	Platform       string    `json:"platform"`
	SubtypeID      string    `json:"subtype_id"`
	Action         string    `json:"action"`
	Intent         string    `json:"intent"`
	ErrorCode      string    `json:"error_code"`
	ErrorMessage   string    `json:"error_message"`
	Timestamp      time.Time `json:"timestamp"`
	Severity       string    `json:"severity"`
	Retryable      bool      `json:"retryable"`
}

type CompilerStatus struct {
	QueueSize       int       `json:"queue_size"`
	ProcessedCount  int       `json:"processed_count"`
	FailedCount     int       `json:"failed_count"`
	LastProcessedAt time.Time `json:"last_processed_at"`
}

type CompiledInstruction struct {
	Instruction    *shared.AutomationInstruction `json:"instruction"`
	Ticket         *nnlp.ProcessResult           `json:"ticket"`
	Notification   *listener.Notification        `json:"notification"`
	UserData       map[string]interface{}        `json:"user_data"`
	CompiledAt     time.Time                     `json:"compiled_at"`
	Success        bool                          `json:"success"`
	Error          string                        `json:"error,omitempty"`
	QueuedForRetry bool                          `json:"queued_for_retry"`
}

func NewCompiler(db *sql.DB, configMgr *config.ConfigManager, llmClient *comms.Client, rl RateLimiter) *Compiler {
	c := &Compiler{
		db:              db,
		configMgr:       configMgr,
		llmClient:       llmClient,
		rateLimiter:     rl,
		queue:           newInstructionQueue(1000),
		errorChan:       make(chan *CompilerError, 100),
		statusChan:      make(chan *CompilerStatus, 10),
		instructionChan: make(chan *CompiledInstruction, 50),
		stopCh:          make(chan struct{}),
	}
	c.wg.Add(5)
	go c.runProcessQueue()
	go c.runReportStatus()
	go c.runCleanupProcessedIDs(1 * time.Hour)
	go c.runCleanupQueue(2 * time.Minute)
	go c.runCleanupExpiredOrders(1 * time.Hour)
	return c
}

func (c *Compiler) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

func (c *Compiler) GetErrorChannel() <-chan *CompilerError             { return c.errorChan }
func (c *Compiler) GetStatusChannel() <-chan *CompilerStatus           { return c.statusChan }
func (c *Compiler) GetInstructionChannel() <-chan *CompiledInstruction { return c.instructionChan }

func (c *Compiler) GetQueueSize() int {
	c.queue.mu.Lock()
	defer c.queue.mu.Unlock()
	return len(c.queue.queue)
}

func (c *Compiler) runProcessQueue() {
	defer c.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.queue.mu.Lock()
			if len(c.queue.queue) == 0 {
				c.queue.mu.Unlock()
				continue
			}
			c.queue.sortByPriority()
			var batch []*QueuedInstruction
			for i := 0; i < len(c.queue.queue) && len(batch) < 5; i++ {
				item := c.queue.queue[i]
				if item.Status == "queued" {
					item.Status = "processing"
					item.LastAttempt = time.Now()
					batch = append(batch, item)
				}
			}
			c.queue.mu.Unlock()
			for _, item := range batch {
				c.wg.Add(1)
				go func(qi *QueuedInstruction) {
					defer c.wg.Done()
					c.processQueuedInstruction(qi)
				}(item)
			}
		}
	}
}

func (c *Compiler) runReportStatus() {
	defer c.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.queue.mu.Lock()
			qSize := 0
			failed := 0
			for _, item := range c.queue.queue {
				if item.Status == "queued" || item.Status == "processing" {
					qSize++
				}
				if item.Status == "failed" {
					failed++
				}
			}
			pCount := len(c.queue.processedIDs)
			c.queue.mu.Unlock()
			select {
			case c.statusChan <- &CompilerStatus{
				QueueSize:       qSize,
				ProcessedCount:  pCount,
				FailedCount:     failed,
				LastProcessedAt: time.Now(),
			}:
			default:
			}
		}
	}
}

func (c *Compiler) runCleanupProcessedIDs(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.queue.pruneExpired()
		}
	}
}

func (c *Compiler) runCleanupQueue(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.queue.mu.Lock()
			cutoff := time.Now().Add(-5 * time.Minute)
			var kept []*QueuedInstruction
			removed := 0
			for _, item := range c.queue.queue {
				if (item.Status == "completed" || item.Status == "failed") && item.QueuedAt.Before(cutoff) {
					removed++
					continue
				}
				if item.Status == "processing" && item.LastAttempt.Before(time.Now().Add(-10*time.Minute)) {
					log.Printf("[Compiler:Queue] Removing stuck processing item: ticket=%s", item.Ticket.TicketID)
					removed++
					continue
				}
				kept = append(kept, item)
			}
			c.queue.queue = kept
			if removed > 0 {
				log.Printf("[Compiler:Queue] Cleaned up %d items (remaining: %d pending)", removed, len(kept))
			}
			c.queue.mu.Unlock()
		}
	}
}

func (c *Compiler) runCleanupExpiredOrders(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-24 * time.Hour)
			rows, err := c.db.Query(`SELECT id, product_id, quantity FROM orders WHERE status = 'pending' AND created_at < ?`, cutoff)
			if err != nil {
				log.Printf("[Compiler] Expired order cleanup error: %v", err)
				continue
			}
			var expiredOrders []struct {
				id, productID string
				qty           int
			}
			for rows.Next() {
				var oid, pid string
				var q int
				if err := rows.Scan(&oid, &pid, &q); err == nil {
					expiredOrders = append(expiredOrders, struct {
						id, productID string
						qty           int
					}{oid, pid, q})
				}
			}
			rows.Close()
			tx, _ := c.db.Begin()
			for _, ord := range expiredOrders {
				tx.Exec(`UPDATE products SET reserved_stock = reserved_stock - ? WHERE id = ?`, ord.qty, ord.productID)
				tx.Exec(`UPDATE orders SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP WHERE id = ?`, ord.id)
			}
			tx.Commit()
		}
	}
}

func (c *Compiler) checkRateLimits(action, platform, subtypeID string) (bool, time.Duration) {
	if c.rateLimiter == nil {
		return true, 0
	}
	return c.rateLimiter.CanProceed(platform, subtypeID, action)
}

func (c *Compiler) recordRateLimitUsage(action, platform, subtypeID string) {
	if c.rateLimiter != nil {
		c.rateLimiter.RecordUsage(platform, subtypeID, action)
	}
	meta := c.configMgr.GetPlatformMetadata(platform)
	meta.LastActive = time.Now().Format(time.RFC3339)
	c.configMgr.SetPlatformMetadata(platform, meta)
}

func (c *Compiler) addRateLimitDelays(instruction *shared.AutomationInstruction) {
	delay := map[string]int{
		"whatsapp":  3000,
		"instagram": 2000,
		"facebook":  1500,
		"twitter":   1000,
		"telegram":  500,
	}
	base := 1000
	if d, ok := delay[instruction.Platform]; ok {
		base = d
	}
	jitter := rand.Intn(500) + 200
	if len(instruction.Steps) > 0 {
		instruction.Steps[0].DelayBefore += base + jitter
	}
}

func newInstructionQueue(maxSize int) *InstructionQueue {
	return &InstructionQueue{
		queue:        make([]*QueuedInstruction, 0),
		maxSize:      maxSize,
		processedIDs: make(map[string]time.Time),
	}
}

func (q *InstructionQueue) enqueue(item *QueuedInstruction) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) >= q.maxSize {
		return fmt.Errorf("queue full")
	}
	for _, existing := range q.queue {
		if existing.Ticket.TicketID == item.Ticket.TicketID &&
			(existing.Status == "queued" || existing.Status == "processing") {
			return fmt.Errorf("ticket %s already in queue", item.Ticket.TicketID)
		}
	}
	item.QueuedAt = time.Now()
	item.Status = "queued"
	q.queue = append(q.queue, item)
	return nil
}

func (q *InstructionQueue) isProcessed(ticketID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	exp, ok := q.processedIDs[ticketID]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(q.processedIDs, ticketID)
		return false
	}
	return true
}

func (q *InstructionQueue) markProcessed(ticketID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.processedIDs[ticketID] = time.Now().Add(24 * time.Hour)
}

func (q *InstructionQueue) pruneExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for id, exp := range q.processedIDs {
		if now.After(exp) {
			delete(q.processedIDs, id)
		}
	}
}

func (q *InstructionQueue) sortByPriority() {
	n := len(q.queue)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if q.queue[j].Priority < q.queue[j+1].Priority {
				q.queue[j], q.queue[j+1] = q.queue[j+1], q.queue[j]
			}
		}
	}
}

func priorityForAction(action string) int {
	m := map[string]int{
		"block": 10, "ai_ticket": 9, "ai_response": 9,
		"pack_price": 8, "send_order_template": 8, "ask_order_confirmation": 8,
		"send_product": 7, "send_stock_warning": 7,
		"send_store_info": 6, "ask_for_order": 6, "ask_product": 6, "send_cancellation": 6,
		"send_greeting": 5, "unfollow": 5,
		"auto_heart": 3, "react": 3, "follow": 3,
		"send_fallback": 2, "queued": 2, "noop": 1,
	}
	if p, ok := m[action]; ok {
		return p
	}
	return 5
}

func (c *Compiler) Compile(ticket *nnlp.ProcessResult) (*shared.AutomationInstruction, error) {
	notification := c.extractNotification(ticket)
	if notification == nil {
		err := fmt.Errorf("no notification in ticket data")
		c.emitError(ticket.TicketID, "", "", "", ticket.Action, ticket.Intent, "MISSING_NOTIFICATION", err.Error(), "error", false)
		return nil, err
	}
	info := c.extractUserInfo(notification)
	userID, _ := info["user_id"].(string)
	if userID == "" {
		return nil, fmt.Errorf("notification has no user ID")
	}
	if c.queue.isProcessed(ticket.TicketID) {
		return nil, fmt.Errorf("ticket already processed")
	}
	canProceed, wait := c.checkRateLimits(ticket.Action, notification.PlatformID, notification.SubtypeID)
	if !canProceed {
		c.emitError(ticket.TicketID, notification.ID, notification.PlatformID, notification.SubtypeID,
			ticket.Action, ticket.Intent, "RATE_LIMITED", fmt.Sprintf("rate limited, retry in %v", wait), "warning", true)
		_ = c.queue.enqueue(&QueuedInstruction{
			Ticket:       ticket,
			Notification: notification,
			Priority:     priorityForAction(ticket.Action),
			QueuedAt:     time.Now(),
			MaxAttempts:  3,
			Status:       "queued",
		})
		return nil, fmt.Errorf("rate limited")
	}
	userData := c.readUserData(notification)
	instruction, err := c.compileAction(ticket, notification, userData)
	if err != nil {
		c.emitError(ticket.TicketID, notification.ID, notification.PlatformID, notification.SubtypeID,
			ticket.Action, ticket.Intent, "COMPILE_FAILED", err.Error(), "error", true)
		c.emitReport(nil, ticket, notification, userData, false, err.Error(), false)
		return nil, err
	}
	c.saveOutgoingMessage(notification, userData, instruction)
	c.addRateLimitDelays(instruction)
	c.recordRateLimitUsage(ticket.Action, notification.PlatformID, notification.SubtypeID)
	c.queue.markProcessed(ticket.TicketID)
	c.emitReport(instruction, ticket, notification, userData, true, "", false)
	return instruction, nil
}

func (c *Compiler) processQueuedInstruction(item *QueuedInstruction) {
	item.Attempts++
	instruction, err := c.Compile(item.Ticket)
	if err != nil {
		errStr := err.Error()
		if isNonRetryable(errStr) {
			item.Status = "failed"
			item.Error = errStr
			c.emitReport(nil, item.Ticket, item.Notification, nil, false, errStr, false)
			return
		}
		if item.Attempts >= item.MaxAttempts {
			item.Status = "failed"
			item.Error = errStr
			c.emitReport(nil, item.Ticket, item.Notification, nil, false, errStr, false)
		} else {
			item.Status = "queued"
		}
		return
	}
	item.Instruction = instruction
	item.Status = "completed"
}

func isNonRetryable(errStr string) bool {
	nonRetryable := []string{"no user ID", "no notification in ticket data", "ticket already processed", "notification has no user ID"}
	for _, k := range nonRetryable {
		if strings.Contains(errStr, k) {
			return true
		}
	}
	return false
}

func (c *Compiler) emitReport(instruction *shared.AutomationInstruction, ticket *nnlp.ProcessResult, notification *listener.Notification, userData map[string]interface{}, success bool, errMsg string, queued bool) {
	report := &CompiledInstruction{
		Instruction:    instruction,
		Ticket:         ticket,
		Notification:   notification,
		UserData:       userData,
		CompiledAt:     time.Now(),
		Success:        success,
		Error:          errMsg,
		QueuedForRetry: queued,
	}
	select {
	case c.instructionChan <- report:
	default:
	}
}

func (c *Compiler) emitError(ticketID, notifID, platform, subtypeID, action, intent, code, msg, severity string, retryable bool) {
	select {
	case c.errorChan <- &CompilerError{
		TicketID: ticketID, NotificationID: notifID, Platform: platform, SubtypeID: subtypeID,
		Action: action, Intent: intent, ErrorCode: code, ErrorMessage: msg, Timestamp: time.Now(),
		Severity: severity, Retryable: retryable,
	}:
	default:
	}
}

func (c *Compiler) readUserData(notification *listener.Notification) map[string]interface{} {
	info := c.extractUserInfo(notification)
	platformUserID, _ := info["user_id"].(string)
	if platformUserID == "" {
		return info
	}
	var id, convState, lastIntent, username, displayName sql.NullString
	var totalMsgs, totalOrders sql.NullInt64
	var totalSpent sql.NullFloat64
	var isBlocked sql.NullInt64
	err := c.db.QueryRow(
		`SELECT id, conversation_state, last_intent, username, display_name, total_messages, total_orders, total_spent, is_blocked FROM platform_users WHERE platform = ? AND platform_user_id = ?`,
		notification.PlatformID, platformUserID,
	).Scan(&id, &convState, &lastIntent, &username, &displayName, &totalMsgs, &totalOrders, &totalSpent, &isBlocked)
	if err != nil {
		return info
	}
	return map[string]interface{}{
		"id":                 id.String,
		"platform_user_id":   platformUserID,
		"username":           username.String,
		"display_name":       displayName.String,
		"conversation_state": convState.String,
		"last_intent":        lastIntent.String,
		"total_messages":     totalMsgs.Int64,
		"total_orders":       totalOrders.Int64,
		"total_spent":        totalSpent.Float64,
		"is_blocked":         isBlocked.Int64 == 1,
	}
}

func (c *Compiler) getConversationHistory(userID string, limit int) ([]map[string]interface{}, error) {
	rows, err := c.db.Query(
		`SELECT direction, message_text, received_at FROM messages WHERE user_id = ? ORDER BY received_at DESC LIMIT ?`,
		userID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []map[string]interface{}
	for rows.Next() {
		var dir, text string
		var ts time.Time
		if err := rows.Scan(&dir, &text, &ts); err != nil {
			continue
		}
		history = append(history, map[string]interface{}{
			"direction": dir,
			"text":      text,
			"time":      ts.Format(time.RFC3339),
		})
	}
	return history, nil
}

func (c *Compiler) saveOutgoingMessage(notification *listener.Notification, userData map[string]interface{}, instruction *shared.AutomationInstruction) {
	if instruction == nil {
		return
	}
	userID, _ := userData["id"].(string)
	if userID == "" {
		return
	}
	text := ""
	if msg, ok := instruction.Data["message"].(string); ok && msg != "" {
		text = msg
	} else {
		for _, step := range instruction.Steps {
			if step.Type == shared.StepTypeSendMessage || step.Type == shared.StepTypeReply {
				text = step.Value
				break
			}
		}
	}
	if text == "" {
		return
	}
	c.db.Exec(
		`INSERT INTO messages (user_id, platform, direction, message_text, intent, received_at) VALUES (?, ?, 'outgoing', ?, ?, CURRENT_TIMESTAMP)`,
		userID, notification.PlatformID, text, instruction.Intent,
	)
}

func (c *Compiler) updateUserStateAfterAI(userID, intent, aiAnswer string) {
	if userID == "" {
		return
	}
	convState := "support"
	switch intent {
	case "order_intent", "order_intent_detected", "awaiting_confirmation":
		convState = "ordering"
	case "product_price_query", "product_availability", "product_confirmation":
		convState = "browsing"
	}
	c.db.Exec(
		`UPDATE platform_users SET last_intent = ?, conversation_state = ?, last_active = CURRENT_TIMESTAMP WHERE id = ?`,
		intent, convState, userID,
	)
}

func (c *Compiler) recordUrgentMessage(userID, platform, msgType, originalText, ticketID string) {
	if userID == "" {
		return
	}
	id := uuid.New().String()
	_, err := c.db.Exec(`
		INSERT INTO urgent_messages
			(id, user_id, platform, message_type, original_text, status, priority, created_at)
		VALUES (?, ?, ?, ?, ?, 'pending', ?, CURRENT_TIMESTAMP)
	`, id, userID, platform, msgType, originalText, 80)
	if err != nil {
		log.Printf("[Compiler] failed to record urgent message (ticket=%s, user=%s): %v", ticketID, userID, err)
	}
}

func (c *Compiler) recordProductView(userID, productID, intent string) {
	if userID == "" || productID == "" {
		return
	}
	c.db.Exec(
		`INSERT INTO product_views (user_id, product_id, view_type, source, viewed_at) VALUES (?, ?, ?, 'compiler', CURRENT_TIMESTAMP)`,
		userID, productID, intent,
	)
}

func (c *Compiler) getEffectiveLanguage(platformID, detectedLang string) string {
	if detectedLang == "ar" || detectedLang == "en" || detectedLang == "ku" {
		return detectedLang
	}
	sysLang := c.configMgr.GetSystem().Language
	if sysLang == "" {
		return "en"
	}
	return sysLang
}

func (c *Compiler) compileAction(ticket *nnlp.ProcessResult, notification *listener.Notification, userData map[string]interface{}) (*shared.AutomationInstruction, error) {
	switch ticket.Action {
	case "block":
		return c.compileBlock(ticket, notification, userData)
	case "unfollow":
		return c.compileUnfollow(ticket, notification, userData)
	case "noop":
		return c.compileNoop(ticket, notification)
	case "queued":
		return c.compileQueued(ticket, notification)
	case "auto_heart", "react", "like":
		return c.compileAutoHeart(ticket, notification, userData)
	case "send_greeting":
		return c.compileGreeting(ticket, notification, userData)
	case "send_store_info":
		return c.compileStoreInfo(ticket, notification, userData)
	case "send_product":
		return c.compileSendProduct(ticket, notification, userData)
	case "send_order_template":
		return c.compileOrderTemplate(ticket, notification, userData)
	case "pack_price":
		return c.compilePackOrder(ticket, notification, userData)
	case "send_stock_warning":
		return c.compileStockWarning(ticket, notification, userData)
	case "send_cancellation":
		return c.compileCancellation(ticket, notification, userData)
	case "send_fallback":
		return c.compileFallback(ticket, notification, userData)
	case "ask_product":
		return c.compileAskProduct(ticket, notification, userData)
	case "ask_for_order":
		return c.compileAskForOrder(ticket, notification, userData)
	case "ask_order_confirmation":
		return c.compileAskOrderConfirmation(ticket, notification, userData)
	case "ai_ticket":
		return c.compileAITicket(ticket, notification, userData)
	case "ai_response":
		return c.compileAIResponse(ticket, notification, userData)
	case "share":
		return c.compileShare(ticket, notification, userData)
	case "save":
		return c.compileSave(ticket, notification, userData)
	case "follow":
		return c.compileFollow(ticket, notification, userData)
	default:
		return nil, fmt.Errorf("unknown action: %s", ticket.Action)
	}
}

func (c *Compiler) steps(platform, action string, notification *listener.Notification, message string, extra map[string]interface{}) []shared.InstructionStep {
	if platform == "whatsapp" {
		return c.whatsappSteps(action, notification, message, extra)
	}
	return c.browserSteps(platform, notification, message)
}

func (c *Compiler) whatsappSteps(action string, notification *listener.Notification, message string, extra map[string]interface{}) []shared.InstructionStep {
	base := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 100}}
	chatJID, _ := notification.RawData["chat_jid"].(string)
	msgID := ""
	if notification.Message != nil {
		msgID = notification.Message.MessageID
	}
	if mid, ok := extra["message_id"].(string); ok && mid != "" {
		msgID = mid
	}
	switch action {
	case "react", "like", "auto_heart":
		emoji := "❤️"
		if e, ok := extra["emoji"].(string); ok && e != "" {
			emoji = e
		}
		fromMe, _ := extra["from_me"].(bool)
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeReact,
			Description: "React to message",
			Options:     map[string]interface{}{"to": chatJID, "message_id": msgID, "from_me": fromMe, "emoji": emoji},
			DelayAfter:  500,
		})
	case "block":
		target := ""
		if jid, ok := extra["user_jid"].(string); ok && jid != "" {
			target = jid
		}
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeBlock,
			Description: "Block user",
			Options:     map[string]interface{}{"to": target},
			DelayAfter:  1000,
		})
	case "unfollow":
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeLog,
			Description: "Unfollow not applicable for WhatsApp",
		})
	case "upload":
		filePath, _ := extra["file_path"].(string)
		mediaType, _ := extra["media_type"].(string)
		if mediaType == "" {
			mediaType = "image"
		}
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeUpload,
			Value:       filePath,
			Description: "Upload media",
			Options:     map[string]interface{}{"to": chatJID, "media_type": mediaType, "caption": message},
			DelayAfter:  2000,
		})
	default:
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeSendMessage,
			Value:       message,
			Description: "Send message",
			Options:     map[string]interface{}{"to": chatJID},
			DelayAfter:  1000,
		})
	}
}

func (c *Compiler) browserSteps(platform string, notification *listener.Notification, message string) []shared.InstructionStep {
	base := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 100}}
	switch platform {
	case "facebook":
		desc := "Send reply"
		if notification.Type == listener.NotificationTypeComment {
			desc = "Send comment reply"
		}
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeReply,
			Value:       message,
			Description: desc,
			DelayAfter:  1000,
		})
	case "instagram":
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeReply,
			Value:       message,
			Description: "Send reply",
			DelayAfter:  1000,
		})
	default:
		opts := map[string]interface{}{}
		if notification.Message != nil && notification.Message.ConversationID != "" {
			opts["to"] = notification.Message.ConversationID
			opts["chat_id"] = notification.Message.ConversationID
		}
		return append(base, shared.InstructionStep{
			Type:        shared.StepTypeSendMessage,
			Value:       message,
			Description: "Send message",
			Options:     opts,
			DelayAfter:  1000,
		})
	}
}

func (c *Compiler) compileNoop(ticket *nnlp.ProcessResult, n *listener.Notification) (*shared.AutomationInstruction, error) {
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "noop", Intent: ticket.Intent, NotificationID: n.ID,
		Steps:     []shared.InstructionStep{{Type: shared.StepTypeLog, Description: "No action required"}},
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileQueued(ticket *nnlp.ProcessResult, n *listener.Notification) (*shared.AutomationInstruction, error) {
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, Action: "queued", Intent: ticket.Intent,
		Steps:     []shared.InstructionStep{{Type: shared.StepTypeLog, Description: "Queued for processing"}},
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileBlock(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	if blocked, _ := ud["is_blocked"].(bool); blocked {
		return c.compileNoop(ticket, n)
	}
	info := c.extractUserInfo(n)
	extra := map[string]interface{}{"user_id": info["user_id"], "username": info["username"], "user_jid": info["user_jid"]}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "block", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 10,
		Data:      map[string]interface{}{"user_id": info["user_id"], "username": info["username"], "reason": ticket.Intent},
		Steps:     c.steps(n.PlatformID, "block", n, "", extra),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileUnfollow(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	info := c.extractUserInfo(n)
	extra := map[string]interface{}{"user_id": info["user_id"], "username": info["username"], "user_jid": info["user_jid"]}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "unfollow", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 5,
		Data:      map[string]interface{}{"user_id": info["user_id"], "username": info["username"]},
		Steps:     c.steps(n.PlatformID, "unfollow", n, "", extra),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileAutoHeart(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	postURL := ""
	if n.Comment != nil {
		postURL = n.Comment.PostURL
	}
	extra := map[string]interface{}{"post_url": postURL, "emoji": "❤️", "from_me": true}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "auto_heart", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 1, Timeout: 15 * time.Second, Priority: 3,
		Data:      map[string]interface{}{"post_url": postURL},
		Steps:     c.steps(n.PlatformID, "react", n, "", extra),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileGreeting(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	store := c.configMgr.GetStore()
	name := c.displayName(ud)
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("👋 Hello %s! Welcome to %s. How can I help you today?", name, store.Name),
		"ar": fmt.Sprintf("👋 مرحباً %s! أهلاً وسهلاً في %s. كيف يمكنني مساعدتك اليوم؟", name, store.Name),
		"ku": fmt.Sprintf("👋 سڵاو %s! بەخێربێیت بۆ %s. چۆن دەتوانم یارمەتیت بدەم ئەمڕۆ؟", name, store.Name),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_greeting", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 4,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_greeting", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileStoreInfo(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	store := c.configMgr.GetStore()
	contact := store.Contact.Phone
	if contact == "" {
		contact = store.Contact.Email
	}
	hours := store.BusinessHours["default"]
	if hours == "" {
		hours = "Please contact us"
	}
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("🏪 Store Information\n\n🏷️ Name: %s\n📱 Contact: %s\n🌐 Address: %s\n⏰ Hours: %s\n\n%s\n\nHow can we help you today?", store.Name, contact, store.Address, hours, store.Description),
		"ar": fmt.Sprintf("🏪 معلومات المتجر\n\n🏷️ الاسم: %s\n📱 الاتصال: %s\n🌐 العنوان: %s\n⏰ ساعات العمل: %s\n\n%s\n\nكيف يمكننا مساعدتك اليوم؟", store.Name, contact, store.Address, hours, store.Description),
		"ku": fmt.Sprintf("🏪 زانیاری دوکان\n\n🏷️ ناو: %s\n📱 پەیوەندی: %s\n🌐 ناونیشان: %s\n⏰ کاتەکانی کار: %s\n\n%s\n\nچۆن دەتوانین یارمەتیت بدەین ئەمڕۆ؟", store.Name, contact, store.Address, hours, store.Description),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_store_info", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 5,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_store_info", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileSendProduct(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	pd := c.productData(ticket, n)
	if pd == nil {
		return c.compileFallback(ticket, n, ud)
	}
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	name, _ := pd["name"].(string)
	price, _ := pd["price"].(float64)
	cur := c.currency(pd)
	desc, _ := pd["description"].(string)
	stock, _ := pd["stock"].(int64)
	sku, _ := pd["sku"].(string)
	if userID, _ := ud["id"].(string); userID != "" {
		if pid, _ := pd["id"].(string); pid != "" {
			c.recordProductView(userID, pid, ticket.Intent)
		}
	}
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("📦 Product Information\n\n🏷️ Name: %s\n💰 Price: %.2f %s\n📝 Description: %s\n📊 Stock: %d units\n🔢 SKU: %s\n\nWould you like to order?", name, price, cur, desc, stock, sku),
		"ar": fmt.Sprintf("📦 معلومات المنتج\n\n🏷️ الاسم: %s\n💰 السعر: %.2f %s\n📝 الوصف: %s\n📊 المخزون: %d وحدة\n🔢 رمز المنتج: %s\n\nهل ترغب في الطلب؟", name, price, cur, desc, stock, sku),
		"ku": fmt.Sprintf("📦 زانیاری بەرهەم\n\n🏷️ ناو: %s\n💰 نرخ: %.2f %s\n📝 وەسف: %s\n📊 کۆگا: %d دانە\n🔢 کۆدی بەرهەم: %s\n\nحەز دەکەیت داوا بکەیت؟", name, price, cur, desc, stock, sku),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_product", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"product": pd, "message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_product", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// errInsufficientStock signals that stock ran out during order creation, so the
// caller can route to compileStockWarning instead of a generic fallback.
var errInsufficientStock = errors.New("insufficient stock")

// createConfirmedOrder reserves stock and creates an order plus its single line
// item in one transaction. Orders are created as 'confirmed' (not 'pending')
// since by the time this runs the customer has already confirmed; this is what
// makes update_stock_on_order_confirmed fire, and what makes a later cancellation
// correctly restore real stock instead of inflating it.
// stockQty is what's checked against and reserved from products.stock (always
// in individual items). lineQty/unitPrice are what's recorded on the order_items
// row: for a plain order these equal stockQty/per-item price; for a pack order
// stockQty is the item count (for stock accounting) while lineQty/unitPrice stay
// in pack terms (e.g. 3 packs at the per-pack price) for correct order display.
func (c *Compiler) createConfirmedOrder(userID, platform, productID string, stockQty, lineQty int, unitPrice, total float64, productName string, sku interface{}, shippingAddr string) (string, error) {
	tx, err := c.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var stock, reserved int
	if err := tx.QueryRow(`SELECT stock, reserved_stock FROM products WHERE id = ?`, productID).Scan(&stock, &reserved); err != nil {
		return "", err
	}
	if stock-reserved < stockQty {
		return "", errInsufficientStock
	}
	if _, err := tx.Exec(`UPDATE products SET reserved_stock = reserved_stock + ? WHERE id = ?`, stockQty, productID); err != nil {
		return "", err
	}

	orderID := uuid.New().String()
	if _, err := tx.Exec(`
		INSERT INTO orders (id, user_id, platform, status, total, subtotal, discount_amount, shipping_address, payment_status, created_at)
		VALUES (?, ?, ?, 'confirmed', ?, ?, 0, ?, 'pending', CURRENT_TIMESTAMP)`,
		orderID, userID, platform, total, total, shippingAddr); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`
		INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price, product_name, product_sku)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		orderID, productID, lineQty, unitPrice, total, productName, sku); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`UPDATE platform_users SET total_orders = total_orders + 1, total_spent = total_spent + ? WHERE id = ?`, total, userID); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return orderID, nil
}

func (c *Compiler) compileOrderTemplate(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	pd := c.productData(ticket, n)
	if pd == nil {
		return c.compileFallback(ticket, n, ud)
	}
	userID, _ := ud["id"].(string)
	if userID == "" {
		return c.compileFallback(ticket, n, ud)
	}
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	name, _ := pd["name"].(string)
	price, _ := pd["price"].(float64)
	cur := c.currency(pd)
	quantity := 1
	if q, ok := ticket.Data["suggested_quantity"].(int); ok && q > 0 {
		quantity = q
	}
	if q, ok := ticket.Data["quantity"].(int); ok && q > 0 {
		quantity = q
	}
	deliveryDetails, _ := ticket.Data["delivery_details"].(map[string]string)

	if ticket.Intent == "order_intent" {
		total := float64(quantity) * price
		msg := c.tmpl(map[string]string{
			"en": fmt.Sprintf("🛒 Order Summary\n\n📦 Product: %s\n🔢 Quantity: %d\n💰 Price: %.2f %s each\n💵 Total: %.2f %s\n\nPlease reply CONFIRM to place this order, or CANCEL.", name, quantity, price, cur, total, cur),
			"ar": fmt.Sprintf("🛒 ملخص الطلب\n\n📦 المنتج: %s\n🔢 الكمية: %d\n💰 السعر: %.2f %s\n💵 الإجمالي: %.2f %s\n\nرد بـ تأكيد للمتابعة، أو إلغاء.", name, quantity, price, cur, total, cur),
			"ku": fmt.Sprintf("🛒 پوختەی داواکاری\n\n📦 بەرهەم: %s\n🔢 ژمارە: %d\n💰 نرخ: %.2f %s\n💵 کۆ: %.2f %s\n\nوەڵام بدەرەوە دڵنیام یان هەڵیوەشێنەوە.", name, quantity, price, cur, total, cur),
		}, lang)
		extra := map[string]interface{}{"product": pd, "quantity": quantity, "delivery_details": deliveryDetails}
		c.updateUserStateAfterAI(userID, "awaiting_confirmation", "")
		return &shared.AutomationInstruction{
			Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
			TicketID: ticket.TicketID, Action: "ask_order_confirmation", Intent: "awaiting_confirmation", NotificationID: n.ID,
			OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
			Data:      extra,
			Steps:     c.steps(n.PlatformID, "ask_order_confirmation", n, msg, nil),
			CreatedAt: time.Now(),
		}, nil
	}
	if ticket.Intent == "order_intent_confirmed" {
		total := float64(quantity) * price
		pid, _ := pd["id"].(string)
		shippingAddr := ""
		if deliveryDetails != nil {
			if a, ok := deliveryDetails["has_address"]; ok && a == "true" {
				shippingAddr = "Address provided"
			}
		}
		orderID, err := c.createConfirmedOrder(userID, n.PlatformID, pid, quantity, quantity, price, total, name, pd["sku"], shippingAddr)
		if errors.Is(err, errInsufficientStock) {
			return c.compileStockWarning(ticket, n, ud)
		}
		if err != nil {
			return c.compileFallback(ticket, n, ud)
		}
		msg := c.tmpl(map[string]string{
			"en": fmt.Sprintf("✅ Order confirmed! Order #%s\nTotal: %.2f %s\nWe'll process it shortly. Thank you!", orderID, total, cur),
			"ar": fmt.Sprintf("✅ تم تأكيد الطلب! رقم الطلب #%s\nالإجمالي: %.2f %s\nسنقوم بمعالجته قريباً. شكراً لك!", orderID, total, cur),
			"ku": fmt.Sprintf("✅ داواکاری پشتڕاستکرایەوە! ژمارەی داواکاری #%s\nکۆ: %.2f %s\nزود بەیەوە دەچێتە ڕێ. سوپاس!", orderID, total, cur),
		}, lang)
		return &shared.AutomationInstruction{
			Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
			TicketID: ticket.TicketID, Action: "send_order_template", Intent: "order_completed", NotificationID: n.ID,
			OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
			Data:      map[string]interface{}{"order_id": orderID, "message": msg, "language": lang},
			Steps:     c.steps(n.PlatformID, "send_order_template", n, msg, nil),
			CreatedAt: time.Now(),
		}, nil
	}
	return c.compileFallback(ticket, n, ud)
}

func (c *Compiler) compilePackOrder(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	packCount, ok1 := ticket.Data["pack_count"].(int)
	pricePerPack, ok2 := ticket.Data["price_per_pack"].(float64)
	totalPrice, ok3 := ticket.Data["total_price"].(float64)
	product, ok4 := ticket.Data["product"].(map[string]interface{})
	itemsPerPack, _ := ticket.Data["items_per_pack"].(int64)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return c.compileFallback(ticket, n, ud)
	}
	userID, _ := ud["id"].(string)
	if userID == "" {
		return c.compileFallback(ticket, n, ud)
	}
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	cur := c.currency(product)
	name, _ := product["name"].(string)
	pid, _ := product["id"].(string)
	quantityItems := int(itemsPerPack) * packCount
	total := totalPrice
	orderID, err := c.createConfirmedOrder(userID, n.PlatformID, pid, quantityItems, packCount, pricePerPack, total, name, product["sku"], "")
	if errors.Is(err, errInsufficientStock) {
		return c.compileStockWarning(ticket, n, ud)
	}
	if err != nil {
		return c.compileFallback(ticket, n, ud)
	}
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("✅ Pack order confirmed! %d box(es) of %s (%d items). Order #%s\nTotal: %.2f %s\nThank you!", packCount, name, quantityItems, orderID, total, cur),
		"ar": fmt.Sprintf("✅ تم تأكيد طلب العبوة! %d صندوق من %s (%d قطعة). رقم الطلب #%s\nالإجمالي: %.2f %s\nشكراً!", packCount, name, quantityItems, orderID, total, cur),
		"ku": fmt.Sprintf("✅ داواکاری پاکەت پشتڕاستکرایەوە! %d کارتۆنی %s (%d دانە). ژمارەی داواکاری #%s\nکۆ: %.2f %s\nسوپاس!", packCount, name, quantityItems, orderID, total, cur),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_order_template", Intent: "order_completed", NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
		Data:      map[string]interface{}{"order_id": orderID, "message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_order_template", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileStockWarning(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	pd, _ := ticket.Data["product"].(map[string]interface{})
	if pd == nil {
		return c.compileFallback(ticket, n, ud)
	}
	reqQty, _ := ticket.Data["requested_quantity"].(int)
	avail, _ := ticket.Data["available_stock"].(int)
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	name, _ := pd["name"].(string)
	price, _ := pd["price"].(float64)
	cur := c.currency(pd)
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("⚠️ Stock Warning\n\n📦 %s\n💰 %.2f %s\nRequested: %d | Available: %d\n\n1. Order %d now\n2. Wait for restock\n3. Similar products?", name, price, cur, reqQty, avail, avail),
		"ar": fmt.Sprintf("⚠️ تحذير المخزون\n\n📦 %s\n💰 %.2f %s\nمطلوب: %d | متاح: %d\n\n1. اطلب %d الآن\n2. انتظر\n3. منتجات مشابهة؟", name, price, cur, reqQty, avail, avail),
		"ku": fmt.Sprintf("⚠️ ئاگاداری کۆگا\n\n📦 %s\n💰 %.2f %s\nداواکراو: %d | بەردەست: %d\n\n1. داوا بکە %d\n2. چاوەڕێ\n3. هاوشێوە؟", name, price, cur, reqQty, avail, avail),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_stock_warning", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"product": pd, "requested": reqQty, "available": avail, "message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_stock_warning", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileCancellation(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	userID, _ := ud["id"].(string)
	if userID == "" {
		return c.compileFallback(ticket, n, ud)
	}
	var orderID, productID string
	var qty int
	err := c.db.QueryRow(`
		SELECT id, product_id, quantity FROM orders WHERE user_id = ? AND status = 'confirmed' ORDER BY created_at DESC LIMIT 1`,
		userID).Scan(&orderID, &productID, &qty)
	if err == nil && orderID != "" {
		tx, _ := c.db.Begin()
		tx.Exec(`UPDATE products SET reserved_stock = reserved_stock - ? WHERE id = ?`, qty, productID)
		tx.Exec(`UPDATE orders SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP WHERE id = ?`, orderID)
		tx.Commit()
	}
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	name := c.displayName(ud)
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("Dear %s, your order has been cancelled.", name),
		"ar": fmt.Sprintf("عزيزي %s، تم إلغاء طلبك.", name),
		"ku": fmt.Sprintf("بەڕێز %s، داواکاریەکەت هەڵوەشایەوە.", name),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_cancellation", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 6,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_cancellation", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileFallback(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	store := c.configMgr.GetStore()
	contact := store.Contact.Phone
	if contact == "" {
		contact = store.Contact.Email
	}
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("I'm not sure I understood. For better help, call %s or ask about products/orders.", contact),
		"ar": fmt.Sprintf("لم أفهم. للمساعدة، اتصل %s أو اسأل عن منتجات/طلبات.", contact),
		"ku": fmt.Sprintf("تێنەگەیشتم. بۆ یارمەتی، پەیوەندی بکە %s یان پرسیاری بەرهەم/داواکاری بکە.", contact),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_fallback", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 1, Timeout: 15 * time.Second, Priority: 3,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_fallback", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileAskProduct(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	msg := c.tmpl(map[string]string{
		"en": "What product are you looking for? Name or code?",
		"ar": "ما المنتج الذي تبحث عنه؟ الاسم أو الرمز؟",
		"ku": "بەرهەمی چیت دەوێ؟ ناو یان کۆد؟",
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_product", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 6,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "ask_product", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileAskForOrder(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	msg := c.tmpl(map[string]string{
		"en": "To order, tell me product name/code, quantity, and delivery address.",
		"ar": "للطلب، أخبرني باسم/رمز المنتج، الكمية، وعنوان التسليم.",
		"ku": "بۆ داواکاری، ناوی بەرهەم/کۆد، ژمارە، و ناونیشانی گەیاندن بنووسە.",
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_for_order", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 6,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "ask_for_order", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileAskOrderConfirmation(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	msg := c.tmpl(map[string]string{
		"en": "Review order and reply CONFIRM, CANCEL, or CHANGE.",
		"ar": "راجع الطلب ورد بـ تأكيد، إلغاء، أو تغيير.",
		"ku": "داواکاری بپشکنە و وەڵام بدەرەوە دڵنیام، هەڵیوەشێنەوە، یان گۆڕانکاری.",
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_order_confirmation", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "ask_order_confirmation", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileAITicket(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	if c.llmClient == nil || !c.llmClient.Enabled() {
		return c.compileFallback(ticket, n, ud)
	}
	userID, _ := ud["id"].(string)
	if userID == "" {
		return c.compileFallback(ticket, n, ud)
	}
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	userMsg := c.notifText(n)
	history, _ := c.getConversationHistory(userID, 5)
	var historyStrs []string
	for _, m := range history {
		dir, _ := m["direction"].(string)
		text, _ := m["text"].(string)
		if dir == "outgoing" {
			historyStrs = append(historyStrs, "assistant: "+text)
		} else {
			historyStrs = append(historyStrs, "user: "+text)
		}
	}
	productCtx, _ := ticket.Data["product_context"].(map[string]interface{})
	req := comms.ReplyRequest{
		UserMessage:    userMsg,
		Language:       lang,
		History:        historyStrs,
		ProductContext: productCtx,
		PlatformID:     n.PlatformID,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := c.llmClient.GenerateReply(ctx, req)
	if err != nil {
		log.Printf("[Compiler] LLM error: %v — fallback", err)
		c.recordUrgentMessage(userID, n.PlatformID, "escalation", userMsg, ticket.TicketID)
		return c.compileFallback(ticket, n, ud)
	}
	if result.Ambiguous || strings.Contains(result.Text, comms.AmbiguousMarker) {
		c.updateUserStateAfterAI(userID, "ambiguous_query", "")
		return c.compileFallback(ticket, n, ud)
	}
	c.updateUserStateAfterAI(userID, "ai_response", result.Text)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ai_response", Intent: "ai_response",
		NotificationID: n.ID, OriginalText: userMsg, MaxRetries: 1, Timeout: 30 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"message": result.Text, "language": lang},
		Steps:     c.steps(n.PlatformID, "ai_response", n, result.Text, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileAIResponse(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	if c.llmClient == nil || !c.llmClient.Enabled() {
		return c.compileFallback(ticket, n, ud)
	}
	userID, _ := ud["id"].(string)
	if userID == "" {
		return c.compileFallback(ticket, n, ud)
	}
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	userMsg := c.notifText(n)
	history, _ := c.getConversationHistory(userID, 5)
	var historyStrs []string
	for _, m := range history {
		dir, _ := m["direction"].(string)
		text, _ := m["text"].(string)
		if dir == "outgoing" {
			historyStrs = append(historyStrs, "assistant: "+text)
		} else {
			historyStrs = append(historyStrs, "user: "+text)
		}
	}
	productCtx, _ := ticket.Data["product_context"].(map[string]interface{})
	req := comms.ReplyRequest{
		UserMessage:    userMsg,
		Language:       lang,
		History:        historyStrs,
		ProductContext: productCtx,
		PlatformID:     n.PlatformID,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := c.llmClient.GenerateReply(ctx, req)
	if err != nil {
		log.Printf("[Compiler] LLM error: %v — fallback", err)
		c.recordUrgentMessage(userID, n.PlatformID, "escalation", userMsg, ticket.TicketID)
		return c.compileFallback(ticket, n, ud)
	}
	if result.Ambiguous {
		return c.compileFallback(ticket, n, ud)
	}
	c.updateUserStateAfterAI(userID, "ai_response", result.Text)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ai_response", Intent: "ai_response",
		NotificationID: n.ID, OriginalText: userMsg, MaxRetries: 1, Timeout: 30 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"message": result.Text, "language": lang},
		Steps:     c.steps(n.PlatformID, "ai_response", n, result.Text, nil),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileShare(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	postURL := ""
	if n.Comment != nil {
		postURL = n.Comment.PostURL
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "share", Intent: ticket.Intent, NotificationID: n.ID,
		MaxRetries: 1, Timeout: 15 * time.Second, Priority: 4,
		Data:      map[string]interface{}{"post_url": postURL},
		Steps:     c.steps(n.PlatformID, "share", n, "", map[string]interface{}{"post_url": postURL}),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileSave(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	postURL := ""
	if n.Comment != nil {
		postURL = n.Comment.PostURL
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "save", Intent: ticket.Intent, NotificationID: n.ID,
		MaxRetries: 1, Timeout: 15 * time.Second, Priority: 3,
		Data:      map[string]interface{}{"post_url": postURL},
		Steps:     c.steps(n.PlatformID, "save", n, "", map[string]interface{}{"post_url": postURL}),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) compileFollow(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	info := c.extractUserInfo(n)
	username, _ := info["username"].(string)
	extra := map[string]interface{}{"user_id": info["user_id"], "username": username, "user_jid": info["user_jid"]}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "follow", Intent: ticket.Intent, NotificationID: n.ID,
		MaxRetries: 1, Timeout: 15 * time.Second, Priority: 4,
		Data:      map[string]interface{}{"user_id": info["user_id"], "username": username},
		Steps:     c.steps(n.PlatformID, "follow", n, "", extra),
		CreatedAt: time.Now(),
	}, nil
}

func (c *Compiler) extractNotification(ticket *nnlp.ProcessResult) *listener.Notification {
	if n, ok := ticket.Data["notification"].(*listener.Notification); ok {
		return n
	}
	return nil
}

func (c *Compiler) extractUserInfo(n *listener.Notification) map[string]interface{} {
	info := map[string]interface{}{}
	if n.Message != nil {
		info["user_id"] = n.Message.Sender.UserID
		info["username"] = n.Message.Sender.Username
		info["display_name"] = n.Message.Sender.DisplayName
	}
	if n.Comment != nil {
		info["user_id"] = n.Comment.CommentAuthor.UserID
		info["username"] = n.Comment.CommentAuthor.Username
		info["display_name"] = n.Comment.CommentAuthor.DisplayName
	}
	if n.RawData != nil {
		if jid, ok := n.RawData["sender_jid"].(string); ok {
			info["user_jid"] = jid
		}
	}
	return info
}

func (c *Compiler) productData(ticket *nnlp.ProcessResult, n *listener.Notification) map[string]interface{} {
	// 1. Try from the ticket (processor puts it here)
	if pd, ok := ticket.Data["product"].(map[string]interface{}); ok && pd != nil {
		return pd
	}
	// 2. Try from notification raw data
	if n.RawData != nil {
		if pd, ok := n.RawData["product_data"].(map[string]interface{}); ok {
			return pd
		}
	}
	return nil
}

func (c *Compiler) notifText(n *listener.Notification) string {
	if n.Message != nil && n.Message.Text != "" {
		return n.Message.Text
	}
	if n.Comment != nil {
		return n.Comment.CommentText
	}
	return ""
}

func (c *Compiler) tmpl(templates map[string]string, lang string) string {
	if v, ok := templates[lang]; ok {
		return v
	}
	return templates["en"]
}

func (c *Compiler) currency(pd map[string]interface{}) string {
	if cur, ok := pd["currency"].(string); ok && cur != "" {
		return cur
	}
	if cur := c.configMgr.GetStore().Currency; cur != "" {
		return cur
	}
	return ""
}

func (c *Compiler) displayName(ud map[string]interface{}) string {
	if v, ok := ud["display_name"].(string); ok && v != "" {
		return v
	}
	if v, ok := ud["username"].(string); ok && v != "" {
		return v
	}
	return ""
}
