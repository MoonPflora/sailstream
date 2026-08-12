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

// RateLimiter's CanProceed now checks AND reserves atomically in one call
// (see Maestro.CanProceed) — a caller that gets true has already consumed
// the slot. ReleaseUsage exists only to give that slot back if the caller
// ends up not using it (e.g. compile failed right after a successful
// reservation).
type RateLimiter interface {
	CanProceed(platform, subtypeID, action string) (bool, time.Duration)
	ReleaseUsage(platform, subtypeID, action string)
}

// LLMRateLimiter tracks LLM provider tokens/min and cost as a separate resource axis.

type LLMRateLimiter struct {
	mu              sync.Mutex
	tokensPerMinute int           // max tokens allowed per rolling window
	costPerMinute   float64       // max cost allowed per rolling window
	window          time.Duration // rolling window (default 1 min)
	tokens          []rateRecord
	costs           []rateRecord
}

type rateRecord struct {
	value float64
	at    time.Time
}

func NewLLMRateLimiter(tokensPerMin int, costPerMin float64) *LLMRateLimiter {
	return &LLMRateLimiter{
		tokensPerMinute: tokensPerMin,
		costPerMinute:   costPerMin,
		window:          1 * time.Minute,
	}
}

func (rl *LLMRateLimiter) Allow(tokens float64, cost float64) (bool, time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-rl.window)

	// Prune old records
	var activeTokens []rateRecord
	for _, r := range rl.tokens {
		if r.at.After(cutoff) {
			activeTokens = append(activeTokens, r)
		}
	}
	rl.tokens = activeTokens
	var activeCosts []rateRecord
	for _, r := range rl.costs {
		if r.at.After(cutoff) {
			activeCosts = append(activeCosts, r)
		}
	}
	rl.costs = activeCosts

	// Sum current usage
	var totalTokens float64
	for _, r := range rl.tokens {
		totalTokens += r.value
	}
	var totalCost float64
	for _, r := range rl.costs {
		totalCost += r.value
	}

	// Check limits
	if rl.tokensPerMinute > 0 && totalTokens+tokens > float64(rl.tokensPerMinute) {
		oldest := now
		if len(rl.tokens) > 0 {
			oldest = rl.tokens[0].at
		}
		retryAfter := rl.window - now.Sub(oldest)
		if retryAfter < 0 {
			retryAfter = time.Second
		}
		return false, retryAfter
	}
	if rl.costPerMinute > 0 && totalCost+cost > rl.costPerMinute {
		oldest := now
		if len(rl.costs) > 0 {
			oldest = rl.costs[0].at
		}
		retryAfter := rl.window - now.Sub(oldest)
		if retryAfter < 0 {
			retryAfter = time.Second
		}
		return false, retryAfter
	}
	return true, 0
}

// LLMRateLimiterSnapshot exposes current usage stats for the dashboard.
type LLMRateLimiterSnapshot struct {
	TokensPerMinute int     `json:"tokens_per_minute"`
	CostPerMinute   float64 `json:"cost_per_minute"`
	CurrentTokens   float64 `json:"current_tokens"`
	CurrentCost     float64 `json:"current_cost"`
}

func (rl *LLMRateLimiter) Snapshot() LLMRateLimiterSnapshot {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-rl.window)
	var totalTokens float64
	for _, r := range rl.tokens {
		if r.at.After(cutoff) {
			totalTokens += r.value
		}
	}
	var totalCost float64
	for _, r := range rl.costs {
		if r.at.After(cutoff) {
			totalCost += r.value
		}
	}
	return LLMRateLimiterSnapshot{
		TokensPerMinute: rl.tokensPerMinute,
		CostPerMinute:   rl.costPerMinute,
		CurrentTokens:   totalTokens,
		CurrentCost:     totalCost,
	}
}

func (rl *LLMRateLimiter) Record(tokens float64, cost float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	rl.tokens = append(rl.tokens, rateRecord{value: tokens, at: now})
	rl.costs = append(rl.costs, rateRecord{value: cost, at: now})
}

type Compiler struct {
	db              *sql.DB
	configMgr       *config.ConfigManager
	llmClient       *comms.Client
	rateLimiter     RateLimiter
	llmRateLimiter  *LLMRateLimiter
	sandboxMode     bool
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
	Instruction  *shared.AutomationInstruction `json:"instruction"`
	Ticket       *nnlp.ProcessResult           `json:"ticket"`
	Notification *listener.Notification        `json:"notification"`
	Priority     int                           `json:"priority"`
	QueuedAt     time.Time                     `json:"queued_at"`
	Attempts     int                           `json:"attempts"`
	MaxAttempts  int                           `json:"max_attempts"`
	Status       string                        `json:"status"`
	Error        string                        `json:"error"`
	LastAttempt  time.Time                     `json:"last_attempt"`
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

func NewCompiler(db *sql.DB, configMgr *config.ConfigManager, llmClient *comms.Client, rl RateLimiter, sandboxMode bool) *Compiler {
	// LLM rate limit defaults — separate resource axis from platform messaging limits
	// Read from config (system.llm_tokens_per_minute / system.llm_cost_per_minute) with fallback
	llmTokensPerMin := 10000
	llmCostPerMin := 0.0
	if cfg := configMgr.GetConfig(); cfg != nil && cfg.System.LLMTokensPerMinute > 0 {
		llmTokensPerMin = cfg.System.LLMTokensPerMinute
	}
	if cfg := configMgr.GetConfig(); cfg != nil && cfg.System.LLMCostPerMinute > 0 {
		llmCostPerMin = cfg.System.LLMCostPerMinute
	}
	c := &Compiler{
		db:              db,
		configMgr:       configMgr,
		llmClient:       llmClient,
		rateLimiter:     rl,
		llmRateLimiter:  NewLLMRateLimiter(llmTokensPerMin, llmCostPerMin),
		sandboxMode:     sandboxMode,
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
	if c.sandboxMode {
		return true, 0
	}
	if c.rateLimiter == nil {
		return true, 0
	}
	return c.rateLimiter.CanProceed(platform, subtypeID, action)
}

func (c *Compiler) checkLLMRateLimit() (bool, time.Duration) {
	if c.llmRateLimiter == nil {
		return true, 0
	}
	// Estimate tokens: average response ~500 tokens, cost ~$0.01
	return c.llmRateLimiter.Allow(500, 0.01)
}

func (c *Compiler) LLMRateLimiterSnapshot() LLMRateLimiterSnapshot {
	if c.llmRateLimiter == nil {
		return LLMRateLimiterSnapshot{}
	}
	return c.llmRateLimiter.Snapshot()
}

func (c *Compiler) recordLLMUsage(tokens float64, cost float64) {
	if c.llmRateLimiter != nil {
		c.llmRateLimiter.Record(tokens, cost)
	}
}

// recordRateLimitUsage no longer increments any counter itself — checkRateLimits
// (CanProceed) already reserved the slot atomically before compileAction ran.
// This just bumps the platform's LastActive metadata on a successful compile.
func (c *Compiler) recordRateLimitUsage(action, platform, subtypeID string) {
	if c.sandboxMode {
		return
	}
	meta := c.configMgr.GetPlatformMetadata(platform)
	meta.LastActive = time.Now().Format(time.RFC3339)
	c.configMgr.SetPlatformMetadata(platform, meta)
}

// releaseRateLimitUsage gives back the slot checkRateLimits reserved when
// compileAction ends up failing — otherwise a compile error would
// permanently burn a rate-limit slot for nothing ever sent.
func (c *Compiler) releaseRateLimitUsage(action, platform, subtypeID string) {
	if c.sandboxMode || c.rateLimiter == nil {
		return
	}
	c.rateLimiter.ReleaseUsage(platform, subtypeID, action)
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
		"block":                     10,
		"ai_ticket":                 9,
		"ai_response":               9,
		"pack_price":                8,
		"send_order_template":       8,
		"ask_order_confirmation":    8,
		"ask_product_confirmation":  7,
		"send_compatibility_answer": 7,
		"send_order_status":         7,
		"send_product":              7,
		"send_stock_warning":        7,
		"send_store_info":           6,
		"ask_for_order":             6,
		"ask_product":               6,
		"ask_product_name":          6,
		"ask_product_name_or_image": 6,
		"ask_clarify_product":       6,
		"send_cancellation":         6,
		"send_greeting":             5,
		"send_message":              5,
		"unfollow":                  5,
		"auto_heart":                3,
		"react":                     3,
		"follow":                    3,
		"send_fallback":             2,
		"queued":                    2,
		"noop":                      1,
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
		// checkRateLimits already reserved a slot above — give it back
		// since nothing is actually going to be sent for this ticket.
		c.releaseRateLimitUsage(ticket.Action, notification.PlatformID, notification.SubtypeID)
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
	case "order_intent", "order_intent_detected", "awaiting_confirmation",
		"awaiting_quantity", "order_intent_confirmed", "awaiting_order_details":
		// awaiting_quantity used to map to the literal string
		// "awaiting_quantity", which isn't one of the four values allowed by
		// platform_users' conversation_state CHECK constraint
		// ('idle', 'browsing', 'ordering', 'support') — that UPDATE was
		// silently failing and rolling back the whole state write. See the
		// matching fix in processor.go's intentToConversationState for the
		// full explanation; this is the same bug in the AI-reply path's
		// separate copy of this mapping.
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

// compileAction dispatches to the correct compile function based on the ticket action.
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
	case "ask_quantity":
		return c.compileAskQuantity(ticket, notification, userData)
	case "send_delivery_cost":
		return c.compileDeliveryCost(ticket, notification, userData)
	case "pack_price":
		return c.compilePackOrder(ticket, notification, userData)
	case "send_stock_warning":
		return c.compileStockWarning(ticket, notification, userData)
	case "send_cancellation":
		return c.compileCancellation(ticket, notification, userData)
	case "send_fallback":
		return c.compileFallback(ticket, notification, userData)
	case "send_message":
		return c.compileSendMessage(ticket, notification, userData)
	case "ask_product":
		return c.compileAskProduct(ticket, notification, userData)
	case "ask_product_name", "ask_product_name_or_image":
		return c.compileAskProductName(ticket, notification, userData)
	case "ask_clarify_product":
		return c.compileAskClarifyProduct(ticket, notification, userData)
	case "ask_for_order":
		return c.compileAskForOrder(ticket, notification, userData)
	case "ask_order_confirmation":
		return c.compileAskOrderConfirmation(ticket, notification, userData)
	case "ask_product_confirmation":
		return c.compileAskProductConfirmation(ticket, notification, userData)
	case "send_compatibility_answer":
		return c.compileCompatibilityAnswer(ticket, notification, userData)
	case "send_order_status":
		return c.compileOrderStatus(ticket, notification, userData)
	case "ask_delivery_details":
		return c.compileAskDeliveryDetails(ticket, notification, userData)
	case "order_created":
		return c.compileOrderCreated(ticket, notification, userData)
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

// compileBlock splits by intent: blocked_user persists to DB_USER (real ban),
// quiet_hours is file-log only (ephemeral, no history needed).
func (c *Compiler) compileBlock(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	// quiet_hours is ephemeral — no DB side effects, just file log
	if ticket.Intent == "quiet_hours" {
		lang := "en"
		if dl, ok := ticket.Data["language"].(string); ok {
			lang = c.getEffectiveLanguage(n.PlatformID, dl)
		}
		msg := c.tmpl(map[string]string{
			"en": "We're currently closed. Our business hours are 9 AM – 9 PM. We'll get back to you when we're open.",
			"ar": "نحن مغلقون حالياً. ساعات العمل من 9 صباحاً إلى 9 مساءً. سنعود إليكم عندما نفتح.",
			"ku": "ئێستا داخراوین. کاتی کار ٩ بەیانی تا ٩ ئێوارە. کاتێک کراینەوە دەگەڕێینەوە.",
		}, lang)
		return &shared.AutomationInstruction{
			Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
			TicketID: ticket.TicketID, Action: "send_message", Intent: "quiet_hours", NotificationID: n.ID,
			OriginalText: c.notifText(n), MaxRetries: 1, Timeout: 15 * time.Second, Priority: 7,
			Data:      map[string]interface{}{"message": msg, "language": lang, "ephemeral": true},
			Steps:     c.steps(n.PlatformID, "send_message", n, msg, nil),
			CreatedAt: time.Now(),
		}, nil
	}

	// blocked_user: persists to DB_USER (real moderation state)
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

// compileAutoHeart is file-log only now, no DB_AUDIT row (ephemeral).
func (c *Compiler) compileAutoHeart(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	postURL := ""
	if n.Comment != nil {
		postURL = n.Comment.PostURL
	}
	extra := map[string]interface{}{"post_url": postURL, "emoji": "❤️", "from_me": true, "ephemeral": true}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "auto_heart", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 1, Timeout: 15 * time.Second, Priority: 3,
		Data:      map[string]interface{}{"post_url": postURL, "ephemeral": true},
		Steps:     c.steps(n.PlatformID, "react", n, "", extra),
		CreatedAt: time.Now(),
	}, nil
}

// pickConfiguredTemplate returns a random entry from a configured message
// list (Messages.Greetings / Messages.Fallback / etc.), so admins who list
// multiple variants in config.json get natural variety instead of always the
// first one. Returns "" if the list is empty, so callers can fall back to
// their hardcoded default.
func pickConfiguredTemplate(list []string) string {
	if len(list) == 0 {
		return ""
	}
	return list[rand.Intn(len(list))]
}

func (c *Compiler) compileGreeting(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	store := c.configMgr.GetStore()
	name := c.displayName(ud)

	// Prefer an admin-configured greeting over the hardcoded default:
	// 1) this platform's messages.greetings list (config.json ->
	//    platforms.<platform>.messages.greetings), 2) the global
	// store.hello_message, 3) the built-in multilingual template.
	var msg string
	if configured := pickConfiguredTemplate(c.configMgr.GetPlatformMessages(n.PlatformID).Greetings); configured != "" {
		msg = configured
	} else if hello := c.configMgr.GetStoreHelloMessage(); hello != "" {
		msg = hello
	} else {
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("👋 Hello %s! Welcome to %s. How can I help you today?", name, store.Name),
			"ar": fmt.Sprintf("👋 مرحباً %s! أهلاً وسهلاً في %s. كيف يمكنني مساعدتك اليوم؟", name, store.Name),
			"ku": fmt.Sprintf("👋 سڵاو %s! بەخێربێیت بۆ %s. چۆن دەتوانم یارمەتیت بدەم ئەمڕۆ؟", name, store.Name),
		}, lang)
	}
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

// compileSendProduct: stock is now mapped to available/unavailable before rendering.
// stock == 0 sends a distinct "no longer available" message instead of a product card with a blanked field.
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

	var msg string
	if stock <= 0 {
		// stock == 0: distinct "no longer available" message
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("📦 %s\n\n💰 Price: %.2f %s\n📝 %s\n🔢 SKU: %s\n\n❌ This product is currently out of stock and no longer available.", name, price, cur, desc, sku),
			"ar": fmt.Sprintf("📦 %s\n\n💰 السعر: %.2f %s\n📝 %s\n🔢 رمز المنتج: %s\n\n❌ هذا المنتج غير متوفر حالياً ولم يعد متاحاً.", name, price, cur, desc, sku),
			"ku": fmt.Sprintf("📦 %s\n\n💰 نرخ: %.2f %s\n📝 %s\n🔢 کۆدی بەرهەم: %s\n\n❌ ئەم بەرهەمە لە کۆگادا نییە و بەردەست نییە.", name, price, cur, desc, sku),
		}, lang)
	} else {
		statusMap := map[string]string{
			"en": "available",
			"ar": "متوفر",
			"ku": "بەردەستە",
		}
		unavailableMap := map[string]string{
			"en": "low stock / limited",
			"ar": "مخزون منخفض / محدود",
			"ku": "کەمە / سنووردارە",
		}
		stockLabel := statusMap["en"]
		if stock <= 3 && stock > 0 {
			stockLabel = unavailableMap["en"]
		}
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("📦 Product Information\n\n🏷️ Name: %s\n💰 Price: %.2f %s\n📝 Description: %s\n📊 Status: %s\n🔢 SKU: %s\n\nWould you like to order?", name, price, cur, desc, stockLabel, sku),
			"ar": fmt.Sprintf("📦 معلومات المنتج\n\n🏷️ الاسم: %s\n💰 السعر: %.2f %s\n📝 الوصف: %s\n📊 الحالة: %s\n🔢 رمز المنتج: %s\n\nهل ترغب في الطلب؟", name, price, cur, desc, stockLabel, sku),
			"ku": fmt.Sprintf("📦 زانیاری بەرهەم\n\n🏷️ ناو: %s\n💰 نرخ: %.2f %s\n📝 وەسف: %s\n📊 باری کۆگا: %s\n🔢 کۆدی بەرهەم: %s\n\nحەز دەکەیت داوا بکەیت؟", name, price, cur, desc, stockLabel, sku),
		}, lang)
	}
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
func (c *Compiler) createConfirmedOrder(userID, platform, productID string, stockQty, lineQty int, unitPrice, total float64, productName string, sku interface{}, shippingAddr, customerNotes, internalNotes, platformConvID string) (string, error) {
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

	trackingNumber := "TRK-" + strings.ToUpper(strings.ReplaceAll(uuid.New().String(), "-", ""))[:12]
	orderID := uuid.New().String()
	if shippingAddr == "" {
		shippingAddr = "To be provided at delivery"
	}
	if internalNotes == "" {
		internalNotes = "Created via automated chat flow"
	}
	if _, err := tx.Exec(`
		INSERT INTO orders (id, user_id, platform, status, total, subtotal, discount_amount, shipping_address, tracking_number, customer_notes, internal_notes, platform_conversation_id, payment_status, created_at)
		VALUES (?, ?, ?, 'confirmed', ?, ?, 0, ?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)`,
		orderID, userID, platform, total, total, shippingAddr, trackingNumber, customerNotes, internalNotes, platformConvID); err != nil {
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
		shippingOpts, _ := ticket.Data["shipping_options"].([]map[string]interface{})
		ratesLine := c.shippingRatesLine(shippingOpts, cur, lang)
		msg := c.tmpl(map[string]string{
			"en": fmt.Sprintf("🛒 Order Summary\n\n📦 Product: %s\n🔢 Quantity: %d\n💰 Price: %.2f %s each\n💵 Total: %.2f %s%s\n\nTo place this order, please send your delivery details:\n1. Full Name\n2. Phone Number\n3. Delivery Address\n\nOr reply CANCEL to cancel.", name, quantity, price, cur, total, cur, ratesLine),
			"ar": fmt.Sprintf("🛒 ملخص الطلب\n\n📦 المنتج: %s\n🔢 الكمية: %d\n💰 السعر: %.2f %s\n💵 الإجمالي: %.2f %s%s\n\nلتأكيد الطلب، يرجى إرسال تفاصيل التوصيل:\n1. الاسم الكامل\n2. رقم الهاتف\n3. عنوان التوصيل\n\nأو رد بـ إلغاء.", name, quantity, price, cur, total, cur, ratesLine),
			"ku": fmt.Sprintf("🛒 پوختەی داواکاری\n\n📦 بەرهەم: %s\n🔢 ژمارە: %d\n💰 نرخ: %.2f %s\n💵 کۆ: %.2f %s%s\n\nبۆ دانانی داواکاری، تکایە زانیاری گەیاندن بنێرە:\n1. ناوی تەواو\n2. ژمارەی تەلەفۆن\n3. ناونیشانی گەیاندن\n\nیان وەڵامی هەڵیوەشێنەوە.", name, quantity, price, cur, total, cur, ratesLine),
		}, lang)
		extra := map[string]interface{}{"product": pd, "quantity": quantity, "delivery_details": deliveryDetails}
		// Stay in order_intent state so handlePreviousIntent routes the next
		// message to collecting delivery details (name/phone/address) before
		// asking for final confirmation. Do NOT jump straight to awaiting_confirmation.
		c.updateUserStateAfterAI(userID, "order_intent", "")
		return &shared.AutomationInstruction{
			Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
			TicketID: ticket.TicketID, Action: "ask_order_confirmation", Intent: "order_intent", NotificationID: n.ID,
			OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
			Data:      extra,
			Steps:     c.steps(n.PlatformID, "ask_order_confirmation", n, msg, nil),
			CreatedAt: time.Now(),
		}, nil
	}
	// NOTE: "order_intent_confirmed" is not currently produced anywhere in
	// processor.go's routing — real single-item orders are created directly by
	// processor.createOrderInDB once the user replies CONFIRM during the
	// awaiting_confirmation state (see compileAskOrderConfirmation /
	// compileOrderCreated), not by re-entering compileOrderTemplate. This
	// branch (and its own separate order-creation logic in createConfirmedOrder
	// below) is therefore currently dead code for the regular order flow — it
	// only still runs for pack orders via compilePackOrder. Left in place
	// rather than deleted since removing it isn't part of the requested fix,
	// but flagging it because two divergent order-creation code paths existing
	// side by side is exactly what caused the status-string mismatch bug fixed
	// in compileCancellation below (regular orders use status='pending',
	// this path uses status='confirmed').
	if ticket.Intent == "order_intent_confirmed" {
		total := float64(quantity) * price
		pid, _ := pd["id"].(string)
		shippingAddr := ""
		customerNotes := ""
		if deliveryDetails != nil {
			// Prefer the full raw paragraph the user sent, then fall back to
			// structured fields so orders never carry a NULL shipping address.
			if raw, ok := deliveryDetails["raw_text"]; ok && raw != "" {
				shippingAddr = raw
				customerNotes = raw
			} else if addr, ok := deliveryDetails["address"]; ok && addr != "" {
				parts := []string{}
				if nm, ok := deliveryDetails["name"]; ok && nm != "" {
					parts = append(parts, nm)
				}
				if ph, ok := deliveryDetails["phone"]; ok && ph != "" {
					parts = append(parts, ph)
				}
				parts = append(parts, addr)
				shippingAddr = strings.Join(parts, ", ")
				customerNotes = shippingAddr
			}
		}
		orderID, err := c.createConfirmedOrder(userID, n.PlatformID, pid, quantity, quantity, price, total, name, pd["sku"], shippingAddr, customerNotes, "Created via automated chat flow", "")
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
	orderID, err := c.createConfirmedOrder(userID, n.PlatformID, pid, quantityItems, packCount, pricePerPack, total, name, product["sku"], "", "", "Created via automated chat flow", "")
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
	// Look up the most recent order regardless of status (not just
	// pending/confirmed) — we need to see 'shipped'/'delivered' orders too,
	// specifically so we can refuse to cancel them instead of silently
	// matching nothing. The old query only matched pending/confirmed, so a
	// shipped order matched no rows and the code below still told the
	// customer "your order has been cancelled" unconditionally — a false
	// confirmation for an order that was never touched.
	var orderID, productID, status string
	var qty int
	var total float64
	err := c.db.QueryRow(`
		SELECT o.id, oi.product_id, oi.quantity, o.total, o.status
		FROM orders o
		JOIN order_items oi ON oi.order_id = o.id
		WHERE o.user_id = ?
		ORDER BY o.created_at DESC LIMIT 1`,
		userID).Scan(&orderID, &productID, &qty, &total, &status)

	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	name := c.displayName(ud)

	var msg string
	switch {
	case err != nil || orderID == "":
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Dear %s, we couldn't find an active order to cancel.", name),
			"ar": fmt.Sprintf("عزيزي %s، لم نتمكن من العثور على طلب نشط لإلغائه.", name),
			"ku": fmt.Sprintf("بەڕێز %s، نەمانتوانی داواکاریەکی چالاک بدۆزینەوە بۆ هەڵوەشاندنەوە.", name),
		}, lang)
	case status == "shipped" || status == "delivered":
		// Rule: once an order has shipped, it's uncancelable — the package
		// is already out. Cancelling in the DB at this point wouldn't stop
		// the shipment, it would just desync stock/spend totals from what
		// actually happened. Leave the order untouched and tell the
		// customer honestly instead of confirming a cancellation that
		// didn't happen.
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Dear %s, this order has already shipped and can no longer be cancelled. Please contact us about a return instead.", name),
			"ar": fmt.Sprintf("عزيزي %s، تم شحن هذا الطلب بالفعل ولا يمكن إلغاؤه الآن. يرجى التواصل معنا بخصوص الإرجاع بدلاً من ذلك.", name),
			"ku": fmt.Sprintf("بەڕێز %s، ئەم داواکاریە پێشتر نێردراوە و ئیتر ناتوانرێت هەڵبوەشێندرێتەوە. تکایە پەیوەندیمان پێوە بکە بۆ گەڕاندنەوە.", name),
		}, lang)
	case status == "cancelled" || status == "refunded":
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Dear %s, that order is already cancelled.", name),
			"ar": fmt.Sprintf("عزيزي %s، تم إلغاء هذا الطلب بالفعل.", name),
			"ku": fmt.Sprintf("بەڕێز %s، ئەو داواکاریە پێشتر هەڵوەشێنراوەتەوە.", name),
		}, lang)
	default: // pending, confirmed, processing — still cancelable
		tx, _ := c.db.Begin()
		tx.Exec(`UPDATE products SET reserved_stock = MAX(0, reserved_stock - ?) WHERE id = ?`, qty, productID)
		tx.Exec(`UPDATE orders SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP WHERE id = ?`, orderID)
		tx.Exec(`UPDATE platform_users SET total_orders = MAX(0, total_orders - 1), total_spent = MAX(0, total_spent - ?) WHERE id = ?`, total, userID)
		tx.Commit()
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Dear %s, your order has been cancelled.", name),
			"ar": fmt.Sprintf("عزيزي %s، تم إلغاء طلبك.", name),
			"ku": fmt.Sprintf("بەڕێز %s، داواکاریەکەت هەڵوەشایەوە.", name),
		}, lang)
	}
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

	// Prefer an admin-configured fallback over the hardcoded default:
	// platforms.<platform>.messages.fallback (config.json), same precedence
	// pattern as compileGreeting. Only fall through to the hardcoded
	// "call us" text if nothing is configured.
	var msg string
	if configured := pickConfiguredTemplate(c.configMgr.GetPlatformMessages(n.PlatformID).Fallback); configured != "" {
		msg = configured
	} else {
		store := c.configMgr.GetStore()
		contact := store.Contact.Phone
		if contact == "" {
			contact = store.Contact.Email
		}
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("I'm not sure I understood. For better help, call %s or ask about products/orders.", contact),
			"ar": fmt.Sprintf("لم أفهم. للمساعدة، اتصل %s أو اسأل عن منتجات/طلبات.", contact),
			"ku": fmt.Sprintf("تێنەگەیشتم. بۆ یارمەتی، پەیوەندی بکە %s یان پرسیاری بەرهەم/داواکاری بکە.", contact),
		}, lang)
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_fallback", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 1, Timeout: 15 * time.Second, Priority: 3,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_fallback", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileSendMessage handles generic send_message actions (used for ephemeral quiet_hours etc.)
func (c *Compiler) compileSendMessage(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	msg, _ := ticket.Data["message"].(string)
	if msg == "" {
		msg, _ = ticket.Data["reply_text"].(string)
	}
	if msg == "" {
		// Fallback to default message
		msg = c.tmpl(map[string]string{
			"en": "for your message.",
			"ar": "شكراً لرسالتك.",
			"ku": "سوپاس بۆ پەیامەکەت.",
		}, lang)
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_message", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 5,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_message", n, msg, nil),
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

// compileAskProductName asks the user which product they are asking about.
// Used when product resolution fails (product_unknown) or the user is in a
// state where we need the product name spelled out (e.g. awaiting_order_details
// with no matching product). This must NOT require a prior greeting — a user
// can go straight to asking about a product, price, or order.
func (c *Compiler) compileAskProductName(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	msg, _ := ticket.Data["prompt"].(string)
	if msg == "" {
		msg = c.tmpl(map[string]string{
			"en": "Which product are you asking about? Please tell me the name.",
			"ar": "ما المنتج الذي تسأل عنه؟ من فضلك أخبرني بالاسم.",
			"ku": "بەرهەمی چیت لێ دەپرسیت؟ تکایە ناوەکەی پێم بڵێ.",
		}, lang)
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_product_name", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 6,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "ask_product_name", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileAskClarifyProduct lists the ambiguous matches so the user can pick one.
func (c *Compiler) compileAskClarifyProduct(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	products, _ := ticket.Data["products"].([]map[string]interface{})
	if len(products) == 0 {
		// No product list — fall back to generic name request
		return c.compileAskProductName(ticket, n, ud)
	}
	var sb strings.Builder
	sb.WriteString(c.tmpl(map[string]string{
		"en": "I found multiple products. Which one do you mean?\n\n",
		"ar": "وجدت عدة منتجات. أي واحد تقصد؟\n\n",
		"ku": "چەند بەرهەمێکم دۆزییەوە. مەبەستت کامەیە؟\n\n",
	}, lang))
	for i, pd := range products {
		name, _ := pd["name"].(string)
		price, _ := pd["price"].(float64)
		cur := c.currency(pd)
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(fmt.Sprintf("%d. %s — %.2f %s", i+1, name, price, cur))
	}
	msg := sb.String()
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_clarify_product", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 6,
		Data:      map[string]interface{}{"message": msg, "language": lang, "products": products},
		Steps:     c.steps(n.PlatformID, "ask_clarify_product", n, msg, nil),
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
	pd, _ := ticket.Data["product"].(map[string]interface{})
	quantity := 1
	if q, ok := ticket.Data["quantity"].(int); ok && q > 0 {
		quantity = q
	}
	var price float64
	var cur string
	var name string
	if pd != nil {
		name, _ = pd["name"].(string)
		switch v := pd["price"].(type) {
		case float64:
			price = v
		case int64:
			price = float64(v)
		case int:
			price = float64(v)
		}
		cur = c.currency(pd)
	}
	subtotal := price * float64(quantity)
	shipLine, shipCost := c.shippingCostLine(ticket, cur, lang)
	total := subtotal + shipCost

	// Show the shipping address the user supplied when the system is waiting
	// for final confirmation, and let the user type CHANGE to replace it.
	// shipping_address is always the customer's whole raw reply text — there
	// is no separate name/phone/address parsing, only a phone-number
	// completeness check upstream (processor.hasCompleteDelivery). The
	// fallback below only fires if a ticket somehow reaches here without
	// shipping_address set at all, showing whatever a phone number was found.
	shippingAddr, _ := ticket.Data["shipping_address"].(string)
	if shippingAddr == "" {
		if dd, ok := ticket.Data["delivery_details"].(map[string]string); ok {
			if ph, ok := dd["phone"]; ok && ph != "" {
				shippingAddr = "Phone: " + ph
			}
		}
	}

	// If this is a re-prompt after a reply we couldn't parse as CONFIRM/CHANGE/
	// CANCEL, lead with a short clarifying note instead of silently repeating
	// the exact same message with no acknowledgement of the confusing reply.
	clarifyRetry, _ := ticket.Data["clarify_retry"].(bool)
	clarifyNote := ""
	if clarifyRetry {
		clarifyNote = c.tmpl(map[string]string{
			"en": "Sorry, I didn't quite get that. ",
			"ar": "عذراً، لم أفهم ذلك تماماً. ",
			"ku": "ببورە، تێنەگەیشتم. ",
		}, lang) + "\n"
	}

	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("%s🛒 Order Summary\n\n📦 Product: %s\n🔢 Quantity: %d\n💰 Price: %.2f %s each\n💵 Subtotal: %.2f %s%s\n💵 Total: %.2f %s\n📦 Shipping Address:\n%s\n\nPlease reply CONFIRM to place this order, CHANGE to update the shipping address, or CANCEL.", clarifyNote, name, quantity, price, cur, subtotal, cur, shipLine, total, cur, shippingAddr),
		"ar": fmt.Sprintf("%s🛒 ملخص الطلب\n\n📦 المنتج: %s\n🔢 الكمية: %d\n💰 السعر: %.2f %s\n💵 المجموع الفرعي: %.2f %s%s\n💵 الإجمالي: %.2f %s\n📦 عنوان التوصيل:\n%s\n\nرد بـ تأكيد لإتمام الطلب، أو تغيير لتحديث عنوان التوصيل، أو إلغاء.", clarifyNote, name, quantity, price, cur, subtotal, cur, shipLine, total, cur, shippingAddr),
		"ku": fmt.Sprintf("%s🛒 پوختەی داواکاری\n\n📦 بەرهەم: %s\n🔢 ژمارە: %d\n💰 نرخ: %.2f %s\n💵 کۆی لاوەکی: %.2f %s%s\n💵 کۆ: %.2f %s\n📦 ناونیشانی گەیاندن:\n%s\n\nوەڵام بدەرەوە دڵنیام بۆ دانانی داواکاری، گۆڕین بۆ نوێکردنەوەی ناونیشان، یان هەڵیوەشێنەوە.", clarifyNote, name, quantity, price, cur, subtotal, cur, shipLine, total, cur, shippingAddr),
	}, lang)
	if userID, _ := ud["id"].(string); userID != "" {
		// After the final confirmation prompt, the user must be in awaiting_confirmation
		// state so the processor's awaiting_confirmation branch handles CONFIRM and
		// creates the order (with the delivery details already captured in pending_data).
		c.updateUserStateAfterAI(userID, "awaiting_confirmation", "")
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_order_confirmation", Intent: "awaiting_confirmation", NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
		Data:      map[string]interface{}{"message": msg, "language": lang, "product": pd, "quantity": quantity},
		Steps:     c.steps(n.PlatformID, "ask_order_confirmation", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileAskQuantity asks the customer how many they want. This exists
// because the processor no longer guesses a quantity of 1 when the customer
// never stated one (e.g. "I want it") — silently assuming a quantity and
// jumping straight to an order summary was the original bug being fixed here.
func (c *Compiler) compileAskQuantity(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	pd, _ := ticket.Data["product"].(map[string]interface{})
	name := "this product"
	if pd != nil {
		if nm, ok := pd["name"].(string); ok && nm != "" {
			name = nm
		}
	}
	retry, _ := ticket.Data["retry"].(bool)
	var msg string
	if retry {
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Sorry, I didn't catch a number. How many of %s would you like? (e.g. 1, 2, 3...)", name),
			"ar": fmt.Sprintf("عذراً، لم أفهم رقماً. كم عدد %s الذي تريده؟ (مثال: 1، 2، 3...)", name),
			"ku": fmt.Sprintf("ببورە، ژمارەیەکم نەدۆزیەوە. چەند دانە لە %s دەتەوێت؟ (نموونە: 1, 2, 3...)", name),
		}, lang)
	} else {
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Sure — how many of %s would you like?", name),
			"ar": fmt.Sprintf("تمام — كم عدد %s الذي تريده؟", name),
			"ku": fmt.Sprintf("باشە — چەند دانە لە %s دەتەوێت؟", name),
		}, lang)
	}
	if userID, _ := ud["id"].(string); userID != "" {
		c.updateUserStateAfterAI(userID, "awaiting_quantity", "")
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_quantity", Intent: "awaiting_quantity", NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"product": pd, "message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "ask_quantity", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileDeliveryCost answers a standalone shipping-cost question directly
// from the shipping table — the specific city's rate if the customer named
// one, or the full rate list otherwise. Used both for a fresh "how much is
// delivery?" question and (via preserve_prior_state, set in processor.go) for
// the same question asked mid-checkout without losing the customer's place in
// an in-progress order. Deliberately does NOT call updateUserStateAfterAI —
// doing so would defeat the point of preserving state for the mid-checkout case.
func (c *Compiler) compileDeliveryCost(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	cur := c.configMgr.GetStore().Currency
	matched, _ := ticket.Data["matched"].(bool)
	city, _ := ticket.Data["matched_city"].(string)
	cost, _ := ticket.Data["matched_cost"].(float64)
	opts, _ := ticket.Data["shipping_options"].([]map[string]interface{})

	var msg string
	if matched && city != "" {
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("🚚 Delivery to %s costs %.2f %s.", city, cost, cur),
			"ar": fmt.Sprintf("🚚 التوصيل إلى %s يكلف %.2f %s.", city, cost, cur),
			"ku": fmt.Sprintf("🚚 گەیاندن بۆ %s تێچووی %.2f %s.", city, cost, cur),
		}, lang)
	} else if len(opts) > 0 {
		parts := make([]string, 0, len(opts))
		for _, o := range opts {
			c2, _ := o["city"].(string)
			cst, _ := o["cost"].(float64)
			if c2 == "" {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s: %.2f %s", c2, cst, cur))
		}
		msg = c.tmpl(map[string]string{
			"en": "🚚 Our delivery rates by city:\n" + strings.Join(parts, "\n"),
			"ar": "🚚 أسعار التوصيل حسب المدينة:\n" + strings.Join(parts, "\n"),
			"ku": "🚚 نرخی گەیاندن بەپێی شار:\n" + strings.Join(parts, "\n"),
		}, lang)
	} else {
		msg = c.tmpl(map[string]string{
			"en": "Sorry, I don't have delivery rates set up right now — please ask our team directly.",
			"ar": "عذراً، لا توجد أسعار توصيل مضافة حالياً — يرجى التواصل مع فريقنا مباشرة.",
			"ku": "ببورە، نرخی گەیاندن ئێستا دانەمەزراوە — تکایە ڕاستەوخۆ لەگەڵ تیمەکەمان پەیوەندی بکە.",
		}, lang)
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_delivery_cost", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 6,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_delivery_cost", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileAskDeliveryDetails asks the user for name, address and phone to proceed with order.
func (c *Compiler) compileAskDeliveryDetails(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	pd, _ := ticket.Data["product"].(map[string]interface{})
	quantity, _ := ticket.Data["quantity"].(int)
	if quantity < 1 {
		quantity = 1
	}
	name := "Product"
	if pd != nil {
		if n, ok := pd["name"].(string); ok && n != "" {
			name = n
		}
	}
	// If the processor supplied a custom follow-up prompt (e.g. listing which
	// fields are still missing), use it verbatim; otherwise ask for all three.
	msg, _ := ticket.Data["prompt"].(string)
	if msg == "" {
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("🛒 Order: %s x%d\n\nPlease provide your delivery details:\n1. Full Name\n2. Phone Number\n3. Delivery Address", name, quantity),
			"ar": fmt.Sprintf("🛒 الطلب: %s x%d\n\nيرجى تقديم تفاصيل التوصيل:\n1. الاسم الكامل\n2. رقم الهاتف\n3. عنوان التوصيل", name, quantity),
			"ku": fmt.Sprintf("🛒 داواکاری: %s x%d\n\nتکایە زانیاری گەیاندن بنووسە:\n1. ناوی تەواو\n2. ژمارەی تەلەفۆن\n3. ناونیشانی گەیاندن", name, quantity),
		}, lang)
	}
	extra := map[string]interface{}{
		"message":  msg,
		"language": lang,
	}
	if pd != nil {
		extra["product"] = pd
	}
	extra["quantity"] = quantity
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_delivery_details", Intent: "awaiting_order_details", NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
		Data:      extra,
		Steps:     c.steps(n.PlatformID, "ask_delivery_details", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileOrderCreated sends the final order confirmation with tracking info.
func (c *Compiler) compileOrderCreated(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	orderID, _ := ticket.Data["order_id"].(string)
	pd, _ := ticket.Data["product"].(map[string]interface{})
	quantity, _ := ticket.Data["quantity"].(int)
	if quantity < 1 {
		quantity = 1
	}
	productName := ""
	var subtotal float64
	cur := c.configMgr.GetStore().Currency
	if pd != nil {
		if n, ok := pd["name"].(string); ok {
			productName = n
		}
		if p, ok := pd["price"].(float64); ok {
			subtotal = float64(quantity) * p
		}
		if cny, ok := pd["currency"].(string); ok && cny != "" {
			cur = cny
		}
	}
	shipLine, shipCost := c.shippingCostLine(ticket, cur, lang)
	total := subtotal + shipCost
	msg := c.tmpl(map[string]string{
		"en": fmt.Sprintf("✅ Order Created! Order #%s\n📦 %s x%d%s\n💰 Total: %.2f %s\n\nWe'll process it and notify you. Thank you!", orderID, productName, quantity, shipLine, total, cur),
		"ar": fmt.Sprintf("✅ تم إنشاء الطلب! رقم الطلب #%s\n📦 %s x%d%s\n💰 الإجمالي: %.2f %s\n\nسنقوم بمعالجته وإعلامك. شكراً!", orderID, productName, quantity, shipLine, total, cur),
		"ku": fmt.Sprintf("✅ داواکاری دروستکرا! ژمارەی داواکاری #%s\n📦 %s x%d%s\n💰 کۆ: %.2f %s\n\nدەچێتە ڕێ و ئاگادارت دەکەینەوە. سوپاس!", orderID, productName, quantity, shipLine, total, cur),
	}, lang)
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "order_created", Intent: "order_completed", NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 8,
		Data:      map[string]interface{}{"order_id": orderID, "message": msg, "language": lang, "product": pd, "quantity": quantity},
		Steps:     c.steps(n.PlatformID, "order_created", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileAskProductConfirmation handles the Level 13b gate action from Processor.
// It asks the user to confirm if the resolved product is the one they want before proceeding to order.
func (c *Compiler) compileAskProductConfirmation(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	msg, _ := ticket.Data["message"].(string)
	product, _ := ticket.Data["product"].(map[string]interface{})
	if msg == "" {
		// Deliberately do NOT include price here — this step only asks the
		// customer to confirm we matched the right product by name; price is
		// shown later via send_product/send_order_template once they've
		// actually confirmed. Quoting a number at this step was the bug.
		name, _ := product["name"].(string)
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("I found %s. Is this the product you're looking for?", name),
			"ar": fmt.Sprintf("وجدت %s. هل هذا هو المنتج الذي تبحث عنه؟", name),
			"ku": fmt.Sprintf("%s دۆزیمەوە. ئایا ئەمە ئەو بەرهەمەیە کە دەتەوێت؟", name),
		}, lang)
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "ask_product_confirmation", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"product": product, "message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "ask_product_confirmation", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileCompatibilityAnswer answers a "does this work for X?" question.
// ticket.Intent carries which of the three outcomes createProductCompatibilityTicket
// landed on: product_compatibility_yes, product_compatibility_no, or
// product_compatibility_unknown (couldn't tell what "X" was).
func (c *Compiler) compileCompatibilityAnswer(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	pd := c.productData(ticket, n)
	name := ""
	if pd != nil {
		name, _ = pd["name"].(string)
	}
	target, _ := ticket.Data["target"].(string)

	var msg string
	switch ticket.Intent {
	case "product_compatibility_yes":
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Yes, %s works for %s. 👍", name, target),
			"ar": fmt.Sprintf("نعم، %s يعمل مع %s. 👍", name, target),
			"ku": fmt.Sprintf("بەڵێ، %s لەگەڵ %s کار دەکات. 👍", name, target),
		}, lang)
	case "product_compatibility_no":
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("I'm not able to confirm %s works for %s — it's not listed for that. Want me to check with the team?", name, target),
			"ar": fmt.Sprintf("لا أستطيع تأكيد أن %s يعمل مع %s — غير مدرج لذلك. هل تريد أن أتحقق مع الفريق؟", name, target),
			"ku": fmt.Sprintf("ناتوانم دڵنیابم %s لەگەڵ %s کار دەکات — بۆ ئەوە تۆمار نەکراوە. دەتەوێت لەگەڵ تیمەکە بپرسم؟", name, target),
		}, lang)
	default:
		msg = c.tmpl(map[string]string{
			"en": fmt.Sprintf("Which device or use are you asking about for %s?", name),
			"ar": fmt.Sprintf("عن أي جهاز أو استخدام تسأل بخصوص %s؟", name),
			"ku": fmt.Sprintf("دەربارەی کام ئامێر یان بەکارهێنان دەپرسیت بۆ %s؟", name),
		}, lang)
	}
	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_compatibility_answer", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"product": pd, "target": target, "message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_compatibility_answer", n, msg, nil),
		CreatedAt: time.Now(),
	}, nil
}

// compileOrderStatus answers a "show me my order(s)" self-serve lookup.
// ticket.Intent is "order_status_found" (with Data["orders"]) or
// "order_status_none".
// orderStatusLabel translates a raw orders.status DB value ("pending",
// "confirmed", "delivered", "cancelled", etc.) into customer-facing text.
// Falls back to the raw value itself for any status not listed, so an
// unrecognized/new status still shows something instead of going blank.
func (c *Compiler) orderStatusLabel(status, lang string) string {
	labels := map[string]map[string]string{
		"pending":    {"en": "Pending", "ar": "قيد الانتظار", "ku": "چاوەڕوانە"},
		"confirmed":  {"en": "Confirmed", "ar": "مؤكد", "ku": "پشتڕاستکراوەتەوە"},
		"processing": {"en": "Processing", "ar": "قيد التجهيز", "ku": "لە ئامادەکاریدایە"},
		"shipped":    {"en": "Shipped", "ar": "تم الشحن", "ku": "نێردراوە"},
		"delivered":  {"en": "Delivered", "ar": "تم التوصيل", "ku": "گەیشتووە"},
		"cancelled":  {"en": "Cancelled", "ar": "ملغى", "ku": "هەڵوەشێنراوەتەوە"},
		"canceled":   {"en": "Cancelled", "ar": "ملغى", "ku": "هەڵوەشێنراوەتەوە"},
	}
	if l, ok := labels[strings.ToLower(status)]; ok {
		if v, ok := l[lang]; ok {
			return v
		}
		return l["en"]
	}
	return status
}

func (c *Compiler) compileOrderStatus(ticket *nnlp.ProcessResult, n *listener.Notification, ud map[string]interface{}) (*shared.AutomationInstruction, error) {
	detectedLang, _ := ticket.Data["language"].(string)
	lang := c.getEffectiveLanguage(n.PlatformID, detectedLang)
	cur := c.configMgr.GetStore().Currency

	var msg string
	if ticket.Intent != "order_status_found" {
		msg = c.tmpl(map[string]string{
			"en": "You don't have any orders yet. Want to browse our products?",
			"ar": "ليس لديك أي طلبات بعد. هل تريد تصفح منتجاتنا؟",
			"ku": "هیچ داواکاریەکت نییە هێشتا. دەتەوێت بەرهەمەکانمان ببینیت؟",
		}, lang)
	} else {
		orders, _ := ticket.Data["orders"].([]map[string]interface{})
		var sb strings.Builder
		sb.WriteString(c.tmpl(map[string]string{
			"en": "📋 Your recent orders:\n\n",
			"ar": "📋 طلباتك الأخيرة:\n\n",
			"ku": "📋 داواکاریە دواییەکانت:\n\n",
		}, lang))
		for i, o := range orders {
			id, _ := o["id"].(string)
			shortID := id
			if len(shortID) > 8 {
				shortID = shortID[:8]
			}
			status, _ := o["status"].(string)
			total, _ := o["total"].(float64)
			createdAt, _ := o["created_at"].(string)
			date := createdAt
			if len(date) > 10 {
				date = date[:10]
			}
			if i > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(fmt.Sprintf("#%s — %s — %.2f %s (%s)", shortID, c.orderStatusLabel(status, lang), total, cur, date))
			if i == 0 {
				if items, ok := o["items"].([]map[string]interface{}); ok && len(items) > 0 {
					for _, it := range items {
						name, _ := it["name"].(string)
						qty, _ := it["quantity"].(int)
						sb.WriteString(fmt.Sprintf("\n   • %s x%d", name, qty))
					}
				}
			}
		}
		msg = sb.String()
	}

	return &shared.AutomationInstruction{
		Platform: n.PlatformID, SubtypeID: n.SubtypeID, AccountID: n.AccountID,
		TicketID: ticket.TicketID, Action: "send_order_status", Intent: ticket.Intent, NotificationID: n.ID,
		OriginalText: c.notifText(n), MaxRetries: 2, Timeout: 20 * time.Second, Priority: 7,
		Data:      map[string]interface{}{"message": msg, "language": lang},
		Steps:     c.steps(n.PlatformID, "send_order_status", n, msg, nil),
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

// shippingRatesLine formats the shipping table's full city/cost list into a
// single line (with a leading newline so it drops in cleanly, and contributes
// nothing when there's no data), used in the very first order-summary message
// (before an address is known, so no specific cost can be picked yet).
func (c *Compiler) shippingRatesLine(opts []map[string]interface{}, cur, lang string) string {
	if len(opts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(opts))
	for _, o := range opts {
		city, _ := o["city"].(string)
		cost, _ := o["cost"].(float64)
		if city == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %.2f %s", city, cost, cur))
	}
	if len(parts) == 0 {
		return ""
	}
	label := c.tmpl(map[string]string{
		"en": "🚚 Delivery rates by city",
		"ar": "🚚 أسعار التوصيل حسب المدينة",
		"ku": "🚚 نرخی گەیاندن بەپێی شار",
	}, lang)
	return "\n" + label + ": " + strings.Join(parts, ", ")
}

// shippingCostLine formats the specific matched-city shipping line for the
// order confirmation and receipt, once an address has been given and matched
// against the shipping table. Returns an empty line and 0 cost when no city
// was matched (e.g. the address didn't mention a known city), so the total
// stays product-only rather than silently charging an unmatched rate.
func (c *Compiler) shippingCostLine(ticket *nnlp.ProcessResult, cur, lang string) (string, float64) {
	city, _ := ticket.Data["shipping_city"].(string)
	if city == "" {
		return "", 0
	}
	cost, _ := ticket.Data["shipping_cost"].(float64)
	line := c.tmpl(map[string]string{
		"en": fmt.Sprintf("🚚 Shipping to %s: %.2f %s", city, cost, cur),
		"ar": fmt.Sprintf("🚚 التوصيل إلى %s: %.2f %s", city, cost, cur),
		"ku": fmt.Sprintf("🚚 گەیاندن بۆ %s: %.2f %s", city, cost, cur),
	}, lang)
	return "\n" + line, cost
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
