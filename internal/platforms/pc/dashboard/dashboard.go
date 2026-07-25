package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"sailstream/internal/config"
	"sailstream/internal/database"
	"sailstream/internal/maestro"
	"sailstream/internal/sandbox"
)

//go:embed dashboard.html
var dashboardHTML string

const logBufferSize = 300

type PlatformLogBuffer struct {
	mu    sync.Mutex
	lines []LogLine
	head  int
	count int
}

type LogLine struct {
	Timestamp time.Time `json:"ts"`
	Level     string    `json:"level"`
	Text      string    `json:"text"`
}

func newPlatformLogBuffer() *PlatformLogBuffer {
	return &PlatformLogBuffer{lines: make([]LogLine, logBufferSize)}
}

func (b *PlatformLogBuffer) Push(level, text string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines[b.head] = LogLine{Timestamp: time.Now(), Level: level, Text: text}
	b.head = (b.head + 1) % logBufferSize
	if b.count < logBufferSize {
		b.count++
	}
}

func (b *PlatformLogBuffer) Last(n int) []LogLine {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.count {
		n = b.count
	}
	out := make([]LogLine, n)
	for i := 0; i < n; i++ {
		idx := (b.head - n + i + logBufferSize*2) % logBufferSize
		out[i] = b.lines[idx]
	}
	return out
}

type logRouter struct {
	buffers map[string]*PlatformLogBuffer
	mu      sync.RWMutex
}

func newLogRouter() *logRouter {
	r := &logRouter{buffers: make(map[string]*PlatformLogBuffer)}
	r.buffers["system"] = newPlatformLogBuffer()
	return r
}

func (r *logRouter) ensureBuffer(key string) *PlatformLogBuffer {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.buffers[key]; !ok {
		r.buffers[key] = newPlatformLogBuffer()
	}
	return r.buffers[key]
}

func (r *logRouter) getBuffer(key string) *PlatformLogBuffer {
	r.mu.RLock()
	b := r.buffers[key]
	r.mu.RUnlock()
	return b
}

func (r *logRouter) allKeys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.buffers))
	for k := range r.buffers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (r *logRouter) Write(p []byte) bool {
	line := strings.TrimRight(string(p), "\n")
	level := "INFO"
	lower := strings.ToLower(line)
	switch {
	case strings.Contains(lower, "panic") ||
		strings.Contains(lower, " failed:") || strings.Contains(lower, " failed ") ||
		strings.Contains(lower, "error:") || strings.Contains(lower, "] error ") ||
		strings.Contains(lower, "fatal"):
		level = "ERROR"
	case strings.Contains(lower, "warn"):
		level = "WARN"
	case strings.Contains(line, "[URGENT]") || strings.Contains(line, "URGENT"):
		level = "URGENT"
	}
	key := "system"
	if start := strings.Index(line, "["); start >= 0 {
		if end := strings.Index(line[start:], "]"); end >= 0 {
			tag := line[start+1 : start+end]
			if strings.Contains(tag, ":") {
				parts := strings.SplitN(tag, ":", 2)
				platform := strings.ToLower(parts[0])
				knownPlatforms := map[string]bool{
					"facebook": true, "instagram": true, "telegram": true, "tg": true,
					"whatsapp": true, "twitter": true, "viber": true, "tiktok": true,
				}
				if knownPlatforms[platform] {
					canonical := platform
					if platform == "tg" {
						canonical = "telegram"
					}
					key = canonical + ":account"
				}
			}
		}
	}
	r.ensureBuffer(key).Push(level, line)
	if key != "system" {
		r.getBuffer("system").Push(level, line)
	}
	return true
}

type logWriter struct {
	router *logRouter
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.router.Write(p)
	return os.Stderr.Write(p)
}

func parseTimestamp(s string) time.Time {
	formats := []string{
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

type DBStats struct {
	TotalUsers     int `json:"total_users"`
	ActiveUsers    int `json:"active_users"`
	TotalMessages  int `json:"total_messages"`
	UnprocessedMsg int `json:"unprocessed_messages"`
	TodaysMessages int `json:"todays_messages"`
	TotalProducts  int `json:"total_products"`
	ActiveProducts int `json:"active_products"`
	LowStock       int `json:"low_stock_products"`
	TotalOrders    int `json:"total_orders"`
	// PendingOrders counts orders awaiting fulfillment (status='confirmed').
	// Orders are created as 'confirmed' directly (never 'pending' — see
	// schema/compiler notes), so this is "needs action" rather than literally
	// status='pending', which no order reaches under normal flow anymore.
	PendingOrders int     `json:"pending_orders"`
	TodaysOrders  int     `json:"todays_orders"`
	TotalRevenue  float64 `json:"total_revenue"`
	TodaysRevenue float64 `json:"todays_revenue"`
	TotalUrgent   int     `json:"total_urgent"`
	PendingUrgent int     `json:"pending_urgent"`
}

type UrgentItem struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	Platform    string    `json:"platform"`
	MessageType string    `json:"message_type"`
	Message     string    `json:"message"`
	Status      string    `json:"status"`
	Priority    int       `json:"priority"`
	CreatedAt   time.Time `json:"created_at"`
	Resolved    bool      `json:"resolved"`
}

type OrderItem struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Platform  string    `json:"platform"`
	Status    string    `json:"status"`
	Total     float64   `json:"total"`
	CreatedAt time.Time `json:"created_at"`
}

type Server struct {
	maestro   *maestro.Maestro
	configMgr *config.ConfigManager
	db        *sql.DB
	router    *logRouter
	httpSrv   *http.Server
	baseDir   string
}

func NewServer(m *maestro.Maestro, db *sql.DB, router *logRouter, baseDir string) *Server {
	s := &Server{
		maestro:   m,
		configMgr: m.GetConfigManager(),
		db:        db,
		router:    router,
		baseDir:   baseDir,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/platforms", s.handlePlatforms)
	mux.HandleFunc("/api/urgent", s.handleUrgent)
	mux.HandleFunc("/api/orders", s.handleOrders)
	mux.HandleFunc("/api/logs", s.handleLogs)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/control", s.handleControl)
	mux.HandleFunc("/api/wizzard", s.handleWizzard)
	mux.HandleFunc("/api/listener_config", s.handleListenerConfig)
	mux.HandleFunc("/api/scheduled_posts", s.handleScheduledPosts)
	mux.HandleFunc("/api/db/tables", s.handleDBTables)
	mux.HandleFunc("/api/db/query", s.handleDBQuery)
	mux.HandleFunc("/api/db/row", s.handleDBRow)

	s.httpSrv = &http.Server{
		Addr:         ":9090",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	log.Printf("[Dashboard] GUI server listening on http://localhost:9090")
	return s.httpSrv.ListenAndServe()
}

func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpSrv.Shutdown(ctx); err != nil {
		log.Printf("[Dashboard] HTTP server shutdown error: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[Dashboard] JSON encode error: %v", err)
	}
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	stats := s.maestro.GetStats()
	rateLimits := s.maestro.GetRateLimitStatus()
	dbStats := s.fetchDBStats()
	state := string(s.maestro.GetState())

	type overviewResp struct {
		State      string                 `json:"state"`
		Stats      *maestro.MaestroStats  `json:"stats"`
		RateLimits map[string]interface{} `json:"rate_limits"`
		DB         DBStats                `json:"db"`
		ServerTime time.Time              `json:"server_time"`
	}
	writeJSON(w, overviewResp{
		State:      state,
		Stats:      stats,
		RateLimits: rateLimits,
		DB:         dbStats,
		ServerTime: time.Now(),
	})
}

func (s *Server) handlePlatforms(w http.ResponseWriter, r *http.Request) {
	statuses := s.maestro.GetAllPlatformStatuses()
	writeJSON(w, statuses)
}

func (s *Server) handleUrgent(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, []UrgentItem{})
		return
	}
	rows, err := s.db.Query(`
		SELECT id, user_id, platform, COALESCE(message_type,''), COALESCE(original_text,''),
		       status, priority, created_at
		FROM urgent_messages
		ORDER BY created_at DESC
		LIMIT 100
	`)
	if err != nil {
		writeJSON(w, []UrgentItem{})
		return
	}
	defer rows.Close()
	var items []UrgentItem
	for rows.Next() {
		var it UrgentItem
		var ts string
		if err := rows.Scan(&it.ID, &it.UserID, &it.Platform, &it.MessageType,
			&it.Message, &it.Status, &it.Priority, &ts); err != nil {
			continue
		}
		it.CreatedAt = parseTimestamp(ts)
		it.Resolved = it.Status == "resolved" || it.Status == "closed"
		items = append(items, it)
	}
	if items == nil {
		items = []UrgentItem{}
	}
	writeJSON(w, items)
}

func (s *Server) handleOrders(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, []OrderItem{})
		return
	}
	rows, err := s.db.Query(`
		SELECT id, user_id, platform, status, total, created_at
		FROM orders
		ORDER BY created_at DESC
		LIMIT 100
	`)
	if err != nil {
		writeJSON(w, []OrderItem{})
		return
	}
	defer rows.Close()
	var items []OrderItem
	for rows.Next() {
		var it OrderItem
		var ts string
		if err := rows.Scan(&it.ID, &it.UserID, &it.Platform, &it.Status, &it.Total, &ts); err != nil {
			continue
		}
		it.CreatedAt = parseTimestamp(ts)
		items = append(items, it)
	}
	if items == nil {
		items = []OrderItem{}
	}
	writeJSON(w, items)
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		key = "system"
	}
	nStr := r.URL.Query().Get("n")
	n := 150
	if v, err := strconv.Atoi(nStr); err == nil && v > 0 && v <= 500 {
		n = v
	}
	type logsResp struct {
		Key   string    `json:"key"`
		Keys  []string  `json:"keys"`
		Lines []LogLine `json:"lines"`
	}
	buf := s.router.getBuffer(key)
	if buf == nil && strings.Contains(key, ":") {
		platform := key[:strings.Index(key, ":")]
		fallbackKey := platform + ":account"
		if fallbackKey != key {
			buf = s.router.getBuffer(fallbackKey)
			if buf != nil {
				key = fallbackKey
			}
		}
	}
	var lines []LogLine
	if buf != nil {
		lines = buf.Last(n)
	} else {
		lines = []LogLine{}
	}
	writeJSON(w, logsResp{Key: key, Keys: s.router.allKeys(), Lines: lines})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.configMgr.GetConfig()
	if cfg == nil {
		http.Error(w, "no config", 500)
		return
	}

	type platformInfo struct {
		ID             string `json:"id"`
		Enabled        bool   `json:"enabled"`
		Type           string `json:"type"`
		DailyMessages  int    `json:"daily_messages"`
		DailyPosts     int    `json:"daily_posts"`
		DailyHearts    int    `json:"daily_hearts"`
		DailyFollows   int    `json:"daily_follows"`
		DailyComments  int    `json:"daily_comments"`
		HourlyPosts    int    `json:"hourly_posts"`
		AutoReply      bool   `json:"auto_reply"`
		AnswerDM       bool   `json:"answer_dm"`
		AnswerComments bool   `json:"answer_comments"`
		WelcomeMsg     bool   `json:"welcome_msg"`
		AutoHeart      bool   `json:"auto_heart"`
		AutoFollow     bool   `json:"auto_follow"`
		AutoRepost     bool   `json:"auto_repost"`
	}

	type irTraining struct {
		Epochs          int  `json:"epochs"`
		BatchSize       int  `json:"batch_size"`
		NumAngles       int  `json:"num_angles"`
		UseHybrid       bool `json:"use_hybrid"`
		UseAugmentation bool `json:"use_augmentation"`
		UseMultilingual bool `json:"use_multilingual"`
		BoostAccuracy   bool `json:"boost_accuracy"`
	}

	type configResp struct {
		AppVersion     string            `json:"app_version"`
		OS             string            `json:"os"`
		Arch           string            `json:"arch"`
		Language       string            `json:"language"`
		OperationMode  string            `json:"operation_mode"`
		WakeInterval   int               `json:"wake_interval_min"`
		IdleSleep      int               `json:"idle_sleep_min"`
		StoreName      string            `json:"store_name"`
		StoreEmail     string            `json:"store_email"`
		StorePhone     string            `json:"store_phone"`
		StoreCurrency  string            `json:"currency"`
		BusinessHours  map[string]string `json:"business_hours"`
		AIProvider     string            `json:"ai_provider"`
		AIModel        string            `json:"ai_model"`
		AIBaseURL      string            `json:"ai_base_url"`
		AIEnabled      bool              `json:"ai_enabled"`
		AIMaxTokens    int               `json:"ai_max_tokens"`
		AITemperature  float64           `json:"ai_temperature"`
		AITopP         float64           `json:"ai_top_p"`
		AIPresence     float64           `json:"ai_presence_penalty"`
		AIFrequency    float64           `json:"ai_frequency_penalty"`
		AITone         string            `json:"ai_tone"`
		AIMaxResp      int               `json:"ai_max_response_length"`
		SysPrompt      string            `json:"system_prompt"`
		PostInstr      string            `json:"post_instructions"`
		ReplyInstr     string            `json:"reply_instructions"`
		SchedPostInstr string            `json:"scheduled_post_instructions"`
		Timezone       string            `json:"timezone"`
		CheckInterval  int               `json:"check_interval_min"`
		MsgPerMin      int               `json:"messages_per_minute"`
		PostsPerHour   int               `json:"posts_per_hour"`
		PostsPerDay    int               `json:"posts_per_day"`
		QuietEnabled   bool              `json:"quiet_hours_enabled"`
		QuietFrom      string            `json:"quiet_from"`
		QuietTo        string            `json:"quiet_to"`
		RotationMode   string            `json:"rotation_mode"`
		Platforms      []platformInfo    `json:"platforms"`
		IREnabled      bool              `json:"ir_enabled"`
		IRModelPath    string            `json:"ir_model_path"`
		IRConfidence   float64           `json:"ir_confidence"`
		IRMaxSize      int               `json:"ir_max_size"`
		IRTraining     irTraining        `json:"ir_training"`
		PathMedia      string            `json:"path_media"`
		PathPostImages string            `json:"path_post_images"`
		PathPostVideos string            `json:"path_post_videos"`
		PathSchedPosts string            `json:"path_scheduled_posts"`
		PathTraining   string            `json:"path_training_images"`
		PathProducts   string            `json:"path_product_images"`
		PathModels     string            `json:"path_models"`
		PathSessions   string            `json:"path_sessions"`
		PathDatabase   string            `json:"path_database"`
		PathLogs       string            `json:"path_logs"`
		PathCache      string            `json:"path_cache"`
		PathTemp       string            `json:"path_temp"`
		PathBackup     string            `json:"path_backup"`
	}

	var platforms []platformInfo
	for id, p := range cfg.Platforms {
		platforms = append(platforms, platformInfo{
			ID:             id,
			Enabled:        p.Enabled,
			Type:           p.Platform.Type,
			DailyMessages:  p.Limits.DailyMessages,
			DailyPosts:     p.Limits.DailyPosts,
			DailyHearts:    p.Limits.DailyHearts,
			DailyFollows:   p.Limits.DailyFollows,
			DailyComments:  p.Limits.DailyComments,
			HourlyPosts:    p.Limits.HourlyPosts,
			AutoReply:      p.Automation.AutoReply.Enabled,
			AnswerDM:       p.Automation.AnswerDM.Enabled,
			AnswerComments: p.Automation.AnswerComments.Enabled,
			WelcomeMsg:     p.Automation.WelcomeMessage.Enabled,
			AutoHeart:      p.Automation.AutoHeart.Enabled,
			AutoFollow:     p.Automation.AutoFollow.Enabled,
			AutoRepost:     p.Automation.AutoRepost.Enabled,
		})
	}
	ir := cfg.ImageRecognition
	writeJSON(w, configResp{
		AppVersion:     cfg.Meta.AppVersion,
		OS:             cfg.Meta.DetectedOS,
		Arch:           cfg.Meta.DetectedArch,
		Language:       cfg.System.Language,
		OperationMode:  cfg.System.OperationMode,
		WakeInterval:   cfg.System.WakePolicy.IntervalMinutes,
		IdleSleep:      cfg.System.WakePolicy.IdleSleepMinutes,
		StoreName:      cfg.Store.Name,
		StoreEmail:     cfg.Store.Contact.Email,
		StorePhone:     cfg.Store.Contact.Phone,
		StoreCurrency:  cfg.Store.Currency,
		BusinessHours:  cfg.Store.BusinessHours,
		AIProvider:     cfg.AI.Provider,
		AIModel:        cfg.AI.Model,
		AIBaseURL:      cfg.AI.BaseURL,
		AIEnabled:      cfg.AI.APIKey != "",
		AIMaxTokens:    cfg.AI.Generation.MaxTokens,
		AITemperature:  cfg.AI.Generation.Temperature,
		AITopP:         cfg.AI.Generation.TopP,
		AIPresence:     cfg.AI.Generation.PresencePenalty,
		AIFrequency:    cfg.AI.Generation.FrequencyPenalty,
		AITone:         cfg.AI.Instructions.Tone,
		AIMaxResp:      cfg.AI.Instructions.MaxResponseLength,
		SysPrompt:      cfg.AI.Instructions.SystemPrompt,
		PostInstr:      cfg.AI.Instructions.PostInstructions,
		ReplyInstr:     cfg.AI.Instructions.ReplyInstructions,
		SchedPostInstr: cfg.AI.Instructions.ScheduledPostInstructions,
		Timezone:       cfg.Scheduler.Timezone,
		CheckInterval:  cfg.Scheduler.CheckIntervalMinutes,
		MsgPerMin:      cfg.Scheduler.RateLimits.MessagesPerMinute,
		PostsPerHour:   cfg.Scheduler.RateLimits.PostsPerHour,
		PostsPerDay:    cfg.Scheduler.RateLimits.PostsPerDay,
		QuietEnabled:   cfg.Scheduler.QuietHours.Enabled,
		QuietFrom:      cfg.Scheduler.QuietHours.From,
		QuietTo:        cfg.Scheduler.QuietHours.To,
		RotationMode:   cfg.Posting.RotationMode,
		Platforms:      platforms,
		IREnabled:      ir.Enabled,
		IRModelPath:    ir.ModelPath,
		IRConfidence:   ir.ConfidenceThreshold,
		IRMaxSize:      ir.MaxImageSizePx,
		IRTraining: irTraining{
			Epochs:          ir.TrainingDefaults.Epochs,
			BatchSize:       ir.TrainingDefaults.BatchSize,
			NumAngles:       ir.TrainingDefaults.NumAngles,
			UseHybrid:       ir.TrainingDefaults.UseHybrid,
			UseAugmentation: ir.TrainingDefaults.UseAugmentation,
			UseMultilingual: ir.TrainingDefaults.UseMultilingual,
			BoostAccuracy:   ir.TrainingDefaults.BoostAccuracy,
		},
		PathMedia:      cfg.Paths.Media,
		PathPostImages: cfg.Paths.PostImages,
		PathPostVideos: cfg.Paths.PostVideos,
		PathSchedPosts: cfg.Paths.ScheduledPosts,
		PathTraining:   cfg.Paths.TrainingImages,
		PathProducts:   cfg.Paths.ProductImages,
		PathModels:     cfg.Paths.Models,
		PathSessions:   cfg.Paths.Sessions,
		PathDatabase:   cfg.Paths.Database,
		PathLogs:       cfg.Paths.Logs,
		PathCache:      cfg.Paths.Cache,
		PathTemp:       cfg.Paths.Temp,
		PathBackup:     cfg.Paths.Backup,
	})
}

func (s *Server) handleScheduledPosts(w http.ResponseWriter, r *http.Request) {
	cfg := s.configMgr.GetConfig()
	if cfg == nil {
		writeJSON(w, []interface{}{})
		return
	}
	posts := cfg.ScheduledPosts
	if posts == nil {
		posts = []config.ScheduledPost{}
	}
	writeJSON(w, posts)
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Action     string                 `json:"action"`
		PlatformID string                 `json:"platform_id"`
		SubtypeID  string                 `json:"subtype_id"`
		Parameters map[string]interface{} `json:"parameters"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad JSON", 400)
		return
	}
	switch body.Action {
	case "pause":
		s.maestro.Pause()
	case "resume":
		s.maestro.Resume()
	case "start_all":
		s.maestro.StartAllPlatforms()
	case "stop_all":
		s.maestro.StopAllPlatforms()
	case "resolve_urgent":
		if id, ok := body.Parameters["id"].(string); ok && id != "" && s.db != nil {
			if _, err := s.db.Exec(`UPDATE urgent_messages SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP WHERE id = ?`, id); err != nil {
				log.Printf("[Dashboard] resolve_urgent DB error: %v", err)
			}
		}
	default:
		cmd := maestro.ControlCommand{
			Action:     body.Action,
			PlatformID: body.PlatformID,
			SubtypeID:  body.SubtypeID,
			Parameters: body.Parameters,
		}
		if err := s.maestro.SendControlCommand(cmd); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *Server) handleListenerConfig(w http.ResponseWriter, r *http.Request) {
	platform := r.URL.Query().Get("platform")
	subtype := r.URL.Query().Get("subtype")
	if platform == "" || subtype == "" {
		http.Error(w, "platform and subtype required", http.StatusBadRequest)
		return
	}
	cfg := s.configMgr.GetConfig()
	if cfg == nil {
		http.Error(w, "no config", http.StatusInternalServerError)
		return
	}
	platCfg, ok := cfg.Platforms[platform]
	if !ok {
		http.Error(w, "platform not found", http.StatusNotFound)
		return
	}
	var target *config.PlatformSubtype
	for i := range platCfg.Subtypes {
		if platCfg.Subtypes[i].ID == subtype {
			target = &platCfg.Subtypes[i]
			break
		}
	}

	platformSpecific := map[string]interface{}{}
	if target != nil && len(target.Auth) > 0 {
		platformSpecific = target.Auth
	} else if pc := platCfg.GetConfig(); pc != nil {
		if b, err := json.Marshal(pc); err == nil {
			var m map[string]interface{}
			if json.Unmarshal(b, &m) == nil {
				platformSpecific = m
			}
		}
	}

	resp := map[string]interface{}{
		"platform_id":       platform,
		"subtype_id":        subtype,
		"enabled":           platCfg.Enabled,
		"automation":        platCfg.Automation,
		"limits":            platCfg.Limits,
		"messages":          platCfg.Messages,
		"settings":          platCfg.Settings,
		"posting":           platCfg.Posting,
		"platform_specific": platformSpecific,
	}
	if target != nil {
		resp["subtype_type"] = target.Type
		resp["subtype_enabled"] = target.Enabled
		resp["subtype_name"] = target.Name
	}
	writeJSON(w, resp)
}

func (s *Server) handleWizzard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	cfg := s.configMgr.GetConfig()
	isMobile := runtime.GOOS == "android" || runtime.GOOS == "ios"
	if cfg != nil && (cfg.Meta.DetectedOS == "android" || cfg.Meta.DetectedOS == "ios") {
		isMobile = true
	}
	if isMobile {
		http.Error(w, "wizzard is only available on desktop platforms", http.StatusBadRequest)
		return
	}

	candidates := []string{
		filepath.Join(s.baseDir, "wizzard.go"),
		filepath.Join(s.baseDir, "cmd", "sailstream", "wizzard.go"),
		filepath.Join(s.baseDir, "internal", "platforms", "pc", "wizzard.go"),
	}
	if execPath, err := os.Executable(); err == nil {
		candidates = append([]string{filepath.Join(filepath.Dir(execPath), "wizzard.go")}, candidates...)
	}

	wizzardPath := ""
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			wizzardPath = c
			break
		}
	}
	if wizzardPath == "" {
		http.Error(w, fmt.Sprintf("wizzard.go not found (searched: %s)", strings.Join(candidates, ", ")), 404)
		return
	}

	configPath := filepath.Join(s.baseDir, "internal", "config", "config.json")
	if _, err := os.Stat(configPath); err != nil {
		configPath = filepath.Join(s.baseDir, "config.json")
	}

	const wizzardAddr = "127.0.0.1:7879"
	log.Printf("[Dashboard] wizzard found at: %s", wizzardPath)
	writeJSON(w, map[string]string{"status": "launching wizzard", "wizzard_url": "http://" + wizzardAddr})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		log.Println("[Dashboard] Shutting down for wizzard launch…")
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpSrv.Shutdown(stopCtx)
		doneCh := make(chan struct{})
		go func() {
			s.maestro.Stop()
			close(doneCh)
		}()
		select {
		case <-doneCh:
		case <-time.After(12 * time.Second):
			log.Printf("[Dashboard] maestro.Stop() timed out during wizzard launch")
		}
		cmd := exec.Command("go", "run", "-tags", "!opengl", wizzardPath, "-config", configPath)
		cmd.Dir = s.baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = append(os.Environ(), "SAILSTREAM_NO_AUTO_OPEN=1")
		if err := cmd.Run(); err != nil {
			log.Printf("[Dashboard] wizzard exited: %v", err)
		}
		os.Exit(0)
	}()
}

func (s *Server) handleDBTables(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, []interface{}{})
		return
	}
	rows, err := s.db.Query(`
		SELECT name, type FROM sqlite_master 
		WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' 
		ORDER BY name
	`)
	if err != nil {
		writeJSON(w, []interface{}{})
		return
	}
	defer rows.Close()

	var tables []map[string]string
	for rows.Next() {
		var name, typ string
		rows.Scan(&name, &typ)
		tables = append(tables, map[string]string{"name": name, "type": typ})
	}
	writeJSON(w, tables)
}

// isValidTable reports whether name is an actual table or view in the
// database, checked live against sqlite_master. This guards handleDBQuery
// against building a SQL string from an arbitrary caller-supplied table name.
func (s *Server) isValidTable(name string) bool {
	if s.db == nil {
		return false
	}
	var found string
	err := s.db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
		name,
	).Scan(&found)
	return err == nil
}

func (s *Server) handleDBQuery(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected")
		return
	}

	table := r.URL.Query().Get("table")
	if table == "" {
		jsonErr(w, 400, "table parameter required")
		return
	}
	if !s.isValidTable(table) {
		jsonErr(w, 400, "unknown table: "+table)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	limit := 50
	offset := 0
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
		limit = l
	}
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = o
	}
	search := r.URL.Query().Get("search")

	pkCol := ""
	cols := []map[string]interface{}{}
	colRows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err == nil {
		defer colRows.Close()
		for colRows.Next() {
			var cid int
			var name, ctype string
			var notnull, pk int
			var dflt sql.NullString
			colRows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk)
			cols = append(cols, map[string]interface{}{
				"name": name, "type": ctype, "pk": pk == 1,
			})
			if pk == 1 {
				pkCol = name
			}
		}
	}

	if pkCol == "" {
		for _, c := range cols {
			if name, ok := c["name"].(string); ok && name == "id" {
				pkCol = "id"
				break
			}
		}
	}

	query := fmt.Sprintf("SELECT * FROM %q", table)
	args := []interface{}{}
	if search != "" && len(cols) > 0 {
		var conditions []string
		for _, c := range cols {
			name, ok := c["name"].(string)
			if ok {
				conditions = append(conditions, fmt.Sprintf("%q LIKE ?", name))
				args = append(args, "%"+search+"%")
			}
		}
		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " OR ")
		}
	}

	countQuery := "SELECT COUNT(*) FROM (" + query + ")"
	var total int
	countRow := s.db.QueryRow(countQuery, args...)
	countRow.Scan(&total)

	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}

	var result []map[string]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(columnNames))
		valPtrs := make([]interface{}, len(columnNames))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			continue
		}
		rowMap := map[string]interface{}{}
		for i, col := range columnNames {
			val := vals[i]
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		result = append(result, rowMap)
	}
	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, map[string]interface{}{
		"table":     table,
		"columns":   cols,
		"rows":      result,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
		"editable":  []string{},
		"is_view":   true,
		"pk_column": pkCol,
	})
}

func (s *Server) handleDBRow(w http.ResponseWriter, r *http.Request) {
	jsonErr(w, 403, "Database editing is disabled in the dashboard. Use the wizzard to modify data.")
}

func (s *Server) fetchDBStats() DBStats {
	d := DBStats{}
	if s.db == nil {
		return d
	}
	scan := func(q string, dest interface{}) {
		s.db.QueryRow(q).Scan(dest)
	}
	scan("SELECT COUNT(*) FROM platform_users", &d.TotalUsers)
	scan("SELECT COUNT(*) FROM platform_users WHERE last_active >= datetime('now','-30 days')", &d.ActiveUsers)
	scan("SELECT COUNT(*) FROM messages", &d.TotalMessages)
	scan("SELECT COUNT(*) FROM messages WHERE processed=0", &d.UnprocessedMsg)
	scan("SELECT COUNT(*) FROM messages WHERE DATE(received_at)=DATE('now')", &d.TodaysMessages)
	scan("SELECT COUNT(*) FROM products", &d.TotalProducts)
	scan("SELECT COUNT(*) FROM products WHERE is_active=1", &d.ActiveProducts)
	scan("SELECT COUNT(*) FROM products WHERE stock<=low_stock_threshold AND is_active=1", &d.LowStock)
	scan("SELECT COUNT(*) FROM orders", &d.TotalOrders)
	scan("SELECT COUNT(*) FROM orders WHERE status='confirmed'", &d.PendingOrders)
	scan("SELECT COUNT(*) FROM orders WHERE DATE(created_at)=DATE('now')", &d.TodaysOrders)
	scan("SELECT COALESCE(SUM(total),0) FROM orders WHERE payment_status='paid'", &d.TotalRevenue)
	scan("SELECT COALESCE(SUM(total),0) FROM orders WHERE DATE(created_at)=DATE('now') AND payment_status='paid'", &d.TodaysRevenue)
	scan("SELECT COUNT(*) FROM urgent_messages", &d.TotalUrgent)
	scan("SELECT COUNT(*) FROM urgent_messages WHERE status NOT IN ('resolved','closed')", &d.PendingUrgent)
	return d
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardHTML)
}

func main() {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	router := newLogRouter()
	log.SetOutput(&logWriter{router: router})
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	log.Printf("[Dashboard] SailStream starting — OS=%s ARCH=%s", runtime.GOOS, runtime.GOARCH)

	baseDir := getBaseDir()
	configPath := filepath.Join(baseDir, "internal", "config", "config.json")
	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("[Dashboard] config.json not found at: %s", configPath)
	}
	log.Printf("[Dashboard] Config: %s", configPath)

	maestroInstance, err := maestro.NewMaestro(configPath, nil)
	if err != nil {
		log.Fatalf("[Dashboard] Maestro init failed: %v", err)
	}

	db := database.GetDB()
	if db == nil {
		log.Fatal("[Dashboard] Database not initialized")
	}
	log.Println("[Dashboard] Database connected")

	if err := maestroInstance.Start(); err != nil {
		log.Fatalf("[Dashboard] Maestro start failed: %v", err)
	}
	log.Println("[Dashboard] Maestro started")

	// Opt-in synthetic testing injector. Never runs unless explicitly enabled —
	// safe to leave this block in place in production builds.
	if os.Getenv("SAILSTREAM_SANDBOX") == "1" {

		sandbox.Enabled = true
		answers := sandbox.NewAnswerLog()
		maestroInstance.SetSandboxTap(answers.Record)

		// Fully isolated lane, only reached if you inject with subtype_id
		// "sandbox" instead of your real listener's subtype_id — never
		// touches a real session at all.
		sc := sandbox.NewCollector()
		maestroInstance.RegisterSandboxListener("whatsapp", "sandbox", "sandbox-account", sc)
		maestroInstance.RegisterSandboxListener("telegram", "sandbox", "sandbox-account", sc)

		addr := os.Getenv("SAILSTREAM_SANDBOX_ADDR")
		if addr == "" {
			addr = "127.0.0.1:9099"
		}
		sandbox.StartHTTP(maestroInstance, answers, addr)
		log.Printf("[Dashboard] SANDBOX MODE ENABLED — synthetic messages can be injected at http://%s/inject", addr)
	}

	srv := NewServer(maestroInstance, db, router, baseDir)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Dashboard] HTTP server error: %v", err)
		}
	}()

	go func() {
		time.Sleep(800 * time.Millisecond)
		if os.Getenv("SAILSTREAM_NO_AUTO_OPEN") == "" {
			openBrowser("http://localhost:9090")
		}
	}()

	log.Println("[Dashboard] GUI available at http://localhost:9090")

	<-sigChan
	log.Println("[Dashboard] Shutdown signal received")

	// Stop Maestro first so every platform status and the overview state
	// flip to "stopped" while the dashboard is still reachable — giving
	// the frontend's next poll (or its disconnect-detection fallback,
	// once the HTTP server actually goes down next) an accurate final
	// state instead of freezing on the last "running" snapshot.
	maestroInstance.Stop()
	srv.Stop()
	database.Close()
	log.Println("[Dashboard] Shutdown complete")
}

func getBaseDir() string {
	cwd, _ := os.Getwd()
	if filepath.Base(cwd) == "sailstream" && filepath.Base(filepath.Dir(cwd)) == "cmd" {
		return filepath.Join(cwd, "..", "..")
	}
	return cwd
}

func openBrowser(url string) {
	switch runtime.GOOS {
	case "windows":
		exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		exec.Command("open", url).Start()
	default:
		if os.Getenv("DISPLAY") != "" || os.Getenv("WAYLAND_DISPLAY") != "" {
			exec.Command("xdg-open", url).Start()
		}
	}
}
