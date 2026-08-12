package maestro

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sailstream/internal/comms"
	"sailstream/internal/config"
	"sailstream/internal/database"
	"sailstream/internal/enviroment"
	"sailstream/internal/listener"
	"sailstream/internal/nnlp"
	"sailstream/internal/platforms/pc/scripts"
	"sailstream/internal/sandbox"
	"sailstream/internal/session"
	"sailstream/internal/shared"
	"sailstream/internal/tasker"
)

type MaestroState string

const (
	StateIdle      MaestroState = "idle"
	StateStarting  MaestroState = "starting"
	StateRunning   MaestroState = "running"
	StatePaused    MaestroState = "paused"
	StateStopping  MaestroState = "stopping"
	StateStopped   MaestroState = "stopped"
	StateError     MaestroState = "error"
	StateScheduled MaestroState = "scheduled"
)

type PlatformCollector interface {
	ReceiveInstructions(instruction *shared.AutomationInstruction) error
	Collect(ctx context.Context, cookies []*listener.CookieData) ([]*listener.Notification, error)
	GetErrorChannel() <-chan *listener.PlatformError
}

type AuthCodeSubmitter interface {
	SubmitAuthCode(code string) error
}

type Connector interface {
	Connect(ctx context.Context) error
}

type TimeSlot struct {
	Hour   int
	Minute int
}

// ScheduleSlot is a fixed daily post time (config: platforms.<platform>.
// posting.schedule_times) tagged with the platform it belongs to.
type ScheduleSlot struct {
	PlatformID string
	Hour       int
	Minute     int
}

type PlatformStatus struct {
	PlatformID        string              `json:"platform_id"`
	SubtypeID         string              `json:"subtype_id"`
	Enabled           bool                `json:"enabled"`
	IsRunning         bool                `json:"is_running"`
	IsPaused          bool                `json:"is_paused"`
	SessionState      string              `json:"session_state"`
	LastCheck         time.Time           `json:"last_check"`
	LastCollect       time.Time           `json:"last_collect"`
	ErrorCount        int32               `json:"error_count"`
	LastError         string              `json:"last_error"`
	NotificationCount int64               `json:"notification_count"`
	StartTime         time.Time           `json:"start_time"`
	LastReconnect     time.Time           `json:"last_reconnect"`
	ReconnectAttempts int32               `json:"reconnect_attempts"`
	DailyStats        *PlatformDailyStats `json:"daily_stats"`
	mu                sync.RWMutex        `json:"-"`
}

type PlatformDailyStats struct {
	PostsToday    int       `json:"posts_today"`
	MessagesToday int       `json:"messages_today"`
	SentToday     int       `json:"sent_today"`
	HeartsToday   int       `json:"hearts_today"`
	FollowsToday  int       `json:"follows_today"`
	CommentsToday int       `json:"comments_today"`
	LastReset     time.Time `json:"last_reset"`
}

type PlatformListener struct {
	PlatformID    string
	SubtypeID     string
	AccountID     string
	Config        *listener.ListenerConfig
	Session       *session.Session
	Cancel        context.CancelFunc
	IsRunning     atomic.Bool
	IsPaused      atomic.Bool
	ErrorCount    int32
	LastError     string
	NotifCount    int64
	ReconnectFail atomic.Bool
	DailyStats    *PlatformDailyStats
	Collector     PlatformCollector
	statsMu       sync.Mutex
}

type ControlCommand struct {
	Action     string
	PlatformID string
	SubtypeID  string
	Parameters map[string]interface{}
}

type PlatformError struct {
	PlatformID      string
	SubtypeID       string
	AccountID       string
	ErrorCode       string
	ErrorMsg        string
	Timestamp       time.Time
	Severity        string
	Reconnect       bool
	DisablePlatform bool
}

type MaestroStats struct {
	StartTime          time.Time              `json:"start_time"`
	TotalNotifications int64                  `json:"total_notifications"`
	TotalErrors        int64                  `json:"total_errors"`
	ActivePlatforms    int                    `json:"active_platforms"`
	TotalPlatforms     int                    `json:"total_platforms"`
	SessionsCreated    int64                  `json:"sessions_created"`
	SessionsRefreshed  int64                  `json:"sessions_refreshed"`
	SessionReconnects  int64                  `json:"session_reconnects"`
	WakeCycles         int64                  `json:"wake_cycles"`
	LastWake           time.Time              `json:"last_wake"`
	PostsToday         int                    `json:"posts_today"`
	PostsThisHour      int                    `json:"posts_this_hour"`
	RandomPostsSent    int                    `json:"random_posts_sent"`
	ScheduledPostsSent int                    `json:"scheduled_posts_sent"`
	TotalSentToday     int                    `json:"total_sent_today"`
	RotationMode       string                 `json:"rotation_mode"`
	NNLPStats          map[string]interface{} `json:"nnlp_stats"`
	PendingMessages    int                    `json:"pending_messages"`
	mu                 sync.RWMutex           `json:"-"`
}

type Maestro struct {
	configManager  *config.ConfigManager
	sessionManager *session.Manager
	db             *sql.DB
	envManager     *enviroment.Environment
	processor      *nnlp.Processor
	compiler       *tasker.Compiler
	poster         *scripts.Poster

	state   atomic.Value
	mu      sync.RWMutex
	running atomic.Bool
	paused  atomic.Bool

	operationMode string
	wakeInterval  time.Duration
	idleTimeout   time.Duration
	timezone      *time.Location

	// ScheduleSlot pairs a fixed daily post time with the platform it
	// belongs to (config: platforms.<platform>.posting.schedule_times).
	// Previously these were flattened into a single global []TimeSlot,
	// which discarded which platform each time was configured for and
	// caused every schedule_times entry, on any platform, to trigger a
	// post to ALL platforms simultaneously. Keeping platformID here fixes
	// that and lets checkScheduleTimes fire per-platform like
	// checkRandomPosting already does.
	scheduleTimes []ScheduleSlot
	lastFired     map[string]time.Time
	nextWake      time.Time
	lastActivity  time.Time

	nextScheduledPostCheck time.Time

	platformStatus map[string]*PlatformStatus
	listeners      map[string]*PlatformListener
	listenersMu    sync.RWMutex

	starting sync.Map

	notificationChan chan *listener.Notification
	nnlpResultChan   chan *nnlp.ProcessResult
	instructionChan  chan *shared.AutomationInstruction
	errorChan        chan *PlatformError
	controlChan      chan ControlCommand
	shutdownChan     chan struct{}

	startTime time.Time
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc

	stats *MaestroStats

	authCodeServer *http.Server

	tapMu      sync.Mutex
	sandboxTap func(*nnlp.ProcessResult, *shared.AutomationInstruction)

	// Cross-goroutine synchronization: lifecycle state, config mutation,
	// schedule map, and in-flight notification processing.
	lifecycleMu    sync.RWMutex
	scheduleMu     sync.Mutex
	configMu       sync.Mutex
	notificationWG sync.WaitGroup
}

func NewMaestro(configPath string, envManager *enviroment.Environment) (*Maestro, error) {
	log.Printf("[Maestro] Initializing orchestrator...")

	configManager := config.NewConfigManager(configPath)
	if err := configManager.Load(); err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}
	cfg := configManager.GetConfig()

	if envManager == nil {
		envManager = enviroment.NewEnvironment(cfg)
	}

	if err := envManager.EnsureDirectories(); err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}

	dbPath := configManager.GetDatabasePath()
	if err := ensureDirectory(filepath.Dir(dbPath)); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	if err := database.Connect(dbPath); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sessionManager, err := session.NewManager(cfg, envManager)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize session manager: %w", err)
	}

	processor := nnlp.NewProcessor(database.GetDB(), configManager)
	llmClient := comms.NewClient(configManager)

	tz, err := time.LoadLocation(cfg.Scheduler.Timezone)
	if err != nil {
		log.Printf("[Maestro] Warning: invalid timezone %s, using UTC", cfg.Scheduler.Timezone)
		tz = time.UTC
	}

	ctx, cancel := context.WithCancel(context.Background())

	instructionChan := make(chan *shared.AutomationInstruction, 1000)

	m := &Maestro{
		configManager:    configManager,
		sessionManager:   sessionManager,
		db:               database.GetDB(),
		envManager:       envManager,
		processor:        processor,
		operationMode:    cfg.System.OperationMode,
		wakeInterval:     time.Duration(cfg.System.WakePolicy.IntervalMinutes) * time.Minute,
		idleTimeout:      time.Duration(cfg.System.WakePolicy.IdleSleepMinutes) * time.Minute,
		timezone:         tz,
		platformStatus:   make(map[string]*PlatformStatus),
		listeners:        make(map[string]*PlatformListener),
		lastFired:        make(map[string]time.Time),
		notificationChan: make(chan *listener.Notification, 1000),
		nnlpResultChan:   make(chan *nnlp.ProcessResult, 1000),
		instructionChan:  instructionChan,
		errorChan:        make(chan *PlatformError, 100),
		controlChan:      make(chan ControlCommand, 50),
		shutdownChan:     make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
		stats: &MaestroStats{
			StartTime: time.Now(),
			NNLPStats: make(map[string]interface{}),
		},
	}

	m.state.Store(StateIdle)
	m.initializePlatformStatuses()

	m.migrateRateLimitsTable()
	m.bootstrapRateLimits()
	// One-time, idempotent: seeds posting_settings/posting_schedule from
	// config.json's old posting.* values if those DB tables are still
	// empty (fresh upgrade from a config-based install). No-ops on every
	// subsequent start once the DB has rows — config.json no longer has
	// these fields at all after this, DB is the only source from here on.
	m.migratePostingSettingsFromConfig()
	m.loadScheduleTimesFromDB()
	m.stats.RotationMode = m.getRotationMode()

	sandboxMode := os.Getenv("SAILSTREAM_SANDBOX") == "1"
	compiler := tasker.NewCompiler(database.GetDB(), configManager, llmClient, m, sandboxMode)
	m.compiler = compiler
	m.poster = scripts.NewPoster(envManager, configManager, instructionChan)

	if cfg.Scheduler.QuietHours.Enabled {
		m.parseQuietHours(cfg.Scheduler.QuietHours)
	}

	log.Printf("[Maestro] Initialized — mode: %s, interval: %v, idle: %v, tz: %s, platforms: %d",
		m.operationMode, m.wakeInterval, m.idleTimeout, tz.String(), len(m.platformStatus))

	return m, nil
}

func ensureDirectory(dir string) error {
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0750)
}

func subtypeHasCredentials(pc config.PlatformConfig, sub config.PlatformSubtype) bool {
	if hasNonEmptyValue(sub.Auth) {
		return true
	}
	if legacy := pc.GetConfig(); legacy != nil {
		if b, err := json.Marshal(legacy); err == nil {
			var m map[string]interface{}
			if json.Unmarshal(b, &m) == nil && hasNonEmptyValue(m) {
				return true
			}
		}
	}
	return false
}

func hasNonEmptyValue(m map[string]interface{}) bool {
	for _, v := range m {
		switch val := v.(type) {
		case nil:
			continue
		case string:
			if strings.TrimSpace(val) != "" {
				return true
			}
		case map[string]interface{}:
			if hasNonEmptyValue(val) {
				return true
			}
		case bool:
			if val {
				return true
			}
		case float64:
			if val != 0 {
				return true
			}
		default:
			return true
		}
	}
	return false
}

func (m *Maestro) initializePlatformStatuses() {
	cfg := m.configManager.GetConfig()
	m.mu.Lock()
	defer m.mu.Unlock()

	for platformID, platformCfg := range cfg.Platforms {
		if !platformCfg.Enabled {
			continue
		}
		subtype := "account"
		if platformCfg.Platform.Subtype != "" {
			subtype = platformCfg.Platform.Subtype
		}
		if len(platformCfg.Subtypes) > 0 {
			for _, sub := range platformCfg.Subtypes {
				if !subtypeHasCredentials(platformCfg, sub) {
					continue
				}
				key := m.platformKey(platformID, sub.ID)
				m.platformStatus[key] = &PlatformStatus{
					PlatformID: platformID,
					SubtypeID:  sub.ID,
					Enabled:    sub.Enabled,
					DailyStats: &PlatformDailyStats{LastReset: time.Now()},
				}
			}
		} else {
			key := m.platformKey(platformID, subtype)
			m.platformStatus[key] = &PlatformStatus{
				PlatformID: platformID,
				SubtypeID:  subtype,
				Enabled:    true,
				DailyStats: &PlatformDailyStats{LastReset: time.Now()},
			}
		}
	}
	m.stats.TotalPlatforms = len(m.platformStatus)
}

// loadScheduleTimesFromDB replaces the old parseScheduleTimes(cfg) — fixed
// daily post times now live in the posting_schedule table instead of
// config.json's platforms.<platform>.posting.schedule_times, so this reads
// the DB instead of walking platform configs.
func (m *Maestro) loadScheduleTimesFromDB() {
	m.scheduleTimes = nil
	if m.db == nil {
		return
	}
	rows, err := m.db.Query(`SELECT platform, subtype, post_time FROM posting_schedule WHERE enabled = 1`)
	if err != nil {
		log.Printf("[Maestro] loadScheduleTimesFromDB query error: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var platformID, subtype, postTime string
		if err := rows.Scan(&platformID, &subtype, &postTime); err != nil {
			continue
		}
		parsedTime, err := time.Parse("15:04", postTime)
		if err != nil {
			continue
		}
		m.scheduleTimes = append(m.scheduleTimes, ScheduleSlot{
			PlatformID: platformID,
			Hour:       parsedTime.Hour(),
			Minute:     parsedTime.Minute(),
		})
	}
}

// PostingSettings mirrors a posting_settings row — the DB-backed
// replacement for config.json's platforms.<platform>.posting.random/.manual.
type PostingSettings struct {
	RandomEnabled          bool
	RandomIntervalMinHours int
	RandomIntervalMaxHours int
	RandomPostsPerCycle    int
	RandomUseGlobal        bool
	ManualEnabled          bool
	ManualTitle            string
	ManualDescription      string
	ManualMediaType        string
	ManualMediaURL         string
	RotationMode           string
}

// getPostingSettings reads a platform+subtype's posting settings from the DB.
// Falls back to the platform-level row (subtype=”) if a subtype-specific
// row doesn't exist, matching the old config fallback behavior between
// platform-level and subtype-level posting blocks. ok=false means neither
// row exists (posting not configured for this platform at all).
func (m *Maestro) getPostingSettings(platform, subtype string) (PostingSettings, bool) {
	var ps PostingSettings
	if m.db == nil {
		return ps, false
	}
	scanRow := func(sub string) bool {
		var randomEnabled, randomUseGlobal, manualEnabled int
		err := m.db.QueryRow(`
			SELECT random_enabled, random_interval_min_hours, random_interval_max_hours,
			       random_posts_per_cycle, random_use_global,
			       manual_enabled, manual_title, manual_description, manual_media_type, manual_media_url,
			       rotation_mode
			FROM posting_settings WHERE platform=? AND subtype=?`, platform, sub).
			Scan(&randomEnabled, &ps.RandomIntervalMinHours, &ps.RandomIntervalMaxHours,
				&ps.RandomPostsPerCycle, &randomUseGlobal,
				&manualEnabled, &ps.ManualTitle, &ps.ManualDescription, &ps.ManualMediaType, &ps.ManualMediaURL,
				&ps.RotationMode)
		if err != nil {
			return false
		}
		ps.RandomEnabled = randomEnabled == 1
		ps.RandomUseGlobal = randomUseGlobal == 1
		ps.ManualEnabled = manualEnabled == 1
		return true
	}
	if subtype != "" && scanRow(subtype) {
		return ps, true
	}
	if scanRow("") {
		return ps, true
	}
	return ps, false
}

func (m *Maestro) parseQuietHours(qh config.QuietHours) {
	if qh.Enabled {
		log.Printf("[Maestro] Quiet hours: %s - %s", qh.From, qh.To)
	}
}

func (m *Maestro) isQuietHours() bool {
	cfg := m.configManager.GetConfig()
	if !cfg.Scheduler.QuietHours.Enabled {
		return false
	}
	now := time.Now().In(m.getTimezone())
	currentMinutes := now.Hour()*60 + now.Minute()
	fromParts := strings.Split(cfg.Scheduler.QuietHours.From, ":")
	toParts := strings.Split(cfg.Scheduler.QuietHours.To, ":")
	if len(fromParts) != 2 || len(toParts) != 2 {
		return false
	}
	fromHour, _ := time.Parse("15:04", cfg.Scheduler.QuietHours.From)
	toHour, _ := time.Parse("15:04", cfg.Scheduler.QuietHours.To)
	fromMinutes := fromHour.Hour()*60 + fromHour.Minute()
	toMinutes := toHour.Hour()*60 + toHour.Minute()
	if fromMinutes > toMinutes {
		return currentMinutes >= fromMinutes || currentMinutes <= toMinutes
	}
	return currentMinutes >= fromMinutes && currentMinutes <= toMinutes
}

func (m *Maestro) Start() error {
	if m.running.Load() {
		return fmt.Errorf("maestro is already running")
	}
	log.Printf("[Maestro] Starting orchestrator...")
	m.state.Store(StateStarting)
	m.running.Store(true)
	m.startTime = time.Now()
	m.lastActivity = time.Now()
	m.nextScheduledPostCheck = time.Now()

	m.processor.SetResultHandler(func(result *nnlp.ProcessResult) {
		select {
		case m.nnlpResultChan <- result:
		default:
			log.Printf("[Maestro] NNLP result channel full, dropping async result: %s", result.TicketID)
		}
	})
	m.processor.Start(m.ctx)

	m.wg.Add(8)
	go m.notificationRouter()
	go m.nnlpResultRouter()
	go m.instructionRouter()
	go m.errorRouter()
	go m.lifecycleManager()
	go m.monitorSessionManagerErrors()
	go m.controlProcessor()
	go m.startAuthCodeHTTP()

	switch m.operationMode {
	case "always_on", "always_awake":
		m.asyncStartAllPlatforms()
		m.state.Store(StateRunning)
	case "scheduled_wake":
		m.state.Store(StateScheduled)
		m.scheduleNextWake()
	default:
		m.operationMode = "scheduled_wake"
		m.state.Store(StateScheduled)
		m.scheduleNextWake()
	}
	log.Printf("[Maestro] Started successfully")
	return nil
}

func (m *Maestro) lifecycleManager() {
	defer m.wg.Done()
	cfg := m.configManager.GetConfig()
	checkInterval := time.Duration(cfg.Scheduler.CheckIntervalMinutes) * time.Minute
	if checkInterval <= 0 {
		checkInterval = 1 * time.Minute
	}
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.performLifecycleChecks()
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) performLifecycleChecks() {
	m.resetPlatformDailyStats()

	switch m.operationMode {
	case "always_on", "always_awake":
		if m.state.Load() != StateRunning {
			m.asyncStartAllPlatforms()
			m.state.Store(StateRunning)
		}
	case "scheduled_wake":
		if time.Now().After(m.nextWake) {
			m.stats.mu.Lock()
			m.stats.WakeCycles++
			m.stats.LastWake = time.Now()
			m.stats.mu.Unlock()
			m.state.Store(StateRunning)
			m.asyncStartAllPlatforms()
			m.scheduleNextWake()
		}
		if time.Since(m.lastActivity) > m.idleTimeout {
			m.asyncStopAllPlatforms()
			m.state.Store(StateScheduled)
		}
	}
	m.checkScheduleTimes()
	m.checkRandomPosting()
	m.poster.CheckAndRunScheduledFromDB()
	m.updateStats()
}

// getRotationMode reads the global posting_settings row's rotation_mode
// (platform='__global__', subtype=”) — replaces config.json's old top-level
// posting.rotation_mode.
func (m *Maestro) getRotationMode() string {
	if m.db == nil {
		return "sequential"
	}
	var mode string
	err := m.db.QueryRow(`SELECT rotation_mode FROM posting_settings WHERE platform='__global__' AND subtype=''`).Scan(&mode)
	if err != nil || mode == "" {
		return "sequential"
	}
	return mode
}

// legacyPostingConfig mirrors the OLD config.json shape for the posting.*
// fields that used to live on config.Config/PlatformConfig/PlatformSubtype,
// before they moved to the posting_settings/posting_schedule tables. It's
// deliberately NOT part of the live config package — those fields don't
// exist on the real Config struct anymore. This type exists only so
// migratePostingSettingsFromConfig can read an old config.json file one
// last time on upgrade, without needing the live struct to still carry
// dead fields forever.
type legacyPostingConfig struct {
	Posting struct {
		Fallback struct {
			Random legacyRandomPosting `json:"random"`
		} `json:"fallback"`
		RotationMode string `json:"rotation_mode"`
	} `json:"posting"`
	Platforms map[string]struct {
		Posting  legacyPlatformPosting `json:"posting"`
		Subtypes []struct {
			ID      string                `json:"id"`
			Posting legacyPlatformPosting `json:"posting"`
		} `json:"subtypes"`
	} `json:"platforms"`
}

type legacyPlatformPosting struct {
	Random        legacyRandomPosting `json:"random"`
	Manual        legacyManualPosting `json:"manual"`
	ScheduleTimes []string            `json:"schedule_times"`
}

type legacyRandomPosting struct {
	Enabled       bool `json:"enabled"`
	IntervalHours struct {
		Min int `json:"min"`
		Max int `json:"max"`
	} `json:"interval_hours"`
	PostsPerCycle int  `json:"posts_per_cycle"`
	UseGlobal     bool `json:"use_global"`
}

type legacyManualPosting struct {
	Enabled bool `json:"enabled"`
	Payload struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		Media       struct {
			Type string `json:"type"`
			URL  string `json:"url"`
		} `json:"media"`
	} `json:"payload"`
}

// migratePostingSettingsFromConfig is a one-time, idempotent seed of
// posting_settings/posting_schedule from config.json's old posting.* values
// (platforms.<platform>.posting.random/.manual/.schedule_times, and the
// top-level posting.fallback.random + posting.rotation_mode). It only runs
// if posting_settings is completely empty, so existing DB-edited settings
// are never clobbered by this on a later restart — this exists purely to
// carry an admin's already-configured settings across the config→DB switch.
// Reads the config.json file directly (not through the live Config struct,
// which no longer has these fields) since this is a read-once upgrade path.
func (m *Maestro) migratePostingSettingsFromConfig() {
	if m.db == nil {
		return
	}
	var existing int
	if err := m.db.QueryRow(`SELECT COUNT(*) FROM posting_settings`).Scan(&existing); err != nil {
		log.Printf("[Maestro] migratePostingSettingsFromConfig count check failed: %v", err)
		return
	}
	if existing > 0 {
		return
	}
	raw, err := os.ReadFile(m.configManager.GetConfigPath())
	if err != nil {
		log.Printf("[Maestro] migratePostingSettingsFromConfig: could not read config.json for legacy migration: %v", err)
		return
	}
	var legacy legacyPostingConfig
	if err := json.Unmarshal(raw, &legacy); err != nil {
		log.Printf("[Maestro] migratePostingSettingsFromConfig: could not parse config.json: %v", err)
		return
	}
	log.Printf("[Maestro] posting_settings is empty — migrating posting settings from config.json")

	upsert := func(platform, subtype string, rp legacyRandomPosting, mp legacyManualPosting, rotationMode string) {
		_, err := m.db.Exec(`
			INSERT INTO posting_settings
				(platform, subtype, random_enabled, random_interval_min_hours, random_interval_max_hours,
				 random_posts_per_cycle, random_use_global,
				 manual_enabled, manual_title, manual_description, manual_media_type, manual_media_url,
				 rotation_mode)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(platform, subtype) DO NOTHING`,
			platform, subtype,
			boolToInt(rp.Enabled), rp.IntervalHours.Min, rp.IntervalHours.Max, rp.PostsPerCycle, boolToInt(rp.UseGlobal),
			boolToInt(mp.Enabled), mp.Payload.Title, mp.Payload.Description, mp.Payload.Media.Type, mp.Payload.Media.URL,
			rotationMode)
		if err != nil {
			log.Printf("[Maestro] migratePostingSettingsFromConfig upsert error for %s/%s: %v", platform, subtype, err)
		}
	}

	// Global fallback row (old config.posting.fallback.random + rotation_mode).
	upsert("__global__", "", legacy.Posting.Fallback.Random, legacyManualPosting{}, legacy.Posting.RotationMode)

	for platformID, pc := range legacy.Platforms {
		upsert(platformID, "", pc.Posting.Random, pc.Posting.Manual, "")
		for _, st := range pc.Posting.ScheduleTimes {
			m.migrateScheduleSlot(platformID, "", st)
		}
		for _, sub := range pc.Subtypes {
			upsert(platformID, sub.ID, sub.Posting.Random, sub.Posting.Manual, "")
			for _, st := range sub.Posting.ScheduleTimes {
				m.migrateScheduleSlot(platformID, sub.ID, st)
			}
		}
	}
}

func (m *Maestro) migrateScheduleSlot(platform, subtype, postTime string) {
	if _, err := time.Parse("15:04", postTime); err != nil {
		return
	}
	if _, err := m.db.Exec(`
		INSERT INTO posting_schedule (platform, subtype, post_time, enabled)
		VALUES (?,?,?,1) ON CONFLICT(platform, subtype, post_time) DO NOTHING`,
		platform, subtype, postTime); err != nil {
		log.Printf("[Maestro] migrateScheduleSlot error for %s/%s@%s: %v", platform, subtype, postTime, err)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (m *Maestro) checkRandomPosting() {
	cfg := m.configManager.GetConfig()
	// Snapshot per-subtype status entries under the lock, then do random
	// posting per subtype so each subtype of a platform respects its own
	// rate limit (previous code used a single subtype per platform).
	m.mu.RLock()
	type entry struct {
		platformID string
		subtypeID  string
		status     *PlatformStatus
		posting    PostingSettings
	}
	var entries []entry
	for _, st := range m.platformStatus {
		if st == nil || !st.Enabled {
			continue
		}
		pc, exists := cfg.Platforms[st.PlatformID]
		if !exists || !pc.Enabled {
			continue
		}
		// Posting timing now lives in posting_settings (DB), not
		// config.json — pc.Enabled above is still the platform on/off
		// switch, but the "should this platform post at random?" answer
		// comes from the DB now.
		ps, ok := m.getPostingSettings(st.PlatformID, st.SubtypeID)
		if !ok || !ps.RandomEnabled {
			continue
		}
		entries = append(entries, entry{platformID: st.PlatformID, subtypeID: st.SubtypeID, status: st, posting: ps})
	}
	m.mu.RUnlock()

	for _, e := range entries {
		intervalMax := e.posting.RandomIntervalMaxHours
		if intervalMax <= 0 {
			intervalMax = 8
		}
		if rand.Float64() < 1.0/float64(intervalMax*60) {
			// CanProceed already reserved the slot atomically — no
			// separate RecordUsage call needed anymore.
			if ok, _ := m.CanProceed(e.platformID, e.subtypeID, "upload"); !ok {
				continue
			}
			m.poster.PostRandomToPlatform(e.platformID, e.subtypeID, "")
			m.stats.mu.Lock()
			m.stats.RandomPostsSent++
			m.stats.mu.Unlock()
			key := m.platformKey(e.platformID, e.subtypeID)
			m.updatePlatformStatus(key, func(status *PlatformStatus) {
				if status.DailyStats != nil {
					status.DailyStats.PostsToday++
				}
			})
		}
	}
}

func (m *Maestro) getTimezone() *time.Location {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()
	if m.timezone == nil {
		return time.UTC
	}
	return m.timezone
}

func (m *Maestro) localStartOfDay(t time.Time) time.Time {
	loc := m.getTimezone()
	y, mo, d := t.In(loc).Date()
	return time.Date(y, mo, d, 0, 0, 0, 0, loc)
}

// checkScheduleTimes fires each platform's own configured schedule_times
// slots independently (platforms.<platform>.posting.schedule_times), gated
// per platform/subtype through the same DB-backed rate_limits table as
// checkRandomPosting — no separate global gate, and one platform hitting its
// scheduled-post limit doesn't hold back another platform's slot.
func (m *Maestro) checkScheduleTimes() {
	now := time.Now().In(m.timezone)
	currentHM := TimeSlot{Hour: now.Hour(), Minute: now.Minute()}
	todayStart := m.localStartOfDay(now)

	cfg := m.configManager.GetConfig()
	m.mu.RLock()
	type entry struct {
		platformID string
		subtypeID  string
	}
	byPlatform := make(map[string][]entry)
	for _, st := range m.platformStatus {
		if st == nil || !st.Enabled {
			continue
		}
		byPlatform[st.PlatformID] = append(byPlatform[st.PlatformID], entry{platformID: st.PlatformID, subtypeID: st.SubtypeID})
	}
	m.mu.RUnlock()

	m.scheduleMu.Lock()
	defer m.scheduleMu.Unlock()
	for _, slot := range m.scheduleTimes {
		if slot.Hour != currentHM.Hour || slot.Minute != currentHM.Minute {
			continue
		}
		pc, exists := cfg.Platforms[slot.PlatformID]
		if !exists || !pc.Enabled {
			continue
		}
		key := fmt.Sprintf("%s@%02d:%02d", slot.PlatformID, slot.Hour, slot.Minute)
		last, fired := m.lastFired[key]
		if fired && !last.Before(todayStart) {
			continue
		}
		m.lastFired[key] = now
		for _, e := range byPlatform[slot.PlatformID] {
			if ok, _ := m.CanProceed(e.platformID, e.subtypeID, "upload"); !ok {
				continue
			}
			m.poster.PostRandomToPlatform(e.platformID, e.subtypeID, "")
			m.stats.mu.Lock()
			m.stats.ScheduledPostsSent++
			m.stats.mu.Unlock()
		}
	}
}

func (m *Maestro) resetPlatformDailyStats() {
	now := time.Now()
	todayStart := m.localStartOfDay(now)
	m.mu.Lock()
	for _, status := range m.platformStatus {
		if status.DailyStats != nil && m.localStartOfDay(status.DailyStats.LastReset).Before(todayStart) {
			status.DailyStats = &PlatformDailyStats{LastReset: now}
		}
	}
	m.mu.Unlock()
	m.listenersMu.Lock()
	for _, pl := range m.listeners {
		pl.statsMu.Lock()
		if pl.DailyStats != nil && m.localStartOfDay(pl.DailyStats.LastReset).Before(todayStart) {
			pl.DailyStats = &PlatformDailyStats{LastReset: now}
		}
		pl.statsMu.Unlock()
	}
	m.listenersMu.Unlock()
}

func (m *Maestro) updateStats() {
	active := 0
	totalSent := 0
	m.mu.RLock()
	for _, status := range m.platformStatus {
		if status.IsRunning {
			active++
		}
		if status.DailyStats != nil {
			totalSent += status.DailyStats.SentToday
		}
	}
	m.mu.RUnlock()

	// PostsToday/PostsThisHour now come straight from the rate_limits
	// table (summed across every platform/subtype's "posts" row) instead
	// of a separate in-memory counter — the DB is the only place post
	// counts are tracked now, so this is just reading it for display.
	var postsToday, postsThisHour int
	if m.db != nil {
		m.db.QueryRow(`SELECT COALESCE(SUM(current_day_count),0), COALESCE(SUM(current_hour_count),0)
			FROM rate_limits WHERE action = 'posts'`).Scan(&postsToday, &postsThisHour)
	}

	m.stats.mu.Lock()
	m.stats.ActivePlatforms = active
	m.stats.TotalSentToday = totalSent
	m.stats.PostsToday = postsToday
	m.stats.PostsThisHour = postsThisHour
	m.stats.mu.Unlock()
}

func (m *Maestro) scheduleNextWake() {
	m.lifecycleMu.Lock()
	m.nextWake = time.Now().Add(m.wakeInterval)
	m.lifecycleMu.Unlock()
	log.Printf("[Maestro] Next wake: %s", m.nextWake.Format(time.RFC3339))
}

func (m *Maestro) platformKey(platformID, subtypeID string) string {
	return fmt.Sprintf("%s:%s", platformID, subtypeID)
}

func (m *Maestro) updatePlatformStatus(key string, updateFunc func(*PlatformStatus)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if status, exists := m.platformStatus[key]; exists {
		updateFunc(status)
	}
}

func (m *Maestro) isPlatformEnabledInConfig(platformID, subtypeID string) bool {
	cfg := m.configManager.GetConfig()
	platformCfg, exists := cfg.Platforms[platformID]
	if !exists || !platformCfg.Enabled {
		return false
	}
	if len(platformCfg.Subtypes) > 0 {
		for _, sub := range platformCfg.Subtypes {
			if sub.ID == subtypeID {
				return sub.Enabled
			}
		}
		return false
	}
	return true
}

func (m *Maestro) asyncStartAllPlatforms() {
	cfg := m.configManager.GetConfig()

	m.mu.RLock()
	toStart := make([]struct{ platformID, subtypeID string }, 0, len(m.platformStatus))
	for _, status := range m.platformStatus {
		if !status.Enabled {
			continue
		}
		platformCfg, exists := cfg.Platforms[status.PlatformID]
		if !exists || !platformCfg.Enabled {
			continue
		}
		if len(platformCfg.Subtypes) > 0 {
			subtypeEnabled := false
			for _, sub := range platformCfg.Subtypes {
				if sub.ID == status.SubtypeID && sub.Enabled {
					subtypeEnabled = true
					break
				}
			}
			if !subtypeEnabled {
				continue
			}
		}
		toStart = append(toStart, struct{ platformID, subtypeID string }{
			status.PlatformID, status.SubtypeID,
		})
	}
	m.mu.RUnlock()

	for _, p := range toStart {
		go func(platformID, subtypeID string) {
			log.Printf("[Maestro] Waiting 3s before starting %s:%s", platformID, subtypeID)
			time.Sleep(3 * time.Second)
			if err := m.startPlatform(platformID, subtypeID); err != nil {
				log.Printf("[Maestro] Failed to start %s:%s: %v", platformID, subtypeID, err)
			}
		}(p.platformID, p.subtypeID)
	}
}

func (m *Maestro) startPlatform(platformID, subtypeID string) error {
	if !m.isPlatformEnabledInConfig(platformID, subtypeID) {
		log.Printf("[Maestro] startPlatform: %s:%s is disabled in config, skipping", platformID, subtypeID)
		return nil
	}

	key := m.platformKey(platformID, subtypeID)
	m.mu.RLock()
	runtimeStatus, statusExists := m.platformStatus[key]
	m.mu.RUnlock()
	if statusExists && !runtimeStatus.Enabled {
		log.Printf("[Maestro] startPlatform: %s:%s disabled at runtime, skipping", platformID, subtypeID)
		return nil
	}

	if _, alreadyStarting := m.starting.LoadOrStore(key, struct{}{}); alreadyStarting {
		return nil
	}
	defer m.starting.Delete(key)

	m.listenersMu.RLock()
	existing, exists := m.listeners[key]
	m.listenersMu.RUnlock()
	if exists && existing.IsRunning.Load() {
		return fmt.Errorf("platform %s already running", key)
	}

	sessionReq := session.SessionRequest{PlatformID: platformID, Subtype: subtypeID, AccountID: "", ForceNew: false}

	if platformID == "telegram" {
		accountID := m.resolveTelegramAccountID(subtypeID)
		if accountID == "" {
			return fmt.Errorf("telegram: no phone_number found for subtype %s in config", subtypeID)
		}
		sessionReq.AccountID = accountID
		if err := m.sessionManager.StartTelegram(sessionReq); err != nil {
			log.Printf("[Maestro] telegram StartTelegram warning (may need auth): %v", err)
		}
		tgDir := filepath.Join(m.sessionManager.GetStorage().GetSessionsDir(), "telegram")
		sessionFile := filepath.Join(tgDir, fmt.Sprintf("tg_%s_%s.session", subtypeID, accountID))
		synthSess := &session.Session{
			PlatformID: platformID,
			Subtype:    subtypeID,
			AccountID:  accountID,
			State:      "authenticated",
			CreatedAt:  time.Now(),
			LastUsed:   time.Now(),
			ExpiresAt:  time.Now().Add(30 * 24 * time.Hour),
			Metadata: map[string]string{
				"session_type": "gogram_mtproto",
				"session_file": sessionFile,
			},
		}
		return m.createListenerWithSession(platformID, subtypeID, synthSess)
	}

	sessionResp, err := m.sessionManager.GetSession(m.ctx, sessionReq)
	if err != nil {
		return fmt.Errorf("session manager error: %w", err)
	}
	if platformID == "whatsapp" {
		if _, err := m.sessionManager.StartWhatsApp(sessionReq); err != nil {
			return fmt.Errorf("whatsapp start failed: %w", err)
		}
	}
	switch sessionResp.State {
	case "authenticated":
		return m.createListenerWithSession(platformID, subtypeID, sessionResp)
	case "unauthenticated":
		return m.handlePendingLogin(platformID, subtypeID, sessionResp)
	default:
		return fmt.Errorf("unexpected session state: %s", sessionResp.State)
	}
}

func (m *Maestro) handlePendingLogin(platformID, subtypeID string, sessionResp *session.Session) error {
	key := m.platformKey(platformID, subtypeID)
	listenerConfig := m.getListenerConfig(platformID, subtypeID)
	_, listenerCancel := context.WithCancel(m.ctx)
	platformListener := &PlatformListener{
		PlatformID: platformID,
		SubtypeID:  subtypeID,
		AccountID:  sessionResp.AccountID,
		Config:     listenerConfig,
		Session:    sessionResp,
		Cancel:     listenerCancel,
		DailyStats: &PlatformDailyStats{LastReset: time.Now()},
	}
	m.listenersMu.Lock()
	m.listeners[key] = platformListener
	m.listenersMu.Unlock()
	m.updatePlatformStatus(key, func(status *PlatformStatus) {
		status.IsRunning = false
		status.SessionState = "needs_browser_login"
		status.StartTime = time.Now()
	})
	platformListener.IsRunning.Store(false)
	return nil
}

func (m *Maestro) resolveTelegramAccountID(subtypeID string) string {
	cfg := m.configManager.GetConfig()
	p, ok := cfg.Platforms["telegram"]
	if !ok {
		return ""
	}
	for _, sub := range p.Subtypes {
		if subtypeID != "" && sub.ID != subtypeID {
			continue
		}
		if sub.Auth == nil {
			continue
		}
		v, ok2 := sub.Auth["phone_number"]
		if !ok2 {
			continue
		}
		phone, _ := v.(string)
		phone = strings.TrimSpace(phone)
		phone = strings.TrimPrefix(phone, "+")
		if phone != "" {
			return phone
		}
	}
	return ""
}

func (m *Maestro) createListenerWithSession(platformID, subtypeID string, sess *session.Session) error {
	key := m.platformKey(platformID, subtypeID)
	m.stats.mu.Lock()
	m.stats.SessionsCreated++
	m.stats.mu.Unlock()

	listenerConfig := m.getListenerConfig(platformID, subtypeID)
	listenerCtx, listenerCancel := context.WithCancel(m.ctx)

	collector := m.createPlatformCollector(platformID, subtypeID, sess.AccountID, listenerConfig)
	if collector == nil {
		listenerCancel()
		m.updatePlatformStatus(key, func(status *PlatformStatus) {
			status.SessionState = "collector_failed"
			status.IsRunning = false
		})
		return fmt.Errorf("failed to create collector for %s:%s", platformID, subtypeID)
	}
	if platformID == "whatsapp" {
		waClient, err := m.sessionManager.GetWhatsAppClient(platformID, subtypeID)
		if err != nil {
			listenerCancel()
			return fmt.Errorf("WhatsApp client error: %w", err)
		}
		if setter, ok := collector.(listener.WhatsAppClientSetter); ok {
			if err := setter.SetClient(waClient); err != nil {
				listenerCancel()
				return fmt.Errorf("set client error: %w", err)
			}
		} else {
			listenerCancel()
			return fmt.Errorf("WhatsApp collector does not implement ClientSetter")
		}
	} else if connector, ok := collector.(Connector); ok {
		if err := connector.Connect(listenerCtx); err != nil {
			if platformID == "telegram" {
				log.Printf("[Maestro] telegram Connect warning (auth may be needed): %v", err)
				m.updatePlatformStatus(key, func(status *PlatformStatus) {
					status.SessionState = "needs_auth"
					status.IsRunning = false
				})
			} else {
				listenerCancel()
				m.updatePlatformStatus(key, func(status *PlatformStatus) {
					status.SessionState = "connect_failed"
					status.IsRunning = false
				})
				return fmt.Errorf("collector connect failed: %w", err)
			}
		}
	}

	platformListener := &PlatformListener{
		PlatformID: platformID,
		SubtypeID:  subtypeID,
		AccountID:  sess.AccountID,
		Config:     listenerConfig,
		Session:    sess,
		Cancel:     listenerCancel,
		DailyStats: &PlatformDailyStats{LastReset: time.Now()},
		Collector:  collector,
	}
	m.listenersMu.Lock()
	m.listeners[key] = platformListener
	m.listenersMu.Unlock()
	m.updatePlatformStatus(key, func(status *PlatformStatus) {
		status.IsRunning = true
		status.StartTime = time.Now()
		status.SessionState = "active"
	})
	platformListener.IsRunning.Store(true)

	if echan := collector.GetErrorChannel(); echan != nil {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			for {
				select {
				case pe, ok := <-echan:
					if !ok {
						return
					}
					m.reportError(pe.PlatformID, pe.SubtypeID, pe.AccountID, pe.ErrorCode, pe.ErrorMsg, pe.Severity,
						pe.Severity != "critical", pe.Severity == "critical")
				case <-listenerCtx.Done():
					return
				}
			}
		}()
	}
	m.wg.Add(1)
	go m.runPlatformListener(listenerCtx, platformListener)
	return nil
}

func (m *Maestro) createPlatformCollector(platformID, subtypeID, accountID string, cfg *listener.ListenerConfig) PlatformCollector {
	if m.db == nil || m.configManager == nil || m.envManager == nil {
		return nil
	}
	switch platformID {
	case "facebook":
		return listener.NewFacebookCollector(platformID, subtypeID, accountID, "account", cfg, m.db, m.configManager, m.envManager, m.sessionManager)
	case "telegram":
		return listener.NewTelegramCollector(platformID, subtypeID, accountID, "account", cfg, m.db, m.configManager, m.envManager, m.sessionManager)
	case "whatsapp":
		return listener.NewWhatsAppCollector(platformID, subtypeID, accountID, "account", cfg, m.db, m.configManager, m.envManager, m.sessionManager)
	case "twitter":
		return listener.NewTwitterCollector(platformID, subtypeID, accountID, "account", cfg, m.db, m.configManager, m.envManager, m.sessionManager)
	case "viber":
		return listener.NewViberCollector(platformID, subtypeID, accountID, "account", cfg, m.db, m.configManager, m.envManager, m.sessionManager)
	default:
		return nil
	}
}

func (m *Maestro) runPlatformListener(ctx context.Context, pl *PlatformListener) {
	defer m.wg.Done()
	defer pl.IsRunning.Store(false)
	defer func() {
		if r := recover(); r != nil {
			m.createUrgentAlert(pl.PlatformID, pl.SubtypeID, "LISTENER_PANIC", fmt.Sprintf("Listener crashed: %v", r))
		}
	}()
	pollingInterval := time.Duration(pl.Config.PollingInterval) * time.Second
	if pollingInterval < 30*time.Second {
		pollingInterval = 60 * time.Second
	}
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()
	m.collectWithErrorHandling(ctx, pl)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pl.IsPaused.Load() {
				continue
			}
			m.collectWithErrorHandling(ctx, pl)
		}
	}
}

func (m *Maestro) stopPlatform(platformID, subtypeID string) error {
	key := m.platformKey(platformID, subtypeID)
	m.listenersMu.Lock()
	pl, exists := m.listeners[key]
	if !exists {
		m.listenersMu.Unlock()
		return fmt.Errorf("platform %s not found", key)
	}
	if pl.Cancel != nil {
		pl.Cancel()
	}
	pl.IsRunning.Store(false)
	pl.IsPaused.Store(false)
	delete(m.listeners, key)
	m.listenersMu.Unlock()
	m.updatePlatformStatus(key, func(status *PlatformStatus) {
		status.IsRunning = false
		status.IsPaused = false
	})
	return nil
}

func (m *Maestro) asyncStopAllPlatforms() {
	m.listenersMu.Lock()
	for key, pl := range m.listeners {
		if pl != nil && pl.Cancel != nil {
			pl.Cancel()
		}
		pl.IsRunning.Store(false)
		pl.IsPaused.Store(false)
		delete(m.listeners, key)
	}
	m.listenersMu.Unlock()

	m.mu.Lock()
	for _, status := range m.platformStatus {
		status.IsRunning = false
		status.IsPaused = false
	}
	m.mu.Unlock()
}

func (m *Maestro) notificationRouter() {
	defer m.wg.Done()
	for {
		select {
		case notif := <-m.notificationChan:
			if notif == nil {
				continue
			}
			// Track this goroutine so Stop() can wait for it before
			// closing nnlpResultChan. Otherwise a send on the closed
			// channel panics the whole process during shutdown.
			m.notificationWG.Add(1)
			go func(n *listener.Notification) {
				defer m.notificationWG.Done()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[Maestro] notification handler panic: %v", r)
					}
				}()
				sandbox.RecordTrace(n.ID, "received", map[string]interface{}{
					"platform":  n.PlatformID,
					"subtype":   n.SubtypeID,
					"sender":    n.Message.Sender.UserID,
					"text":      n.Message.Text,
					"timestamp": n.Timestamp.UTC().Format(time.RFC3339Nano),
				})
				result, err := m.processor.ProcessNotification(m.ctx, n)
				if err != nil {
					sandbox.RecordTrace(n.ID, "processor_error", map[string]interface{}{
						"error": err.Error(),
					})
					return
				}
				if result.Action == "queued" {
					sandbox.RecordTrace(n.ID, "queued", map[string]interface{}{
						"ticket_id": result.TicketID,
					})
					return
				}
				// Use non-blocking send + done-check so a shutdown that
				// closes nnlpResultChan can never panic.
				select {
				case <-m.shutdownChan:
					log.Printf("[Maestro] dropping result %s during shutdown", result.TicketID)
				default:
				}
				select {
				case m.nnlpResultChan <- result:
				default:
					log.Printf("[Maestro] NNLP result channel full, dropping: %s", result.TicketID)
				}
			}(notif)
			m.stats.mu.Lock()
			m.stats.TotalNotifications++
			m.stats.mu.Unlock()
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) nnlpResultRouter() {
	defer m.wg.Done()
	for {
		select {
		case result := <-m.nnlpResultChan:
			if result == nil {
				continue
			}
			m.handleNNLPResult(result)
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) handleNNLPResult(result *nnlp.ProcessResult) {
	m.stats.mu.Lock()
	if current, ok := m.stats.NNLPStats[result.Action]; ok {
		if count, ok := current.(int); ok {
			m.stats.NNLPStats[result.Action] = count + 1
		}
	} else {
		m.stats.NNLPStats[result.Action] = 1
	}
	m.stats.mu.Unlock()

	notifID, _ := result.Data["notification_id"].(string)

	sandbox.RecordTrace(notifID, "nlp_result", map[string]interface{}{
		"action":    result.Action,
		"intent":    result.Intent,
		"ticket_id": result.TicketID,
		"products":  result.Data["products"],
		"source":    result.Data["source"],
		"user_data": result.Data["user_data"],
		"raw_text":  result.Data["raw_text"],
	})

	// Surface actual DB mutations (account creation, message insert,
	// conversation-state/intent overwrite, AI ticket insert, ...) that
	// processor.go recorded into result.Data["db_writes"]. This can't be
	// recorded from processor.go itself — package nnlp is imported BY
	// sandbox, so nnlp calling into sandbox would be an import cycle.
	if writes, ok := result.Data["db_writes"].([]map[string]interface{}); ok {
		for _, w := range writes {
			sandbox.RecordTrace(notifID, "db_write", w)
		}
	}

	m.sendToCompiler(result)
}

func (m *Maestro) sendToCompiler(result *nnlp.ProcessResult) {
	instruction, err := m.compiler.Compile(result)
	if err != nil {
		notifID, _ := result.Data["notification_id"].(string)
		sandbox.RecordTrace(notifID, "compile_failed", map[string]interface{}{
			"error":  err.Error(),
			"action": result.Action,
			"intent": result.Intent,
		})
		log.Printf("[Maestro] Compile failed for ticket %s (action=%s intent=%s): %v",
			result.TicketID, result.Action, result.Intent, err)
		m.createUrgentAlert("", "", "compile_failed",
			fmt.Sprintf("ticket %s (action=%s intent=%s) failed to compile: %v",
				result.TicketID, result.Action, result.Intent, err))
		return
	}

	m.tapMu.Lock()
	tap := m.sandboxTap
	m.tapMu.Unlock()
	if tap != nil {
		tap(result, instruction)
	}

	select {
	case m.instructionChan <- instruction:
	default:
		log.Printf("[Maestro] Instruction channel full, dropping: %s", result.TicketID)
		m.createUrgentAlert(instruction.Platform, instruction.SubtypeID, "instruction_channel_full",
			fmt.Sprintf("instruction %s (%s) dropped — instruction channel full", instruction.TicketID, instruction.Action))
	}
}

func isSendAction(action string) bool {
	lower := strings.ToLower(action)
	return strings.HasPrefix(lower, "send_") ||
		strings.Contains(lower, "reply") ||
		strings.Contains(lower, "respond") ||
		lower == "send"
}

func (m *Maestro) instructionRouter() {
	defer m.wg.Done()
	for {
		select {
		case instruction := <-m.instructionChan:
			if instruction == nil {
				continue
			}
			key := m.platformKey(instruction.Platform, instruction.SubtypeID)
			m.listenersMu.RLock()
			pl, exists := m.listeners[key]
			m.listenersMu.RUnlock()
			if !exists {
				log.Printf("[Maestro] no listener registered for %s, dropping instruction %s", key, instruction.TicketID)
				m.createUrgentAlert(instruction.Platform, instruction.SubtypeID, "instruction_undeliverable",
					fmt.Sprintf("no listener registered for %s — instruction %s (%s) dropped",
						key, instruction.TicketID, instruction.Action))
				continue
			}
			if pl.Collector == nil {
				continue
			}

			const maxDeliveryAttempts = 3
			var deliverErr error
			for attempt := 1; attempt <= maxDeliveryAttempts; attempt++ {
				if deliverErr = pl.Collector.ReceiveInstructions(instruction); deliverErr == nil {
					break
				}
				if attempt < maxDeliveryAttempts {
					time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				}
			}

			if deliverErr != nil {
				sandbox.RecordTrace(instruction.NotificationID, "delivery_failed", map[string]interface{}{
					"error":    deliverErr.Error(),
					"attempts": maxDeliveryAttempts,
				})
				m.reportError(instruction.Platform, instruction.SubtypeID, instruction.AccountID,
					"INSTRUCTION_DELIVERY_FAILED", deliverErr.Error(), "warning", false, false)
				m.createUrgentAlert(instruction.Platform, instruction.SubtypeID, "instruction_delivery_failed",
					fmt.Sprintf("instruction %s (%s) failed delivery after %d attempts: %v",
						instruction.TicketID, instruction.Action, maxDeliveryAttempts, deliverErr))
				continue
			}

			sandbox.RecordTrace(instruction.NotificationID, "delivered", map[string]interface{}{
				"platform": instruction.Platform,
				"action":   instruction.Action,
				"intent":   instruction.Intent,
				"ticket":   instruction.TicketID,
			})

			if isSendAction(instruction.Action) {
				pl.statsMu.Lock()
				if pl.DailyStats != nil {
					pl.DailyStats.SentToday++
				}
				pl.statsMu.Unlock()
				m.updatePlatformStatus(key, func(status *PlatformStatus) {
					if status.DailyStats != nil {
						status.DailyStats.SentToday++
					}
				})
				m.stats.mu.Lock()
				m.stats.TotalSentToday++
				m.stats.mu.Unlock()
			}
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) errorRouter() {
	defer m.wg.Done()
	for {
		select {
		case err := <-m.errorChan:
			if err == nil {
				continue
			}
			m.handlePlatformError(err)
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) monitorSessionManagerErrors() {
	defer m.wg.Done()
	if m.sessionManager == nil {
		return
	}
	sessionErrorChan := m.sessionManager.GetErrorChannel()
	if sessionErrorChan == nil {
		return
	}
	for {
		select {
		case sessionErr, ok := <-sessionErrorChan:
			if !ok {
				return
			}
			maestroErr := &PlatformError{
				PlatformID:      sessionErr.PlatformID,
				SubtypeID:       sessionErr.Subtype,
				AccountID:       sessionErr.AccountID,
				ErrorCode:       sessionErr.Type,
				ErrorMsg:        sessionErr.Error,
				Timestamp:       sessionErr.Timestamp,
				Severity:        sessionErr.Severity,
				Reconnect:       sessionErr.Severity != "critical" && !strings.Contains(sessionErr.Error, "disable"),
				DisablePlatform: sessionErr.Severity == "critical" || strings.Contains(sessionErr.Error, "disable"),
			}
			select {
			case m.errorChan <- maestroErr:
			default:
			}
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) handlePlatformError(err *PlatformError) {
	if err.ErrorCode == "LOGGED_OUT" {
		log.Printf("[Maestro] LOGGED_OUT for %s:%s – cleaning up session and restarting",
			err.PlatformID, err.SubtypeID)
		if m.sessionManager != nil {
			m.sessionManager.MarkCorrupted(err.PlatformID, err.SubtypeID, true)
		}
		m.stopPlatform(err.PlatformID, err.SubtypeID)
		go func() {
			time.Sleep(2 * time.Second)
			if startErr := m.startPlatform(err.PlatformID, err.SubtypeID); startErr != nil {
				log.Printf("[Maestro] Failed to restart %s:%s after LOGGED_OUT: %v",
					err.PlatformID, err.SubtypeID, startErr)
			}
		}()
		return
	}

	if err.ErrorCode == "MANUAL_SUCCESS" {
		go func() {
			time.Sleep(500 * time.Millisecond)
			placeholderKey := m.platformKey(err.PlatformID, err.SubtypeID)
			m.listenersMu.Lock()
			if pl, exists := m.listeners[placeholderKey]; exists && !pl.IsRunning.Load() {
				if pl.Cancel != nil {
					pl.Cancel()
				}
				delete(m.listeners, placeholderKey)
			}
			m.listenersMu.Unlock()
			m.updatePlatformStatus(placeholderKey, func(status *PlatformStatus) { status.Enabled = true })
			m.startPlatform(err.PlatformID, err.SubtypeID)
		}()
		return
	}

	if err.DisablePlatform {
		m.disablePlatformInConfig(err.PlatformID, err.SubtypeID, fmt.Sprintf("%s: %s", err.ErrorCode, err.ErrorMsg))
		m.stopPlatform(err.PlatformID, err.SubtypeID)
	}
	if err.Reconnect && !err.DisablePlatform {
		key := m.platformKey(err.PlatformID, err.SubtypeID)
		m.listenersMu.RLock()
		pl, exists := m.listeners[key]
		m.listenersMu.RUnlock()
		if exists && pl.IsRunning.Load() {
			go func() {
				if reconErr := m.reconnectPlatform(m.ctx, pl); reconErr != nil {
					log.Printf("[Maestro] Auto-reconnect failed: %v", reconErr)
				} else {
					m.resumeListenerAfterRecovery(err.PlatformID, err.SubtypeID)
				}
			}()
		}
	}
	m.stats.mu.Lock()
	m.stats.TotalErrors++
	m.stats.mu.Unlock()
}

func (m *Maestro) handleSessionError(platformID, subtypeID, accountID, errorCode, errorMsg string) {
	key := m.platformKey(platformID, subtypeID)
	m.listenersMu.RLock()
	pl, exists := m.listeners[key]
	m.listenersMu.RUnlock()
	if exists {
		pl.IsPaused.Store(true)
		go m.scheduleListenerResume(pl, 30*time.Second)
	}
	select {
	case m.errorChan <- &PlatformError{
		PlatformID:      platformID,
		SubtypeID:       subtypeID,
		AccountID:       accountID,
		ErrorCode:       errorCode,
		ErrorMsg:        errorMsg,
		Timestamp:       time.Now(),
		Severity:        "error",
		Reconnect:       true,
		DisablePlatform: false,
	}:
	default:
	}
}

func (m *Maestro) scheduleListenerResume(pl *PlatformListener, delay time.Duration) {
	select {
	case <-time.After(delay):
		key := m.platformKey(pl.PlatformID, pl.SubtypeID)
		m.listenersMu.RLock()
		_, exists := m.listeners[key]
		m.listenersMu.RUnlock()
		if exists && pl.IsPaused.Load() {
			pl.IsPaused.Store(false)
		}
	case <-m.ctx.Done():
		return
	}
}

func (m *Maestro) resumeListenerAfterRecovery(platformID, subtypeID string) {
	key := m.platformKey(platformID, subtypeID)
	m.listenersMu.RLock()
	pl, exists := m.listeners[key]
	m.listenersMu.RUnlock()
	if !exists {
		go m.startPlatform(platformID, subtypeID)
		return
	}
	pl.ReconnectFail.Store(false)
	if pl.IsPaused.Load() {
		pl.IsPaused.Store(false)
	}
	if !pl.IsRunning.Load() {
		listenerCtx, listenerCancel := context.WithCancel(m.ctx)
		m.listenersMu.Lock()
		currentPl, stillExists := m.listeners[key]
		if stillExists && currentPl == pl {
			pl.Cancel = listenerCancel
		} else {
			m.listenersMu.Unlock()
			listenerCancel()
			return
		}
		m.listenersMu.Unlock()
		pl.IsRunning.Store(true)
		m.wg.Add(1)
		go m.runPlatformListener(listenerCtx, pl)
	}
}

func isShutdownError(listenerCtx context.Context, maestroCtx context.Context, errMsg string) bool {
	lower := strings.ToLower(errMsg)
	if strings.Contains(lower, "operation was canceled") {
		return true
	}
	if strings.Contains(lower, "context canceled") || strings.Contains(lower, "context deadline exceeded") {
		if listenerCtx != nil {
			select {
			case <-listenerCtx.Done():
				return true
			default:
			}
		}
		if maestroCtx != nil {
			select {
			case <-maestroCtx.Done():
				return true
			default:
			}
		}
		return false
	}
	return false
}

func (m *Maestro) collectWithErrorHandling(ctx context.Context, pl *PlatformListener) {
	if pl.ReconnectFail.Load() {
		return
	}
	m.lastActivity = time.Now()
	if pl.PlatformID == "facebook" {
		if err := m.validateAndRefreshSession(ctx, pl); err != nil {
			if isShutdownError(ctx, m.ctx, err.Error()) {
				return
			}
			if reconErr := m.reconnectPlatform(ctx, pl); reconErr != nil {
				pl.ReconnectFail.Store(true)
				m.handleSessionError(pl.PlatformID, pl.SubtypeID, pl.AccountID, "SESSION_RECONNECT_FAILED", fmt.Sprintf("Session reconnection failed: %v", reconErr))
				m.createUrgentAlert(pl.PlatformID, pl.SubtypeID, "RECONNECT_FAILED", fmt.Sprintf("Auto-reconnect failed: %v. Manual intervention required.", reconErr))
				m.reportError(pl.PlatformID, pl.SubtypeID, pl.AccountID, "RECONNECT_FAILED", reconErr.Error(), "critical", false, true)
				return
			}
		}
	}
	pl.statsMu.Lock()
	if pl.DailyStats != nil && m.localStartOfDay(pl.DailyStats.LastReset).Before(m.localStartOfDay(time.Now())) {
		pl.DailyStats = &PlatformDailyStats{LastReset: time.Now()}
	}
	pl.statsMu.Unlock()

	var cookies []*listener.CookieData
	if pl.Session != nil {
		for _, c := range pl.Session.Cookies {
			cookies = append(cookies, &listener.CookieData{
				Name: c.Name, Value: c.Value, Domain: c.Domain,
				Path: c.Path, Expires: c.Expires, Secure: c.Secure, HTTPOnly: c.HTTPOnly,
			})
		}
	}
	notifications, err := pl.Collector.Collect(ctx, cookies)
	if err != nil {
		if isShutdownError(ctx, m.ctx, err.Error()) {
			return
		}
		if m.isSessionError(err.Error()) {
			m.handleSessionError(pl.PlatformID, pl.SubtypeID, pl.AccountID, "COLLECTION_SESSION_ERROR", err.Error())
		}
		atomic.AddInt32(&pl.ErrorCount, 1)
		pl.LastError = err.Error()
		key := m.platformKey(pl.PlatformID, pl.SubtypeID)
		m.updatePlatformStatus(key, func(status *PlatformStatus) {
			atomic.AddInt32(&status.ErrorCount, 1)
			status.LastError = err.Error()
		})
		shouldReconnect := m.shouldReconnect(err)
		shouldDisable := m.shouldDisablePlatform(err, atomic.LoadInt32(&pl.ErrorCount))
		m.reportError(pl.PlatformID, pl.SubtypeID, pl.AccountID, "COLLECTION_FAILED", err.Error(), "warning", shouldReconnect, shouldDisable)
		if shouldReconnect && !shouldDisable {
			if reconErr := m.reconnectPlatform(ctx, pl); reconErr != nil {
				key := m.platformKey(pl.PlatformID, pl.SubtypeID)
				m.updatePlatformStatus(key, func(status *PlatformStatus) {
					atomic.AddInt32(&status.ReconnectAttempts, 1)
					status.LastReconnect = time.Now()
				})
			}
		}
		if shouldDisable {
			m.disablePlatformInConfig(pl.PlatformID, pl.SubtypeID, fmt.Sprintf("Too many consecutive errors: %v", err))
		}
		return
	}
	atomic.StoreInt32(&pl.ErrorCount, 0)
	pl.LastError = ""
	key := m.platformKey(pl.PlatformID, pl.SubtypeID)
	m.updatePlatformStatus(key, func(status *PlatformStatus) {
		status.LastCollect = time.Now()
		status.LastError = ""
		atomic.StoreInt32(&status.ErrorCount, 0)
	})
	for _, notif := range notifications {
		select {
		case m.notificationChan <- notif:
			atomic.AddInt64(&pl.NotifCount, 1)
			pl.statsMu.Lock()
			if pl.DailyStats != nil {
				pl.DailyStats.MessagesToday++
			}
			pl.statsMu.Unlock()
			m.updatePlatformStatus(key, func(status *PlatformStatus) {
				atomic.AddInt64(&status.NotificationCount, 1)
				status.LastCheck = time.Now()
				if status.DailyStats != nil {
					status.DailyStats.MessagesToday++
				}
			})
		case <-ctx.Done():
			return
		}
	}
}

func (m *Maestro) validateAndRefreshSession(ctx context.Context, pl *PlatformListener) error {
	if pl.Session == nil {
		return fmt.Errorf("session is nil")
	}
	sessionReq := session.SessionRequest{PlatformID: pl.PlatformID, Subtype: pl.SubtypeID, AccountID: pl.AccountID, ForceNew: false}
	existingSession, err := m.sessionManager.GetSession(ctx, sessionReq)
	if err == nil && existingSession.State == "authenticated" {
		pl.Session = existingSession
		m.stats.mu.Lock()
		m.stats.SessionsRefreshed++
		m.stats.mu.Unlock()
		return nil
	}
	return fmt.Errorf("no valid session")
}

func (m *Maestro) reconnectPlatform(ctx context.Context, pl *PlatformListener) error {
	key := m.platformKey(pl.PlatformID, pl.SubtypeID)
	m.updatePlatformStatus(key, func(status *PlatformStatus) {
		atomic.AddInt32(&status.ReconnectAttempts, 1)
		status.LastReconnect = time.Now()
	})
	sessionReq := session.SessionRequest{PlatformID: pl.PlatformID, Subtype: pl.SubtypeID, AccountID: pl.AccountID, ForceNew: false}
	sessionResp, err := m.sessionManager.GetSession(ctx, sessionReq)
	if err != nil {
		sessionReq.ForceNew = true
		sessionResp, err = m.sessionManager.GetSession(ctx, sessionReq)
		if err != nil {
			return fmt.Errorf("failed to get session: %w", err)
		}
	}
	if sessionResp.State == "unauthenticated" {
		m.createUrgentAlert(pl.PlatformID, pl.SubtypeID, "MANUAL_LOGIN_REQUIRED",
			fmt.Sprintf("Manual login required. Callback: %s", sessionResp.Metadata["callback_id"]))
		return fmt.Errorf("manual login required")
	}
	if sessionResp.State != "authenticated" {
		return fmt.Errorf("unexpected state: %s", sessionResp.State)
	}
	pl.Session = sessionResp
	pl.ReconnectFail.Store(false)
	m.stats.mu.Lock()
	m.stats.SessionReconnects++
	m.stats.mu.Unlock()
	return nil
}

func (m *Maestro) shouldReconnect(err error) bool {
	if err == nil || isShutdownError(nil, m.ctx, err.Error()) {
		return false
	}
	errStr := strings.ToLower(err.Error())
	for _, kw := range []string{"session", "auth", "token", "expired", "unauthorized", "forbidden", "credentials"} {
		if strings.Contains(errStr, kw) {
			return true
		}
	}
	return false
}

func (m *Maestro) shouldDisablePlatform(err error, errorCount int32) bool {
	if errorCount >= 20 {
		return true
	}
	errStr := strings.ToLower(err.Error())
	for _, kw := range []string{"banned", "suspended", "permanently", "account disabled", "account_disabled", "invalid credentials"} {
		if strings.Contains(errStr, kw) {
			return true
		}
	}
	return false
}

func (m *Maestro) isSessionError(errorMsg string) bool {
	if isShutdownError(nil, m.ctx, errorMsg) {
		return false
	}
	lower := strings.ToLower(errorMsg)
	for _, kw := range []string{"session", "auth", "token", "expired", "unauthorized", "forbidden",
		"credentials", "captcha", "rate limit", "rate_limit", "suspended", "banned",
		"cookie", "authentication", "authorization"} {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

func (m *Maestro) disablePlatformInConfig(platformID, subtypeID, reason string) error {
	key := m.platformKey(platformID, subtypeID)
	m.createUrgentAlert(platformID, subtypeID, "PLATFORM_DISABLED", reason)
	// Use the ConfigManager's locked accessors instead of mutating the live
	// config map returned by GetConfig(). GetPlatform returns a value copy;
	// the subtype slice is copied before mutation so the shared backing array
	// is never written concurrently with readers.
	pc, ok := m.configManager.GetPlatform(platformID)
	if !ok {
		return fmt.Errorf("platform %s not found", platformID)
	}
	wasEnabled := pc.Enabled
	if subtypeID == "account" || len(pc.Subtypes) == 0 {
		if wasEnabled {
			pc.Enabled = false
			m.configManager.SetPlatform(platformID, pc)
			m.configManager.Save()
		}
	} else {
		subs := make([]config.PlatformSubtype, len(pc.Subtypes))
		copy(subs, pc.Subtypes)
		found := false
		for i := range subs {
			if subs[i].ID == subtypeID {
				wasEnabled = subs[i].Enabled
				subs[i].Enabled = false
				found = true
				break
			}
		}
		if found && wasEnabled {
			pc.Subtypes = subs
			m.configManager.SetPlatform(platformID, pc)
			m.configManager.Save()
		}
	}
	m.stopPlatform(platformID, subtypeID)
	m.updatePlatformStatus(key, func(status *PlatformStatus) {
		status.Enabled = false
		status.IsRunning = false
		status.IsPaused = false
	})
	return nil
}

func (m *Maestro) enablePlatformInConfig(platformID, subtypeID string) error {
	key := m.platformKey(platformID, subtypeID)
	// Locked accessors only: GetPlatform returns a value copy, and the
	// subtype slice is copied before mutation so the shared backing array
	// is never written concurrently with readers of the live config.
	pc, ok := m.configManager.GetPlatform(platformID)
	if !ok {
		return fmt.Errorf("platform %s not found", platformID)
	}
	if subtypeID == "account" || len(pc.Subtypes) == 0 {
		pc.Enabled = true
	} else {
		subs := make([]config.PlatformSubtype, len(pc.Subtypes))
		copy(subs, pc.Subtypes)
		for i := range subs {
			if subs[i].ID == subtypeID {
				subs[i].Enabled = true
				break
			}
		}
		pc.Subtypes = subs
	}
	m.configManager.SetPlatform(platformID, pc)
	m.configManager.Save()
	m.updatePlatformStatus(key, func(status *PlatformStatus) { status.Enabled = true })

	go func(pID, sID string) {
		log.Printf("[Maestro] Waiting 3s before starting %s:%s (enabled via dashboard)", pID, sID)
		time.Sleep(3 * time.Second)
		if err := m.startPlatform(pID, sID); err != nil {
			log.Printf("[Maestro] Failed to start %s:%s after enabling: %v", pID, sID, err)
		}
	}(platformID, subtypeID)

	return nil
}

func (m *Maestro) reportError(platformID, subtypeID, accountID, errorCode, errorMsg, severity string, reconnect, disable bool) {
	select {
	case m.errorChan <- &PlatformError{
		PlatformID:      platformID,
		SubtypeID:       subtypeID,
		AccountID:       accountID,
		ErrorCode:       errorCode,
		ErrorMsg:        errorMsg,
		Timestamp:       time.Now(),
		Severity:        severity,
		Reconnect:       reconnect,
		DisablePlatform: disable,
	}:
	default:
	}
}

func (m *Maestro) createUrgentAlert(platformID, subtypeID, alertType, message string) {
	log.Printf("[URGENT] %s:%s - %s: %s", platformID, subtypeID, alertType, message)
	if m.db == nil {
		return
	}
	id := fmt.Sprintf("urgent_%s_%s_%d", platformID, alertType, time.Now().UnixNano())
	m.db.Exec(`INSERT INTO urgent_messages (id, platform_id, subtype_id, alert_type, message, created_at, severity, resolved) VALUES (?,?,?,?,?,?,?,?)`,
		id, platformID, subtypeID, alertType, message, time.Now(), "critical", false)
}

func (m *Maestro) getListenerConfig(platformID, subtypeID string) *listener.ListenerConfig {
	cfg := m.configManager.GetConfig()
	platformCfg, exists := cfg.Platforms[platformID]
	if !exists {
		return m.defaultListenerConfig()
	}
	return &listener.ListenerConfig{
		ListenComments:  platformCfg.Automation.AnswerComments.Enabled,
		ListenMessages:  platformCfg.Automation.AnswerDM.Enabled,
		ListenLikes:     platformCfg.Automation.AutoHeart.Enabled,
		ListenMentions:  true,
		PollingInterval: 60,
		MaxHistory:      10,
		SaveRawData:     true,
		IgnoreKeywords:  platformCfg.Automation.Filters.IgnoreWords,
	}
}

func (m *Maestro) defaultListenerConfig() *listener.ListenerConfig {
	return &listener.ListenerConfig{
		ListenComments:  true,
		ListenMessages:  true,
		ListenLikes:     false,
		ListenMentions:  true,
		PollingInterval: 60,
		MaxHistory:      10,
		SaveRawData:     true,
	}
}

func (m *Maestro) SetSandboxTap(fn func(*nnlp.ProcessResult, *shared.AutomationInstruction)) {
	m.tapMu.Lock()
	m.sandboxTap = fn
	m.tapMu.Unlock()
}

func (m *Maestro) InjectNotification(n *listener.Notification) error {
	if n == nil {
		return fmt.Errorf("nil notification")
	}
	select {
	case m.notificationChan <- n:
		return nil
	default:
		return fmt.Errorf("notification channel full")
	}
}

func (m *Maestro) RegisterSandboxListener(platformID, subtypeID, accountID string, collector PlatformCollector) {
	key := m.platformKey(platformID, subtypeID)
	pl := &PlatformListener{
		PlatformID: platformID,
		SubtypeID:  subtypeID,
		AccountID:  accountID,
		Collector:  collector,
		DailyStats: &PlatformDailyStats{LastReset: time.Now()},
	}
	pl.IsRunning.Store(true)
	m.listenersMu.Lock()
	m.listeners[key] = pl
	m.listenersMu.Unlock()
	log.Printf("[Maestro] sandbox listener registered for %s", key)
}

func (m *Maestro) startAuthCodeHTTP() {
	defer m.wg.Done()
	mux := http.NewServeMux()
	mux.HandleFunc("/submit_auth_code", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			http.Error(w, "only POST or GET", http.StatusMethodNotAllowed)
			return
		}
		token := r.FormValue("token")
		platformID := r.FormValue("platform_id")
		subtypeID := r.FormValue("subtype_id")
		code := r.FormValue("code")
		if platformID == "" || code == "" {
			http.Error(w, "missing platform_id or code", http.StatusBadRequest)
			return
		}
		if token != "" && m.sessionManager != nil {
			m.sessionManager.SubmitTelegramAuthCode(token, code)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "code received for token %s\n", token)
			return
		}
		cmd := ControlCommand{
			Action:     "submit_auth_code",
			PlatformID: platformID,
			SubtypeID:  subtypeID,
			Parameters: map[string]interface{}{"code": code},
		}
		select {
		case m.controlChan <- cmd:
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "auth code accepted for %s:%s\n", platformID, subtypeID)
		default:
			http.Error(w, "control channel full", http.StatusServiceUnavailable)
		}
	})
	m.authCodeServer = &http.Server{Addr: ":8086", Handler: mux}
	if err := m.authCodeServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[Maestro] Auth-code HTTP server stopped: %v", err)
	}
}

func (m *Maestro) controlProcessor() {
	defer m.wg.Done()
	for {
		select {
		case cmd := <-m.controlChan:
			m.handleControlCommand(cmd)
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Maestro) handleControlCommand(cmd ControlCommand) {
	switch cmd.Action {
	case "start":
		if cmd.PlatformID != "" {
			if cmd.PlatformID == "telegram" {
				go func() {
					if err := m.startPlatform(cmd.PlatformID, cmd.SubtypeID); err != nil {
						log.Printf("[Maestro] telegram start failed: %v", err)
					}
				}()
			} else {
				m.startPlatform(cmd.PlatformID, cmd.SubtypeID)
			}
		} else {
			m.asyncStartAllPlatforms()
		}
	case "stop":
		if cmd.PlatformID != "" {
			m.stopPlatform(cmd.PlatformID, cmd.SubtypeID)
		} else {
			m.asyncStopAllPlatforms()
		}
	case "restart":
		if cmd.PlatformID != "" {
			m.configManager.Load()
			m.stopPlatform(cmd.PlatformID, cmd.SubtypeID)
			time.Sleep(2 * time.Second)
			m.startPlatform(cmd.PlatformID, cmd.SubtypeID)
		}
	case "disable":
		if cmd.PlatformID != "" {
			reason := "manual disable"
			if r, ok := cmd.Parameters["reason"].(string); ok {
				reason = r
			}
			m.disablePlatformInConfig(cmd.PlatformID, cmd.SubtypeID, reason)
		}
	case "enable":
		if cmd.PlatformID != "" {
			m.enablePlatformInConfig(cmd.PlatformID, cmd.SubtypeID)
		}
	case "submit_auth_code":
		code, _ := cmd.Parameters["code"].(string)
		if code == "" {
			return
		}
		key := m.platformKey(cmd.PlatformID, cmd.SubtypeID)
		m.listenersMu.RLock()
		pl, exists := m.listeners[key]
		m.listenersMu.RUnlock()
		if !exists {
			return
		}
		if submitter, ok := pl.Collector.(AuthCodeSubmitter); ok {
			submitter.SubmitAuthCode(code)
		}
	}
}

func (m *Maestro) Pause() error {
	if m.paused.Load() {
		return fmt.Errorf("already paused")
	}
	m.paused.Store(true)
	m.state.Store(StatePaused)
	m.listenersMu.RLock()
	for _, pl := range m.listeners {
		if pl.IsRunning.Load() {
			pl.IsPaused.Store(true)
		}
	}
	m.listenersMu.RUnlock()
	return nil
}

func (m *Maestro) Resume() error {
	if !m.paused.Load() {
		return fmt.Errorf("not paused")
	}
	m.configManager.Load()
	cfg := m.configManager.GetConfig()
	if tz, err := time.LoadLocation(cfg.Scheduler.Timezone); err == nil {
		m.timezone = tz
	}
	m.operationMode = cfg.System.OperationMode
	m.wakeInterval = time.Duration(cfg.System.WakePolicy.IntervalMinutes) * time.Minute
	m.idleTimeout = time.Duration(cfg.System.WakePolicy.IdleSleepMinutes) * time.Minute
	// Ensure a rate_limits row exists for any platform/subtype that's new
	// since last run (e.g. added via the dashboard while paused). This no
	// longer re-seeds limits from config — config carries no rate limit
	// values anymore, the DB row is authoritative and edited directly.
	m.bootstrapRateLimits()
	m.refreshPlatformStatuses(cfg)
	m.loadScheduleTimesFromDB()
	m.lastFired = make(map[string]time.Time)
	m.stats.mu.Lock()
	m.stats.RotationMode = m.getRotationMode()
	m.stats.mu.Unlock()
	m.starting = sync.Map{}
	m.paused.Store(false)
	m.state.Store(StateRunning)
	m.listenersMu.RLock()
	for _, pl := range m.listeners {
		if pl.IsRunning.Load() && pl.IsPaused.Load() {
			pl.IsPaused.Store(false)
		}
	}
	m.listenersMu.RUnlock()
	return nil
}

func (m *Maestro) StartAllPlatforms() error {
	if !m.running.Load() {
		return fmt.Errorf("maestro not running")
	}
	m.asyncStartAllPlatforms()
	return nil
}

func (m *Maestro) StopAllPlatforms() error {
	m.asyncStopAllPlatforms()
	return nil
}

func (m *Maestro) SendControlCommand(cmd ControlCommand) error {
	select {
	case m.controlChan <- cmd:
		return nil
	case <-time.After(2 * time.Second):
		return fmt.Errorf("control channel full")
	}
}

func (m *Maestro) Stop() error {
	if !m.running.Load() {
		return fmt.Errorf("not running")
	}
	m.state.Store(StateStopping)
	m.running.Store(false)
	m.cancel()
	close(m.shutdownChan)
	m.asyncStopAllPlatforms()

	// Shut down the auth-code HTTP server BEFORE waiting on wg.
	// startAuthCodeHTTP is wg-tracked and blocks in ListenAndServe;
	// waiting on wg first with Shutdown afterwards deadlocks every
	// shutdown attempt for the full wg.Wait() timeout.
	if m.authCodeServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m.authCodeServer.Shutdown(shutdownCtx)
		cancel()
	}

	// Wait for the notification-hander goroutines (not wg-tracked) to
	// finish before closing nnlpResultChan, preventing panic on send.
	m.notificationWG.Wait()

	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		m.processor.Stop()
		close(m.notificationChan)
		close(m.nnlpResultChan)
		close(m.instructionChan)
		close(m.errorChan)
		close(m.controlChan)
	case <-time.After(15 * time.Second):
		m.processor.Stop()
		return fmt.Errorf("shutdown timed out")
	}
	m.sessionManager.Close()
	m.configManager.Save()
	m.state.Store(StateStopped)
	return nil
}

func (m *Maestro) GetState() MaestroState {
	return m.state.Load().(MaestroState)
}

func (m *Maestro) GetStats() *MaestroStats {
	m.updateStats()
	m.stats.mu.RLock()
	defer m.stats.mu.RUnlock()
	nnlpStats := m.processor.GetQueueStats()

	var pendingMessages int
	if m.db != nil {
		m.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE processed = 0`).Scan(&pendingMessages)
	}

	return &MaestroStats{
		StartTime:          m.stats.StartTime,
		TotalNotifications: m.stats.TotalNotifications,
		TotalErrors:        m.stats.TotalErrors,
		ActivePlatforms:    m.stats.ActivePlatforms,
		TotalPlatforms:     m.stats.TotalPlatforms,
		SessionsCreated:    m.stats.SessionsCreated,
		SessionsRefreshed:  m.stats.SessionsRefreshed,
		SessionReconnects:  m.stats.SessionReconnects,
		WakeCycles:         m.stats.WakeCycles,
		LastWake:           m.stats.LastWake,
		PostsToday:         m.stats.PostsToday,
		PostsThisHour:      m.stats.PostsThisHour,
		RandomPostsSent:    m.stats.RandomPostsSent,
		ScheduledPostsSent: m.stats.ScheduledPostsSent,
		TotalSentToday:     m.stats.TotalSentToday,
		RotationMode:       m.stats.RotationMode,
		NNLPStats:          nnlpStats,
		PendingMessages:    pendingMessages,
	}
}

func (m *Maestro) GetAllPlatformStatuses() []*PlatformStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*PlatformStatus, 0, len(m.platformStatus))
	for _, st := range m.platformStatus {
		cp := &PlatformStatus{
			PlatformID:        st.PlatformID,
			SubtypeID:         st.SubtypeID,
			Enabled:           st.Enabled,
			IsRunning:         st.IsRunning,
			IsPaused:          st.IsPaused,
			SessionState:      st.SessionState,
			LastCheck:         st.LastCheck,
			ErrorCount:        atomic.LoadInt32(&st.ErrorCount),
			LastError:         st.LastError,
			NotificationCount: atomic.LoadInt64(&st.NotificationCount),
			StartTime:         st.StartTime,
			LastReconnect:     st.LastReconnect,
			ReconnectAttempts: atomic.LoadInt32(&st.ReconnectAttempts),
		}
		if st.DailyStats != nil {
			cp.DailyStats = &PlatformDailyStats{
				PostsToday:    st.DailyStats.PostsToday,
				MessagesToday: st.DailyStats.MessagesToday,
				SentToday:     st.DailyStats.SentToday,
				HeartsToday:   st.DailyStats.HeartsToday,
				FollowsToday:  st.DailyStats.FollowsToday,
				CommentsToday: st.DailyStats.CommentsToday,
				LastReset:     st.DailyStats.LastReset,
			}
		}
		out = append(out, cp)
	}
	return out
}

func (m *Maestro) GetPlatformStatus(platformID, subtypeID string) *PlatformStatus {
	key := m.platformKey(platformID, subtypeID)
	m.mu.RLock()
	st, exists := m.platformStatus[key]
	m.mu.RUnlock()
	if !exists {
		return nil
	}
	cp := &PlatformStatus{
		PlatformID:        st.PlatformID,
		SubtypeID:         st.SubtypeID,
		Enabled:           st.Enabled,
		IsRunning:         st.IsRunning,
		IsPaused:          st.IsPaused,
		SessionState:      st.SessionState,
		LastCheck:         st.LastCheck,
		ErrorCount:        atomic.LoadInt32(&st.ErrorCount),
		LastError:         st.LastError,
		NotificationCount: atomic.LoadInt64(&st.NotificationCount),
		StartTime:         st.StartTime,
		LastReconnect:     st.LastReconnect,
		ReconnectAttempts: atomic.LoadInt32(&st.ReconnectAttempts),
	}
	if st.DailyStats != nil {
		cp.DailyStats = &PlatformDailyStats{
			PostsToday:    st.DailyStats.PostsToday,
			MessagesToday: st.DailyStats.MessagesToday,
			SentToday:     st.DailyStats.SentToday,
			HeartsToday:   st.DailyStats.HeartsToday,
			FollowsToday:  st.DailyStats.FollowsToday,
			CommentsToday: st.DailyStats.CommentsToday,
			LastReset:     st.DailyStats.LastReset,
		}
	}
	return cp
}

func (m *Maestro) GetRateLimitStatus() map[string]interface{} {
	result := make(map[string]interface{})
	if m.db == nil {
		return result
	}
	rows, err := m.db.Query(`
		SELECT platform, subtype, action, hourly_limit, daily_limit,
		       current_hour_count, current_day_count
		FROM rate_limits
		WHERE action IN ('messages','posts')
		ORDER BY platform, subtype, action`)
	if err != nil {
		return result
	}
	defer rows.Close()
	type entry struct {
		HourlyLimit int `json:"hourly_limit"`
		DailyLimit  int `json:"daily_limit"`
		HourCount   int `json:"current_hour_count"`
		DayCount    int `json:"current_day_count"`
	}
	for rows.Next() {
		var platform, subtype, action string
		var e entry
		if err := rows.Scan(&platform, &subtype, &action, &e.HourlyLimit, &e.DailyLimit, &e.HourCount, &e.DayCount); err != nil {
			continue
		}
		key := platform
		if subtype != "" {
			key = platform + ":" + subtype
		}
		if _, ok := result[key]; !ok {
			result[key] = make(map[string]interface{})
		}
		result[key].(map[string]interface{})[action] = e
	}
	return result
}

func actionCategory(action string) string {
	switch action {
	case "upload", "share", "save":
		return "posts"
	case "noop", "queued", "block", "unfollow", "follow":
		return ""
	default:
		return "messages"
	}
}

// rateLimitSubtype maps a real per-account subtype ID to the bucket that was
// actually seeded into the rate_limits table by bootstrapRateLimits. For
// WhatsApp specifically, every whatsapp_account_* subtype is collapsed into
// one shared "whatsapp_account" row (all sub-accounts share one limit) —
// CanProceed/RecordUsage need to look up that same normalized name, or every
// lookup misses (no row found → CanProceed silently returns false forever).
// This mismatch never showed up in testing because sandboxMode bypasses
// checkRateLimits entirely; it would have blocked every WhatsApp send once
// running for real.
func rateLimitSubtype(platform, subtypeID string) string {
	if platform == "whatsapp" {
		return "whatsapp_account"
	}
	return subtypeID
}

// defaultRateLimits are only used the very first time a platform+subtype+
// action row is created. After that the row is authoritative and edited
// directly (dashboard UI, DB-backed) — config.json no longer carries rate
// limit values at all, so there's nothing left to "refresh" limits from.
func defaultRateLimits(action string) (hourly, daily int) {
	switch action {
	case "messages":
		return 30, 200
	case "posts":
		return 2, 5
	}
	return 0, 0
}

func (m *Maestro) migrateRateLimitsTable() {
	if m.db == nil {
		return
	}
	var count int
	err := m.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rate_limits') WHERE name='subtype'`).Scan(&count)
	if err != nil || count > 0 {
		return
	}
	tx, err := m.db.Begin()
	if err != nil {
		log.Printf("[Maestro] migrateRateLimitsTable begin: %v", err)
		return
	}
	steps := []string{
		`ALTER TABLE rate_limits ADD COLUMN subtype TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS rate_limits_new (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			platform TEXT NOT NULL,
			subtype  TEXT NOT NULL DEFAULT '',
			action TEXT NOT NULL,
			hourly_limit INTEGER DEFAULT 0,
			daily_limit INTEGER DEFAULT 0,
			current_hour_count INTEGER DEFAULT 0,
			current_day_count INTEGER DEFAULT 0,
			last_reset_hour TIMESTAMP,
			last_reset_day TIMESTAMP,
			UNIQUE(platform, subtype, action)
		)`,
		`INSERT OR IGNORE INTO rate_limits_new
			(platform, subtype, action, hourly_limit, daily_limit,
			 current_hour_count, current_day_count, last_reset_hour, last_reset_day)
		SELECT platform, '', action, hourly_limit, daily_limit,
			   current_hour_count, current_day_count, last_reset_hour, last_reset_day
		FROM rate_limits`,
		`DROP TABLE rate_limits`,
		`ALTER TABLE rate_limits_new RENAME TO rate_limits`,
		`CREATE INDEX IF NOT EXISTS idx_rate_limits_platform ON rate_limits(platform, subtype, action)`,
	}
	for _, q := range steps {
		if _, err := tx.Exec(q); err != nil {
			tx.Rollback()
			log.Printf("[Maestro] migrateRateLimitsTable step error: %v", err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[Maestro] migrateRateLimitsTable commit: %v", err)
	} else {
		log.Printf("[Maestro] rate_limits table migrated to platform+subtype+action schema")
	}
}

func (m *Maestro) bootstrapRateLimits() {
	if m.db == nil {
		return
	}
	cfg := m.configManager.GetConfig()
	actions := []string{"messages", "posts"}

	m.db.Exec(`DELETE FROM rate_limits WHERE action NOT IN ('messages','posts')`)

	for platformID, pc := range cfg.Platforms {
		if !pc.Enabled {
			continue
		}
		subtypes := pc.Subtypes
		if len(subtypes) == 0 {
			for _, act := range actions {
				h, d := defaultRateLimits(act)
				m.insertRateLimitRow(platformID, "", act, h, d)
			}
			continue
		}
		// Normalize WhatsApp: all whatsapp_account_* → whatsapp_account
		// (see rateLimitSubtype) — every WhatsApp sub-account shares one
		// combined limit, so only one row is created regardless of how
		// many enabled subtypes exist.
		if platformID == "whatsapp" {
			for _, act := range actions {
				h, d := defaultRateLimits(act)
				m.insertRateLimitRow("whatsapp", "whatsapp_account", act, h, d)
			}
			continue
		}
		for i := range subtypes {
			sub := &subtypes[i]
			if !sub.Enabled {
				continue
			}
			for _, act := range actions {
				h, d := defaultRateLimits(act)
				m.insertRateLimitRow(platformID, sub.ID, act, h, d)
			}
		}
	}
}

func (m *Maestro) insertRateLimitRow(platform, subtype, action string, hourly, daily int) {
	// INSERT-only: creates the row with sane defaults if missing. Once a
	// row exists it's edited directly via the dashboard — this must never
	// overwrite hourly_limit/daily_limit on an existing row (that would
	// silently blow away an admin's saved limit on every Resume/restart).
	_, err := m.db.Exec(`
		INSERT OR IGNORE INTO rate_limits
		(platform, subtype, action, hourly_limit, daily_limit,
		 current_hour_count, current_day_count,
		 last_reset_hour, last_reset_day)
		VALUES (?,?,?,?,?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		platform, subtype, action, hourly, daily)
	if err != nil {
		log.Printf("[Maestro] bootstrapRateLimits insert error for %s/%s/%s: %v", platform, subtype, action, err)
	}
}

// CanProceed atomically checks AND reserves capacity for platform+subtype+
// action in one step. Previously this was two separate calls (CanProceed to
// check, RecordUsage to increment) with a real gap between them — two
// concurrent compiles for the same platform/subtype/action could both read
// "4 of 5 used", both pass, and both increment, landing at 6/5. Folding the
// reset-rollover + limit check + increment into a single guarded UPDATE
// closes that gap: the WHERE clause only lets the increment happen if the
// row is still under its limit at the moment SQLite executes the write, and
// SQLite serializes writes to a single row.
//
// A caller that gets true=proceed has ALREADY consumed one unit of the
// limit — there's no separate RecordUsage step to call afterward. If the
// caller ends up not actually using the reservation (e.g. compile failed
// before anything was sent), call ReleaseUsage to give the slot back.
func (m *Maestro) CanProceed(platform, subtypeID, action string) (bool, time.Duration) {
	cat := actionCategory(action)
	if cat == "" || m.db == nil {
		return true, 0
	}
	subtypeID = rateLimitSubtype(platform, subtypeID)
	now := time.Now()

	// Roll over hour/day windows first. These only touch a row whose
	// window has actually elapsed, so they're safe to run unconditionally
	// before every reservation attempt.
	if _, err := m.db.Exec(`
		UPDATE rate_limits SET current_hour_count = 0, last_reset_hour = CURRENT_TIMESTAMP
		WHERE platform=? AND subtype=? AND action=?
		  AND last_reset_hour IS NOT NULL
		  AND (strftime('%s','now') - strftime('%s', last_reset_hour)) >= 3600`,
		platform, subtypeID, cat); err != nil {
		log.Printf("[Maestro] CanProceed hourly rollover error: %v", err)
	}
	if _, err := m.db.Exec(`
		UPDATE rate_limits SET current_day_count = 0, last_reset_day = CURRENT_TIMESTAMP
		WHERE platform=? AND subtype=? AND action=?
		  AND last_reset_day IS NOT NULL
		  AND date(last_reset_day) != date('now')`,
		platform, subtypeID, cat); err != nil {
		log.Printf("[Maestro] CanProceed daily rollover error: %v", err)
	}

	res, err := m.db.Exec(`
		UPDATE rate_limits
		SET current_hour_count = current_hour_count + 1,
		    current_day_count  = current_day_count  + 1
		WHERE platform=? AND subtype=? AND action=?
		  AND (hourly_limit <= 0 OR current_hour_count < hourly_limit)
		  AND (daily_limit  <= 0 OR current_day_count  < daily_limit)`,
		platform, subtypeID, cat)
	if err != nil {
		log.Printf("[Maestro] CanProceed reserve error for %s/%s/%s: %v", platform, subtypeID, cat, err)
		return false, 0
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, 0
	}

	// Reservation didn't happen — either there's no row at all for this
	// platform/subtype/action (never bootstrapped: fail closed rather than
	// silently allowing unlimited usage, matching the old behavior), or a
	// real limit was hit. Read current values to report a useful wait time.
	var hourlyLimit, dailyLimit, hourCount, dayCount int
	scanErr := m.db.QueryRow(`
		SELECT hourly_limit, daily_limit, current_hour_count, current_day_count
		FROM rate_limits WHERE platform=? AND subtype=? AND action=?`,
		platform, subtypeID, cat).Scan(&hourlyLimit, &dailyLimit, &hourCount, &dayCount)
	if scanErr != nil {
		log.Printf("[Maestro] CanProceed: no rate_limits row for %s/%s/%s — blocking", platform, subtypeID, cat)
		return false, time.Minute
	}
	if hourlyLimit > 0 && hourCount >= hourlyLimit {
		return false, time.Hour - now.Sub(now.Truncate(time.Hour))
	}
	midnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	return false, midnight.Sub(now)
}

// ReleaseUsage gives back a reservation made by CanProceed when the caller
// ends up not using it (e.g. the instruction failed to compile after the
// slot was already reserved). Best-effort, floors at 0.
func (m *Maestro) ReleaseUsage(platform, subtypeID, action string) {
	cat := actionCategory(action)
	if cat == "" || m.db == nil {
		return
	}
	subtypeID = rateLimitSubtype(platform, subtypeID)
	if _, err := m.db.Exec(`
		UPDATE rate_limits
		SET current_hour_count = MAX(current_hour_count - 1, 0),
		    current_day_count  = MAX(current_day_count  - 1, 0)
		WHERE platform=? AND subtype=? AND action=?`, platform, subtypeID, cat); err != nil {
		log.Printf("[Maestro] ReleaseUsage DB error for %s/%s/%s: %v", platform, subtypeID, cat, err)
	}
}

// RecordUsage is kept only so any external caller still wired to the old
// two-step check-then-record pattern doesn't fail to compile. CanProceed
// now reserves atomically on its own, so calling this afterward would
// double-count — it's a deliberate no-op. New code should just call
// CanProceed (and ReleaseUsage on failure, if applicable) and nothing else.
func (m *Maestro) RecordUsage(platform, subtypeID, action string) {}

func (m *Maestro) GetConfigManager() *config.ConfigManager {
	return m.configManager
}

func (m *Maestro) GetOperationMode() string {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()
	return m.operationMode
}

func (m *Maestro) refreshPlatformStatuses(cfg *config.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newPlatforms := make(map[string]bool)
	for platformID, platformCfg := range cfg.Platforms {
		if !platformCfg.Enabled {
			continue
		}
		subtype := "account"
		if platformCfg.Platform.Subtype != "" {
			subtype = platformCfg.Platform.Subtype
		}
		if len(platformCfg.Subtypes) > 0 {
			for _, sub := range platformCfg.Subtypes {
				if !subtypeHasCredentials(platformCfg, sub) {
					continue
				}
				key := m.platformKey(platformID, sub.ID)
				newPlatforms[key] = true
				if existing, exists := m.platformStatus[key]; !exists {
					m.platformStatus[key] = &PlatformStatus{
						PlatformID: platformID,
						SubtypeID:  sub.ID,
						Enabled:    sub.Enabled,
						DailyStats: &PlatformDailyStats{LastReset: time.Now()},
					}
				} else {
					existing.Enabled = sub.Enabled
				}
			}
		} else {
			key := m.platformKey(platformID, subtype)
			newPlatforms[key] = true
			if _, exists := m.platformStatus[key]; !exists {
				m.platformStatus[key] = &PlatformStatus{
					PlatformID: platformID,
					SubtypeID:  subtype,
					Enabled:    true,
					DailyStats: &PlatformDailyStats{LastReset: time.Now()},
				}
			}
		}
	}
	for key := range m.platformStatus {
		if !newPlatforms[key] {
			delete(m.platformStatus, key)
		}
	}
	m.stats.TotalPlatforms = len(newPlatforms)
}