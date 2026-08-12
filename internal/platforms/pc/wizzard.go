package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Config struct {
	Meta             MetaConfig                `json:"meta"`
	System           SystemConfig              `json:"system"`
	AI               AIConfig                  `json:"ai"`
	Scheduler        SchedulerConfig           `json:"scheduler"`
	Store            StoreConfig               `json:"store"`
	Platforms        map[string]PlatformConfig `json:"platforms"`
	Paths            PathsConfig               `json:"paths"`
	Content          ContentPool               `json:"content"`
	ScheduledPosts   []ScheduledPost           `json:"scheduled_posts"`
	ImageRecognition ImageRecognitionConfig    `json:"image_recognition"`
}

type MetaConfig struct {
	DetectedOS   string         `json:"detected_os"`
	DetectedArch string         `json:"detected_arch"`
	DetectedEnv  string         `json:"detected_environment"`
	AppVersion   string         `json:"app_version"`
	InstalledAt  string         `json:"installed_at"`
	LastUpdated  string         `json:"last_updated"`
	Database     DatabaseConfig `json:"database"`
}

type DatabaseConfig struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	SSLMode  string `json:"ssl_mode"`
}

type SystemConfig struct {
	Language           string     `json:"language"`
	OperationMode      string     `json:"operation_mode"`
	WakePolicy         WakePolicy `json:"wake_policy"`
	LLMTokensPerMinute int        `json:"llm_tokens_per_minute"`
	LLMCostPerMinute   float64    `json:"llm_cost_per_minute"`
}

type WakePolicy struct {
	IntervalMinutes  int `json:"interval_minutes"`
	IdleSleepMinutes int `json:"idle_sleep_minutes"`
}

type AIConfig struct {
	Provider     string             `json:"provider"`
	Model        string             `json:"model"`
	APIKey       string             `json:"api_key"`
	BaseURL      string             `json:"base_url"`
	Generation   GenerationSettings `json:"generation"`
	Instructions AIInstructions     `json:"instructions"`
}

type AIInstructions struct {
	SystemPrompt              string `json:"system_prompt"`
	PostInstructions          string `json:"post_instructions"`
	ReplyInstructions         string `json:"reply_instructions"`
	Tone                      string `json:"tone"`
	MaxResponseLength         int    `json:"max_response_length"`
	ScheduledPostInstructions string `json:"scheduled_post_instructions"`
}

type GenerationSettings struct {
	MaxTokens        int     `json:"max_tokens"`
	Temperature      float64 `json:"temperature"`
	TopP             float64 `json:"top_p"`
	PresencePenalty  float64 `json:"presence_penalty"`
	FrequencyPenalty float64 `json:"frequency_penalty"`
}

type ImageRecognitionConfig struct {
	Enabled             bool    `json:"enabled"`
	ModelPath           string  `json:"model_path"`
	ConfidenceThreshold float64 `json:"confidence_threshold"`
	MaxImageSizePx      int     `json:"max_image_size_px"`
	TrainingDefaults    struct {
		Epochs          int  `json:"epochs"`
		BatchSize       int  `json:"batch_size"`
		NumAngles       int  `json:"num_angles"`
		UseHybrid       bool `json:"use_hybrid"`
		UseAugmentation bool `json:"use_augmentation"`
		UseMultilingual bool `json:"use_multilingual"`
		BoostAccuracy   bool `json:"boost_accuracy"`
	} `json:"training_defaults"`
}

type SchedulerConfig struct {
	Timezone             string     `json:"timezone"`
	QuietHours           QuietHours `json:"quiet_hours"`
	CheckIntervalMinutes int        `json:"check_interval_minutes"`
}

type QuietHours struct {
	Enabled bool   `json:"enabled"`
	From    string `json:"from"`
	To      string `json:"to"`
}

type StoreConfig struct {
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	HelloMessage  string            `json:"hello_message"`
	Address       string            `json:"address"`
	Contact       ContactInfo       `json:"contact"`
	BusinessHours map[string]string `json:"business_hours"`
	Currency      string            `json:"currency"`
}

type ContactInfo struct {
	Email string `json:"email"`
	Phone string `json:"phone"`
}

type ContentPool struct {
	Posts         []PostContent `json:"posts"`
	Media         []MediaItem   `json:"media"`
	Hashtags      []string      `json:"hashtags"`
	Categories    []string      `json:"categories"`
	LastUsedIndex int           `json:"last_used_index"`
}

type PostContent struct {
	ID        string   `json:"id"`
	Text      string   `json:"text"`
	Category  string   `json:"category"`
	MediaIDs  []string `json:"media_ids"`
	Hashtags  []string `json:"hashtags"`
	Platforms []string `json:"platforms"`
	UsedCount int      `json:"used_count"`
	LastUsed  string   `json:"last_used"`
	Weight    int      `json:"weight"`
}

type MediaItem struct {
	ID          string `json:"id"`
	Path        string `json:"path"`
	URL         string `json:"url"`
	Type        string `json:"type"`
	Description string `json:"description"`
	SizeBytes   int64  `json:"size_bytes"`
	Dimensions  string `json:"dimensions"`
}

type MessageTemplates struct {
	Greetings []string      `json:"greetings"`
	Farewells []string      `json:"farewells"`
	Questions []string      `json:"questions"`
	Answers   []string      `json:"answers"`
	Keywords  []KeywordRule `json:"keywords"`
	Fallback  []string      `json:"fallback"`
}

type KeywordRule struct {
	Keyword      string `json:"keyword"`
	Response     string `json:"response"`
	ResponseType string `json:"response_type"`
	ExactMatch   bool   `json:"exact_match"`
	Priority     int    `json:"priority"`
}

type PlatformConfig struct {
	Enabled    bool              `json:"enabled"`
	Platform   PlatformType      `json:"platform"`
	Subtypes   []PlatformSubtype `json:"subtypes"`
	Instagram  *InstagramConfig  `json:"instagram,omitempty"`
	Facebook   *FacebookConfig   `json:"facebook,omitempty"`
	Telegram   *TelegramConfig   `json:"telegram,omitempty"`
	TikTok     *TikTokConfig     `json:"tiktok,omitempty"`
	Twitter    *TwitterConfig    `json:"twitter,omitempty"`
	WhatsApp   *WhatsAppConfig   `json:"whatsapp,omitempty"`
	Viber      *ViberConfig      `json:"viber,omitempty"`
	Automation AutomationConfig  `json:"automation"`
	Metadata   PlatformMetadata  `json:"metadata"`
	Settings   PlatformSettings  `json:"settings"`
	Messages   MessageTemplates  `json:"messages"`
}

type PlatformSubtype struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Enabled    bool                   `json:"enabled"`
	Auth       map[string]interface{} `json:"auth"`
	Metadata   map[string]interface{} `json:"metadata"`
	Automation SubtypeAutomation      `json:"automation"`
}

type SubtypeAutomation struct {
	AnswerDM       AnswerDMConfig       `json:"answer_dm"`
	AnswerComments AnswerCommentsConfig `json:"answer_comments"`
	WelcomeMessage WelcomeMessageConfig `json:"welcome_message"`
	AutoHeart      AutoHeartConfig      `json:"auto_heart"`
	AutoFollow     AutoFollowConfig     `json:"auto_follow"`
	AutoRepost     AutoRepostConfig     `json:"auto_repost"`
}

type PlatformSettings struct {
	PostHashtags      []string `json:"post_hashtags"`
	MentionAccounts   []string `json:"mention_accounts"`
	AllowedPostTypes  []string `json:"allowed_post_types"`
	MaxPostLength     int      `json:"max_post_length"`
	MaxImageSizeMB    int      `json:"max_image_size_mb"`
	MaxVideoSizeMB    int      `json:"max_video_size_mb"`
	AllowedMediaTypes []string `json:"allowed_media_types"`
}

type PlatformType struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
}

type FacebookConfig struct {
	Account *FacebookAccount `json:"account,omitempty"`
	Page    *FacebookPage    `json:"page,omitempty"`
	Groups  []FacebookGroup  `json:"groups,omitempty"`
}

type FacebookAccount struct {
	UserID   string `json:"user_id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type FacebookPage struct {
	PageID      string `json:"page_id"`
	PageName    string `json:"page_name"`
	AccessToken string `json:"access_token"`
	Category    string `json:"category"`
}

type FacebookGroup struct {
	GroupID   string `json:"group_id"`
	GroupName string `json:"group_name"`
	GroupURL  string `json:"group_url"`
	Admin     bool   `json:"admin"`
}

type InstagramConfig struct {
	Account  *InstagramAccount  `json:"account,omitempty"`
	Business *InstagramBusiness `json:"business,omitempty"`
}

type InstagramAccount struct {
	Username string `json:"username"`
	Password string `json:"password"`
	UserID   string `json:"user_id"`
}

type InstagramBusiness struct {
	PageID      string `json:"page_id"`
	AccessToken string `json:"access_token"`
	Username    string `json:"username,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type WhatsAppConfig struct {
	Phone     string `json:"phone"`
	Password  string `json:"password"`
	SessionID string `json:"session_id"`
	TwoFAKey  string `json:"two_fa_key"`
}

type TelegramConfig struct {
	Bot     *TelegramBot     `json:"bot,omitempty"`
	Account *TelegramAccount `json:"account,omitempty"`
	APIID   string           `json:"api_id"`
	APIHash string           `json:"api_hash"`
}

type TelegramBot struct {
	BotToken    string `json:"bot_token"`
	BotHash     string `json:"bot_hash"`
	BotUsername string `json:"bot_username"`
	WebhookURL  string `json:"webhook_url"`
}

type TelegramAccount struct {
	PhoneNumber string `json:"phone_number"`
	Password    string `json:"password"`
	SessionFile string `json:"session_file"`
	TwoFAKey    string `json:"two_fa_key"`
}

type TikTokConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Session  string `json:"session"`
	DeviceID string `json:"device_id"`
}

type TwitterConfig struct {
	APIKey       string `json:"api_key"`
	APISecret    string `json:"api_secret"`
	AccessToken  string `json:"access_token"`
	AccessSecret string `json:"access_secret"`
	BearerToken  string `json:"bearer_token"`
}

type ViberConfig struct {
	BotToken    string `json:"bot_token,omitempty"`
	PhoneNumber string `json:"phone_number,omitempty"`
	Password    string `json:"password,omitempty"`
	WebhookPort string `json:"webhook_port"`
	WebhookURL  string `json:"webhook_url"`
}

type AutomationConfig struct {
	AutoHeart      AutoHeartConfig      `json:"auto_heart"`
	AutoFollow     AutoFollowConfig     `json:"auto_follow"`
	AutoRepost     AutoRepostConfig     `json:"auto_repost"`
	AnswerDM       AnswerDMConfig       `json:"answer_dm"`
	AnswerComments AnswerCommentsConfig `json:"answer_comments"`
	WelcomeMessage WelcomeMessageConfig `json:"welcome_message"`
	Filters        MessageFilters       `json:"filters"`
}

type MessageFilters struct {
	IgnoreWords   []string `json:"ignore_words"`
	MinWordCount  int      `json:"min_word_count"`
	ReplyDelay    int      `json:"reply_delay"`
	Language      string   `json:"language"`
	BlockKeywords []string `json:"block_keywords"`
	AllowKeywords []string `json:"allow_keywords"`
	MinCharCount  int      `json:"min_char_count"`
}

type AutoHeartConfig struct {
	Enabled     bool `json:"enabled"`
	DelayMin    int  `json:"delay_min"`
	DelayMax    int  `json:"delay_max"`
	MaxPerDay   int  `json:"max_per_day"`
	SkipOwn     bool `json:"skip_own"`
	SkipPrivate bool `json:"skip_private"`
}

type AutoFollowConfig struct {
	Enabled        bool     `json:"enabled"`
	MaxPerDay      int      `json:"max_per_day"`
	TargetHashtags []string `json:"target_hashtags"`
	TargetAccounts []string `json:"target_accounts"`
	UnfollowAfter  int      `json:"unfollow_after"`
}

type AutoRepostConfig struct {
	Enabled      bool     `json:"enabled"`
	SourceTags   []string `json:"source_tags"`
	CreditSource bool     `json:"credit_source"`
	MaxPerDay    int      `json:"max_per_day"`
}

type AnswerDMConfig struct {
	Enabled bool `json:"enabled"`
	// IncludeGroups mirrors config.go's AnswerDMConfig.IncludeGroups — kept in
	// sync manually since this file doesn't import the shared config package.
	IncludeGroups bool `json:"include_groups"`
}
type AnswerCommentsConfig struct {
	Enabled bool `json:"enabled"`
}

type WelcomeMessageConfig struct {
	Enabled bool   `json:"enabled"`
	Text    string `json:"text"`
}

type PlatformMetadata struct {
	CreatedAt      string `json:"created_at"`
	LastActive     string `json:"last_active"`
	Notes          string `json:"notes"`
	TotalPosts     int    `json:"total_posts"`
	TotalFollowers int    `json:"total_followers"`
	TotalFollowing int    `json:"total_following"`
}

type PathsConfig struct {
	Logs           string `json:"logs"`
	Config         string `json:"config"`
	Cache          string `json:"cache"`
	Media          string `json:"media"`
	Models         string `json:"cnn"`
	Temp           string `json:"temp"`
	Sessions       string `json:"sessions"`
	Database       string `json:"database"`
	Backup         string `json:"backup"`
	PostImages     string `json:"post_images"`
	ProductImages  string `json:"product_images"`
	PostVideos     string `json:"post_videos"`
	ScheduledPosts string `json:"scheduled_posts"`
	TrainingImages string `json:"training_images"`
}

type ScheduledPost struct {
	ID          string                  `json:"id"`
	Title       string                  `json:"title"`
	Description string                  `json:"description"`
	Platforms   []ScheduledPostPlatform `json:"platforms"`
	Media       []ScheduledMedia        `json:"media"`
	Schedule    PostSchedule            `json:"schedule"`
	Status      string                  `json:"status"`
	Order       int                     `json:"order"`
	Hashtags    []string                `json:"hashtags"`
	CreatedAt   string                  `json:"created_at"`
	UpdatedAt   string                  `json:"updated_at"`
}

type ScheduledPostPlatform struct {
	PlatformID string `json:"platform_id"`
	SubtypeID  string `json:"subtype_id"`
	Enabled    bool   `json:"enabled"`
	CustomText string `json:"custom_text"`
}

type ScheduledMedia struct {
	ID          string `json:"id"`
	FilePath    string `json:"file_path"`
	URL         string `json:"url"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Thumbnail   string `json:"thumbnail"`
}

type PostSchedule struct {
	Type     string `json:"type"`
	DateTime string `json:"date_time"`
	Interval string `json:"interval"`
	Time     string `json:"time"`
	Days     []int  `json:"days"`
}

type IRJobStatus string

const (
	IRJobPending IRJobStatus = "pending"
	IRJobRunning IRJobStatus = "running"
	IRJobDone    IRJobStatus = "done"
	IRJobFailed  IRJobStatus = "failed"
)

type IRJob struct {
	ID        string      `json:"id"`
	Status    IRJobStatus `json:"status"`
	StartedAt time.Time   `json:"started_at"`
	EndedAt   *time.Time  `json:"ended_at,omitempty"`
	Result    interface{} `json:"result,omitempty"`
	Logs      string      `json:"logs"`
	Error     string      `json:"error,omitempty"`
}

type Server struct {
	configPath string
	cfg        *Config
	cfgMu      sync.RWMutex
	db         *sql.DB
	baseDir    string
	srv        *http.Server

	irJobs   map[string]*IRJob
	irJobsMu sync.Mutex

	dbTableCache   []string
	dbTableCacheMu sync.Mutex
}

func findProjectRoot() string {
	if root := os.Getenv("SAILSTREAM_ROOT"); root != "" {
		if fi, err := os.Stat(root); err == nil && fi.IsDir() {
			return root
		}
	}
	execPath, err := os.Executable()
	if err == nil {
		dir := filepath.Dir(execPath)
		for range 6 {
			if isSailStreamRoot(dir) {
				return dir
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	cwd, _ := os.Getwd()
	dir := cwd
	for range 6 {
		if isSailStreamRoot(dir) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	home, _ := os.UserHomeDir()
	commonDirs := []string{
		filepath.Join(home, "SailStream"),
		filepath.Join(home, "sailstream"),
		"C:\\Users\\hawka\\SailStream",
		"/data/data/com.termux/files/home/SailStream",
	}
	for _, d := range commonDirs {
		if isSailStreamRoot(d) {
			return d
		}
	}
	return cwd
}

func isSailStreamRoot(dir string) bool {
	indicators := []string{"go.mod", "main.go", filepath.Join("internal", "config", "config.json")}
	found := 0
	for _, ind := range indicators {
		if _, err := os.Stat(filepath.Join(dir, ind)); err == nil {
			found++
		}
	}
	return found >= 2
}

func findConfigFile(baseDir string) string {
	if len(os.Args) > 1 {
		if _, err := os.Stat(os.Args[1]); err == nil {
			return os.Args[1]
		}
	}
	if env := os.Getenv("SAILSTREAM_CONFIG"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env
		}
	}
	candidates := []string{
		filepath.Join(baseDir, "internal", "config", "config.json"),
		filepath.Join(baseDir, "config.json"),
		filepath.Join(baseDir, "data", "config.json"),
	}
	cwd, _ := os.Getwd()
	candidates = append(candidates,
		filepath.Join(cwd, "internal", "config", "config.json"),
		filepath.Join(cwd, "config.json"),
	)
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

func (s *Server) findWizzardHTML() string {
	candidate := filepath.Join(s.baseDir, "internal", "platforms", "pc", "wizzard.html")
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	execPath, err := os.Executable()
	if err == nil {
		sibling := filepath.Join(filepath.Dir(execPath), "wizzard.html")
		if _, err := os.Stat(sibling); err == nil {
			return sibling
		}
	}
	cwd, _ := os.Getwd()
	cwdCandidate := filepath.Join(cwd, "wizzard.html")
	if _, err := os.Stat(cwdCandidate); err == nil {
		return cwdCandidate
	}
	return ""
}

func openURL(u string) {
	switch runtime.GOOS {
	case "windows":
		exec.Command("rundll32", "url.dll,FileProtocolHandler", u).Start()
	case "darwin":
		exec.Command("open", u).Start()
	default:
		exec.Command("xdg-open", u).Start()
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	baseDir := findProjectRoot()
	configPath := findConfigFile(baseDir)
	if configPath == "" {
		log.Fatal("[wizzard] config.json not found. Set SAILSTREAM_ROOT or SAILSTREAM_CONFIG.")
	}
	log.Printf("[wizzard] root=%s config=%s", baseDir, configPath)

	srv := &Server{
		configPath: configPath,
		baseDir:    baseDir,
		irJobs:     make(map[string]*IRJob),
	}
	if err := srv.loadConfig(); err != nil {
		log.Printf("[wizzard] Warning: config load error (%v), starting empty", err)
		srv.cfg = &Config{
			Platforms: make(map[string]PlatformConfig),
			Meta: MetaConfig{
				DetectedOS:   runtime.GOOS,
				DetectedArch: runtime.GOARCH,
			},
		}
	}

	srv.cfgMu.Lock()
	if srv.cfg != nil {
		srv.cfg.System.OperationMode = normaliseOperationMode(srv.cfg.System.OperationMode)
	}
	srv.cfgMu.Unlock()

	srv.initDB()
	srv.registerRoutes()

	addr := "127.0.0.1:7879"
	srv.srv = &http.Server{Addr: addr}
	log.Printf("[wizzard] Listening on http://%s", addr)

	go func() {
		time.Sleep(500 * time.Millisecond)
		if os.Getenv("SAILSTREAM_NO_AUTO_OPEN") == "" {
			openURL("http://" + addr)
		}
	}()

	if err := srv.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func normaliseOperationMode(mode string) string {
	switch mode {
	case "always_awake":
		return "always_on"
	case "scheduled_wake", "always_on", "manual_only":
		return mode
	default:
		if mode == "" {
			return "scheduled_wake"
		}
		return mode
	}
}

func (s *Server) loadConfig() error {
	data, err := os.ReadFile(s.configPath)
	if err != nil {
		return err
	}
	cfg := &Config{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return err
	}
	s.cfgMu.Lock()
	s.cfg = cfg
	s.cfgMu.Unlock()
	return nil
}

func (s *Server) saveConfig() error {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.cfg == nil {
		return fmt.Errorf("no config")
	}
	s.cfg.System.OperationMode = normaliseOperationMode(s.cfg.System.OperationMode)
	s.cfg.Meta.LastUpdated = time.Now().Format(time.RFC3339)
	data, err := json.MarshalIndent(s.cfg, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.configPath)
	tmp, err := os.CreateTemp(dir, ".wizzard-cfg-*.json")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync temp: %w", err)
	}
	tmp.Close()
	if err := os.Rename(tmpName, s.configPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

func (s *Server) initDB() {
	s.cfgMu.RLock()
	dbPathCfg := ""
	if s.cfg != nil {
		dbPathCfg = s.cfg.Paths.Database
	}
	s.cfgMu.RUnlock()

	if dbPathCfg == "" {
		return
	}
	dbPath := dbPathCfg
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(s.baseDir, dbPath)
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			log.Printf("[wizzard] Warning: error closing previous DB connection: %v", err)
		}
		s.db = nil
	}

	os.MkdirAll(filepath.Dir(dbPath), 0755)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Printf("[wizzard] DB open error: %v", err)
		return
	}
	if err := db.Ping(); err != nil {
		log.Printf("[wizzard] DB ping error: %v", err)
		db.Close()
		return
	}
	s.db = db
	log.Printf("[wizzard] DB connected: %s", dbPath)
	s.ensureSchemaColumns()
}

// ensureSchemaColumns adds columns that schema.sql defines but an
// already-existing database file (created before those columns were added)
// might be missing. This matters because CREATE TABLE IF NOT EXISTS — wherever
// it runs, whether in the main bot process or elsewhere — never retrofits new
// columns onto a table that already exists; it's a no-op once the table is
// there. wizzard.go is a standalone tool that doesn't share the main bot's DB
// initialization, so it can't assume the connected database is already
// up to date. This only adds columns to tables that already exist — it
// doesn't create tables from scratch, which stays the main process's job.
func (s *Server) ensureSchemaColumns() {
	if s.db == nil {
		return
	}
	type col struct{ name, ddlType string }
	migrations := map[string][]col{
		"products": {
			{"aliases_en", "TEXT"}, {"aliases_ar", "TEXT"}, {"aliases_ku", "TEXT"},
			{"uses_en", "TEXT"}, {"uses_ar", "TEXT"}, {"uses_ku", "TEXT"},
		},
		"platform_users": {
			{"last_product_sku", "TEXT"}, {"pending_data", "TEXT"},
		},
	}
	for table, cols := range migrations {
		existing := map[string]bool{}
		rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
		if err != nil {
			log.Printf("[wizzard] schema check failed for %s: %v", table, err)
			continue
		}
		for rows.Next() {
			var cid int
			var name, ctype string
			var notnull, pk int
			var dflt sql.NullString
			if rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk) == nil {
				existing[name] = true
			}
		}
		rows.Close()
		if len(existing) == 0 {
			// Table doesn't exist yet — not wizzard.go's job to create it.
			continue
		}
		for _, c := range cols {
			if existing[c.name] {
				continue
			}
			if _, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %q ADD COLUMN %s %s", table, c.name, c.ddlType)); err != nil {
				log.Printf("[wizzard] failed to add column %s.%s: %v", table, c.name, err)
			} else {
				log.Printf("[wizzard] added missing column %s.%s (schema was out of date)", table, c.name)
			}
		}
	}
	s.invalidateTableCache()
}

func genID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(t>>uint(i*3)) ^ byte(os.Getpid()>>uint(i))
		}
	}
	return hex.EncodeToString(b)
}

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (s *Server) isValidTable(name string) bool {
	s.dbTableCacheMu.Lock()
	defer s.dbTableCacheMu.Unlock()
	if len(s.dbTableCache) == 0 && s.db != nil {
		rows, err := s.db.Query("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'")
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var n string
				if rows.Scan(&n) == nil {
					s.dbTableCache = append(s.dbTableCache, n)
				}
			}
		}
	}
	for _, t := range s.dbTableCache {
		if t == name {
			return true
		}
	}
	return false
}

func (s *Server) invalidateTableCache() {
	s.dbTableCacheMu.Lock()
	s.dbTableCache = nil
	s.dbTableCacheMu.Unlock()
}

func parsePythonOutput(output []byte) (map[string]interface{}, string) {
	raw := string(output)
	lines := strings.Split(raw, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
			var m map[string]interface{}
			if json.Unmarshal([]byte(line), &m) == nil {
				return m, raw
			}
		}
	}
	if start := strings.Index(raw, "{"); start >= 0 {
		var m map[string]interface{}
		if json.Unmarshal([]byte(raw[start:]), &m) == nil {
			return m, raw
		}
	}
	return nil, raw
}

func checkPythonEnv(pythonPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, pythonPath, "-c", "import sys; print('ok')")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("python check failed: %v — %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (s *Server) registerRoutes() {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/products/images/", s.handleProductImages)
	mux.HandleFunc("/api/products/", s.handleProduct)
	mux.HandleFunc("/api/products", s.handleProducts)

	mux.HandleFunc("/api/shipping/", s.handleShippingItem)
	mux.HandleFunc("/api/shipping", s.handleShipping)

	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/config/save", s.handleSaveConfig)
	mux.HandleFunc("/api/restart-dashboard", s.handleRestartDashboard)

	mux.HandleFunc("/api/scheduled-posts", s.handleScheduledPosts)
	mux.HandleFunc("/api/scheduled-posts/", s.handleScheduledPost)
	mux.HandleFunc("/api/upload/post-media", s.handlePostMediaUpload)

	mux.HandleFunc("/api/db/tables", s.handleDBTables)
	mux.HandleFunc("/api/db/query", s.handleDBQuery)
	mux.HandleFunc("/api/db/row", s.handleDBRow)

	mux.HandleFunc("/api/ir/train", s.handleIRTrain)
	mux.HandleFunc("/api/ir/production-train", s.handleIRProductionTrain)
	mux.HandleFunc("/api/ir/multilingual", s.handleIRMultilingual)
	mux.HandleFunc("/api/ir/predict", s.handleIRPredict)
	mux.HandleFunc("/api/ir/models", s.handleIRModels)
	mux.HandleFunc("/api/ir/models/", s.handleIRDeleteModel)
	mux.HandleFunc("/api/ir/upload-image", s.handleIRUploadImage)
	mux.HandleFunc("/api/ir/browse", s.handleIRBrowse)
	mux.HandleFunc("/api/ir/jobs/", s.handleIRJob)

	mux.Handle("/media/", http.StripPrefix("/media/", http.FileServer(http.Dir(s.mediaDir()))))
	mux.HandleFunc("/", s.handleIndex)

	http.Handle("/", mux)
}

func (s *Server) mediaDir() string {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfg != nil && s.cfg.Paths.Media != "" {
		dir := s.cfg.Paths.Media
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(s.baseDir, dir)
		}
		return dir
	}
	return filepath.Join(s.baseDir, "media")
}

func (s *Server) productImagesDir(productID string) string {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	base := filepath.Join(s.mediaDir(), "product_images")
	if s.cfg != nil && s.cfg.Paths.ProductImages != "" {
		base = s.cfg.Paths.ProductImages
		if !filepath.IsAbs(base) {
			base = filepath.Join(s.baseDir, base)
		}
	}
	return filepath.Join(base, productID)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	htmlPath := s.findWizzardHTML()
	if htmlPath == "" {
		http.Error(w, "wizzard.html not found — place it in internal/platforms/pc/ relative to the project root, or next to the binary.", http.StatusNotFound)
		return
	}
	http.ServeFile(w, r, htmlPath)
}

// legacyPostingAndLimits mirrors the OLD wizard JSON shape for the
// posting.*, platforms.*.posting, platforms.*.limits, and
// scheduler.rate_limits fields — all removed from the live Config struct
// now that they live in posting_settings/posting_schedule/rate_limits
// (DB). wizard.html still sends this exact shape (unchanged), so this type
// exists purely so handleConfig's PUT case can pull those fields out of the
// raw request body and write them to the DB instead of silently dropping
// them when decoding into the trimmed Config struct.
type legacyPostingAndLimits struct {
	Scheduler struct {
		RateLimits struct {
			MessagesPerMinute int `json:"messages_per_minute"`
			PostsPerHour      int `json:"posts_per_hour"`
			PostsPerDay       int `json:"posts_per_day"`
		} `json:"rate_limits"`
	} `json:"scheduler"`
	Posting struct {
		Fallback struct {
			Random legacyRandomPosting `json:"random"`
		} `json:"fallback"`
		RotationMode string `json:"rotation_mode"`
	} `json:"posting"`
	Platforms map[string]struct {
		Posting  legacyPlatformPosting `json:"posting"`
		Limits   legacyPlatformLimits  `json:"limits"`
		Subtypes []struct {
			ID      string                `json:"id"`
			Posting legacyPlatformPosting `json:"posting"`
			Limits  legacyPlatformLimits  `json:"limits"`
		} `json:"subtypes"`
	} `json:"platforms"`
}

type legacyPlatformPosting struct {
	Random        legacyRandomPosting `json:"random"`
	Manual        legacyManualPosting `json:"manual"`
	ScheduleTimes []string            `json:"schedule_times"`
}

type legacyPlatformLimits struct {
	DailyMessages int `json:"daily_messages"`
	DailyPosts    int `json:"daily_posts"`
	HourlyPosts   int `json:"hourly_posts"`
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// applyLegacyPostingAndLimits writes the posting/rate-limit fields the
// wizard form still submits into the DB tables that now own them
// (posting_settings, posting_schedule, rate_limits) — config.json no
// longer carries these fields at all after this save. Unlike
// maestro's bootstrapRateLimits (insert-only, never overwrites), THIS is
// the actual edit path: the wizard is where an admin changes these values,
// so it intentionally overwrites hourly_limit/daily_limit and the
// posting_settings row on every save, without touching current usage
// counts.
func (s *Server) applyLegacyPostingAndLimits(raw []byte) {
	if s.db == nil {
		return
	}
	var legacy legacyPostingAndLimits
	if err := json.Unmarshal(raw, &legacy); err != nil {
		log.Printf("[Wizzard] applyLegacyPostingAndLimits: could not parse posting/limits from request body: %v", err)
		return
	}

	upsertPosting := func(platform, subtype string, rp legacyRandomPosting, mp legacyManualPosting, rotationMode string) {
		_, err := s.db.Exec(`
			INSERT INTO posting_settings
				(platform, subtype, random_enabled, random_interval_min_hours, random_interval_max_hours,
				 random_posts_per_cycle, random_use_global,
				 manual_enabled, manual_title, manual_description, manual_media_type, manual_media_url,
				 rotation_mode)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(platform, subtype) DO UPDATE SET
				random_enabled=excluded.random_enabled,
				random_interval_min_hours=excluded.random_interval_min_hours,
				random_interval_max_hours=excluded.random_interval_max_hours,
				random_posts_per_cycle=excluded.random_posts_per_cycle,
				random_use_global=excluded.random_use_global,
				manual_enabled=excluded.manual_enabled,
				manual_title=excluded.manual_title,
				manual_description=excluded.manual_description,
				manual_media_type=excluded.manual_media_type,
				manual_media_url=excluded.manual_media_url,
				rotation_mode=CASE WHEN excluded.rotation_mode != '' THEN excluded.rotation_mode ELSE posting_settings.rotation_mode END`,
			platform, subtype,
			boolToInt(rp.Enabled), rp.IntervalHours.Min, rp.IntervalHours.Max, rp.PostsPerCycle, boolToInt(rp.UseGlobal),
			boolToInt(mp.Enabled), mp.Payload.Title, mp.Payload.Description, mp.Payload.Media.Type, mp.Payload.Media.URL,
			rotationMode)
		if err != nil {
			log.Printf("[Wizzard] posting_settings upsert error for %s/%s: %v", platform, subtype, err)
		}
	}
	upsertLimits := func(platform, subtype string, l legacyPlatformLimits) {
		if l.DailyMessages == 0 && l.DailyPosts == 0 && l.HourlyPosts == 0 {
			return // nothing submitted for this platform/subtype — don't clobber existing limits with zeros
		}
		if l.DailyMessages > 0 {
			s.db.Exec(`
				INSERT INTO rate_limits (platform, subtype, action, hourly_limit, daily_limit, current_hour_count, current_day_count, last_reset_hour, last_reset_day)
				VALUES (?,?,'messages',0,?,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
				ON CONFLICT(platform, subtype, action) DO UPDATE SET daily_limit=excluded.daily_limit`,
				platform, subtype, l.DailyMessages)
		}
		if l.DailyPosts > 0 || l.HourlyPosts > 0 {
			s.db.Exec(`
				INSERT INTO rate_limits (platform, subtype, action, hourly_limit, daily_limit, current_hour_count, current_day_count, last_reset_hour, last_reset_day)
				VALUES (?,?,'posts',?,?,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
				ON CONFLICT(platform, subtype, action) DO UPDATE SET
					hourly_limit=CASE WHEN excluded.hourly_limit > 0 THEN excluded.hourly_limit ELSE rate_limits.hourly_limit END,
					daily_limit=CASE WHEN excluded.daily_limit > 0 THEN excluded.daily_limit ELSE rate_limits.daily_limit END`,
				platform, subtype, l.HourlyPosts, l.DailyPosts)
		}
	}
	upsertSchedule := func(platform, subtype string, times []string) {
		for _, t := range times {
			if _, err := time.Parse("15:04", t); err != nil {
				continue
			}
			s.db.Exec(`
				INSERT INTO posting_schedule (platform, subtype, post_time, enabled)
				VALUES (?,?,?,1) ON CONFLICT(platform, subtype, post_time) DO UPDATE SET enabled=1`,
				platform, subtype, t)
		}
	}

	upsertPosting("__global__", "", legacy.Posting.Fallback.Random, legacyManualPosting{}, legacy.Posting.RotationMode)
	// Old global scheduler.rate_limits had no per-platform identity — it
	// was only ever used as a fallback seed, so there's no single DB row
	// left for it to write into once every platform/subtype has its own
	// row. Nothing to migrate here beyond what bootstrapRateLimits already
	// seeds with sane defaults on first run.

	for platformID, pc := range legacy.Platforms {
		upsertPosting(platformID, "", pc.Posting.Random, pc.Posting.Manual, "")
		upsertLimits(platformID, "", pc.Limits)
		upsertSchedule(platformID, "", pc.Posting.ScheduleTimes)
		for _, sub := range pc.Subtypes {
			upsertPosting(platformID, sub.ID, sub.Posting.Random, sub.Posting.Manual, "")
			upsertLimits(platformID, sub.ID, sub.Limits)
			upsertSchedule(platformID, sub.ID, sub.Posting.ScheduleTimes)
		}
	}
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.cfgMu.RLock()
		cfg := s.cfg
		s.cfgMu.RUnlock()
		jsonOK(w, cfg)
	case http.MethodPut:
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		var newCfg Config
		if err := json.Unmarshal(bodyBytes, &newCfg); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		newCfg.System.OperationMode = normaliseOperationMode(newCfg.System.OperationMode)
		s.cfgMu.Lock()
		s.cfg = &newCfg
		s.cfgMu.Unlock()
		// Posting timing and rate limits no longer live in config.json —
		// pull them out of the same request body and persist to the DB
		// tables that now own them. wizard.html doesn't need to change;
		// it still sends this data, it just gets routed differently now.
		s.applyLegacyPostingAndLimits(bodyBytes)
		s.invalidateTableCache()
		jsonOK(w, map[string]bool{"ok": true})
	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	if err := s.saveConfig(); err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	s.cfgMu.RLock()
	dbPath := ""
	if s.cfg != nil {
		dbPath = s.cfg.Paths.Database
	}
	s.cfgMu.RUnlock()
	if dbPath != "" {
		s.initDB()
	}
	jsonOK(w, map[string]string{"saved": s.configPath})
}

func (s *Server) handleRestartDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	if err := s.saveConfig(); err != nil {
		jsonErr(w, 500, "save failed: "+err.Error())
		return
	}

	go func() {
		time.Sleep(200 * time.Millisecond)

		maestroMain := filepath.Join(s.baseDir, "maestro_main.go")
		if _, err := os.Stat(maestroMain); os.IsNotExist(err) {
			maestroMain = filepath.Join(s.baseDir, "cmd", "maestro", "main.go")
		}
		if _, err := os.Stat(maestroMain); os.IsNotExist(err) {
			maestroMain = filepath.Join(s.baseDir, "cmd", "sailstream", "main.go")
		}
		if _, err := os.Stat(maestroMain); os.IsNotExist(err) {
			log.Printf("[wizzard] Maestro entry-point not found under %s", s.baseDir)
			return
		}
		cmd := exec.Command("go", "run", maestroMain, "--config", s.configPath)
		cmd.Dir = s.baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = append(os.Environ(), "SAILSTREAM_NO_AUTO_OPEN=1")

		if err := cmd.Start(); err != nil {
			log.Printf("[wizzard] Failed to start maestro: %v", err)
			return
		}

		log.Printf("[wizzard] Maestro started successfully with go run, PID: %d", cmd.Process.Pid)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.srv.Shutdown(ctx)
	}()

	dashPort := "9090"
	jsonOK(w, map[string]string{"status": "restarting", "dashboard_url": "http://localhost:" + dashPort})
}

func (s *Server) handleProducts(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected — save config with a valid database path first")
		return
	}
	switch r.Method {
	case http.MethodGet:
		rows, err := s.db.Query(`
			SELECT p.id, p.name, COALESCE(p.sku,''), COALESCE(p.description,''),
			       COALESCE(p.category,''), COALESCE(p.subcategory,''), COALESCE(p.tags,''),
			       p.price, COALESCE(p.price_per_pack,0), COALESCE(p.quantity_per_pack,0),
			       p.currency, p.stock, p.low_stock_threshold, p.image_count,
			       COALESCE(p.thumbnail_url,''), COALESCE(p.image_url,''),
			       COALESCE(p.weight_kg,0), COALESCE(p.dimensions,''),
			       p.is_active, p.is_featured,
			       COALESCE(p.aliases_en,''), COALESCE(p.aliases_ar,''), COALESCE(p.aliases_ku,''),
			       COALESCE(p.uses_en,''), COALESCE(p.uses_ar,''), COALESCE(p.uses_ku,''),
			       p.created_at, p.updated_at
			FROM products p ORDER BY p.created_at DESC`)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		defer rows.Close()
		var products []map[string]interface{}
		for rows.Next() {
			var id, name, sku, desc, cat, subcat, tags string
			var price, pricePerPack, weightKg float64
			var qtyPerPack, stock, lowStock, imageCount, isActive, isFeatured int
			var currency, thumbURL, imgURL, dims string
			var aEn, aAr, aKu, uEn, uAr, uKu string
			var created, updated string
			if err := rows.Scan(&id, &name, &sku, &desc, &cat, &subcat, &tags,
				&price, &pricePerPack, &qtyPerPack, &currency, &stock, &lowStock, &imageCount,
				&thumbURL, &imgURL, &weightKg, &dims, &isActive, &isFeatured,
				&aEn, &aAr, &aKu, &uEn, &uAr, &uKu, &created, &updated); err != nil {
				continue
			}
			products = append(products, map[string]interface{}{
				"id": id, "name": name, "sku": sku, "description": desc,
				"category": cat, "subcategory": subcat, "tags": tags,
				"price": price, "price_per_pack": pricePerPack, "qty_per_pack": qtyPerPack,
				"currency": currency, "stock": stock, "low_stock_threshold": lowStock,
				"image_count": imageCount, "thumbnail_url": thumbURL, "image_url": imgURL,
				"weight_kg": weightKg, "dimensions": dims,
				"is_active": isActive, "is_featured": isFeatured,
				"aliases_en": aEn, "aliases_ar": aAr, "aliases_ku": aKu,
				"uses_en": uEn, "uses_ar": uAr, "uses_ku": uKu,
				"created_at": created, "updated_at": updated,
			})
		}
		if products == nil {
			products = []map[string]interface{}{}
		}
		jsonOK(w, products)

	case http.MethodPost:
		var p map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		name, _ := p["name"].(string)
		if name == "" {
			jsonErr(w, 400, "name required")
			return
		}
		id := genID()
		sku, _ := p["sku"].(string)
		desc, _ := p["description"].(string)
		cat, _ := p["category"].(string)
		subcat, _ := p["subcategory"].(string)
		tags, _ := p["tags"].(string)
		currency, _ := p["currency"].(string)
		if currency == "" {
			currency = "IQD"
		}
		price, _ := p["price"].(float64)
		ppp, _ := p["price_per_pack"].(float64)
		qpp, _ := p["qty_per_pack"].(float64)
		stock, _ := p["stock"].(float64)
		lowStock, _ := p["low_stock_threshold"].(float64)
		if lowStock == 0 {
			lowStock = 10
		}
		weightKg, _ := p["weight_kg"].(float64)
		dims, _ := p["dimensions"].(string)
		isActive := 1
		if v, ok := p["is_active"].(bool); ok && !v {
			isActive = 0
		}
		isFeatured := 0
		if v, ok := p["is_featured"].(bool); ok && v {
			isFeatured = 1
		}
		aEn, _ := p["aliases_en"].(string)
		aAr, _ := p["aliases_ar"].(string)
		aKu, _ := p["aliases_ku"].(string)
		uEn, _ := p["uses_en"].(string)
		uAr, _ := p["uses_ar"].(string)
		uKu, _ := p["uses_ku"].(string)

		_, err := s.db.Exec(`
			INSERT INTO products
			(id, name, sku, description, category, subcategory, tags,
			 price, price_per_pack, quantity_per_pack, currency,
			 stock, low_stock_threshold, weight_kg, dimensions,
			 is_active, is_featured,
			 aliases_en, aliases_ar, aliases_ku,
			 uses_en, uses_ar, uses_ku)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, name, nullStr(sku), nullStr(desc), cat, nullStr(subcat), nullStr(tags),
			price, nullFloat(ppp), nullInt(int(qpp)), currency,
			int(stock), int(lowStock), nullFloat(weightKg), nullStr(dims),
			isActive, isFeatured,
			nullStr(aEn), nullStr(aAr), nullStr(aKu),
			nullStr(uEn), nullStr(uAr), nullStr(uKu),
		)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		jsonOK(w, map[string]string{"id": id})

	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) handleProduct(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/products/")
	id := strings.SplitN(path, "/", 2)[0]
	if id == "" {
		jsonErr(w, 400, "product id required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		row := s.db.QueryRow(`
			SELECT id, name, COALESCE(sku,''), COALESCE(description,''),
			       COALESCE(category,''), COALESCE(subcategory,''), COALESCE(tags,''),
			       price, COALESCE(price_per_pack,0), COALESCE(quantity_per_pack,0),
			       currency, stock, low_stock_threshold, image_count,
			       COALESCE(thumbnail_url,''), COALESCE(image_url,''),
			       COALESCE(weight_kg,0), COALESCE(dimensions,''),
			       is_active, is_featured,
			       COALESCE(aliases_en,''), COALESCE(aliases_ar,''), COALESCE(aliases_ku,''),
			       COALESCE(uses_en,''), COALESCE(uses_ar,''), COALESCE(uses_ku,'')
			FROM products WHERE id = ?`, id)
		var pid, name, sku, desc, cat, subcat, tags string
		var price, ppp, wkg float64
		var qpp, stock, lowStock, imgCount, isActive, isFeatured int
		var currency, thumbURL, imgURL, dims string
		var aEn, aAr, aKu, uEn, uAr, uKu string
		err := row.Scan(&pid, &name, &sku, &desc, &cat, &subcat, &tags,
			&price, &ppp, &qpp, &currency, &stock, &lowStock, &imgCount,
			&thumbURL, &imgURL, &wkg, &dims, &isActive, &isFeatured,
			&aEn, &aAr, &aKu, &uEn, &uAr, &uKu)
		if err == sql.ErrNoRows {
			jsonErr(w, 404, "not found")
			return
		}
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		imgRows, _ := s.db.Query(`SELECT id, filename, file_path, is_primary, img_order FROM product_images WHERE product_id = ? ORDER BY img_order`, id)
		var images []map[string]interface{}
		if imgRows != nil {
			defer imgRows.Close()
			for imgRows.Next() {
				var iid, fname, fpath string
				var isPrimary, order int
				if err := imgRows.Scan(&iid, &fname, &fpath, &isPrimary, &order); err != nil {
					continue
				}
				images = append(images, map[string]interface{}{
					"id": iid, "filename": fname, "file_path": fpath,
					"is_primary": isPrimary == 1, "order": order,
					"url": "/media/product_images/" + id + "/" + fname,
				})
			}
		}
		if images == nil {
			images = []map[string]interface{}{}
		}
		jsonOK(w, map[string]interface{}{
			"id": pid, "name": name, "sku": sku, "description": desc,
			"category": cat, "subcategory": subcat, "tags": tags,
			"price": price, "price_per_pack": ppp, "qty_per_pack": qpp,
			"currency": currency, "stock": stock, "low_stock_threshold": lowStock,
			"image_count": imgCount, "thumbnail_url": thumbURL, "image_url": imgURL,
			"weight_kg": wkg, "dimensions": dims,
			"is_active": isActive, "is_featured": isFeatured,
			"aliases_en": aEn, "aliases_ar": aAr, "aliases_ku": aKu,
			"uses_en": uEn, "uses_ar": uAr, "uses_ku": uKu,
			"images": images,
		})

	case http.MethodPut:
		var p map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		name, _ := p["name"].(string)
		sku, _ := p["sku"].(string)
		desc, _ := p["description"].(string)
		cat, _ := p["category"].(string)
		subcat, _ := p["subcategory"].(string)
		tags, _ := p["tags"].(string)
		price, _ := p["price"].(float64)
		ppp, _ := p["price_per_pack"].(float64)
		qpp, _ := p["qty_per_pack"].(float64)
		currency, _ := p["currency"].(string)
		stock, _ := p["stock"].(float64)
		lowStock, _ := p["low_stock_threshold"].(float64)
		wkg, _ := p["weight_kg"].(float64)
		dims, _ := p["dimensions"].(string)
		isActive := 1
		if v, ok := p["is_active"].(bool); ok && !v {
			isActive = 0
		}
		isFeatured := 0
		if v, ok := p["is_featured"].(bool); ok && v {
			isFeatured = 1
		}
		aEn, _ := p["aliases_en"].(string)
		aAr, _ := p["aliases_ar"].(string)
		aKu, _ := p["aliases_ku"].(string)
		uEn, _ := p["uses_en"].(string)
		uAr, _ := p["uses_ar"].(string)
		uKu, _ := p["uses_ku"].(string)
		_, err := s.db.Exec(`
			UPDATE products SET
			  name=?, sku=?, description=?, category=?, subcategory=?, tags=?,
			  price=?, price_per_pack=?, quantity_per_pack=?, currency=?,
			  stock=?, low_stock_threshold=?, weight_kg=?, dimensions=?,
			  is_active=?, is_featured=?,
			  aliases_en=?, aliases_ar=?, aliases_ku=?,
			  uses_en=?, uses_ar=?, uses_ku=?
			WHERE id=?`,
			name, nullStr(sku), nullStr(desc), cat, nullStr(subcat), nullStr(tags),
			price, nullFloat(ppp), nullInt(int(qpp)), currency,
			int(stock), int(lowStock), nullFloat(wkg), nullStr(dims),
			isActive, isFeatured,
			nullStr(aEn), nullStr(aAr), nullStr(aKu),
			nullStr(uEn), nullStr(uAr), nullStr(uKu),
			id)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		jsonOK(w, map[string]bool{"ok": true})

	case http.MethodDelete:
		dir := s.productImagesDir(id)
		os.RemoveAll(dir)
		s.db.Exec("DELETE FROM products WHERE id = ?", id)
		jsonOK(w, map[string]bool{"ok": true})

	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) handleProductImages(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/products/images/")
	parts := strings.SplitN(path, "/", 3)
	productID := parts[0]

	switch r.Method {
	case http.MethodPost:
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		dir := s.productImagesDir(productID)
		os.MkdirAll(dir, 0755)

		var existingCount int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM product_images WHERE product_id = ?", productID).Scan(&existingCount); err != nil {
			log.Printf("[wizzard] existingCount error: %v", err)
			existingCount = 0
		}

		files := r.MultipartForm.File["images"]
		var uploaded []map[string]interface{}
		for i, fh := range files {
			imgID, filename, filePath, err := s.saveUploadedImage(fh, dir, existingCount+i+1)
			if err != nil {
				log.Printf("image upload error: %v", err)
				continue
			}
			isPrimary := 0
			if existingCount == 0 && i == 0 {
				isPrimary = 1
			}
			ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(filename)), ".")
			_, err = s.db.Exec(`
				INSERT INTO product_images (id, product_id, filename, file_path, extension, img_order, is_primary)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				imgID, productID, filename, filePath, ext, existingCount+i+1, isPrimary)
			if err != nil {
				log.Printf("DB insert image error: %v", err)
				continue
			}
			if isPrimary == 1 {
				s.db.Exec("UPDATE products SET thumbnail_url=?, image_url=? WHERE id=?", filePath, filePath, productID)
			}
			uploaded = append(uploaded, map[string]interface{}{
				"id": imgID, "filename": filename, "file_path": filePath,
				"is_primary": isPrimary == 1, "order": existingCount + i + 1,
				"url": "/media/product_images/" + productID + "/" + filename,
			})
		}
		if uploaded == nil {
			uploaded = []map[string]interface{}{}
		}
		jsonOK(w, uploaded)

	case http.MethodDelete:
		if len(parts) < 2 || parts[1] == "" {
			jsonErr(w, 400, "image id required")
			return
		}
		imageID := parts[1]
		var fp string
		var isPrimary int
		err := s.db.QueryRow("SELECT file_path, is_primary FROM product_images WHERE id = ?", imageID).Scan(&fp, &isPrimary)
		if err != nil && err != sql.ErrNoRows {
			jsonErr(w, 500, err.Error())
			return
		}
		if fp != "" {
			os.Remove(fp)
		}
		s.db.Exec("DELETE FROM product_images WHERE id = ?", imageID)
		if isPrimary == 1 {
			var newID, newPath string
			err := s.db.QueryRow("SELECT id, file_path FROM product_images WHERE product_id = ? ORDER BY img_order LIMIT 1", productID).Scan(&newID, &newPath)
			if err == nil && newID != "" {
				s.db.Exec("UPDATE product_images SET is_primary=1 WHERE id=?", newID)
				s.db.Exec("UPDATE products SET thumbnail_url=? WHERE id=?", newPath, productID)
			} else {
				s.db.Exec("UPDATE products SET thumbnail_url=NULL, image_url=NULL WHERE id=?", productID)
			}
		}
		jsonOK(w, map[string]bool{"ok": true})

	case http.MethodPatch:
		if len(parts) < 3 || parts[2] != "primary" {
			jsonErr(w, 400, "use /primary suffix")
			return
		}
		imageID := parts[1]
		s.db.Exec("UPDATE product_images SET is_primary=0 WHERE product_id=?", productID)
		s.db.Exec("UPDATE product_images SET is_primary=1 WHERE id=?", imageID)
		var fp string
		err := s.db.QueryRow("SELECT file_path FROM product_images WHERE id=?", imageID).Scan(&fp)
		if err != nil && err != sql.ErrNoRows {
			jsonErr(w, 500, err.Error())
			return
		}
		s.db.Exec("UPDATE products SET thumbnail_url=? WHERE id=?", fp, productID)
		jsonOK(w, map[string]bool{"ok": true})

	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) saveUploadedImage(fh *multipart.FileHeader, dir string, order int) (id, filename, filePath string, err error) {
	src, err := fh.Open()
	if err != nil {
		return
	}
	defer src.Close()

	buf := make([]byte, 512)
	n, _ := src.Read(buf)
	mimeType := http.DetectContentType(buf[:n])

	type seeker interface {
		Seek(int64, int) (int64, error)
	}
	if sk, ok := src.(seeker); ok {
		sk.Seek(0, io.SeekStart)
	} else {
		err = fmt.Errorf("uploaded file not seekable")
		return
	}

	allowedMIME := map[string]bool{
		"image/jpeg": true, "image/png": true, "image/gif": true,
		"image/webp": true, "video/mp4": true, "video/quicktime": true,
		"video/x-msvideo": true, "application/octet-stream": true,
	}
	if !allowedMIME[mimeType] {
		err = fmt.Errorf("disallowed MIME type: %s", mimeType)
		return
	}

	id = genID()
	ext := strings.ToLower(filepath.Ext(fh.Filename))
	if ext == "" {
		ext = ".jpg"
	}
	filename = fmt.Sprintf("%d_%s%s", order, id[:8], ext)
	filePath = filepath.Join(dir, filename)

	dst, err := os.Create(filePath)
	if err != nil {
		return
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return
}

func (s *Server) handleScheduledPosts(w http.ResponseWriter, r *http.Request) {
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg == nil {
		jsonErr(w, 503, "config not loaded")
		return
	}
	switch r.Method {
	case http.MethodGet:
		posts := cfg.ScheduledPosts
		if posts == nil {
			posts = []ScheduledPost{}
		}
		jsonOK(w, posts)
	case http.MethodPost:
		var post ScheduledPost
		if err := json.NewDecoder(r.Body).Decode(&post); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		post.ID = genID()
		post.CreatedAt = time.Now().Format(time.RFC3339)
		post.UpdatedAt = post.CreatedAt
		if post.Status == "" {
			post.Status = "pending"
		}
		if post.Schedule.Type == "" {
			post.Schedule.Type = "manual"
		}
		if post.Media == nil {
			post.Media = []ScheduledMedia{}
		}
		if post.Platforms == nil {
			post.Platforms = []ScheduledPostPlatform{}
		}
		if post.Hashtags == nil {
			post.Hashtags = []string{}
		}
		s.cfgMu.Lock()
		post.Order = len(s.cfg.ScheduledPosts) + 1
		if s.cfg.ScheduledPosts == nil {
			s.cfg.ScheduledPosts = []ScheduledPost{}
		}
		s.cfg.ScheduledPosts = append(s.cfg.ScheduledPosts, post)
		s.cfgMu.Unlock()
		if err := s.saveConfig(); err != nil {
			log.Printf("[wizzard] auto-save after post create: %v", err)
		}
		jsonOK(w, post)
	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) handleScheduledPost(w http.ResponseWriter, r *http.Request) {
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg == nil {
		jsonErr(w, 503, "config not loaded")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/scheduled-posts/")

	findIdx := func() int {
		s.cfgMu.RLock()
		defer s.cfgMu.RUnlock()
		for i, p := range s.cfg.ScheduledPosts {
			if p.ID == id {
				return i
			}
		}
		return -1
	}

	switch r.Method {
	case http.MethodGet:
		idx := findIdx()
		if idx < 0 {
			jsonErr(w, 404, "not found")
			return
		}
		s.cfgMu.RLock()
		post := s.cfg.ScheduledPosts[idx]
		s.cfgMu.RUnlock()
		jsonOK(w, post)
	case http.MethodPut:
		var post ScheduledPost
		if err := json.NewDecoder(r.Body).Decode(&post); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		idx := findIdx()
		if idx < 0 {
			jsonErr(w, 404, "not found")
			return
		}
		s.cfgMu.Lock()
		post.ID = id
		post.CreatedAt = s.cfg.ScheduledPosts[idx].CreatedAt
		post.UpdatedAt = time.Now().Format(time.RFC3339)
		post.Order = s.cfg.ScheduledPosts[idx].Order
		s.cfg.ScheduledPosts[idx] = post
		s.cfgMu.Unlock()
		if err := s.saveConfig(); err != nil {
			log.Printf("[wizzard] auto-save after post update: %v", err)
		}
		jsonOK(w, post)
	case http.MethodDelete:
		idx := findIdx()
		if idx < 0 {
			jsonErr(w, 404, "not found")
			return
		}
		s.cfgMu.Lock()
		s.cfg.ScheduledPosts = append(s.cfg.ScheduledPosts[:idx], s.cfg.ScheduledPosts[idx+1:]...)
		for i := range s.cfg.ScheduledPosts {
			s.cfg.ScheduledPosts[i].Order = i + 1
		}
		s.cfgMu.Unlock()
		if err := s.saveConfig(); err != nil {
			log.Printf("[wizzard] auto-save after post delete: %v", err)
		}
		jsonOK(w, map[string]bool{"ok": true})
	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) handlePostMediaUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	if err := r.ParseMultipartForm(128 << 20); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	dir := filepath.Join(s.mediaDir(), "post_images")
	if r.FormValue("type") == "video" {
		dir = filepath.Join(s.mediaDir(), "post_videos")
	}
	os.MkdirAll(dir, 0755)

	var result []map[string]interface{}
	for _, fh := range r.MultipartForm.File["files"] {
		imgID, filename, filePath, err := s.saveUploadedImage(fh, dir, int(time.Now().UnixNano()%100000))
		if err != nil {
			log.Printf("[wizzard] post media upload error: %v", err)
			continue
		}
		mediaType := "image"
		ext := strings.ToLower(filepath.Ext(filename))
		if ext == ".mp4" || ext == ".mov" || ext == ".avi" {
			mediaType = "video"
		}
		result = append(result, map[string]interface{}{
			"id": imgID, "filename": filename, "file_path": filePath,
			"type": mediaType, "url": "/media/post_images/" + filename,
		})
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	jsonOK(w, result)
}

func (s *Server) handleDBTables(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected")
		return
	}
	s.invalidateTableCache()
	rows, err := s.db.Query("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	var tables []map[string]string
	for rows.Next() {
		var name, typ string
		rows.Scan(&name, &typ)
		tables = append(tables, map[string]string{"name": name, "type": typ})
	}
	jsonOK(w, tables)
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
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 500 {
		limit = l
	}
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = o
	}
	search := r.URL.Query().Get("search")

	pkCol := ""
	isView := false
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

	typeRow := s.db.QueryRow("SELECT type FROM sqlite_master WHERE name=?", table)
	var objType string
	if err := typeRow.Scan(&objType); err == nil && objType == "view" {
		isView = true
	}

	if pkCol == "" && !isView {
		for _, c := range cols {
			if c["name"] == "id" {
				pkCol = "id"
				break
			}
		}
	}

	baseQuery := fmt.Sprintf("SELECT * FROM %q", table)
	args := []interface{}{}
	if search != "" && len(cols) > 0 {
		var conditions []string
		for _, c := range cols {
			conditions = append(conditions, fmt.Sprintf("%q LIKE ?", c["name"]))
			args = append(args, "%"+search+"%")
		}
		baseQuery += " WHERE " + strings.Join(conditions, " OR ")
	}

	countQuery := "SELECT COUNT(*) FROM (" + baseQuery + ")"
	var total int
	s.db.QueryRow(countQuery, args...).Scan(&total)

	fullQuery := baseQuery + fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	rows, err := s.db.Query(fullQuery, args...)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	defer rows.Close()

	columnNames, _ := rows.Columns()
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

	editable := []string{}
	if !isView {
		for _, c := range cols {
			if c["pk"] == true {
				continue
			}
			editable = append(editable, c["name"].(string))
		}
	}

	jsonOK(w, map[string]interface{}{
		"table":     table,
		"columns":   cols,
		"rows":      result,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
		"editable":  editable,
		"is_view":   isView,
		"pk_column": pkCol,
	})
}

func (s *Server) handleDBRow(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected")
		return
	}
	table := r.URL.Query().Get("table")
	id := r.URL.Query().Get("id")
	idCol := r.URL.Query().Get("id_col")
	if table == "" || id == "" {
		jsonErr(w, 400, "table and id required")
		return
	}
	if !s.isValidTable(table) {
		jsonErr(w, 400, "unknown table: "+table)
		return
	}
	if idCol == "" {
		idCol = "id"
	}
	if !s.isValidColumn(table, idCol) {
		jsonErr(w, 400, "unknown column: "+idCol)
		return
	}

	switch r.Method {
	case http.MethodPut:
		var updates map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
			jsonErr(w, 400, err.Error())
			return
		}
		var setClauses []string
		var args []interface{}
		for k, v := range updates {
			if !s.isValidColumn(table, k) {
				continue
			}
			setClauses = append(setClauses, fmt.Sprintf("%q = ?", k))
			args = append(args, v)
		}
		if len(setClauses) == 0 {
			jsonErr(w, 400, "no valid fields to update")
			return
		}
		args = append(args, id)
		query := fmt.Sprintf("UPDATE %q SET %s WHERE %q = ?", table, strings.Join(setClauses, ", "), idCol)
		_, err := s.db.Exec(query, args...)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		jsonOK(w, map[string]bool{"ok": true})

	case http.MethodDelete:
		query := fmt.Sprintf("DELETE FROM %q WHERE %q = ?", table, idCol)
		_, err := s.db.Exec(query, id)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		jsonOK(w, map[string]bool{"ok": true})

	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) isValidColumn(table, col string) bool {
	if s.db == nil {
		return false
	}
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk) == nil {
			if name == col {
				return true
			}
		}
	}
	return false
}

func (s *Server) resolvePython() string {
	s.cfgMu.RLock()
	detectedOS := ""
	if s.cfg != nil {
		detectedOS = s.cfg.Meta.DetectedOS
	}
	s.cfgMu.RUnlock()

	if detectedOS == "android" {
		const termuxPy = "/data/data/com.termux/files/usr/bin/python3"
		if _, err := os.Stat(termuxPy); err == nil {
			return termuxPy
		}
	}
	if runtime.GOOS == "windows" {
		for _, p := range []string{"py", "python", "python3"} {
			if path, err := exec.LookPath(p); err == nil {
				return path
			}
		}
	}
	for _, p := range []string{"python3", "python"} {
		if path, err := exec.LookPath(p); err == nil {
			return path
		}
	}
	return "python3"
}

func (s *Server) cnnDir() string {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfg != nil && s.cfg.Paths.Models != "" {
		d := s.cfg.Paths.Models
		if !filepath.IsAbs(d) {
			d = filepath.Join(s.baseDir, d)
		}
		return d
	}
	return filepath.Join(s.baseDir, "cnn")
}

func (s *Server) irScriptsDir() string {
	candidate := filepath.Join(s.baseDir, "internal", "cnn")
	if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
		return candidate
	}
	scriptsNext := filepath.Join(s.cnnDir(), "scripts")
	if fi, err := os.Stat(scriptsNext); err == nil && fi.IsDir() {
		return scriptsNext
	}
	return s.cnnDir()
}

func (s *Server) runPythonScript(args []string, timeout time.Duration) ([]byte, error) {
	python := s.resolvePython()
	if err := checkPythonEnv(python); err != nil {
		return nil, fmt.Errorf("python environment error: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, args...)
	cmd.Dir = s.baseDir
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return out, fmt.Errorf("python script timed out after %v", timeout)
	}
	return out, err
}

func (s *Server) newIRJob() *IRJob {
	job := &IRJob{
		ID:        genID(),
		Status:    IRJobPending,
		StartedAt: time.Now(),
	}
	s.irJobsMu.Lock()
	s.irJobs[job.ID] = job
	s.irJobsMu.Unlock()
	return job
}

func (s *Server) handleIRJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonErr(w, 405, "GET only")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/ir/jobs/")
	s.irJobsMu.Lock()
	job, ok := s.irJobs[id]
	s.irJobsMu.Unlock()
	if !ok {
		jsonErr(w, 404, "job not found")
		return
	}
	jsonOK(w, job)
}

func (s *Server) handleIRTrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	var p struct {
		DataDir         string `json:"data_dir"`
		ModelName       string `json:"model_name"`
		ModelsDir       string `json:"models_dir"`
		Epochs          int    `json:"epochs"`
		BatchSize       int    `json:"batch_size"`
		NumAngles       int    `json:"num_angles"`
		UseHybrid       bool   `json:"use_hybrid"`
		UseAugmentation bool   `json:"use_augmentation"`
		UseMultilingual bool   `json:"use_multilingual"`
		BoostAccuracy   bool   `json:"boost_accuracy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	if p.DataDir == "" {
		jsonErr(w, 400, "data_dir required")
		return
	}
	if p.ModelName == "" {
		jsonErr(w, 400, "model_name required")
		return
	}
	dataDir := p.DataDir
	if !filepath.IsAbs(dataDir) {
		dataDir = filepath.Join(s.baseDir, dataDir)
	}
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		jsonErr(w, 400, "data_dir not found: "+dataDir)
		return
	}
	modelsDir := p.ModelsDir
	if modelsDir == "" {
		modelsDir = s.cnnDir()
	} else if !filepath.IsAbs(modelsDir) {
		modelsDir = filepath.Join(s.baseDir, modelsDir)
	}
	os.MkdirAll(modelsDir, 0755)

	s.cfgMu.RLock()
	irCfg := ImageRecognitionConfig{}
	if s.cfg != nil {
		irCfg = s.cfg.ImageRecognition
	}
	s.cfgMu.RUnlock()

	epochs := p.Epochs
	if epochs == 0 {
		epochs = irCfg.TrainingDefaults.Epochs
	}
	if epochs == 0 {
		epochs = 30
	}
	batchSize := p.BatchSize
	if batchSize == 0 {
		batchSize = irCfg.TrainingDefaults.BatchSize
	}
	if batchSize == 0 {
		batchSize = 8
	}
	numAngles := p.NumAngles
	if numAngles == 0 {
		numAngles = irCfg.TrainingDefaults.NumAngles
	}
	if numAngles == 0 {
		numAngles = 3
	}

	scriptsDir := s.irScriptsDir()
	args := []string{
		filepath.Join(scriptsDir, "main_trainer.py"),
		"--train-new",
		"--data-dir", dataDir,
		"--output-dir", modelsDir,
		"--model-name", p.ModelName,
		"--epochs", fmt.Sprintf("%d", epochs),
		"--batch-size", fmt.Sprintf("%d", batchSize),
		"--num-angles", fmt.Sprintf("%d", numAngles),
		"--json",
	}
	if irCfg.MaxImageSizePx > 0 {
		args = append(args, "--max-image-size", fmt.Sprintf("%d", irCfg.MaxImageSizePx))
	}
	if p.UseHybrid {
		args = append(args, "--use-hybrid")
	} else {
		args = append(args, "--use-basic")
	}
	if p.UseAugmentation {
		args = append(args, "--augment")
	}

	job := s.newIRJob()
	job.Status = IRJobRunning

	go func() {
		type TrainResult struct {
			Success      bool                   `json:"success"`
			ModelPath    string                 `json:"model_path"`
			Accuracy     float64                `json:"accuracy"`
			NumClasses   int                    `json:"num_classes"`
			TrainingTime string                 `json:"training_time"`
			Logs         string                 `json:"logs"`
			Metrics      map[string]interface{} `json:"metrics"`
		}
		result := TrainResult{Metrics: map[string]interface{}{}}

		output, err := s.runPythonScript(args, 4*time.Hour)
		result.Logs = string(output)
		if err != nil {
			result.Logs += "\nError: " + err.Error()
			s.irJobsMu.Lock()
			now := time.Now()
			job.Status = IRJobFailed
			job.EndedAt = &now
			job.Error = err.Error()
			job.Result = result
			job.Logs = result.Logs
			s.irJobsMu.Unlock()
			return
		}

		if data, _ := parsePythonOutput(output); data != nil {
			if v, ok := data["model_path"].(string); ok {
				result.ModelPath = v
			}
			if v, ok := data["final_accuracy"].(float64); ok {
				result.Accuracy = v
			}
			if v, ok := data["num_classes"].(float64); ok {
				result.NumClasses = int(v)
			}
			if v, ok := data["training_time"].(string); ok {
				result.TrainingTime = v
			}
			result.Metrics = data
		}

		if p.UseMultilingual {
			mlOut, _ := s.runPythonScript([]string{
				filepath.Join(scriptsDir, "multilingual_augmentation.py"),
				"--data-dir", dataDir, "--apply", "--json",
			}, 30*time.Minute)
			result.Logs += "\n[multilingual]\n" + string(mlOut)
		}
		if p.BoostAccuracy && result.ModelPath != "" {
			boostOut, _ := s.runPythonScript([]string{
				filepath.Join(scriptsDir, "accuracy_booster.py"),
				"--model", result.ModelPath, "--boost", "--json",
			}, 2*time.Hour)
			result.Logs += "\n[accuracy_booster]\n" + string(boostOut)
		}
		result.Success = true

		s.irJobsMu.Lock()
		now := time.Now()
		job.Status = IRJobDone
		job.EndedAt = &now
		job.Result = result
		job.Logs = result.Logs
		s.irJobsMu.Unlock()
	}()

	jsonOK(w, map[string]string{"job_id": job.ID, "status": "running"})
}

func (s *Server) handleIRProductionTrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	var p struct {
		BaseModel       string `json:"base_model"`
		NewDataDir      string `json:"new_data_dir"`
		OutputName      string `json:"output_name"`
		Strategy        string `json:"strategy"`
		Epochs          int    `json:"epochs"`
		BatchSize       int    `json:"batch_size"`
		UseAugmentation bool   `json:"use_augmentation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	baseModel := p.BaseModel
	if !filepath.IsAbs(baseModel) {
		baseModel = filepath.Join(s.baseDir, baseModel)
	}
	if _, err := os.Stat(baseModel); os.IsNotExist(err) {
		jsonErr(w, 400, "base model not found: "+baseModel)
		return
	}
	newDataDir := p.NewDataDir
	if !filepath.IsAbs(newDataDir) {
		newDataDir = filepath.Join(s.baseDir, newDataDir)
	}
	if _, err := os.Stat(newDataDir); os.IsNotExist(err) {
		jsonErr(w, 400, "new_data_dir not found: "+newDataDir)
		return
	}
	outputDir := filepath.Join(filepath.Dir(baseModel), "production")
	os.MkdirAll(outputDir, 0755)

	s.cfgMu.RLock()
	irCfg := ImageRecognitionConfig{}
	if s.cfg != nil {
		irCfg = s.cfg.ImageRecognition
	}
	s.cfgMu.RUnlock()

	epochs := p.Epochs
	if epochs == 0 {
		epochs = irCfg.TrainingDefaults.Epochs
	}
	if epochs == 0 {
		epochs = 30
	}
	batchSize := p.BatchSize
	if batchSize == 0 {
		batchSize = irCfg.TrainingDefaults.BatchSize
	}
	if batchSize == 0 {
		batchSize = 8
	}
	strategy := p.Strategy
	if strategy == "" {
		strategy = "fine_tune"
	}
	outputName := p.OutputName
	if outputName == "" {
		outputName = "retrained_" + filepath.Base(baseModel)
	}

	scriptsDir := s.irScriptsDir()
	args := []string{
		filepath.Join(scriptsDir, "production_trainer.py"),
		"--base-model", baseModel,
		"--new-data", newDataDir,
		"--output-name", outputName,
		"--strategy", strategy,
		"--epochs", fmt.Sprintf("%d", epochs),
		"--batch-size", fmt.Sprintf("%d", batchSize),
		"--output-dir", outputDir,
		"--json",
	}
	if p.UseAugmentation {
		args = append(args, "--augment")
	}

	job := s.newIRJob()
	job.Status = IRJobRunning

	go func() {
		type TrainResult struct {
			Success      bool    `json:"success"`
			ModelPath    string  `json:"model_path"`
			Accuracy     float64 `json:"accuracy"`
			NumClasses   int     `json:"num_classes"`
			TrainingTime string  `json:"training_time"`
			Logs         string  `json:"logs"`
		}
		result := TrainResult{}

		output, err := s.runPythonScript(args, 4*time.Hour)
		result.Logs = string(output)
		if err != nil {
			result.Logs += "\nError: " + err.Error()
			s.irJobsMu.Lock()
			now := time.Now()
			job.Status = IRJobFailed
			job.EndedAt = &now
			job.Error = err.Error()
			job.Result = result
			job.Logs = result.Logs
			s.irJobsMu.Unlock()
			return
		}
		if data, _ := parsePythonOutput(output); data != nil {
			if v, ok := data["model_path"].(string); ok {
				result.ModelPath = v
			}
			if v, ok := data["final_accuracy"].(float64); ok {
				result.Accuracy = v
			}
			if v, ok := data["num_classes"].(float64); ok {
				result.NumClasses = int(v)
			}
			if v, ok := data["training_time"].(string); ok {
				result.TrainingTime = v
			}
		}
		result.Success = true

		s.irJobsMu.Lock()
		now := time.Now()
		job.Status = IRJobDone
		job.EndedAt = &now
		job.Result = result
		job.Logs = result.Logs
		s.irJobsMu.Unlock()
	}()

	jsonOK(w, map[string]string{"job_id": job.ID, "status": "running"})
}

func (s *Server) handleIRMultilingual(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	var p struct {
		DataDir string `json:"data_dir"`
	}
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	dataDir := p.DataDir
	if !filepath.IsAbs(dataDir) {
		dataDir = filepath.Join(s.baseDir, dataDir)
	}
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		jsonErr(w, 400, "data_dir not found: "+dataDir)
		return
	}
	scriptsDir := s.irScriptsDir()
	script := filepath.Join(scriptsDir, "multilingual_augmentation.py")
	if _, err := os.Stat(script); os.IsNotExist(err) {
		jsonOK(w, map[string]interface{}{"success": false, "message": "multilingual_augmentation.py not found at " + script})
		return
	}

	job := s.newIRJob()
	job.Status = IRJobRunning

	go func() {
		output, err := s.runPythonScript([]string{script, "--data-dir", dataDir, "--apply", "--json"}, 30*time.Minute)
		s.irJobsMu.Lock()
		now := time.Now()
		job.EndedAt = &now
		job.Logs = string(output)
		if err != nil {
			job.Status = IRJobFailed
			job.Error = err.Error()
			job.Result = map[string]interface{}{"success": false, "message": err.Error()}
		} else {
			job.Status = IRJobDone
			job.Result = map[string]interface{}{"success": true, "output": string(output)}
		}
		s.irJobsMu.Unlock()
	}()

	jsonOK(w, map[string]string{"job_id": job.ID, "status": "running"})
}

func (s *Server) handleIRPredict(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	var p struct {
		ModelPath string `json:"model_path"`
		ImagePath string `json:"image_path"`
		TopK      int    `json:"top_k"`
		UseTTA    bool   `json:"use_tta"`
	}
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}

	s.cfgMu.RLock()
	irCfg := ImageRecognitionConfig{}
	if s.cfg != nil {
		irCfg = s.cfg.ImageRecognition
	}
	s.cfgMu.RUnlock()

	modelPath := p.ModelPath
	if modelPath == "" {
		modelPath = irCfg.ModelPath
	}
	if modelPath == "" {
		jsonErr(w, 400, "no model_path provided — set one under IR → Settings → Default Model Path")
		return
	}
	if !filepath.IsAbs(modelPath) {
		modelPath = filepath.Join(s.baseDir, modelPath)
	}
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		jsonErr(w, 400, "model not found: "+modelPath)
		return
	}
	imagePath := p.ImagePath
	if !filepath.IsAbs(imagePath) {
		imagePath = filepath.Join(s.baseDir, imagePath)
	}
	if _, err := os.Stat(imagePath); os.IsNotExist(err) {
		jsonErr(w, 400, "image not found: "+imagePath)
		return
	}
	topK := p.TopK
	if topK == 0 {
		topK = 3
	}
	scriptsDir := s.irScriptsDir()
	args := []string{
		filepath.Join(scriptsDir, "main_trainer.py"),
		"--predict",
		"--model", modelPath,
		"--image", imagePath,
		"--top-k", fmt.Sprintf("%d", topK),
		"--json",
	}
	if irCfg.MaxImageSizePx > 0 {
		args = append(args, "--max-image-size", fmt.Sprintf("%d", irCfg.MaxImageSizePx))
	}
	if p.UseTTA {
		args = append(args, "--use-tta")
	} else {
		args = append(args, "--no-tta")
	}

	output, err := s.runPythonScript(args, 30*time.Second)

	type Prediction struct {
		PID         string  `json:"pid"`
		Confidence  float64 `json:"confidence"`
		ClassIndex  int     `json:"class_index"`
		ProductName string  `json:"product_name"`
		ClassName   string  `json:"class_name"`
	}
	type PredictResult struct {
		Success       bool         `json:"success"`
		Predictions   []Prediction `json:"predictions"`
		TopPrediction *Prediction  `json:"top_prediction"`
		Logs          string       `json:"logs"`
		InferenceTime string       `json:"inference_time"`
		Error         string       `json:"error,omitempty"`
	}
	result := PredictResult{Logs: string(output)}
	if err != nil {
		result.Error = err.Error()
		result.Logs += "\nError: " + err.Error()
		jsonOK(w, result)
		return
	}

	if data, _ := parsePythonOutput(output); data != nil {
		if t, ok := data["inference_time"].(string); ok {
			result.InferenceTime = t
		}
		threshold := irCfg.ConfidenceThreshold
		if preds, ok := data["predictions"].([]interface{}); ok {
			for _, p2 := range preds {
				m, ok := p2.(map[string]interface{})
				if !ok {
					continue
				}
				pred := Prediction{}
				if v, ok := m["pid"].(string); ok {
					pred.PID = v
				} else if v, ok := m["product_id"].(string); ok {
					pred.PID = v
				}
				if v, ok := m["confidence"].(float64); ok {
					pred.Confidence = v
				}
				if v, ok := m["class_index"].(float64); ok {
					pred.ClassIndex = int(v)
				}
				if v, ok := m["product_name"].(string); ok {
					pred.ProductName = v
				} else if v, ok := m["class_name"].(string); ok {
					pred.ProductName = v
				}
				if v, ok := m["class_name"].(string); ok {
					pred.ClassName = v
				}
				if threshold > 0 && pred.Confidence < threshold {
					continue
				}
				result.Predictions = append(result.Predictions, pred)
			}
		}
	}
	if len(result.Predictions) > 0 {
		top := result.Predictions[0]
		result.TopPrediction = &top
	}
	result.Success = true
	jsonOK(w, result)
}

func (s *Server) handleIRModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonErr(w, 405, "GET only")
		return
	}
	dir := s.cnnDir()
	type ModelInfo struct {
		Name     string `json:"name"`
		Path     string `json:"path"`
		SizeMB   string `json:"size_mb"`
		Modified string `json:"modified"`
	}
	var models []ModelInfo
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		switch strings.ToLower(filepath.Ext(path)) {
		case ".onnx", ".h5", ".pkl", ".pt", ".pth":
			models = append(models, ModelInfo{
				Name:     info.Name(),
				Path:     path,
				SizeMB:   fmt.Sprintf("%.2f", float64(info.Size())/1024/1024),
				Modified: info.ModTime().Format("2006-01-02 15:04"),
			})
		}
		return nil
	})
	if models == nil {
		models = []ModelInfo{}
	}
	jsonOK(w, map[string]interface{}{"models": models, "models_dir": dir})
}

func (s *Server) handleIRDeleteModel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		jsonErr(w, 405, "DELETE only")
		return
	}
	rawPath := strings.TrimPrefix(r.URL.Path, "/api/ir/models/")
	if rawPath == "" {
		jsonErr(w, 400, "model path required")
		return
	}
	modelPath, err := url.PathUnescape(rawPath)
	if err != nil {
		jsonErr(w, 400, "invalid path encoding")
		return
	}
	if !filepath.IsAbs(modelPath) {
		modelPath = filepath.Join(s.baseDir, modelPath)
	}
	cnnD := s.cnnDir()
	absBase, _ := filepath.Abs(s.baseDir)
	absPath, _ := filepath.Abs(modelPath)
	if !strings.HasPrefix(absPath, absBase) && !strings.HasPrefix(absPath, cnnD) {
		jsonErr(w, 403, "path outside project directory")
		return
	}
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		jsonErr(w, 404, "model not found: "+modelPath)
		return
	}
	deleted := []string{modelPath}
	os.Remove(modelPath)
	base := strings.TrimSuffix(modelPath, filepath.Ext(modelPath))
	for _, ext := range []string{".json", "_meta.json", "_metadata.json", ".tflite", "_labels.json", "_config.json"} {
		f := base + ext
		if _, err := os.Stat(f); err == nil {
			os.Remove(f)
			deleted = append(deleted, f)
		}
	}
	jsonOK(w, map[string]interface{}{"ok": true, "deleted": deleted})
}

func (s *Server) handleIRUploadImage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, 405, "POST only")
		return
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		jsonErr(w, 400, err.Error())
		return
	}
	files := r.MultipartForm.File["file"]
	if len(files) == 0 {
		jsonErr(w, 400, "no file uploaded")
		return
	}
	src, err := files[0].Open()
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	defer src.Close()

	ext := strings.ToLower(filepath.Ext(files[0].Filename))
	if ext == "" {
		ext = ".jpg"
	}
	tmpDir := filepath.Join(s.baseDir, "tmp")
	os.MkdirAll(tmpDir, 0755)
	tmp, err := os.CreateTemp(tmpDir, "predict_*"+ext)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	destPath := tmp.Name()
	if _, err := io.Copy(tmp, src); err != nil {
		tmp.Close()
		os.Remove(destPath)
		jsonErr(w, 500, err.Error())
		return
	}
	tmp.Close()

	go func() {
		time.Sleep(10 * time.Minute)
		os.Remove(destPath)
	}()

	jsonOK(w, map[string]string{
		"path":     destPath,
		"filename": filepath.Base(destPath),
	})
}

func (s *Server) handleIRBrowse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonErr(w, 405, "GET only")
		return
	}
	browseType := r.URL.Query().Get("type")
	var out []byte
	var err error

	switch runtime.GOOS {
	case "windows":
		var psScript string
		if browseType == "dir" {
			psScript = `Add-Type -AssemblyName System.Windows.Forms; $f=New-Object System.Windows.Forms.FolderBrowserDialog; $f.ShowDialog()|Out-Null; Write-Output $f.SelectedPath`
		} else {
			psScript = `Add-Type -AssemblyName System.Windows.Forms; $f=New-Object System.Windows.Forms.OpenFileDialog; $f.ShowDialog()|Out-Null; Write-Output $f.FileName`
		}
		out, err = exec.Command("powershell", "-NoProfile", "-Command", psScript).Output()
	case "darwin":
		if browseType == "dir" {
			out, err = exec.Command("osascript", "-e", `choose folder`).Output()
		} else {
			out, err = exec.Command("osascript", "-e", `choose file`).Output()
		}
	default:
		if browseType == "dir" {
			out, err = exec.Command("zenity", "--file-selection", "--directory", "--title=Select Directory").Output()
			if err != nil {
				out, err = exec.Command("kdialog", "--getexistingdirectory", ".").Output()
			}
		} else {
			out, err = exec.Command("zenity", "--file-selection", "--title=Select File").Output()
			if err != nil {
				out, err = exec.Command("kdialog", "--getopenfilename", ".", "*").Output()
			}
		}
	}

	if err != nil {
		jsonErr(w, 501, "file browser not available on this system ("+runtime.GOOS+")")
		return
	}
	jsonOK(w, map[string]string{"path": strings.TrimSpace(string(out))})
}

// handleShipping and handleShippingItem manage the `shipping` table
// (city, cost — a flat per-city delivery cost). This is deliberately kept
// in sync with dashboard.go's handlers of the same name: the wizard and the
// dashboard are separate binaries/servers, each with their own *sql.DB, so
// the CRUD logic has to be duplicated rather than shared.
func (s *Server) handleShipping(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected — save config with a valid database path first")
		return
	}

	switch r.Method {
	case http.MethodGet:
		rows, err := s.db.Query(`SELECT id, city, cost, created_at, updated_at FROM shipping ORDER BY city`)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		defer rows.Close()

		var entries []map[string]interface{}
		for rows.Next() {
			var id int
			var city, createdAt, updatedAt string
			var cost float64
			if err := rows.Scan(&id, &city, &cost, &createdAt, &updatedAt); err != nil {
				continue
			}
			entries = append(entries, map[string]interface{}{
				"id": id, "city": city, "cost": cost,
				"created_at": createdAt, "updated_at": updatedAt,
			})
		}
		if entries == nil {
			entries = []map[string]interface{}{}
		}
		jsonOK(w, entries)

	case http.MethodPost:
		var input struct {
			City string  `json:"city"`
			Cost float64 `json:"cost"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			jsonErr(w, 400, "invalid JSON")
			return
		}
		if input.City == "" || input.Cost < 0 {
			jsonErr(w, 400, "city and valid cost required")
			return
		}
		result, err := s.db.Exec(`INSERT INTO shipping (city, cost) VALUES (?, ?)`, input.City, input.Cost)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint") {
				jsonErr(w, 409, "city already exists")
			} else {
				jsonErr(w, 500, err.Error())
			}
			return
		}
		id, _ := result.LastInsertId()
		jsonOK(w, map[string]interface{}{"id": id, "city": input.City, "cost": input.Cost})

	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func (s *Server) handleShippingItem(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		jsonErr(w, 503, "database not connected")
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/api/shipping/")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		jsonErr(w, 400, "invalid shipping ID")
		return
	}

	switch r.Method {
	case http.MethodPut:
		var input struct {
			City string  `json:"city"`
			Cost float64 `json:"cost"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			jsonErr(w, 400, "invalid JSON")
			return
		}
		if input.City == "" || input.Cost < 0 {
			jsonErr(w, 400, "city and valid cost required")
			return
		}
		result, err := s.db.Exec(`UPDATE shipping SET city = ?, cost = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, input.City, input.Cost, id)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint") {
				jsonErr(w, 409, "city already exists")
			} else {
				jsonErr(w, 500, err.Error())
			}
			return
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			jsonErr(w, 404, "shipping entry not found")
			return
		}
		jsonOK(w, map[string]bool{"ok": true})

	case http.MethodDelete:
		result, err := s.db.Exec("DELETE FROM shipping WHERE id = ?", id)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			jsonErr(w, 404, "shipping entry not found")
			return
		}
		jsonOK(w, map[string]bool{"ok": true})

	default:
		jsonErr(w, 405, "method not allowed")
	}
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
func nullFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}
func nullInt(i int) interface{} {
	if i == 0 {
		return nil
	}
	return i
}