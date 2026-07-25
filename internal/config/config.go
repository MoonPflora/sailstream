package config

type Config struct {
	Meta             MetaConfig                `json:"meta"`
	System           SystemConfig              `json:"system"`
	AI               AIConfig                  `json:"ai"`
	Scheduler        SchedulerConfig           `json:"scheduler"`
	Store            StoreConfig               `json:"store"`
	Platforms        map[string]PlatformConfig `json:"platforms"`
	Posting          PostingConfig             `json:"posting"`
	Paths            PathsConfig               `json:"paths"`
	Content          ContentPool               `json:"content"`
	ScheduledPosts   []ScheduledPost           `json:"scheduled_posts"`
	ImageRecognition ImageRecognition          `json:"image_recognition"`
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
	DateTime string `json:"date_time"` // for "once"
	Interval string `json:"interval"`  // for "recurring": daily, weekly, monthly
	Time     string `json:"time"`
	Days     []int  `json:"days"`
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
	Language      string     `json:"language"`
	OperationMode string     `json:"operation_mode"`
	WakePolicy    WakePolicy `json:"wake_policy"`
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

type ImageRecognition struct {
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
	RateLimits           RateLimits `json:"rate_limits"`
	CheckIntervalMinutes int        `json:"check_interval_minutes"`
}

type QuietHours struct {
	Enabled bool   `json:"enabled"`
	From    string `json:"from"`
	To      string `json:"to"`
}

type RateLimits struct {
	MessagesPerMinute int `json:"messages_per_minute"`
	PostsPerHour      int `json:"posts_per_hour"`
	PostsPerDay       int `json:"posts_per_day"`
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
	Enabled bool `json:"enabled"`

	Platform PlatformType      `json:"platform"`
	Subtypes []PlatformSubtype `json:"subtypes"`

	Instagram *InstagramConfig `json:"instagram,omitempty"`
	Facebook  *FacebookConfig  `json:"facebook,omitempty"`
	Telegram  *TelegramConfig  `json:"telegram,omitempty"`
	TikTok    *TikTokConfig    `json:"tiktok,omitempty"`
	Twitter   *TwitterConfig   `json:"twitter,omitempty"`
	WhatsApp  *WhatsAppConfig  `json:"whatsapp,omitempty"`
	Viber     *ViberConfig     `json:"viber,omitempty"`

	Automation AutomationConfig      `json:"automation"`
	Posting    PlatformPostingConfig `json:"posting"`
	Limits     PlatformLimits        `json:"limits"`
	Metadata   PlatformMetadata      `json:"metadata"`
	Settings   PlatformSettings      `json:"settings"`
	Messages   MessageTemplates      `json:"messages"`
}

type PlatformSubtype struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Enabled    bool                   `json:"enabled"`
	Auth       map[string]interface{} `json:"auth"`
	Metadata   map[string]interface{} `json:"metadata"`
	Limits     PlatformLimits         `json:"limits"`
	Automation AutomationConfig       `json:"automation"`
	Posting    PlatformPostingConfig  `json:"posting"`
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

func (p *PlatformConfig) GetConfig() interface{} {
	switch p.Platform.Type {
	case "instagram":
		return p.Instagram
	case "facebook":
		return p.Facebook
	case "telegram":
		return p.Telegram
	case "tiktok":
		return p.TikTok
	case "twitter":
		return p.Twitter
	case "whatsapp":
		return p.WhatsApp
	case "viber":
		return p.Viber
	default:
		return nil
	}
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
	BotToken      string `json:"bot_token,omitempty"`
	PhoneNumber   string `json:"phone_number,omitempty"`
	Password      string `json:"password,omitempty"`
	WebhookDomain string `json:"webhook_domain"`
	WebhookPort   string `json:"webhook_port"` // was mapstructure:"webhook_port"
	WebhookURL    string `json:"webhook_url"`  // was mapstructure:"webhook_url"
}

type AutomationConfig struct {
	AutoReply      AutoReplyConfig      `json:"auto_reply"`
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

type AutoReplyConfig struct {
	Enabled bool `json:"enabled"`
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
	// IncludeGroups controls whether DM automation also applies inside group
	// chats (as opposed to 1:1 only). Off by default: a group has multiple
	// participants and no guarantee any given message is directed at the
	// bot, so auto-responding there is a materially different risk than a
	// 1:1 DM (e.g. mid-conversation product/pricing replies to the wrong
	// audience, or reacting to messages nobody meant for it).
	IncludeGroups bool `json:"include_groups"`
}

type AnswerCommentsConfig struct {
	Enabled bool `json:"enabled"`
}

type WelcomeMessageConfig struct {
	Enabled bool   `json:"enabled"`
	Text    string `json:"text"`
}

type PlatformPostingConfig struct {
	Random        RandomPostingConfig `json:"random"`
	Manual        ManualPostingConfig `json:"manual"`
	ScheduleTimes []string            `json:"schedule_times"`
}

type RandomPostingConfig struct {
	Enabled       bool          `json:"enabled"`
	IntervalHours IntervalHours `json:"interval_hours"`
	PostsPerCycle int           `json:"posts_per_cycle"`
	UseGlobal     bool          `json:"use_global"`
}

type IntervalHours struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type ManualPostingConfig struct {
	Enabled bool           `json:"enabled"`
	Payload PostingPayload `json:"payload"`
}

type PostingPayload struct {
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Media       PostingMedia `json:"media"`
}

type PostingMedia struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type PlatformLimits struct {
	DailyMessages int `json:"daily_messages"`
	DailyPosts    int `json:"daily_posts"`
	DailyHearts   int `json:"daily_hearts"`
	DailyFollows  int `json:"daily_follows"`
	DailyComments int `json:"daily_comments"`
	HourlyPosts   int `json:"hourly_posts"`
}

type PlatformMetadata struct {
	CreatedAt      string `json:"created_at"`
	LastActive     string `json:"last_active"`
	Notes          string `json:"notes"`
	TotalPosts     int    `json:"total_posts"`
	TotalFollowers int    `json:"total_followers"`
	TotalFollowing int    `json:"total_following"`
}

type PostingConfig struct {
	Fallback              FallbackPosting `json:"fallback"`
	RotationMode          string          `json:"rotation_mode"`
	ScheduledPostsSummary map[string]int  `json:"scheduled_posts_summary"`
}

type FallbackPosting struct {
	Random RandomPostingConfig `json:"random"`
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

func (m *MetaConfig) Validate() error              { return nil }
func (s *SystemConfig) Validate() error            { return nil }
func (a *AIConfig) Validate() error                { return nil }
func (g *GenerationSettings) Validate() error      { return nil }
func (i *ImageRecognition) Validate() error        { return nil }
func (s *SchedulerConfig) Validate() error         { return nil }
func (q *QuietHours) Validate() error              { return nil }
func (r *RateLimits) Validate() error              { return nil }
func (s *StoreConfig) Validate() error             { return nil }
func (c *ContactInfo) Validate() error             { return nil }
func (c *ContentPool) Validate() error             { return nil }
func (p *PostContent) Validate() error             { return nil }
func (m *MediaItem) Validate() error               { return nil }
func (p *PlatformConfig) Validate() error          { return nil }
func (ps *PlatformSubtype) Validate() error        { return nil }
func (ps *PlatformSettings) Validate() error       { return nil }
func (pt *PlatformType) Validate() error           { return nil }
func (p *PathsConfig) Validate() error             { return nil }
func (sp *ScheduledPost) Validate() error          { return nil }
func (sm *ScheduledMedia) Validate() error         { return nil }
func (ps *PostSchedule) Validate() error           { return nil }
func (f *FacebookConfig) Validate() error          { return nil }
func (fa *FacebookAccount) Validate() error        { return nil }
func (fp *FacebookPage) Validate() error           { return nil }
func (i *InstagramConfig) Validate() error         { return nil }
func (ia *InstagramAccount) Validate() error       { return nil }
func (w *WhatsAppConfig) Validate() error          { return nil }
func (t *TelegramConfig) Validate() error          { return nil }
func (tb *TelegramBot) Validate() error            { return nil }
func (ta *TelegramAccount) Validate() error        { return nil }
func (tt *TikTokConfig) Validate() error           { return nil }
func (tw *TwitterConfig) Validate() error          { return nil }
func (v *ViberConfig) Validate() error             { return nil }
func (ac *AutomationConfig) Validate() error       { return nil }
func (mf *MessageFilters) Validate() error         { return nil }
func (ar *AutoReplyConfig) Validate() error        { return nil }
func (ah *AutoHeartConfig) Validate() error        { return nil }
func (af *AutoFollowConfig) Validate() error       { return nil }
func (ar *AutoRepostConfig) Validate() error       { return nil }
func (ad *AnswerDMConfig) Validate() error         { return nil }
func (ac *AnswerCommentsConfig) Validate() error   { return nil }
func (wm *WelcomeMessageConfig) Validate() error   { return nil }
func (pc *PlatformPostingConfig) Validate() error  { return nil }
func (rp *RandomPostingConfig) Validate() error    { return nil }
func (ih *IntervalHours) Validate() error          { return nil }
func (mp *ManualPostingConfig) Validate() error    { return nil }
func (pp *PostingPayload) Validate() error         { return nil }
func (pm *PostingMedia) Validate() error           { return nil }
func (pl *PlatformLimits) Validate() error         { return nil }
func (pm *PlatformMetadata) Validate() error       { return nil }
func (pc *PostingConfig) Validate() error          { return nil }
func (fp *FallbackPosting) Validate() error        { return nil }
func (mt *MessageTemplates) Validate() error       { return nil }
func (kr *KeywordRule) Validate() error            { return nil }
func (spp *ScheduledPostPlatform) Validate() error { return nil }
