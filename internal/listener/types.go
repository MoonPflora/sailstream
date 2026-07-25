package listener

import (
	"time"
)

type NotificationType string

const (
	NotificationTypeComment NotificationType = "comment"
	NotificationTypeMessage NotificationType = "message"
	NotificationTypeLike    NotificationType = "like"
	NotificationTypeMention NotificationType = "mention"
	NotificationTypeFollow  NotificationType = "follow"
	NotificationTypePost    NotificationType = "post"
)

type UserInfo struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	ProfileURL  string `json:"profile_url"`
	AvatarURL   string `json:"avatar_url"`
	IsVerified  bool   `json:"is_verified"`
	IsBusiness  bool   `json:"is_business"`
}

type MediaAttachment struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	URL        string `json:"url"`
	Thumbnail  string `json:"thumbnail"`
	Filename   string `json:"filename"`
	SizeBytes  int64  `json:"size_bytes"`
	Duration   int    `json:"duration,omitempty"`
	Dimensions string `json:"dimensions,omitempty"`
	Caption    string `json:"caption,omitempty"`
}

type CommentData struct {
	PostID        string    `json:"post_id"`
	PostURL       string    `json:"post_url"`
	PostContent   string    `json:"post_content"`
	PostAuthor    UserInfo  `json:"post_author"`
	PostMediaURLs []string  `json:"post_media_urls"`
	PostTimestamp time.Time `json:"post_timestamp"`

	CommentID       string    `json:"comment_id"`
	CommentText     string    `json:"comment_text"`
	CommentAuthor   UserInfo  `json:"comment_author"`
	ParentCommentID *string   `json:"parent_comment_id,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
	LikeCount       int       `json:"like_count"`
	ReplyCount      int       `json:"reply_count"`
	IsEdited        bool      `json:"is_edited"`

	MediaAttached []MediaAttachment `json:"media_attached"`

	UserTags []UserInfo `json:"user_tags"`
	Hashtags []string   `json:"hashtags"`
	Emojis   []string   `json:"emojis"`

	PlatformData map[string]interface{} `json:"platform_data"`
}

type MessageData struct {
	ConversationID   string     `json:"conversation_id"`
	ConversationName string     `json:"conversation_name,omitempty"`
	IsGroup          bool       `json:"is_group"`
	GroupMembers     []UserInfo `json:"group_members,omitempty"`

	MessageID  string     `json:"message_id"`
	Sender     UserInfo   `json:"sender"`
	Recipients []UserInfo `json:"recipients"`
	Text       string     `json:"text"`
	Timestamp  time.Time  `json:"timestamp"`

	IsRead         bool   `json:"is_read"`
	IsEdited       bool   `json:"is_edited"`
	IsForwarded    bool   `json:"is_forwarded"`
	DeliveryStatus string `json:"delivery_status"`

	ReplyTo   *string `json:"reply_to,omitempty"`
	ReplyText string  `json:"reply_text,omitempty"`

	MediaAttached []MediaAttachment `json:"media_attached"`

	PlatformData map[string]interface{} `json:"platform_data"`
}

type Notification struct {
	ID         string `json:"id"`
	PlatformID string `json:"platform_id"`
	SubtypeID  string `json:"subtype_id"`
	AccountID  string `json:"account_id"`

	Type      NotificationType `json:"type"`
	Timestamp time.Time        `json:"timestamp"`
	Urgent    bool             `json:"urgent"`

	RawData map[string]interface{} `json:"raw_data"`

	Comment *CommentData `json:"comment,omitempty"`
	Message *MessageData `json:"message,omitempty"`

	CollectedAt time.Time `json:"collected_at"`
	Processed   bool      `json:"processed"`
	Error       string    `json:"error,omitempty"`
}

type ListenerStatus struct {
	PlatformID string    `json:"platform_id"`
	SubtypeID  string    `json:"subtype_id"`
	IsRunning  bool      `json:"is_running"`
	IsPaused   bool      `json:"is_paused"`
	LastCheck  time.Time `json:"last_check"`
	ErrorCount int       `json:"error_count"`
	LastError  string    `json:"last_error,omitempty"`
	Stats      Stats     `json:"stats"`
}

type Stats struct {
	NotificationsCollected int           `json:"notifications_collected"`
	CommentsCollected      int           `json:"comments_collected"`
	MessagesCollected      int           `json:"messages_collected"`
	ErrorsEncountered      int           `json:"errors_encountered"`
	LastCollectionDuration time.Duration `json:"last_collection_duration"`
}

type ControlCommand struct {
	Command    string                 `json:"command"`
	PlatformID string                 `json:"platform_id,omitempty"`
	SubtypeID  string                 `json:"subtype_id,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

type PlatformError struct {
	PlatformID string    `json:"platform_id"`
	SubtypeID  string    `json:"subtype_id"`
	AccountID  string    `json:"account_id,omitempty"`
	ErrorCode  string    `json:"error_code"`
	ErrorMsg   string    `json:"error_msg"`
	Timestamp  time.Time `json:"timestamp"`
	Severity   string    `json:"severity"`
}

type ListenerConfig struct {
	ListenComments bool `json:"listen_comments"`
	ListenMessages bool `json:"listen_messages"`
	ListenLikes    bool `json:"listen_likes"`
	ListenMentions bool `json:"listen_mentions"`

	// ListenGroupMessages gates automation for group chats (as opposed to 1:1
	// DMs). Separate from ListenComments, which gates post-comment collection
	// on platforms like Facebook — group chats and post comments are
	// different things and shouldn't share a flag.
	ListenGroupMessages bool `json:"listen_group_messages"`

	PollingInterval int  `json:"polling_interval"`
	MaxHistory      int  `json:"max_history"`
	SaveRawData     bool `json:"save_raw_data"`

	IgnoreKeywords []string `json:"ignore_keywords"`
	UrgentKeywords []string `json:"urgent_keywords"`

	PageIDs    []string `json:"page_ids,omitempty"`
	GroupIDs   []string `json:"group_ids,omitempty"`
	ChannelIDs []string `json:"channel_ids,omitempty"`
	ChatIDs    []string `json:"chat_ids,omitempty"`
	Hashtags   []string `json:"hashtags,omitempty"`
}

type CookieData struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Domain   string `json:"domain"`
	Path     string `json:"path"`
	Secure   bool   `json:"secure"`
	HTTPOnly bool   `json:"http_only"`
	Expires  int64  `json:"expires,omitempty"`
	SameSite string `json:"same_site,omitempty"`
}

type ProductData struct {
	ID  string `json:"id"`
	SKU string `json:"sku"`

	Name        string  `json:"name"`
	Description *string `json:"description"`
	Category    *string `json:"category"`
	Subcategory *string `json:"subcategory"`
	Tags        *string `json:"tags"`

	Price           float64  `json:"price"`
	PricePerPack    *float64 `json:"price_per_pack"`
	QuantityPerPack *int     `json:"quantity_per_pack"`
	Currency        string   `json:"currency"`

	Stock             int `json:"stock"`
	ReservedStock     int `json:"reserved_stock"`
	LowStockThreshold int `json:"low_stock_threshold"`

	ImageURL     *string `json:"image_url"`
	ThumbnailURL *string `json:"thumbnail_url"`

	WeightKG   *float64 `json:"weight_kg"`
	Dimensions *string  `json:"dimensions"`
	IsActive   bool     `json:"is_active"`
	IsFeatured bool     `json:"is_featured"`

	Metadata *string `json:"metadata"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type UserHistoryData struct {
	UserID            string                 `json:"user_id"`
	PlatformUserID    string                 `json:"platform_user_id"`
	Platform          string                 `json:"platform"`
	Username          string                 `json:"username"`
	DisplayName       string                 `json:"display_name"`
	ProfileURL        string                 `json:"profile_url"`
	AvatarURL         string                 `json:"avatar_url"`
	ConversationState string                 `json:"conversation_state"`
	TotalMessages     int                    `json:"total_messages"`
	TotalOrders       int                    `json:"total_orders"`
	TotalSpent        float64                `json:"total_spent"`
	LastActive        time.Time              `json:"last_active"`
	FirstSeen         time.Time              `json:"first_seen"`
	IsBanned          bool                   `json:"is_banned"`
	BanReason         string                 `json:"ban_reason,omitempty"`
	Metadata          map[string]interface{} `json:"metadata"`
}

type ConversationContextData struct {
	ID                string                 `json:"id"`
	UserID            string                 `json:"user_id"`
	ContextJSON       map[string]interface{} `json:"context_json"`
	IntentHistory     []string               `json:"intent_history"`
	LastProductViewed string                 `json:"last_product_viewed"`
	LastSearchResults []string               `json:"last_search_results"`
	SearchQuery       string                 `json:"search_query"`
	CartItems         []string               `json:"cart_items"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
	LastAccessed      time.Time              `json:"last_accessed"`
}

type ProductMention struct {
	ProductID   string       `json:"product_id"`
	SKU         string       `json:"sku"`
	ProductName string       `json:"product_name"`
	Price       float64      `json:"price"`
	FoundIn     string       `json:"found_in"`
	Context     string       `json:"context"`
	Source      string       `json:"source"`
	ProductData *ProductData `json:"product_data,omitempty"`
	Timestamp   time.Time    `json:"timestamp"`
}

type CollectionContext struct {
	SessionID  string       `json:"session_id"`
	Platform   string       `json:"platform"`
	AccountID  string       `json:"account_id"`
	StartedAt  time.Time    `json:"started_at"`
	Cookies    []CookieData `json:"cookies,omitempty"`
	UserAgent  string       `json:"user_agent"`
	Proxy      string       `json:"proxy,omitempty"`
	IsHeadless bool         `json:"is_headless"`
	Timeout    int          `json:"timeout"`
}

type SessionResponse struct {
	Success   bool         `json:"success"`
	SessionID string       `json:"session_id,omitempty"`
	Cookies   []CookieData `json:"cookies,omitempty"`
	UserAgent string       `json:"user_agent,omitempty"`
	Error     string       `json:"error,omitempty"`
	ExpiresAt time.Time    `json:"expires_at,omitempty"`
}

type pauseSignal struct {
	pause   bool
	respond chan error
}
