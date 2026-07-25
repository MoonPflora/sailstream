package listener

// =============================================================================
// CONCEPTUAL SKETCH — twitter.go rebuilt on github.com/imperatrona/twitter-scraper
// =============================================================================
//
// This replaces the chromedp/browser-driven twitter.go with a design that
// matches the architecture already used by telegram.go / viber.go / whatsapp.go:
// a real client library, cookie/credential-based auth, a dedupe+cursor model,
// an instruction queue decoupled from collection, and the same Notification/
// CommentData/MessageData shapes the rest of the system expects.
//
// IMPORTANT — what's confirmed vs. what needs verification before shipping:
//
//   CONFIRMED against the library's public godoc (pkg.go.dev/github.com/
//   imperatrona/twitter-scraper) at research time:
//     - Read:  GetTweets, FetchTweets, FetchTweetsByUserID, GetTweetReplies,
//              GetTweetRetweeters, FetchSearchTweets, FetchSearchProfiles,
//              FetchFollowers, FetchFollowing, FetchBookmarks, GetTrends,
//              GetProfile, SetCookies/SetAuthToken/IsLoggedIn.
//     - Write: CreateTweet, CreateRetweet, DeleteTweet, DeleteRetweet,
//              UploadMedia, CreateScheduledTweet/DeleteScheduledTweet.
//
//   NOT CONFIRMED — I could not verify these exist in the library from docs:
//     - Like / Unlike, Follow / Unfollow, SendDirectMessage / GetDirectMessages,
//              Bookmark-create (FetchBookmarks is read-only).
//     Steps that map to these are written as explicit stubs that log and
//     return a clear "not implemented" error — the same honest pattern
//     whatsapp.go already uses for steps that don't apply to a platform —
//     rather than guessing at method names that might not compile or might
//     silently do the wrong thing. Check
//     https://pkg.go.dev/github.com/imperatrona/twitter-scraper before
//     wiring these up for real, and replace the stub bodies with real calls.
//
//   Also note: `authStr()` is already defined once, package-wide, in
//   telegram.go — it is NOT redefined here. Same package, same helper.
//
// =============================================================================

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	twitterscraper "github.com/imperatrona/twitter-scraper"

	"sailstream/internal/config"
	"sailstream/internal/enviroment"
	sessionmgr "sailstream/internal/session"
	"sailstream/internal/shared"
)

var debugTW = os.Getenv("TW_DEBUG") == "1"

func twDbg(accountID, format string, args ...interface{}) {
	if !debugTW {
		return
	}
	log.Printf("[TW:DBG:%s] "+format, append([]interface{}{accountID}, args...)...)
}

const (
	twNotifBufferSize     = 500
	twDedupeWindow        = 24 * time.Hour
	twCollectDrainWindow  = 100 * time.Millisecond
	twMaxBatchSize        = 200
	twPauseAckTimeout     = 15 * time.Second
	twConnectTimeout      = 20 * time.Second
	twMentionSearchCount  = 50
	twOwnTweetsForReplies = 10 // how many of the account's recent tweets to check for new replies
)

// =============================================================================
// Struct + constructor
// =============================================================================

type TwitterCollector struct {
	platformID string
	subtypeID  string
	accountID  string
	subtype    string

	config     *ListenerConfig
	db         *sql.DB
	configMgr  *config.ConfigManager
	envMgr     *enviroment.Environment
	sessionMgr *sessionmgr.Manager

	// scraper is the imperatrona/twitter-scraper client. Unlike gogram/MTProto
	// or a webhook listener, this is a stateless HTTP+cookie client — there is
	// no persistent connection to hold open and no event push, so there is no
	// "Idle()" goroutine and no registerHandlers() equivalent. All collection
	// happens by polling inside Collect().
	scraper   *twitterscraper.Scraper
	clientMu  sync.Mutex
	connected atomic.Bool

	notifBuffer      chan *Notification
	instructionQueue chan *shared.AutomationInstruction
	errorChan        chan *PlatformError

	pollRunning atomic.Bool

	drainPaused atomic.Bool
	// pauseCount is a reference count, not a bool — see fix N4 in whatsapp.go.
	pauseCount atomic.Int32
	pauseAck   chan struct{}
	resumeMu   sync.Mutex
	resumeReq  chan struct{}

	executionMu sync.Mutex

	seenMu     sync.Mutex
	seenTweets map[string]time.Time
	seenFile   string

	// pendingCursorID tracks the highest tweet ID (as a decimal string,
	// compared numerically — Twitter snowflake IDs sort chronologically)
	// seen during the current poll cycle. Flushed to global_cursors at the
	// end of Collect, same batch-safe stage/flush pattern as telegram.go.
	pendingCursorMu sync.Mutex
	pendingCursorID string

	// dmUnsupportedWarned avoids re-logging the "DMs not confirmed" notice
	// on every single poll cycle.
	dmUnsupportedWarned atomic.Bool

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func NewTwitterCollector(
	platformID, subtypeID, accountID, subtype string,
	listenerConfig *ListenerConfig,
	db *sql.DB,
	configMgr *config.ConfigManager,
	envMgr *enviroment.Environment,
	sessionMgr *sessionmgr.Manager,
) *TwitterCollector {
	log.Printf("[TW:INIT] NewTwitterCollector platformID=%s subtypeID=%s accountID=%s subtype=%s",
		platformID, subtypeID, accountID, subtype)
	log.Printf("[TW:INIT] Transport: imperatrona/twitter-scraper (X internal GraphQL API via cookies). No browser, no developer API key.")

	tc := &TwitterCollector{
		platformID:       platformID,
		subtypeID:        subtypeID,
		accountID:        accountID,
		subtype:          subtype,
		config:           listenerConfig,
		db:               db,
		configMgr:        configMgr,
		envMgr:           envMgr,
		sessionMgr:       sessionMgr,
		notifBuffer:      make(chan *Notification, twNotifBufferSize),
		instructionQueue: make(chan *shared.AutomationInstruction, 100),
		errorChan:        make(chan *PlatformError, 50),
		pauseAck:         make(chan struct{}, 1),
		resumeReq:        make(chan struct{}),
		seenTweets:       make(map[string]time.Time),
		shutdown:         make(chan struct{}),
	}

	if err := os.MkdirAll("./cache/sessions/twitter", 0750); err == nil {
		tc.seenFile = filepath.Join("./cache/sessions/twitter", fmt.Sprintf("seen_%s.json", accountID))
		tc.loadSeenTweets()
	}

	return tc
}

// =============================================================================
// Dedupe (same disk-backed sliding-TTL-map pattern as telegram.go)
// =============================================================================

func (tc *TwitterCollector) loadSeenTweets() {
	if tc.seenFile == "" {
		return
	}
	data, err := os.ReadFile(tc.seenFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[TW:SEEN:%s] failed to read dedupe file: %v", tc.accountID, err)
		}
		return
	}
	var m map[string]time.Time
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("[TW:SEEN:%s] failed to parse dedupe file: %v", tc.accountID, err)
		return
	}
	tc.seenMu.Lock()
	defer tc.seenMu.Unlock()
	tc.seenTweets = m
	log.Printf("[TW:SEEN:%s] loaded %d dedupe entries from disk", tc.accountID, len(m))
}

func (tc *TwitterCollector) saveSeenTweets() {
	if tc.seenFile == "" {
		return
	}
	tc.seenMu.Lock()
	now := time.Now()
	for k, exp := range tc.seenTweets {
		if now.After(exp) {
			delete(tc.seenTweets, k)
		}
	}
	mCopy := make(map[string]time.Time, len(tc.seenTweets))
	for k, v := range tc.seenTweets {
		mCopy[k] = v
	}
	tc.seenMu.Unlock()

	data, err := json.Marshal(mCopy)
	if err != nil {
		log.Printf("[TW:SEEN:%s] failed to marshal dedupe: %v", tc.accountID, err)
		return
	}
	if err := os.WriteFile(tc.seenFile, data, 0600); err != nil {
		log.Printf("[TW:SEEN:%s] failed to save dedupe: %v", tc.accountID, err)
	} else {
		twDbg(tc.accountID, "saved %d dedupe entries", len(mCopy))
	}
}

func (tc *TwitterCollector) checkAndMarkSeen(tweetID string) bool {
	tc.seenMu.Lock()
	defer tc.seenMu.Unlock()
	now := time.Now()
	for k, exp := range tc.seenTweets {
		if now.After(exp) {
			delete(tc.seenTweets, k)
		}
	}
	if _, seen := tc.seenTweets[tweetID]; seen {
		return false
	}
	tc.seenTweets[tweetID] = now.Add(twDedupeWindow)
	go tc.saveSeenTweets()
	return true
}

// =============================================================================
// Cursor (global_cursors row, cursor_type='tweet_id' instead of 'timestamp' —
// Twitter snowflake IDs are monotonically increasing and a more reliable
// pagination boundary than a wall-clock timestamp for search-based polling)
// =============================================================================

func (tc *TwitterCollector) getCursorSubtypeID() string { return "global" }
func (tc *TwitterCollector) getCursorType() string      { return "tweet_id" }

func (tc *TwitterCollector) getLastSeenTweetID() (string, error) {
	var cursorValue string
	err := tc.db.QueryRow(`
		SELECT cursor_value FROM global_cursors
		WHERE platform = 'twitter' AND subtype = ? AND account_id = ? AND subtype_id = ? AND cursor_type = ?
	`, tc.subtype, tc.accountID, tc.getCursorSubtypeID(), tc.getCursorType()).Scan(&cursorValue)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return cursorValue, nil
}

func (tc *TwitterCollector) updateLastSeenTweetID(id string) error {
	_, err := tc.db.Exec(`
		INSERT INTO global_cursors (platform, subtype, account_id, subtype_id, cursor_type, cursor_value, updated_at)
		VALUES ('twitter', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(platform, subtype, account_id, subtype_id, cursor_type) DO UPDATE SET
			cursor_value = excluded.cursor_value,
			updated_at = CURRENT_TIMESTAMP
	`, tc.subtype, tc.accountID, tc.getCursorSubtypeID(), tc.getCursorType(), id)
	return err
}

// tweetIDGreater compares two decimal snowflake-ID strings numerically.
func tweetIDGreater(a, b string) bool {
	if b == "" {
		return a != ""
	}
	if len(a) != len(b) {
		return len(a) > len(b)
	}
	return a > b
}

func (tc *TwitterCollector) stageCursorUpdate(tweetID string) {
	if tweetID == "" {
		return
	}
	tc.pendingCursorMu.Lock()
	defer tc.pendingCursorMu.Unlock()
	if tweetIDGreater(tweetID, tc.pendingCursorID) {
		tc.pendingCursorID = tweetID
	}
}

func (tc *TwitterCollector) flushPendingCursor() {
	tc.pendingCursorMu.Lock()
	id := tc.pendingCursorID
	tc.pendingCursorID = ""
	tc.pendingCursorMu.Unlock()

	if id == "" {
		return
	}
	if err := tc.updateLastSeenTweetID(id); err != nil {
		log.Printf("[TW:CURSOR:%s] failed to flush cursor: %v", tc.accountID, err)
	} else {
		twDbg(tc.accountID, "cursor flushed → tweet_id=%s", id)
	}
}

// =============================================================================
// Pause/resume (identical pattern to telegram.go — lets executeInstruction
// pause polling while a write action is in flight, then resume cleanly)
// =============================================================================

func (tc *TwitterCollector) pauseCollection() error {
	if tc.pauseCount.Add(1) > 1 {
		return nil
	}
	tc.drainPaused.Store(true)
	log.Printf("[TW:PAUSE:%s] pause requested (drainPaused=true)", tc.accountID)

	if !tc.pollRunning.Load() {
		log.Printf("[TW:PAUSE:%s] poll not running, skipping wait", tc.accountID)
		return nil
	}
	select {
	case <-tc.pauseAck:
		log.Printf("[TW:PAUSE:%s] ✓ pause ack received", tc.accountID)
		return nil
	case <-time.After(twPauseAckTimeout):
		log.Printf("[TW:PAUSE:%s] pause ack timeout (drainPaused remains set)", tc.accountID)
		return nil
	}
}

func (tc *TwitterCollector) resumeCollection() {
	if tc.pauseCount.Add(-1) > 0 {
		return
	}
	newResume := make(chan struct{})
	tc.resumeMu.Lock()
	old := tc.resumeReq
	tc.resumeReq = newResume
	tc.resumeMu.Unlock()
	tc.drainPaused.Store(false)
	close(old)
	log.Printf("[TW:RESUME:%s] ✓ collection resumed", tc.accountID)
}

func (tc *TwitterCollector) checkPause(ctx context.Context) bool {
	if !tc.drainPaused.Load() {
		return true
	}
	log.Printf("[TW:PAUSE:%s] paused, sending ack and waiting for resume", tc.accountID)
	select {
	case tc.pauseAck <- struct{}{}:
	default:
	}
	tc.resumeMu.Lock()
	resumeCh := tc.resumeReq
	tc.resumeMu.Unlock()
	select {
	case <-resumeCh:
		return true
	case <-ctx.Done():
		return false
	}
}

// =============================================================================
// Auth / session bootstrap
//
// Three auth modes, tried in this order, mirroring how telegram.go falls back
// from a saved session to interactive auth via sessionMgr:
//   1. Saved cookie jar from a previous run (./cache/sessions/twitter/*.json,
//      or sessionMgr's storage dir if configured).
//   2. auth_token + ct0 supplied directly in subtype config/env — note from
//      research: SetAuthToken only supports a subset of cookies, so POST
//      (write) endpoints are NOT guaranteed to work via this path. Prefer #1
//      or #3 if you need write access.
//   3. username + password (+ email if 2FA/confirmation is enabled) via
//      scraper.Login — the same approach twikit/twscrape/Scweet all use.
// =============================================================================

func (tc *TwitterCollector) getSessionPath() string {
	if tc.sessionMgr != nil {
		if store := tc.sessionMgr.GetStorage(); store != nil {
			dir := filepath.Join(store.GetSessionsDir(), "twitter")
			return filepath.Join(dir, fmt.Sprintf("tw_%s_%s.cookies.json", tc.subtypeID, tc.accountID))
		}
	}
	return filepath.Join("./cache/sessions/twitter", fmt.Sprintf("tw_%s_%s.cookies.json", tc.subtypeID, tc.accountID))
}

// twitterAuth pulls credentials from subtype config (auth map), falling back
// to environment variables. Reuses authStr(), already defined in telegram.go.
func (tc *TwitterCollector) twitterAuth() (authToken, ct0, username, password, email string) {
	if tc.configMgr != nil {
		if cfg := tc.configMgr.GetConfig(); cfg != nil {
			if p, ok := cfg.Platforms[tc.platformID]; ok {
				for i := range p.Subtypes {
					sub := &p.Subtypes[i]
					if tc.subtypeID != "" && sub.ID == tc.subtypeID {
						authToken = authStr(sub.Auth, "auth_token")
						ct0 = authStr(sub.Auth, "ct0")
						username = authStr(sub.Auth, "username")
						password = authStr(sub.Auth, "password")
						email = authStr(sub.Auth, "email")
						break
					}
				}
			}
		}
	}
	if authToken == "" {
		authToken = os.Getenv("TWITTER_AUTH_TOKEN")
	}
	if ct0 == "" {
		ct0 = os.Getenv("TWITTER_CT0")
	}
	if username == "" {
		username = os.Getenv("TWITTER_USERNAME")
	}
	if password == "" {
		password = os.Getenv("TWITTER_PASSWORD")
	}
	if email == "" {
		email = os.Getenv("TWITTER_EMAIL")
	}
	return
}

func (tc *TwitterCollector) saveCookies(scraper *twitterscraper.Scraper) {
	cookies := scraper.GetCookies()
	data, err := json.Marshal(cookies)
	if err != nil {
		log.Printf("[TW:SESSION:%s] failed to marshal cookies: %v", tc.accountID, err)
		return
	}
	path := tc.getSessionPath()
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		log.Printf("[TW:SESSION:%s] failed to create session dir: %v", tc.accountID, err)
		return
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		log.Printf("[TW:SESSION:%s] failed to save cookies: %v", tc.accountID, err)
	} else {
		twDbg(tc.accountID, "cookies saved to %s", path)
	}
}

func (tc *TwitterCollector) loadCookies(scraper *twitterscraper.Scraper) bool {
	path := tc.getSessionPath()
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return false
	}
	var cookies []*http.Cookie
	if err := json.Unmarshal(data, &cookies); err != nil {
		log.Printf("[TW:SESSION:%s] failed to parse saved cookies: %v", tc.accountID, err)
		return false
	}
	scraper.SetCookies(cookies)
	return scraper.IsLoggedIn()
}

func (tc *TwitterCollector) ensureClient(ctx context.Context) error {
	tc.clientMu.Lock()
	defer tc.clientMu.Unlock()

	if tc.scraper != nil && tc.connected.Load() {
		return nil
	}
	log.Printf("[TW:CONNECT:%s] initialising twitter-scraper client (subtype=%s)", tc.accountID, tc.subtype)

	scraper := twitterscraper.New()

	connectCtx, cancel := context.WithTimeout(ctx, twConnectTimeout)
	defer cancel()
	_ = connectCtx // the underlying library doesn't take a context per-call; timeout
	// is enforced at the http.Client level inside the library's own transport.

	// 1) Try a previously saved cookie jar.
	if tc.loadCookies(scraper) {
		log.Printf("[TW:CONNECT:%s] ✓ authenticated from saved cookies", tc.accountID)
		tc.scraper = scraper
		tc.connected.Store(true)
		return nil
	}

	authToken, ct0, username, password, email := tc.twitterAuth()

	// 2) Try auth_token/ct0 pair directly, if configured.
	if authToken != "" && ct0 != "" {
		scraper.SetAuthToken(twitterscraper.AuthToken{Token: authToken, CSRFToken: ct0})
		if scraper.IsLoggedIn() {
			log.Printf("[TW:CONNECT:%s] ✓ authenticated via auth_token/ct0 (NOTE: write/POST endpoints "+
				"may not work via this path — see SetAuthToken limitation in package docs)", tc.accountID)
			tc.scraper = scraper
			tc.connected.Store(true)
			tc.saveCookies(scraper)
			return nil
		}
		log.Printf("[TW:CONNECT:%s] auth_token/ct0 provided but IsLoggedIn() returned false", tc.accountID)
	}

	// 3) Fall back to username/password login, same approach twikit/Scweet use.
	if username != "" && password != "" {
		log.Printf("[TW:CONNECT:%s] no valid cookies/token, attempting username/password login", tc.accountID)
		var err error
		if email != "" {
			err = scraper.Login(username, password, email)
		} else {
			err = scraper.Login(username, password)
		}
		if err != nil {
			tc.reportError("LOGIN_FAILED", err.Error(), "critical")
			return fmt.Errorf("twitter-scraper login: %w", err)
		}
		if !scraper.IsLoggedIn() {
			return fmt.Errorf("login call succeeded but IsLoggedIn() is false — account may need manual verification")
		}
		log.Printf("[TW:CONNECT:%s] ✓ authenticated via username/password login", tc.accountID)
		tc.scraper = scraper
		tc.connected.Store(true)
		tc.saveCookies(scraper)
		return nil
	}

	return fmt.Errorf("no usable credentials for %s: need saved cookies, auth_token+ct0, or username+password " +
		"(set via subtype auth config or TWITTER_* env vars)")
}

func (tc *TwitterCollector) safeScraper() *twitterscraper.Scraper {
	tc.clientMu.Lock()
	defer tc.clientMu.Unlock()
	return tc.scraper
}

// =============================================================================
// Enrichment helpers — identical logic/shape to the other 5 platforms so
// downstream consumers (orchestrator, AI reply pipeline) see the same fields
// regardless of source platform.
// =============================================================================

func (tc *TwitterCollector) generateUserID(raw string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(raw))))
	return "tw_" + hex.EncodeToString(h[:8])
}

var twProductPatterns = []*regexp.Regexp{
	regexp.MustCompile(`SKU\s*=\s*["']([^"']+)["']`),
	regexp.MustCompile(`SKU\s*=\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)product\s*id\s*:\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)sku\s*:\s*([^\s<>"']+)`),
}

func (tc *TwitterCollector) extractProductID(text string) string {
	for _, p := range twProductPatterns {
		if m := p.FindStringSubmatch(text); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

// getProductData: same exact-match-first, guarded-fuzzy-fallback strategy
// already applied to facebook.go/instagram.go/telegram.go/viber.go/whatsapp.go
// after the earlier subcategory Scan-mismatch bug fix. Kept identical here
// for consistency rather than reinventing a sixth variant.
func (tc *TwitterCollector) getProductData(productID string) (map[string]interface{}, error) {
	if tc.db == nil || productID == "" {
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
		imageURL, thumbURL, dimensions         sql.NullString
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
	err := scanInto(tc.db.QueryRow(exactQ, productID))

	// Fall back to a fuzzy name match only when nothing exact was found and
	// the identifier is specific enough (>=4 chars) to keep the scan rare
	// and avoid matching unrelated products on short/common substrings.
	if err == sql.ErrNoRows && len(productID) >= 4 {
		err = scanInto(tc.db.QueryRow(fuzzyQ, "%"+productID+"%"))
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
		json.Unmarshal([]byte(s.String), &v)
		return v
	}
	return map[string]interface{}{
		"id": id.String, "sku": sku.String, "name": name.String,
		"description": desc.String, "category": cat.String, "subcategory": subcat.String,
		"tags": unmarshal(tags), "price": price.Float64, "price_per_pack": pricePerPack.Float64,
		"quantity_per_pack": qtyPerPack.Int64, "currency": currency.String,
		"stock": stock.Int64, "reserved_stock": reservedStock.Int64, "low_stock_threshold": lowStockThr.Int64,
		"image_url": imageURL.String, "thumbnail_url": thumbURL.String,
		"weight_kg": weightKg.Float64, "dimensions": dimensions.String,
		"is_active": isActive.Bool, "is_featured": isFeatured.Bool,
		"metadata": unmarshal(metadata), "created_at": createdAt.Time, "updated_at": updatedAt.Time,
	}, nil
}

func (tc *TwitterCollector) getUserInfoForNotification(
	ctx context.Context,
	platformUserID string,
) (userData map[string]interface{}, recentMessages []string, isNew bool, err error) {
	if tc.db == nil {
		return nil, nil, true, nil
	}
	const q = `
		SELECT id, display_name, is_blocked, last_intent
		  FROM platform_users
		 WHERE platform = ? AND platform_user_id = ?
	`
	var id, displayName, lastIntent sql.NullString
	var blocked sql.NullBool
	err = tc.db.QueryRowContext(ctx, q, "twitter", platformUserID).Scan(
		&id, &displayName, &blocked, &lastIntent,
	)
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
	if id.Valid && id.String != "" {
		recentMessages, _ = tc.getRecentMessages(ctx, id.String, 3)
	}
	return userData, recentMessages, false, nil
}

func (tc *TwitterCollector) getRecentMessages(ctx context.Context, userID string, limit int) ([]string, error) {
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
	rows, err := tc.db.QueryContext(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var texts []string
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err == nil && text != "" {
			texts = append(texts, text)
		}
	}
	return texts, rows.Err()
}

// =============================================================================
// pushNotification / reportError — identical pattern to every other platform
// =============================================================================

func (tc *TwitterCollector) pushNotification(n *Notification) bool {
	select {
	case tc.notifBuffer <- n:
		twDbg(tc.accountID, "notification pushed (buf=%d/%d)", len(tc.notifBuffer), twNotifBufferSize)
		return true
	default:
		log.Printf("[TW:WARN:%s] notification buffer full (%d), notification dropped", tc.accountID, twNotifBufferSize)
		tc.reportError("BUFFER_FULL", "notification buffer full, notification dropped", "warning")
		return false
	}
}

func (tc *TwitterCollector) reportError(code, msg, severity string) {
	log.Printf("[TW:ERROR:%s] code=%s msg=%q severity=%s", tc.accountID, code, msg, severity)
	select {
	case tc.errorChan <- &PlatformError{
		PlatformID: tc.platformID,
		SubtypeID:  tc.subtypeID,
		AccountID:  tc.accountID,
		ErrorCode:  code,
		ErrorMsg:   msg,
		Timestamp:  time.Now(),
		Severity:   severity,
	}:
	default:
		log.Printf("[TW:ERROR:%s] error chan full, dropped: code=%s msg=%s", tc.accountID, code, msg)
	}
}

func (tc *TwitterCollector) GetErrorChannel() <-chan *PlatformError {
	return tc.errorChan
}

// =============================================================================
// Tweet → Notification mapping
// =============================================================================

// processMentionTweet maps a tweet that @mentions this account into a
// NotificationTypeMention notification. Field names (Hashtags, Mentions,
// TimeParsed, Username, Name, UserID, Likes, Replies) are per the library's
// Tweet struct — double-check the exact casing/shape against your vendored
// version before relying on this verbatim.
func (tc *TwitterCollector) processMentionTweet(ctx context.Context, tw *twitterscraper.Tweet) (*Notification, error) {
	if tw == nil || tw.ID == "" {
		return nil, nil
	}
	if !tc.checkAndMarkSeen("mention_" + tw.ID) {
		twDbg(tc.accountID, "duplicate mention id=%s, skipping", tw.ID)
		return nil, nil
	}

	log.Printf("[TW:PROCESS:%s] mention id=%s from=@%s text_len=%d",
		tc.accountID, tw.ID, tw.Username, len(tw.Text))

	productID := tc.extractProductID(tw.Text)
	var productData map[string]interface{}
	if productID != "" {
		log.Printf("[TW:PROCESS:%s] product ID detected: %s", tc.accountID, productID)
		if data, err := tc.getProductData(productID); err == nil {
			productData = data
		} else {
			log.Printf("[TW:PROCESS:%s] product lookup %q failed: %v", tc.accountID, productID, err)
		}
	}

	platformUserID := tw.UserID
	if platformUserID == "" {
		platformUserID = tw.Username
	}
	userData, recentMsgs, isNewUser, err := tc.getUserInfoForNotification(ctx, platformUserID)
	if err != nil {
		log.Printf("[TW:USER:%s] user lookup error for %s: %v – dropping mention for safety",
			tc.accountID, platformUserID, err)
		return nil, nil
	}
	if !isNewUser && userData != nil {
		if blocked, ok := userData["is_blocked"].(bool); ok && blocked {
			log.Printf("[TW:BLOCKED:%s] user %s is blocked, dropping mention", tc.accountID, platformUserID)
			return nil, nil
		}
	}

	raw := map[string]interface{}{
		"tweet_id":     tw.ID,
		"permalink":    tw.PermanentURL,
		"hashtags":     tw.Hashtags,
		"platform":     "twitter",
		"subtype":      tc.subtype,
		"collected_at": time.Now().Format(time.RFC3339),
	}
	if productData != nil {
		raw["product_id"] = productID
		raw["product_data"] = productData
	}
	if isNewUser {
		raw["is_new_user"] = true
	} else {
		raw["user_data"] = userData
		if len(recentMsgs) > 0 {
			raw["recent_messages"] = recentMsgs
		}
	}

	notif := &Notification{
		ID:         fmt.Sprintf("tw_mention_%s_%s_%d", tc.accountID, tw.ID, time.Now().UnixNano()),
		PlatformID: tc.platformID,
		SubtypeID:  tc.subtypeID,
		AccountID:  tc.accountID,
		Type:       NotificationTypeMention,
		Timestamp:  tw.TimeParsed,
		Comment: &CommentData{
			PostID:      tw.ID,
			PostURL:     tw.PermanentURL,
			PostContent: tw.Text,
			CommentID:   tw.ID,
			CommentText: tw.Text,
			CommentAuthor: UserInfo{
				UserID:      tc.generateUserID(platformUserID),
				Username:    tw.Username,
				DisplayName: tw.Name,
				ProfileURL:  "https://x.com/" + tw.Username,
			},
			Timestamp:  tw.TimeParsed,
			LikeCount:  tw.Likes,
			ReplyCount: tw.Replies,
			Hashtags:   tw.Hashtags,
		},
		RawData:     raw,
		CollectedAt: time.Now(),
	}
	return notif, nil
}

// processReplyTweet maps a reply to one of *our own* tweets into a
// NotificationTypeComment notification — structurally the same idea as a
// Facebook/Instagram comment on a post: someone responding to content we
// published, as opposed to a mention buried in someone else's tweet.
func (tc *TwitterCollector) processReplyTweet(ctx context.Context, parentTweetID string, tw *twitterscraper.Tweet) (*Notification, error) {
	if tw == nil || tw.ID == "" {
		return nil, nil
	}
	if !tc.checkAndMarkSeen("reply_" + tw.ID) {
		return nil, nil
	}
	log.Printf("[TW:PROCESS:%s] reply id=%s to=%s from=@%s", tc.accountID, tw.ID, parentTweetID, tw.Username)

	platformUserID := tw.UserID
	if platformUserID == "" {
		platformUserID = tw.Username
	}
	userData, recentMsgs, isNewUser, err := tc.getUserInfoForNotification(ctx, platformUserID)
	if err != nil {
		log.Printf("[TW:USER:%s] user lookup error for %s: %v – dropping reply for safety",
			tc.accountID, platformUserID, err)
		return nil, nil
	}
	if !isNewUser && userData != nil {
		if blocked, ok := userData["is_blocked"].(bool); ok && blocked {
			return nil, nil
		}
	}

	raw := map[string]interface{}{
		"tweet_id":        tw.ID,
		"parent_tweet_id": parentTweetID,
		"permalink":       tw.PermanentURL,
		"hashtags":        tw.Hashtags,
		"platform":        "twitter",
		"subtype":         tc.subtype,
		"collected_at":    time.Now().Format(time.RFC3339),
	}
	if isNewUser {
		raw["is_new_user"] = true
	} else {
		raw["user_data"] = userData
		if len(recentMsgs) > 0 {
			raw["recent_messages"] = recentMsgs
		}
	}

	notif := &Notification{
		ID:         fmt.Sprintf("tw_reply_%s_%s_%d", tc.accountID, tw.ID, time.Now().UnixNano()),
		PlatformID: tc.platformID,
		SubtypeID:  tc.subtypeID,
		AccountID:  tc.accountID,
		Type:       NotificationTypeComment,
		Timestamp:  tw.TimeParsed,
		Comment: &CommentData{
			PostID:      parentTweetID,
			CommentID:   tw.ID,
			CommentText: tw.Text,
			CommentAuthor: UserInfo{
				UserID:      tc.generateUserID(platformUserID),
				Username:    tw.Username,
				DisplayName: tw.Name,
				ProfileURL:  "https://x.com/" + tw.Username,
			},
			Timestamp:  tw.TimeParsed,
			LikeCount:  tw.Likes,
			ReplyCount: tw.Replies,
			Hashtags:   tw.Hashtags,
		},
		RawData:     raw,
		CollectedAt: time.Now(),
	}
	return notif, nil
}

// =============================================================================
// Polling — replaces telegram's registerHandlers()+fetchMissedMessages() pair.
// There is no event push in this library, so Collect() itself drives both
// "what's new" discovery and the buffer drain in one pass.
// =============================================================================

func (tc *TwitterCollector) pollMentions(ctx context.Context, scraper *twitterscraper.Scraper, sinceID string) (int, error) {
	query := "@" + tc.accountUsername()
	if query == "@" {
		return 0, fmt.Errorf("pollMentions: account username not configured")
	}
	tweets, _, err := scraper.FetchSearchTweets(query, twMentionSearchCount, "")
	if err != nil {
		return 0, fmt.Errorf("FetchSearchTweets: %w", err)
	}
	count := 0
	for _, tw := range tweets {
		if sinceID != "" && !tweetIDGreater(tw.ID, sinceID) {
			continue
		}
		notif, err := tc.processMentionTweet(ctx, tw)
		if err != nil {
			log.Printf("[TW:POLL:%s] processMentionTweet error: %v", tc.accountID, err)
			continue
		}
		if notif != nil {
			if tc.pushNotification(notif) {
				tc.stageCursorUpdate(tw.ID)
			}
			count++
		}
	}
	return count, nil
}

func (tc *TwitterCollector) pollReplies(ctx context.Context, scraper *twitterscraper.Scraper) (int, error) {
	username := tc.accountUsername()
	if username == "" {
		return 0, fmt.Errorf("pollReplies: account username not configured")
	}
	ownTweets, _, err := scraper.FetchTweets(username, twOwnTweetsForReplies, "")
	if err != nil {
		return 0, fmt.Errorf("FetchTweets (own): %w", err)
	}
	count := 0
	for _, own := range ownTweets {
		replies, _, err := scraper.GetTweetReplies(own.ID, "")
		if err != nil {
			log.Printf("[TW:POLL:%s] GetTweetReplies(%s) error: %v", tc.accountID, own.ID, err)
			continue
		}
		for _, reply := range replies {
			if reply.Username == username {
				continue // skip our own follow-up replies in the thread
			}
			notif, err := tc.processReplyTweet(ctx, own.ID, reply)
			if err != nil {
				log.Printf("[TW:POLL:%s] processReplyTweet error: %v", tc.accountID, err)
				continue
			}
			if notif != nil {
				if tc.pushNotification(notif) {
					tc.stageCursorUpdate(reply.ID)
				}
				count++
			}
		}
	}
	return count, nil
}

// pollDirectMessages: stubbed out. I could not confirm a DM-read method on
// this library from its public docs. Logs once per process (not per poll
// cycle) so it doesn't spam logs, and returns cleanly so the rest of
// Collect() keeps working. Replace with a real call once verified.
func (tc *TwitterCollector) pollDirectMessages(ctx context.Context, scraper *twitterscraper.Scraper) (int, error) {
	if !tc.dmUnsupportedWarned.Swap(true) {
		log.Printf("[TW:POLL:%s] DM polling not enabled — could not confirm a DM-read method on "+
			"github.com/imperatrona/twitter-scraper from its docs. Verify pkg.go.dev before enabling; "+
			"this is a deliberate no-op, not a bug.", tc.accountID)
	}
	return 0, nil
}

func (tc *TwitterCollector) accountUsername() string {
	_, _, username, _, _ := tc.twitterAuth()
	return username
}

func (tc *TwitterCollector) Collect(ctx context.Context, _ []*CookieData) ([]*Notification, error) {
	log.Printf("[TW:COLLECT:%s] starting collection (subtype=%s)", tc.accountID, tc.subtype)

	if !tc.pollRunning.CompareAndSwap(false, true) {
		log.Printf("[TW:COLLECT:%s] collect already in progress, skipping", tc.accountID)
		return nil, fmt.Errorf("collect already in progress for %s", tc.accountID)
	}
	defer tc.pollRunning.Store(false)

	if tc.config != nil && !tc.config.ListenMentions && !tc.config.ListenComments {
		log.Printf("[TW:COLLECT:%s] ListenMentions/ListenComments both disabled, returning empty", tc.accountID)
		return []*Notification{}, nil
	}

	if err := tc.ensureClient(ctx); err != nil {
		tc.reportError("CLIENT_ERROR", err.Error(), "error")
		return nil, fmt.Errorf("twitter-scraper client setup: %w", err)
	}
	scraper := tc.safeScraper()
	if scraper == nil {
		return []*Notification{}, nil
	}

	if !tc.checkPause(ctx) {
		return nil, ctx.Err()
	}

	log.Printf("[TW:COLLECT:%s] processing pending instructions before poll", tc.accountID)
	tc.ProcessPendingInstructions()

	sinceID, err := tc.getLastSeenTweetID()
	if err != nil {
		log.Printf("[TW:COLLECT:%s] could not read cursor, relying on dedupe only: %v", tc.accountID, err)
	}

	if tc.config == nil || tc.config.ListenMentions {
		if n, err := tc.pollMentions(ctx, scraper, sinceID); err != nil {
			log.Printf("[TW:COLLECT:%s] pollMentions error: %v", tc.accountID, err)
			tc.reportError("POLL_MENTIONS_FAILED", err.Error(), "warning")
		} else {
			twDbg(tc.accountID, "pollMentions found %d new", n)
		}
	}
	if tc.config == nil || tc.config.ListenComments {
		if n, err := tc.pollReplies(ctx, scraper); err != nil {
			log.Printf("[TW:COLLECT:%s] pollReplies error: %v", tc.accountID, err)
			tc.reportError("POLL_REPLIES_FAILED", err.Error(), "warning")
		} else {
			twDbg(tc.accountID, "pollReplies found %d new", n)
		}
	}
	if tc.config != nil && tc.config.ListenMessages {
		_, _ = tc.pollDirectMessages(ctx, scraper)
	}

	var notifications []*Notification
	for len(notifications) < twMaxBatchSize {
		if !tc.checkPause(ctx) {
			break
		}
		select {
		case n := <-tc.notifBuffer:
			notifications = append(notifications, n)
		case <-ctx.Done():
			goto done
		default:
			select {
			case n := <-tc.notifBuffer:
				notifications = append(notifications, n)
			case <-time.After(twCollectDrainWindow):
				goto done
			case <-ctx.Done():
				goto done
			}
		}
	}

done:
	tc.flushPendingCursor()
	tc.ProcessPendingInstructions()
	log.Printf("[TW:COLLECT:%s] ✓ returning %d notifications", tc.accountID, len(notifications))
	return notifications, nil
}

// =============================================================================
// Instruction queue (identical pattern to telegram.go)
// =============================================================================

func (tc *TwitterCollector) ReceiveInstructions(inst *shared.AutomationInstruction) error {
	if inst.Platform != "twitter" && inst.Platform != tc.platformID {
		return fmt.Errorf("wrong platform: %s (expected twitter or %s)", inst.Platform, tc.platformID)
	}
	if inst.TicketID == "" {
		return fmt.Errorf("empty ticket ID")
	}
	if inst.CreatedAt.IsZero() {
		inst.CreatedAt = time.Now()
	}
	select {
	case tc.instructionQueue <- inst:
		log.Printf("[TW:INSTR:%s] queued ticket=%s action=%s steps=%d",
			tc.accountID, inst.TicketID, inst.Action, len(inst.Steps))
		return nil
	case <-time.After(2 * time.Second):
		tc.reportError("QUEUE_FULL", "instruction queue full", "warning")
		return fmt.Errorf("instruction queue full for %s", tc.accountID)
	}
}

func (tc *TwitterCollector) ProcessPendingInstructions() {
	tc.executionMu.Lock()
	defer tc.executionMu.Unlock()
	count := 0
	for {
		select {
		case inst := <-tc.instructionQueue:
			count++
			log.Printf("[TW:INSTR:%s] executing ticket=%s action=%s", tc.accountID, inst.TicketID, inst.Action)
			if err := tc.executeInstruction(inst); err != nil {
				log.Printf("[TW:INSTR:%s] ticket=%s failed: %v", tc.accountID, inst.TicketID, err)
			}
		default:
			if count > 0 {
				log.Printf("[TW:INSTR:%s] processed %d instructions", tc.accountID, count)
			}
			return
		}
	}
}

func (tc *TwitterCollector) executeInstruction(inst *shared.AutomationInstruction) error {
	start := time.Now()
	log.Printf("[TW:EXEC:%s] start ticket=%s action=%s steps=%d",
		tc.accountID, inst.TicketID, inst.Action, len(inst.Steps))

	if err := tc.pauseCollection(); err != nil {
		log.Printf("[TW:EXEC:%s] pause before instruction (proceeding anyway): %v", tc.accountID, err)
	}
	defer tc.resumeCollection()

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
		log.Printf("[TW:EXEC:%s] step %d/%d type=%s", tc.accountID, i+1, len(inst.Steps), step.Type)

		maxAttempts := step.RetryCount
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		var stepErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			stepErr = tc.executeStep(ctx, step)
			if stepErr == nil {
				log.Printf("[TW:EXEC:%s] step %d/%d type=%s ✓", tc.accountID, i+1, len(inst.Steps), step.Type)
				break
			}
			log.Printf("[TW:EXEC:%s] step %d/%d type=%s attempt %d/%d error: %v",
				tc.accountID, i+1, len(inst.Steps), step.Type, attempt, maxAttempts, stepErr)
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if stepErr != nil {
			lastErr = stepErr
			if len(inst.FallbackSteps) > 0 {
				log.Printf("[TW:EXEC:%s] step %d/%d failed after %d attempt(s), running fallback steps",
					tc.accountID, i+1, len(inst.Steps), maxAttempts)
				lastErr = tc.runFallbackSteps(ctx, inst.FallbackSteps)
			}
			break
		}

		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	log.Printf("[TW:EXEC:%s] ✓ ticket=%s done in %v (err=%v)",
		tc.accountID, inst.TicketID, time.Since(start), lastErr)
	return lastErr
}

// runFallbackSteps runs an instruction's FallbackSteps after its main steps
// failed; its result replaces the main loop's error.
func (tc *TwitterCollector) runFallbackSteps(ctx context.Context, steps []shared.InstructionStep) error {
	var lastErr error
	for i, step := range steps {
		if err := tc.executeStep(ctx, step); err != nil {
			log.Printf("[TW:EXEC:%s] fallback step %d/%d error: %v", tc.accountID, i+1, len(steps), err)
			lastErr = err
		}
		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	return lastErr
}

func (tc *TwitterCollector) executeStep(ctx context.Context, step shared.InstructionStep) error {
	if step.DelayBefore > 0 {
		time.Sleep(time.Duration(step.DelayBefore) * time.Millisecond)
	}
	switch step.Type {
	case shared.StepTypeShare:
		return tc.stepRetweet(ctx, step)
	case shared.StepTypeReply:
		return tc.stepReply(ctx, step)
	case shared.StepTypeUpload:
		return tc.stepUploadAndPost(ctx, step)
	case shared.StepTypeSearch:
		return tc.stepSearch(ctx, step)
	case shared.StepTypeDownload:
		return tc.stepDownloadMedia(ctx, step)
	case shared.StepTypeWait:
		return tc.stepWait(step)
	case shared.StepTypeAPICall:
		return tc.stepAPICallTW(ctx, step)
	case shared.StepTypeLog:
		log.Printf("[TW:LOG:%s] %s", tc.accountID, step.Value)
		return nil
	case shared.StepTypeRateLimitCheck:
		log.Printf("[TW:RATELIMIT:%s] rate-limit check: no native limiter in this library — "+
			"self-throttle in the orchestrator (≥1.5s between calls, ~150 req/15min per endpoint per account)",
			tc.accountID)
		return nil

	// --- Unconfirmed against the library's docs: honest stubs, not guesses ---
	case shared.StepTypeSendMessage:
		return tc.stepSendMessageUnsupported(step)
	case shared.StepTypeReact, shared.StepTypeLike:
		return tc.stepLikeUnsupported(step)
	case shared.StepTypeFollow:
		return tc.stepFollowUnsupported(step)
	case shared.StepTypeUnfollow, shared.StepTypeBlock:
		return tc.stepFollowUnsupported(step)
	case shared.StepTypeSave:
		return tc.stepBookmarkUnsupported(step)

	case shared.StepTypeDBUpdate, shared.StepTypeDBRecord, shared.StepTypeAIGenerate:
		log.Printf("[TW:SKIP:%s] step=%s is orchestrator-side, skipping in collector", tc.accountID, step.Type)
		return nil
	case shared.StepTypeNavigate, shared.StepTypeClick, shared.StepTypeType,
		shared.StepTypeScroll, shared.StepTypeJavaScript, shared.StepTypePress:
		// Correctly rejected now — there is no browser in this implementation
		// at all, unlike the old chromedp-based twitter.go where these were
		// the primary mechanism. Same stance whatsapp.go/telegram.go/viber.go
		// already take.
		log.Printf("[TW:REJECT:%s] step=%s is browser-only, not applicable to this implementation", tc.accountID, step.Type)
		return nil
	default:
		return fmt.Errorf("unknown step type: %s", step.Type)
	}
}

// --- Confirmed, real implementations -----------------------------------

func (tc *TwitterCollector) stepRetweet(ctx context.Context, step shared.InstructionStep) error {
	tweetID, _ := step.Options["tweet_id"].(string)
	if tweetID == "" {
		tweetID = step.Value
	}
	if tweetID == "" {
		return fmt.Errorf("stepRetweet: tweet_id required")
	}
	scraper := tc.safeScraper()
	if scraper == nil {
		return fmt.Errorf("stepRetweet: not connected")
	}
	log.Printf("[TW:RETWEET:%s] retweeting %s", tc.accountID, tweetID)
	if _, err := scraper.CreateRetweet(tweetID); err != nil {
		return fmt.Errorf("stepRetweet: %w", err)
	}
	log.Printf("[TW:RETWEET:%s] ✓ retweeted %s", tc.accountID, tweetID)
	return nil
}

// stepReply posts a tweet. NOTE: I could not confirm a native in-reply-to
// field on this library's NewTweet struct from docs alone — verify against
// your vendored source before trusting silent threading. As a safe fallback
// that's guaranteed to work regardless, this prepends an @mention of the
// target author so the reply is at least contextually addressed, the same
// degraded-but-functional approach other unofficial libraries fall back to
// when no native reply parameter exists.
func (tc *TwitterCollector) stepReply(ctx context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepReply: no message text")
	}
	scraper := tc.safeScraper()
	if scraper == nil {
		return fmt.Errorf("stepReply: not connected")
	}
	text := step.Value
	if toUser, ok := step.Options["reply_to_username"].(string); ok && toUser != "" && !strings.HasPrefix(text, "@") {
		text = "@" + toUser + " " + text
	}
	log.Printf("[TW:REPLY:%s] posting reply-style tweet (len=%d) — verify native in-reply-to support "+
		"before relying on real threading", tc.accountID, len(text))
	tweet, err := scraper.CreateTweet(twitterscraper.NewTweet{Text: text})
	if err != nil {
		return fmt.Errorf("stepReply: %w", err)
	}
	log.Printf("[TW:REPLY:%s] ✓ posted id=%s", tc.accountID, tweet.ID)
	return nil
}

func (tc *TwitterCollector) stepUploadAndPost(ctx context.Context, step shared.InstructionStep) error {
	filePath, _ := step.Options["file_path"].(string)
	if filePath == "" {
		return fmt.Errorf("stepUploadAndPost: file_path required")
	}
	scraper := tc.safeScraper()
	if scraper == nil {
		return fmt.Errorf("stepUploadAndPost: not connected")
	}
	log.Printf("[TW:UPLOAD:%s] uploading media %s", tc.accountID, filePath)
	media, err := scraper.UploadMedia(filePath)
	if err != nil {
		return fmt.Errorf("stepUploadAndPost: upload: %w", err)
	}
	tweet, err := scraper.CreateTweet(twitterscraper.NewTweet{
		Text:   step.Value,
		Medias: []*twitterscraper.Media{media},
	})
	if err != nil {
		return fmt.Errorf("stepUploadAndPost: create tweet: %w", err)
	}
	log.Printf("[TW:UPLOAD:%s] ✓ posted id=%s with media", tc.accountID, tweet.ID)
	return nil
}

func (tc *TwitterCollector) stepSearch(ctx context.Context, step shared.InstructionStep) error {
	query, _ := step.Options["query"].(string)
	if query == "" {
		query = step.Value
	}
	if query == "" {
		return fmt.Errorf("stepSearch: query required")
	}
	scraper := tc.safeScraper()
	if scraper == nil {
		return fmt.Errorf("stepSearch: not connected")
	}
	log.Printf("[TW:SEARCH:%s] searching %q", tc.accountID, query)
	tweets, _, err := scraper.FetchSearchTweets(query, twMentionSearchCount, "")
	if err != nil {
		return fmt.Errorf("stepSearch: %w", err)
	}
	log.Printf("[TW:SEARCH:%s] query=%q → %d result(s)", tc.accountID, query, len(tweets))
	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	if encoded, err := json.Marshal(tweets); err == nil {
		step.Options["result"] = string(encoded)
	}
	return nil
}

func (tc *TwitterCollector) stepDownloadMedia(ctx context.Context, step shared.InstructionStep) error {
	mediaURL, _ := step.Options["url"].(string)
	if mediaURL == "" {
		mediaURL = step.Value
	}
	if mediaURL == "" {
		return fmt.Errorf("stepDownloadMedia: url required")
	}
	log.Printf("[TW:DOWNLOAD:%s] GET %s", tc.accountID, mediaURL)
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Get(mediaURL)
	if err != nil {
		return fmt.Errorf("stepDownloadMedia: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stepDownloadMedia: HTTP %d", resp.StatusCode)
	}
	destPath, _ := step.Options["dest_path"].(string)
	if destPath == "" {
		destPath = filepath.Join("./media/twitter", fmt.Sprintf("%d_download", time.Now().UnixNano()))
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0750); err != nil {
		return fmt.Errorf("stepDownloadMedia: mkdir: %w", err)
	}
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("stepDownloadMedia: create file: %w", err)
	}
	defer out.Close()
	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("stepDownloadMedia: write: %w", err)
	}
	log.Printf("[TW:DOWNLOAD:%s] ✓ saved %d bytes to %s", tc.accountID, written, destPath)
	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	step.Options["downloaded_path"] = destPath
	return nil
}

func (tc *TwitterCollector) stepWait(step shared.InstructionStep) error {
	ms := step.DelayAfter
	if ms <= 0 {
		ms = 2000
	}
	log.Printf("[TW:WAIT:%s] sleeping %dms", tc.accountID, ms)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return nil
}

func (tc *TwitterCollector) stepAPICallTW(ctx context.Context, step shared.InstructionStep) error {
	targetURL := step.Value
	if u, ok := step.Options["url"].(string); ok && u != "" {
		targetURL = u
	}
	if targetURL == "" {
		return fmt.Errorf("stepAPICallTW: url required (set step.Value or options.url)")
	}
	method, _ := step.Options["method"].(string)
	if method == "" {
		method = http.MethodGet
	}
	method = strings.ToUpper(method)
	var bodyReader *strings.Reader
	if body, ok := step.Options["body"].(string); ok && body != "" {
		bodyReader = strings.NewReader(body)
	}
	var req *http.Request
	var err error
	if bodyReader != nil {
		req, err = http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, targetURL, nil)
	}
	if err != nil {
		return fmt.Errorf("stepAPICallTW: build request: %w", err)
	}
	if headers, ok := step.Options["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			if vs, ok := v.(string); ok {
				req.Header.Set(k, vs)
			}
		}
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stepAPICallTW: request failed: %w", err)
	}
	defer resp.Body.Close()
	log.Printf("[TW:API:%s] %s %s → HTTP %d", tc.accountID, method, targetURL, resp.StatusCode)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("stepAPICallTW: server returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// --- Unconfirmed: honest stubs (same pattern as whatsapp.go's no-ops) ----

func (tc *TwitterCollector) stepSendMessageUnsupported(step shared.InstructionStep) error {
	log.Printf("[TW:UNSUPPORTED:%s] step=send_message: direct-message sending is not confirmed to exist on "+
		"github.com/imperatrona/twitter-scraper — verify pkg.go.dev before enabling this step for Twitter",
		tc.accountID)
	return fmt.Errorf("twitter: send_message not implemented (DM write unconfirmed in this library)")
}

func (tc *TwitterCollector) stepLikeUnsupported(step shared.InstructionStep) error {
	log.Printf("[TW:UNSUPPORTED:%s] step=like/react: not confirmed to exist on this library — "+
		"verify pkg.go.dev before enabling", tc.accountID)
	return fmt.Errorf("twitter: like/react not implemented (unconfirmed in this library)")
}

func (tc *TwitterCollector) stepFollowUnsupported(step shared.InstructionStep) error {
	log.Printf("[TW:UNSUPPORTED:%s] step=follow/unfollow/block: not confirmed to exist on this library — "+
		"verify pkg.go.dev before enabling", tc.accountID)
	return fmt.Errorf("twitter: follow/unfollow/block not implemented (unconfirmed in this library)")
}

func (tc *TwitterCollector) stepBookmarkUnsupported(step shared.InstructionStep) error {
	log.Printf("[TW:UNSUPPORTED:%s] step=save/bookmark: FetchBookmarks (read) is confirmed but a "+
		"bookmark-create call is not — verify pkg.go.dev before enabling write", tc.accountID)
	return fmt.Errorf("twitter: bookmark-create not implemented (only read confirmed in this library)")
}

// =============================================================================
// Shutdown
// =============================================================================

func (tc *TwitterCollector) Close() {
	tc.clientMu.Lock()
	if tc.scraper != nil {
		tc.saveCookies(tc.scraper)
	}
	tc.connected.Store(false)
	tc.clientMu.Unlock()
	close(tc.shutdown)
	tc.wg.Wait()
	log.Printf("[TW:CLOSE:%s] collector closed, cookies persisted", tc.accountID)
}
