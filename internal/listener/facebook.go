package listener

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"sailstream/internal/config"
	"sailstream/internal/enviroment"
	"sailstream/internal/session"
	"sailstream/internal/shared"
)

const (
	navTimeout        = 60 * time.Second
	actionTimeout     = 20 * time.Second
	badgeReadTimeout  = 10 * time.Second
	idlePollInterval  = 30 * time.Second
	defaultCooldown   = 60 * time.Second
	fbPauseAckTimeout = 15 * time.Second
)

type fbNotifItem struct {
	URL     string
	Text    string
	DetType NotificationType
}

type platformUserData struct {
	ID          string
	DisplayName string
	IsBlocked   bool
	LastIntent  string
	Found       bool
}

type FacebookCollector struct {
	platformID string
	subtypeID  string
	accountID  string
	subtype    string

	config     *ListenerConfig
	db         *sql.DB
	configMgr  *config.ConfigManager
	envMgr     *enviroment.Environment
	sessionMgr *session.Manager

	platformConfig *config.PlatformConfig
	pageConfig     *config.FacebookPage
	groupConfig    *config.FacebookGroup

	instructionQueue chan *shared.AutomationInstruction
	errorChan        chan *PlatformError

	collectMu   sync.Mutex
	browserMu   sync.Mutex
	executionMu sync.Mutex
	// pauseCount is a reference count, not a bool — see fix N4 in whatsapp.go.
	pauseCount     atomic.Int32
	drainPaused    atomic.Bool
	pauseAck       chan struct{}
	resumeMu       sync.Mutex
	resumeReq      chan struct{}
	collectRunning atomic.Bool

	// CDP browser session — launched against the environment's own
	// default CDP-compatible browser (see enviroment.go), the same way
	// session_manager.go does it: chromedp.ExecAllocator + browserFlags,
	// no manual --remote-debugging-port juggling and no Selenium-style
	// disable-blink-features flags.
	allocCancel context.CancelFunc
	taskCancel  context.CancelFunc
	taskCtx     context.Context

	browserReady    bool
	cookiesInjected bool
	authenticated   bool

	seenNotifications sync.Map
	seenMessages      sync.Map
	seenPageComments  sync.Map

	lastErrorTime  time.Time
	cooldownPeriod time.Duration
}

func NewFacebookCollector(
	platformID, subtypeID, accountID, subtype string,
	listenerConfig *ListenerConfig,
	db *sql.DB,
	configMgr *config.ConfigManager,
	envMgr *enviroment.Environment,
	sessionMgr *session.Manager,
) *FacebookCollector {
	resolvedAccountID := sessionMgr.ResolveAccountID(platformID, subtypeID, accountID)
	if profileID := sessionMgr.GetProfileID(platformID, subtypeID); profileID != "" {
		resolvedAccountID = profileID
	}

	fc := &FacebookCollector{
		platformID:       platformID,
		subtypeID:        subtypeID,
		accountID:        resolvedAccountID,
		subtype:          subtype,
		config:           listenerConfig,
		db:               db,
		configMgr:        configMgr,
		envMgr:           envMgr,
		sessionMgr:       sessionMgr,
		instructionQueue: make(chan *shared.AutomationInstruction, 100),
		errorChan:        make(chan *PlatformError, 50),
		pauseAck:         make(chan struct{}, 1),
		resumeReq:        make(chan struct{}),
		cooldownPeriod:   defaultCooldown,
	}
	fc.loadPlatformConfig()
	return fc
}

func (fc *FacebookCollector) loadPlatformConfig() {
	cfg := fc.configMgr.GetConfig()
	if cfg == nil || cfg.Platforms == nil {
		fc.reportError("CONFIG_LOAD_FAILED", "no configuration available", "critical")
		return
	}
	platformCfg, ok := cfg.Platforms["facebook"]
	if !ok {
		fc.reportError("PLATFORM_NOT_CONFIGURED", "facebook not in config", "critical")
		return
	}
	fc.platformConfig = &platformCfg

	var ownSubtype *config.PlatformSubtype
	for i := range platformCfg.Subtypes {
		if platformCfg.Subtypes[i].ID == fc.subtypeID {
			ownSubtype = &platformCfg.Subtypes[i]
			break
		}
	}

	switch fc.subtype {
	case "page":
		if platformCfg.Facebook != nil && platformCfg.Facebook.Page != nil {
			fc.pageConfig = platformCfg.Facebook.Page
			log.Printf("[Facebook:Page:%s] page config loaded (legacy): %s", fc.subtypeID, fc.pageConfig.PageName)
		} else if ownSubtype != nil {
			fc.pageConfig = &config.FacebookPage{
				PageID:      subtypeField(ownSubtype, "page_id", "id"),
				PageName:    firstNonEmpty(subtypeField(ownSubtype, "page_name", "name"), ownSubtype.Name),
				AccessToken: subtypeField(ownSubtype, "access_token", "page_access_token"),
				Category:    subtypeField(ownSubtype, "category"),
			}
			log.Printf("[Facebook:Page:%s] page config loaded (subtypes[]): %s", fc.subtypeID, fc.pageConfig.PageName)
		}
	case "group":
		if platformCfg.Facebook != nil {
			for _, g := range platformCfg.Facebook.Groups {
				if g.GroupID == fc.subtypeID {
					cp := g
					fc.groupConfig = &cp
					log.Printf("[Facebook:Group:%s] group config loaded (legacy): %s", fc.subtypeID, fc.groupConfig.GroupName)
					break
				}
			}
		}
		if fc.groupConfig == nil && ownSubtype != nil {
			fc.groupConfig = &config.FacebookGroup{
				GroupID:   firstNonEmpty(subtypeField(ownSubtype, "group_id", "id"), fc.subtypeID),
				GroupName: firstNonEmpty(subtypeField(ownSubtype, "group_name", "name"), ownSubtype.Name),
				GroupURL:  subtypeField(ownSubtype, "group_url", "url"),
				Admin:     subtypeBoolField(ownSubtype, "admin", "is_admin"),
			}
			log.Printf("[Facebook:Group:%s] group config loaded (subtypes[]): %s", fc.subtypeID, fc.groupConfig.GroupName)
		}
	default:
		log.Printf("[Facebook:Account:%s] account subtype", fc.accountID)
	}
}

func subtypeField(st *config.PlatformSubtype, keys ...string) string {
	for _, m := range []map[string]interface{}{st.Auth, st.Metadata} {
		if m == nil {
			continue
		}
		for _, k := range keys {
			if v, ok := m[k]; ok {
				if s := fmt.Sprintf("%v", v); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

func subtypeBoolField(st *config.PlatformSubtype, keys ...string) bool {
	for _, m := range []map[string]interface{}{st.Auth, st.Metadata} {
		if m == nil {
			continue
		}
		for _, k := range keys {
			if v, ok := m[k]; ok {
				if b, ok2 := v.(bool); ok2 {
					return b
				}
				if s := fmt.Sprintf("%v", v); s == "true" {
					return true
				}
			}
		}
	}
	return false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func (fc *FacebookCollector) reportError(code, msg, severity string) {
	select {
	case fc.errorChan <- &PlatformError{
		PlatformID: fc.platformID,
		SubtypeID:  fc.subtypeID,
		AccountID:  fc.accountID,
		ErrorCode:  code,
		ErrorMsg:   msg,
		Timestamp:  time.Now(),
		Severity:   severity,
	}:
	default:
		log.Printf("[Facebook:%s:%s] error channel full, dropped: %s — %s",
			fc.subtypeID, fc.accountID, code, msg)
	}
}

func (fc *FacebookCollector) GetErrorChannel() <-chan *PlatformError { return fc.errorChan }

func (fc *FacebookCollector) jitter(minMs, maxMs int) {
	time.Sleep(time.Duration(minMs+rand.Intn(maxMs-minMs)) * time.Millisecond)
}

// ---------- CDP page-interaction adapter ----------
//
// facebook.go used to drive Playwright, which launched its own Chrome
// process manually (--remote-debugging-port, polling the port, hitting
// /json/version, then playwright.Chromium.ConnectOverCDP). It now uses
// chromedp directly against the environment's real default browser,
// exactly like session_manager.go does. These small helpers translate the
// handful of Playwright verbs (Evaluate/Goto/Locator+WaitFor/Click/Fill/
// SetInputFiles/Press) this file relied on into chromedp actions, so the
// rest of the collection/automation logic below reads the same way it
// always did.

// selectorBy picks ByQuery for CSS selectors and BySearch for the XPath-ish
// selectors used in a couple of places (matches session_manager.go's
// clickIfPresent, which also uses chromedp.BySearch for such queries).
func selectorBy(sel string) chromedp.QueryOption {
	if strings.HasPrefix(sel, "//") {
		return chromedp.BySearch
	}
	return chromedp.ByQuery
}

// evalJS runs arbitrary JS in the page and JSON-decodes the result into an
// interface{}, mirroring how playwright's page.Evaluate() was used
// throughout this file (callers then type-assert to string/bool/etc).
func evalJS(page context.Context, js string) (interface{}, error) {
	var res interface{}
	err := chromedp.Run(page, chromedp.Evaluate(js, &res))
	return res, err
}

// waitVisible waits (up to timeout) for sel to become visible.
func waitVisible(page context.Context, sel string, timeout time.Duration) error {
	tctx, cancel := context.WithTimeout(page, timeout)
	defer cancel()
	return chromedp.Run(tctx, chromedp.WaitVisible(sel, selectorBy(sel)))
}

// clickSel clicks the first element matching sel.
func clickSel(page context.Context, sel string) error {
	return chromedp.Run(page, chromedp.Click(sel, selectorBy(sel)))
}

// fillSel clears then types text into sel — works for both real inputs and
// the contenteditable divs Facebook's composer/message boxes use.
func fillSel(page context.Context, sel, text string) error {
	by := selectorBy(sel)
	if err := chromedp.Run(page, chromedp.Click(sel, by)); err != nil {
		return err
	}
	if !strings.HasPrefix(sel, "//") {
		clearJS := fmt.Sprintf(`
			(function(){
				var el=document.querySelector(%q);
				if(!el)return;
				if('value' in el){el.value='';}
				else{el.innerText='';}
			})()
		`, sel)
		_, _ = evalJS(page, clearJS)
	}
	return chromedp.Run(page, chromedp.SendKeys(sel, text, by))
}

// pressKeyOn sends a key (e.g. "\r" for Enter) to sel.
func pressKeyOn(page context.Context, sel, key string) error {
	return chromedp.Run(page, chromedp.SendKeys(sel, key, selectorBy(sel)))
}

// setInputFilesSel uploads local files through a file input.
func setInputFilesSel(page context.Context, sel string, files []string) error {
	return chromedp.Run(page, chromedp.SetUploadFiles(sel, files, selectorBy(sel)))
}

// ---------- CDP connection ----------
//
// Launches (or reuses) a chromedp session against the environment's
// default CDP-compatible browser, the same way session_manager.go's
// browserFlags()/loginWithRealProfile() do: chromedp.ExecAllocator with
// ExecPath(env.GetBrowserPath()) and UserDataDir(<isolated profile copy>),
// no manual --remote-debugging-port/exec.Command dance, no Selenium-style
// disable-blink-features flags. Deliberately does NOT launch against
// env.GetProfilePath() (the live default-browser profile) — see the
// comment inside for why.
func (fc *FacebookCollector) ensureCDPPage() (context.Context, error) {
	fc.browserMu.Lock()
	defer fc.browserMu.Unlock()

	if fc.taskCtx != nil {
		select {
		case <-fc.taskCtx.Done():
			fc.taskCtx = nil
		default:
			return fc.taskCtx, nil
		}
	}

	browserPath := fc.envMgr.GetBrowserPath()
	if browserPath == "" {
		return nil, fmt.Errorf("no CDP-compatible browser found")
	}

	// Use an isolated, app-owned copy of the real profile — the same
	// approach session_manager.go uses — rather than launching directly
	// against env.GetProfilePath() (the live default-browser profile).
	// Pointing chromedp at the live profile trips Chrome's singleton
	// instance guard whenever the real browser is already open: Chrome
	// sees the existing SingletonLock and just forwards the request to
	// that already-running window ("Opening in existing browser
	// session."), so chromedp never gets a debuggable target of its own.
	profilePath := fc.sessionMgr.GetOrCreateIsolatedProfilePath(fc.platformID, fc.subtypeID)
	if err := fc.sessionMgr.EnsureIsolatedProfile(profilePath); err != nil {
		return nil, fmt.Errorf("isolated profile setup failed: %w", err)
	}

	opts := []chromedp.ExecAllocatorOption{
		chromedp.ExecPath(browserPath),
		chromedp.UserDataDir(profilePath),
		chromedp.Flag("profile-directory", "Default"),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("headless", false),
		chromedp.Flag("start-maximized", true),
	}

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	taskCtx, taskCancel := chromedp.NewContext(allocCtx)

	if err := chromedp.Run(taskCtx, chromedp.Navigate("about:blank")); err != nil {
		taskCancel()
		allocCancel()
		return nil, fmt.Errorf("browser init failed: %w", err)
	}

	fc.allocCancel = allocCancel
	fc.taskCancel = taskCancel
	fc.taskCtx = taskCtx
	fc.browserReady = true
	return taskCtx, nil
}

// injectCookies pushes the session's stored cookies (the actual source of
// truth from session_manager's JSON session file) into the live CDP
// browser via Network.setCookie. This is deliberately explicit rather than
// relying on whatever happens to already be on disk in the isolated
// profile's own cookie jar — a profile copied before login finished, or a
// non-clean browser shutdown that never flushed its Cookies DB, would
// otherwise silently land back on the login screen despite Collect()
// being handed perfectly good, non-expired session cookies.
func (fc *FacebookCollector) injectCookies(page context.Context, cookies []*CookieData) error {
	if len(cookies) == 0 {
		return nil
	}
	var params []*network.CookieParam
	for _, c := range cookies {
		if c == nil || c.Name == "" || c.Value == "" {
			continue
		}
		domain := c.Domain
		if domain == "" {
			domain = ".facebook.com"
		}
		path := c.Path
		if path == "" {
			path = "/"
		}
		param := &network.CookieParam{
			Name:     c.Name,
			Value:    c.Value,
			Domain:   domain,
			Path:     path,
			Secure:   c.Secure,
			HTTPOnly: c.HTTPOnly,
		}
		if c.Expires > 0 {
			exp := cdp.TimeSinceEpoch(time.Unix(c.Expires, 0))
			param.Expires = &exp
		}
		params = append(params, param)
	}
	if len(params) == 0 {
		return nil
	}
	return chromedp.Run(page,
		network.Enable(),
		chromedp.ActionFunc(func(ctx context.Context) error {
			return network.SetCookies(params).Do(ctx)
		}),
	)
}

func (fc *FacebookCollector) Close() error {
	fc.browserMu.Lock()
	defer fc.browserMu.Unlock()
	if fc.taskCancel != nil {
		fc.taskCancel()
		fc.taskCancel = nil
	}
	if fc.allocCancel != nil {
		fc.allocCancel()
		fc.allocCancel = nil
	}
	fc.taskCtx = nil
	fc.browserReady = false
	fc.cookiesInjected = false
	fc.authenticated = false
	return nil
}

func (fc *FacebookCollector) getValidPage() (context.Context, error) {
	fc.browserMu.Lock()
	defer fc.browserMu.Unlock()
	if fc.taskCtx == nil {
		return nil, fmt.Errorf("no active CDP page")
	}
	return fc.taskCtx, nil
}

func (fc *FacebookCollector) pauseCollection() error {
	if fc.pauseCount.Add(1) > 1 {
		return nil
	}
	fc.drainPaused.Store(true)
	log.Printf("[Facebook:%s] pause requested", fc.accountID)

	if !fc.collectRunning.Load() {
		return nil
	}

	select {
	case <-fc.pauseAck:
		return nil
	case <-time.After(fbPauseAckTimeout):
		log.Printf("[Facebook:%s] pause ack timeout (proceeding, drainPaused is set)", fc.accountID)
		return nil
	}
}

func (fc *FacebookCollector) resumeCollection() {
	if fc.pauseCount.Add(-1) > 0 {
		return
	}
	newResume := make(chan struct{})
	fc.resumeMu.Lock()
	old := fc.resumeReq
	fc.resumeReq = newResume
	fc.resumeMu.Unlock()

	fc.drainPaused.Store(false)
	close(old)
	log.Printf("[Facebook:%s] collection resumed", fc.accountID)
}

func (fc *FacebookCollector) checkPause(ctx context.Context) bool {
	if !fc.drainPaused.Load() {
		return true
	}
	select {
	case fc.pauseAck <- struct{}{}:
	default:
	}
	fc.resumeMu.Lock()
	resumeCh := fc.resumeReq
	fc.resumeMu.Unlock()

	select {
	case <-resumeCh:
		return true
	case <-ctx.Done():
		return false
	}
}

// ---------- Collect – implements PlatformCollector interface ----------
func (fc *FacebookCollector) Collect(ctx context.Context, cookies []*CookieData) ([]*Notification, error) {
	if !fc.collectMu.TryLock() {
		return nil, fmt.Errorf("collection already in progress for %s", fc.accountID)
	}
	defer fc.collectMu.Unlock()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("collect aborted: %w", ctx.Err())
	default:
	}

	if time.Since(fc.lastErrorTime) < fc.cooldownPeriod {
		log.Printf("[FB:%s] in cooldown, skipping", fc.accountID)
		return nil, nil
	}
	if fc.drainPaused.Load() {
		log.Printf("[FB:%s] paused for automation, skipping", fc.accountID)
		return nil, nil
	}
	if fc.config != nil &&
		!fc.config.ListenMessages &&
		!fc.config.ListenComments &&
		!fc.config.ListenMentions {
		log.Printf("[FB:%s] all listeners disabled", fc.accountID)
		return nil, nil
	}

	// Validate cookies (just log expiration)
	for _, c := range cookies {
		if c.Expires > 0 && time.Now().After(time.Unix(c.Expires, 0)) {
			log.Printf("[Facebook:%s] cookie %q expired", fc.accountID, c.Name)
		}
	}

	if !fc.collectRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("collect already running for %s", fc.accountID)
	}
	defer fc.collectRunning.Store(false)

	log.Printf("[FB:%s] step 1: ensure CDP page", fc.accountID)
	page, err := fc.ensureCDPPage()
	if err != nil {
		fc.lastErrorTime = time.Now()
		return nil, fmt.Errorf("CDP page setup failed: %w", err)
	}

	if !fc.cookiesInjected {
		if err := fc.injectCookies(page, cookies); err != nil {
			log.Printf("[FB:%s] cookie injection failed (continuing on profile's own cookies): %v", fc.accountID, err)
		} else {
			fc.cookiesInjected = true
			log.Printf("[FB:%s] injected %d session cookies into browser", fc.accountID, len(cookies))
		}
	}

	if err := fc.navigateTo(page, "https://facebook.com"); err != nil {
		fc.lastErrorTime = time.Now()
		return nil, fmt.Errorf("home navigation failed: %w", err)
	}
	time.Sleep(3 * time.Second)
	fc.dismissAntiBot(page)

	if !fc.isLoggedIn(page) {
		fc.lastErrorTime = time.Now()
		return nil, fmt.Errorf("not logged in after cookie injection — session cookies may be stale or rejected")
	}

	if !fc.checkPause(ctx) {
		return nil, ctx.Err()
	}

	log.Printf("[FB:%s] step 2: drain pending instructions", fc.accountID)
	fc.ProcessPendingInstructions()

	if !fc.checkPause(ctx) {
		return nil, ctx.Err()
	}

	log.Printf("[FB:%s] step 3: start collection (subtype=%s)", fc.accountID, fc.subtype)
	cycleStart := time.Now()

	var notifications []*Notification
	switch fc.subtype {
	case "page":
		notifications = fc.collectPageData(ctx, page)
	case "group":
		notifications = fc.collectGroupData(ctx, page)
	default:
		notifications = fc.collectAccountData(ctx, page)
	}
	log.Printf("[FB:%s] step 3: done — %d notifications in %v", fc.accountID, len(notifications), time.Since(cycleStart))

	fc.ProcessPendingInstructions()

	log.Printf("[FB:%s] step 4: returning home for idle watch", fc.accountID)
	if err := fc.navigateTo(page, "https://facebook.com"); err != nil {
		log.Printf("[FB:%s] could not navigate home after collection: %v", fc.accountID, err)
	}
	fc.idleLoop(ctx, page)

	fc.lastErrorTime = time.Time{}
	return notifications, nil
}

// ---------- Navigation and page helpers ----------
func (fc *FacebookCollector) navigateTo(page context.Context, url string) error {
	fc.jitter(800, 1500)
	tctx, cancel := context.WithTimeout(page, navTimeout)
	defer cancel()
	if err := chromedp.Run(tctx,
		chromedp.Navigate(url),
		chromedp.WaitReady("body", chromedp.ByQuery),
	); err != nil {
		return fmt.Errorf("navigate %s: %w", url, err)
	}
	fc.jitter(1500, 3000)
	return nil
}

func (fc *FacebookCollector) dismissAntiBot(page context.Context) {
	_, _ = evalJS(page, `
		(function(){
			var exact=['continue','log in','login','ok','accept','allow','got it'];
			var els=document.querySelectorAll('div[role="button"],button,a[role="button"]');
			for(var el of els){
				var t=(el.innerText||el.textContent||'').trim().toLowerCase();
				if(exact.indexOf(t)!==-1||t.includes('continue')||t.includes('log in')){
					el.click();return true;
				}
			}
			return false;
		})()
	`)
}

// isLoggedIn checks for Facebook's c_user/xs auth cookies via CDP, the same
// cookie check session_manager.go's login flow performs.
func (fc *FacebookCollector) isLoggedIn(page context.Context) bool {
	time.Sleep(2 * time.Second)
	var cookies []*network.Cookie
	err := chromedp.Run(page, chromedp.ActionFunc(func(ctx context.Context) error {
		var err error
		cookies, err = network.GetCookies().Do(ctx)
		return err
	}))
	if err != nil {
		return false
	}
	hasCUser, hasXs := false, false
	for _, c := range cookies {
		if c.Name == "c_user" && c.Value != "" {
			hasCUser = true
		}
		if c.Name == "xs" && c.Value != "" {
			hasXs = true
		}
	}
	return hasCUser && hasXs
}

func (fc *FacebookCollector) detectBlocks(page context.Context) (bool, string) {
	blockedRaw, _ := evalJS(page, `
		(function(){
			if(document.querySelector('[data-testid="checkpoint"],form[action*="checkpoint"],input[name="captcha"]'))return true;
			var b=document.body?document.body.innerText:'';
			if(b.includes('rate limit')||b.includes('too many requests'))return true;
			if(b.includes('account disabled')||b.includes('suspended'))return true;
			if(document.querySelector('[title="reCAPTCHA"],iframe[src*="recaptcha"]'))return true;
			return false;
		})()
	`)
	blocked, _ := blockedRaw.(bool)
	if !blocked {
		return false, ""
	}
	textRaw, _ := evalJS(page, `document.body?document.body.innerText:''`)
	text, _ := textRaw.(string)
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "checkpoint") || strings.Contains(lower, "two-factor"):
		return true, "2FA or checkpoint required"
	case strings.Contains(lower, "rate limit"):
		return true, "rate limit detected"
	default:
		return true, "security check required"
	}
}

func (fc *FacebookCollector) readUnreadBadges(page context.Context) (int, int, error) {
	js := `
		(function(){
			var msgs=0,notifs=0;
			document.querySelectorAll('a[href*="/messages"]').forEach(function(a){
				var b=a.querySelector('span[aria-label],span[data-count]');
				if(b){var n=parseInt(b.getAttribute('data-count')||b.textContent,10);if(!isNaN(n)&&n>0)msgs=n;}
			});
			document.querySelectorAll('a[href*="/notifications"]').forEach(function(a){
				var b=a.querySelector('span[aria-label],span[data-count]');
				if(b){var n=parseInt(b.getAttribute('data-count')||b.textContent,10);if(!isNaN(n)&&n>0)notifs=n;}
			});
			return JSON.stringify({msgs:msgs,notifs:notifs});
		})()
	`
	raw, err := evalJS(page, js)
	if err != nil {
		return 0, 0, err
	}
	s, _ := raw.(string)
	var result struct {
		Msgs   int `json:"msgs"`
		Notifs int `json:"notifs"`
	}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return 0, 0, err
	}
	return result.Msgs, result.Notifs, nil
}

func (fc *FacebookCollector) hasUnreadActivity(page context.Context) bool {
	msgs, notifs, err := fc.readUnreadBadges(page)
	if err != nil {
		return true
	}
	log.Printf("[Facebook:%s] idle badge poll: messages=%d notifications=%d", fc.accountID, msgs, notifs)
	return msgs > 0 || notifs > 0
}

func (fc *FacebookCollector) idleLoop(listenerCtx context.Context, page context.Context) {
	log.Printf("[Facebook:%s] → idle mode (checking every %v)", fc.accountID, idlePollInterval)
	ticker := time.NewTicker(idlePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-listenerCtx.Done():
			return
		case <-ticker.C:
			if fc.drainPaused.Load() {
				continue
			}
			if fc.hasUnreadActivity(page) {
				log.Printf("[Facebook:%s] idle: unread activity detected → leaving idle", fc.accountID)
				return
			}
		}
	}
}

// ---------- Classification and filter helpers ----------
func (fc *FacebookCollector) classifyNotification(text, url string) NotificationType {
	lower := strings.ToLower(text)
	if strings.Contains(lower, "liked") || strings.Contains(lower, "reacted") ||
		strings.Contains(lower, "love your") || strings.Contains(lower, "haha your") ||
		strings.Contains(lower, "wow your") {
		return NotificationTypeLike
	}
	if strings.Contains(lower, "commented") || strings.Contains(lower, "replied") {
		return NotificationTypeComment
	}
	if strings.Contains(lower, "mentioned you") || strings.Contains(lower, "tagged you") {
		return NotificationTypeMention
	}
	if strings.Contains(lower, "started following") || strings.Contains(lower, "is now following") {
		return NotificationTypeFollow
	}
	if strings.Contains(lower, "sent you a message") || strings.Contains(lower, "messaged you") {
		return NotificationTypeMessage
	}
	switch {
	case strings.Contains(url, "notif_t=comment") || strings.Contains(url, "comment_id"):
		return NotificationTypeComment
	case strings.Contains(url, "notif_t=like") || strings.Contains(url, "reaction"):
		return NotificationTypeLike
	case strings.Contains(url, "notif_t=mention") || strings.Contains(url, "notif_t=tag"):
		return NotificationTypeMention
	case strings.Contains(url, "notif_t=follow") || strings.Contains(url, "notif_t=friend"):
		return NotificationTypeFollow
	case strings.Contains(url, "/posts/"):
		return NotificationTypeComment
	}
	return ""
}

func (fc *FacebookCollector) isTypeEnabled(t NotificationType) bool {
	if fc.config == nil {
		return false
	}
	switch t {
	case NotificationTypeMessage:
		return fc.config.ListenMessages
	case NotificationTypeComment:
		return fc.config.ListenComments
	case NotificationTypeMention:
		return fc.config.ListenMentions
	case NotificationTypeFollow:
		return fc.config.ListenComments || fc.config.ListenMentions
	case NotificationTypeLike:
		return false
	default:
		return false
	}
}

func (fc *FacebookCollector) clickUnreadFilter(page context.Context) bool {
	raw, err := evalJS(page, `
		(function(){
			for(var s of document.querySelectorAll('span')){
				if(s.textContent.trim()==='Unread'){
					var el=s.parentElement;
					while(el){
						if(el.tagName==='DIV'&&el.getAttribute('role')==='button'){el.click();return 'div';}
						el=el.parentElement;
					}
					s.click();return 'span';
				}
			}
			return '';
		})()
	`)
	if err != nil {
		return false
	}
	result, _ := raw.(string)
	if result != "" {
		time.Sleep(2 * time.Second)
		return true
	}
	return false
}

// clickUnreadFilterRetry retries clickUnreadFilter a few times with a short
// wait for the tab bar to render — a bare page load doesn't guarantee the
// "Unread" tab element exists yet by the time we look for it.
func (fc *FacebookCollector) clickUnreadFilterRetry(page context.Context, attempts int) bool {
	for i := 0; i < attempts; i++ {
		if fc.clickUnreadFilter(page) {
			return true
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func (fc *FacebookCollector) markConvReadFromList(page context.Context, convURL string) {
	js := fmt.Sprintf(`
		(function(){
			for(var row of document.querySelectorAll('[role="row"]')){
				var a=row.querySelector('a[href*="/messages/"]');
				if(a&&a.href.includes(%q)){
					row.dispatchEvent(new MouseEvent('mouseover',{bubbles:true}));
					for(var b of row.querySelectorAll('div[role="button"],button')){
						var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
						if(lbl.includes('mark as read')||lbl.includes('mark read')){b.click();return true;}
					}
					return false;
				}
			}
			return false;
		})()
	`, convURL)
	markedRaw, _ := evalJS(page, js)
	if marked, _ := markedRaw.(bool); marked {
		time.Sleep(300 * time.Millisecond)
	}
}

func (fc *FacebookCollector) markAllNotifsRead(page context.Context) {
	_, _ = evalJS(page, `
		(function(){
			var el=document.querySelector('div[aria-label="Notification Actions"]');
			if(el){el.click();return true;}return false;
		})()
	`)
	time.Sleep(1 * time.Second)
	_, _ = evalJS(page, `
		(function(){
			for(var s of document.querySelectorAll('span')){
				if(s.textContent.trim()==='Mark all as read'){s.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(1500 * time.Millisecond)
}

// ---------- Account-level collection ----------
func (fc *FacebookCollector) collectAccountData(ctx context.Context, page context.Context) []*Notification {
	var out []*Notification

	if fc.config != nil && fc.config.ListenMessages {
		msgs, err := fc.collectMessages(ctx, page)
		if err != nil {
			log.Printf("[FB:%s] messages error: %v", fc.accountID, err)
		} else {
			out = append(out, msgs...)
		}
	}

	if fc.config != nil && (fc.config.ListenComments || fc.config.ListenMentions) {
		notifs, err := fc.collectNotifications(ctx, page)
		if err != nil {
			log.Printf("[FB:%s] notifications error: %v", fc.accountID, err)
		} else {
			out = append(out, notifs...)
		}
	}

	return out
}

// ---------- Messages collection ----------
func (fc *FacebookCollector) collectMessages(ctx context.Context, page context.Context) ([]*Notification, error) {
	const messagesURL = "https://www.facebook.com/messages/"

	if err := fc.navigateTo(page, messagesURL); err != nil {
		return nil, err
	}

	_ = waitVisible(page, `[role="grid"],[role="row"]`, 20*time.Second)
	time.Sleep(3 * time.Second)

	for i := 0; i < 3; i++ {
		if fc.clickUnreadFilter(page) {
			break
		}
		time.Sleep(2 * time.Second)
	}

	for i := 0; i < 8; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		_, _ = evalJS(page, `(function(){var c=document.querySelector('[role="grid"]')||document.body;c.scrollTop=c.scrollHeight;})()`)
		time.Sleep(1200 * time.Millisecond)
	}

	raw, err := evalJS(page, `
		(function(){
			var links=[];
			var seen={};
			document.querySelectorAll('[role="row"]').forEach(function(row){
				var a=row.querySelector('a[href*="/messages/"]');
				if(a&&a.href&&!seen[a.href]){seen[a.href]=true;links.push(a.href);}
			});
			return JSON.stringify(links);
		})()
	`)
	if err != nil {
		return nil, fmt.Errorf("extract conversation URLs: %w", err)
	}
	var urls []string
	if s, ok := raw.(string); ok {
		if err := json.Unmarshal([]byte(s), &urls); err != nil {
			return nil, fmt.Errorf("parse conversation URLs: %w", err)
		}
	}
	log.Printf("[FB:%s] messages: found %d conversations to process", fc.accountID, len(urls))

	var out []*Notification
	for _, convURL := range urls {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		if _, seen := fc.seenMessages.LoadOrStore(convURL, true); seen {
			continue
		}
		log.Printf("[FB:%s] messages: processing %s", fc.accountID, convURL)

		fc.markConvReadFromList(page, convURL)

		notif, err := fc.extractMessageDetails(ctx, page, convURL)
		if err != nil {
			log.Printf("[FB:%s] messages: extract failed for %s: %v", fc.accountID, convURL, err)
		} else if notif != nil {
			out = append(out, notif)
		}

		if err := fc.navigateTo(page, messagesURL); err != nil {
			break
		}
		fc.clickUnreadFilter(page)
		fc.jitter(1500, 2500)
	}
	return out, nil
}

func (fc *FacebookCollector) extractMessageDetails(ctx context.Context, page context.Context, convURL string) (*Notification, error) {
	if err := fc.navigateTo(page, convURL); err != nil {
		return nil, fmt.Errorf("navigate to thread: %w", err)
	}

	_ = waitVisible(page, `[role="article"]`, 15*time.Second)
	time.Sleep(2 * time.Second)

	_, _ = evalJS(page, `
		(function(){
			for(var b of document.querySelectorAll('div[role="button"],button')){
				if((b.getAttribute('aria-label')||b.textContent||'').toLowerCase().includes('mark as read')){b.click();return true;}
			}
			return false;
		})()
	`)

	raw, err := evalJS(page, `
		(function(){
			var msgs=document.querySelectorAll('[role="article"]');
			if(!msgs.length)return null;
			var last=msgs[msgs.length-1];
			var sEl=last.querySelector('h4,strong,[data-testid="message-sender"]');
			var sName=sEl?sEl.innerText.trim():'';
			var sLink=(sEl&&sEl.closest('a'))?sEl.closest('a').href:'';
			var tEl=last.querySelector('[dir="auto"],._2vja');
			var text=tEl?tEl.innerText.trim():'';
			var imgs=[];
			for(var img of last.querySelectorAll('img[src*="facebook"],img[src*="fbcdn"]')){
				if(img.src&&!img.src.includes('emoji')&&!img.src.includes('sticker')){
					var u=img.src;
					if(u.includes('&width='))u=u.replace(/&width=\d+/,'&width=1080');
					imgs.push(u);
				}
			}
			var ts=Date.now();
			var tEl2=last.querySelector('abbr,time');
			if(tEl2&&tEl2.getAttribute('data-tooltip-content')){
				ts=new Date(tEl2.getAttribute('data-tooltip-content')).getTime();
			}
			var isGroup=window.location.href.includes('/group/')||!!document.querySelector('[aria-label*="Group"]');
			var groupName='';
			if(isGroup){var gEl=document.querySelector('h1,[role="heading"]');groupName=gEl?gEl.innerText.trim():'';}
			return JSON.stringify({sender_name:sName,sender_url:sLink,message_text:text,image_urls:imgs,timestamp:ts,is_group:isGroup,group_name:groupName});
		})()
	`)
	if err != nil {
		return nil, fmt.Errorf("extract message JS: %w", err)
	}
	s, _ := raw.(string)
	if s == "" {
		return nil, nil
	}
	var result struct {
		SenderName string   `json:"sender_name"`
		SenderURL  string   `json:"sender_url"`
		Text       string   `json:"message_text"`
		ImageURLs  []string `json:"image_urls"`
		Timestamp  int64    `json:"timestamp"`
		IsGroup    bool     `json:"is_group"`
		GroupName  string   `json:"group_name"`
	}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, fmt.Errorf("parse message JS: %w", err)
	}
	if result.SenderName == "" && result.Text == "" {
		return nil, nil
	}
	if result.IsGroup && fc.config != nil && !fc.config.ListenGroupMessages {
		log.Printf("[FB:%s] ListenGroupMessages disabled, dropping group message", fc.accountID)
		return nil, nil
	}

	uid := fc.genUserID(result.SenderName)
	userData, recentMsgs, drop, enrichErr := fc.buildEnrichment(uid)
	if enrichErr != nil {
		log.Printf("[FB:%s] enrichment error: %v", fc.accountID, enrichErr)
	}
	if drop {
		return nil, nil
	}

	dlPaths := fc.downloadImages(result.ImageURLs, NotificationTypeMessage)
	mediaAttachments := fc.buildMediaAttachments(result.ImageURLs, dlPaths, NotificationTypeMessage)
	productID := fc.extractProductID(result.Text)
	var productData map[string]interface{}
	if productID != "" {
		productData, _ = fc.getProductData(productID)
	}

	rawData := map[string]interface{}{
		"conversation_url":  convURL,
		"image_urls":        result.ImageURLs,
		"downloaded_images": dlPaths,
		"platform":          "facebook",
		"subtype":           fc.subtype,
		"collected_at":      time.Now().Format(time.RFC3339),
	}
	if productData != nil {
		rawData["product_id"] = productID
		rawData["product_data"] = productData
	}
	if userData != nil {
		rawData["user_data"] = userData
		rawData["recent_messages"] = recentMsgs
	} else {
		rawData["is_new_user"] = true
		rawData["user_data"] = nil
	}

	return &Notification{
		ID:         fmt.Sprintf("fb_msg_%s_%d", fc.accountID, time.Now().UnixNano()),
		PlatformID: fc.platformID,
		SubtypeID:  fc.subtypeID,
		AccountID:  fc.accountID,
		Type:       NotificationTypeMessage,
		Timestamp:  time.Unix(result.Timestamp/1000, 0),
		Message: &MessageData{
			ConversationID:   convURL,
			ConversationName: result.GroupName,
			IsGroup:          result.IsGroup,
			MessageID:        fmt.Sprintf("fb_msg_%d", time.Now().UnixNano()),
			Sender: UserInfo{
				UserID:      uid,
				Username:    result.SenderName,
				DisplayName: result.SenderName,
				ProfileURL:  result.SenderURL,
			},
			Text:           result.Text,
			Timestamp:      time.Unix(result.Timestamp/1000, 0),
			IsRead:         true,
			DeliveryStatus: "delivered",
			MediaAttached:  mediaAttachments,
		},
		RawData:     rawData,
		CollectedAt: time.Now(),
	}, nil
}

// ---------- Notifications collection ----------
func (fc *FacebookCollector) collectNotifications(ctx context.Context, page context.Context) ([]*Notification, error) {
	const notifsURL = "https://www.facebook.com/notifications"

	if err := fc.navigateTo(page, notifsURL); err != nil {
		return nil, err
	}
	fc.jitter(2000, 3000)

	if !fc.clickUnreadFilterRetry(page, 3) {
		log.Printf("[FB:%s] notifications: Unread tab not found, reading current view", fc.accountID)
	}

	for i := 0; i < 20; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		_, _ = evalJS(page, `(function(){var c=document.querySelector('[role="feed"]')||document.querySelector('div[role="list"]')||document.body;c.scrollTop=c.scrollHeight;})()`)
		time.Sleep(1000 * time.Millisecond)
	}
	fc.jitter(800, 1500)

	raw, err := evalJS(page, `
		(function(){
			var out=[];
			document.querySelectorAll('div[role="listitem"]').forEach(function(item){
				var a=item.querySelector('a[role="link"]');
				if(!a||!a.href)return;
				var tEl=item.querySelector('span[dir="auto"],[data-ad-comet-preview="message"]');
				var text=tEl?tEl.innerText.trim():item.innerText.trim();
				out.push({url:a.href,text:text});
			});
			return JSON.stringify(out);
		})()
	`)
	if err != nil {
		return nil, fmt.Errorf("extract notification items: %w", err)
	}
	type rawItem struct {
		URL  string `json:"url"`
		Text string `json:"text"`
	}
	var rawItems []rawItem
	if s, ok := raw.(string); ok {
		if err := json.Unmarshal([]byte(s), &rawItems); err != nil {
			return nil, fmt.Errorf("parse notification items: %w", err)
		}
	}

	var likeCount int
	var pending []fbNotifItem
	for _, ri := range rawItems {
		t := fc.classifyNotification(ri.Text, ri.URL)
		if t == "" {
			continue
		}
		if t == NotificationTypeLike {
			likeCount++
			continue
		}
		if !fc.isTypeEnabled(t) {
			continue
		}
		pending = append(pending, fbNotifItem{URL: ri.URL, Text: ri.Text, DetType: t})
	}
	log.Printf("[FB:%s] notifications: %d items, %d likes skipped, %d to process",
		fc.accountID, len(rawItems), likeCount, len(pending))

	var out []*Notification
	for i, item := range pending {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		if _, seen := fc.seenNotifications.LoadOrStore(item.URL, true); seen {
			continue
		}
		log.Printf("[FB:%s] notifications: processing %d/%d type=%s", fc.accountID, i+1, len(pending), item.DetType)

		// Each item is opened directly by its own permalink URL, so there's
		// no need to bounce back through /notifications + re-click Unread
		// between items — that round trip is exactly what was landing back
		// on the "All" tab (Facebook doesn't preserve the Unread filter
		// across a fresh page load, and the click can race the tab bar
		// re-rendering). We exhaust the entire unread batch this way, then
		// only return to the Unread tab once at the end to mark it all read.
		notif, err := fc.extractNotificationDetails(ctx, page, item.URL, item.DetType)
		if err != nil {
			log.Printf("[FB:%s] notifications: extract failed: %v", fc.accountID, err)
			continue
		}
		if notif != nil {
			out = append(out, notif)
		}
		fc.jitter(1200, 2200)
	}

	if len(out) > 0 {
		if err := fc.navigateTo(page, notifsURL); err != nil {
			log.Printf("[FB:%s] notifications: could not return to notifications page to mark read: %v", fc.accountID, err)
			return out, nil
		}
		if !fc.clickUnreadFilterRetry(page, 3) {
			log.Printf("[FB:%s] notifications: Unread tab not found before mark-all-read, proceeding anyway", fc.accountID)
		}
		fc.markAllNotifsRead(page)
	}
	return out, nil
}

func (fc *FacebookCollector) extractNotificationDetails(
	ctx context.Context, page context.Context, url string, t NotificationType,
) (*Notification, error) {
	if err := fc.navigateTo(page, url); err != nil {
		return nil, fmt.Errorf("navigate to notification: %w", err)
	}
	_ = waitVisible(page, `[role="article"],[dir="auto"]`, 15*time.Second)
	time.Sleep(2 * time.Second)

	var commentData *CommentData
	var rawData map[string]interface{}
	var userData map[string]interface{}
	var recentMsgs []string

	switch t {
	case NotificationTypeComment:
		postContent, productID, productData, postImgs := fc.extractPostContent(page, url)
		comment, err := fc.extractCommentFromPage(page)
		if err != nil {
			return nil, nil
		}
		comment.PostContent = postContent
		comment.PostMediaURLs = postImgs
		commentData = comment
		rawData = map[string]interface{}{
			"post_url":   comment.PostURL,
			"comment_id": comment.CommentID,
		}
		if productData != nil {
			rawData["product_id"] = productID
			rawData["product_data"] = productData
		}

		uid := fc.genUserID(commentData.CommentAuthor.Username)
		ud, rm, drop, _ := fc.buildEnrichment(uid)
		if drop {
			return nil, nil
		}
		userData = ud
		recentMsgs = rm
		if rawData["product_data"] == nil {
			if pid := fc.extractProductID(commentData.CommentText); pid != "" {
				if pd, err := fc.getProductData(pid); err == nil && pd != nil {
					rawData["product_id"] = pid
					rawData["product_data"] = pd
				}
			}
		}

	case NotificationTypeMention:
		_ = waitVisible(page, `[data-testid="comment_author"],h4,strong`, 6*time.Second)
		raw, err := evalJS(page, `
			(function(){
				var aEl = document.querySelector('[data-testid="comment_author"],h4,strong');
				var author = aEl ? aEl.innerText.trim() : '';
				var cEl = document.querySelector('[data-testid="comment_text"],[dir="auto"]');
				var comment = cEl ? cEl.innerText.trim() : '';
				var pl = document.querySelector('a[href*="/posts/"]');
				var postUrl = pl ? pl.href : '';
				return JSON.stringify({author:author, comment:comment, post_url:postUrl});
			})()
		`)
		if err != nil {
			return nil, fmt.Errorf("mention evaluation: %w", err)
		}
		s, _ := raw.(string)
		type mentionData struct {
			Author  string `json:"author"`
			Comment string `json:"comment"`
			PostURL string `json:"post_url"`
		}
		var md mentionData
		if s != "" {
			json.Unmarshal([]byte(s), &md)
		}
		if md.Author == "" && md.Comment == "" {
			fallback, _ := evalJS(page, `document.body.innerText.trim().substring(0,200)`)
			if fs, ok := fallback.(string); ok {
				md.Comment = fs
			}
		}
		if md.Author != "" {
			uid := fc.genUserID(md.Author)
			ud, rm, drop, _ := fc.buildEnrichment(uid)
			if !drop {
				userData = ud
				recentMsgs = rm
			}
		}
		rawData = map[string]interface{}{
			"mentioned_in":   url,
			"text":           md.Comment,
			"mention_author": md.Author,
			"post_url":       md.PostURL,
			"platform":       "facebook",
			"subtype":        fc.subtype,
			"collected_at":   time.Now().Format(time.RFC3339),
		}

	case NotificationTypeFollow:
		followerRaw, _ := evalJS(page, `(function(){var a=document.querySelector('a[role="link"]');return a?a.innerText.trim():'';})()`)
		follower, _ := followerRaw.(string)
		rawData = map[string]interface{}{
			"follower":         follower,
			"notification_url": url,
		}
		if follower != "" {
			uid := fc.genUserID(follower)
			ud, rm, drop, _ := fc.buildEnrichment(uid)
			if !drop {
				userData = ud
				recentMsgs = rm
			}
		}

	default:
		return nil, nil
	}

	base := map[string]interface{}{
		"notification_url": url,
		"platform":         "facebook",
		"subtype":          fc.subtype,
		"collected_at":     time.Now().Format(time.RFC3339),
	}
	if userData != nil {
		base["user_data"] = userData
		base["recent_messages"] = recentMsgs
	} else {
		base["is_new_user"] = true
		base["user_data"] = nil
	}
	for k, v := range rawData {
		base[k] = v
	}

	return &Notification{
		ID:          fmt.Sprintf("fb_notif_%s_%d", fc.accountID, time.Now().UnixNano()),
		PlatformID:  fc.platformID,
		SubtypeID:   fc.subtypeID,
		AccountID:   fc.accountID,
		Type:        t,
		Timestamp:   time.Now(),
		Comment:     commentData,
		RawData:     base,
		CollectedAt: time.Now(),
	}, nil
}

func (fc *FacebookCollector) extractCommentFromPage(page context.Context) (*CommentData, error) {
	raw, err := evalJS(page, `
		(function(){
			var pl=document.querySelector('a[href*="/posts/"]');
			var postUrl=pl?pl.href:'';
			var aEl=document.querySelector('[data-testid="comment_author"],h4,strong');
			var author=aEl?aEl.innerText.trim():'';
			var cEl=document.querySelector('[data-testid="comment_text"],[dir="auto"]');
			var comment=cEl?cEl.innerText.trim():'';
			var id=window.location.href.split('?')[0].split('/').pop();
			return JSON.stringify({post_url:postUrl,author:author,comment:comment,comment_id:id});
		})()
	`)
	if err != nil {
		return nil, err
	}
	s, _ := raw.(string)
	var result struct {
		PostURL   string `json:"post_url"`
		Author    string `json:"author"`
		Comment   string `json:"comment"`
		CommentID string `json:"comment_id"`
	}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, err
	}
	return &CommentData{
		PostURL:     result.PostURL,
		PostID:      fc.extractPostID(result.PostURL),
		CommentText: result.Comment,
		CommentID:   result.CommentID,
		CommentAuthor: UserInfo{
			Username:    result.Author,
			DisplayName: result.Author,
		},
		Timestamp: time.Now(),
	}, nil
}

func (fc *FacebookCollector) extractPostContent(page context.Context, postURL string) (string, string, map[string]interface{}, []string) {
	currentURLRaw, _ := evalJS(page, `window.location.href`)
	currentURL, _ := currentURLRaw.(string)
	if currentURL != postURL {
		if err := fc.navigateTo(page, postURL); err != nil {
			return "", "", nil, nil
		}
		time.Sleep(2 * time.Second)
	}
	raw, err := evalJS(page, `
		(function(){
			var e=document.querySelector('[data-ad-preview="message"],[dir="auto"]');
			var content=e?e.textContent.trim():'';
			var imgs=[];
			for(var img of document.querySelectorAll('img[src*="facebook"],img[src*="fbcdn"]')){
				if(img.src&&!img.src.includes('emoji')&&!img.src.includes('sticker')){
					var u=img.src;
					if(u.includes('&width='))u=u.replace(/&width=\d+/,'&width=1080');
					imgs.push(u);
				}
			}
			return JSON.stringify({content:content,images:imgs});
		})()
	`)
	if err != nil {
		return "", "", nil, nil
	}
	s, _ := raw.(string)
	var result struct {
		Content string   `json:"content"`
		Images  []string `json:"images"`
	}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return "", "", nil, nil
	}
	productID := fc.extractProductID(result.Content)
	var productData map[string]interface{}
	if productID != "" {
		productData, _ = fc.getProductData(productID)
	}
	return result.Content, productID, productData, result.Images
}

// ---------- Page / Group collection ----------
func (fc *FacebookCollector) collectPageData(ctx context.Context, page context.Context) []*Notification {
	if fc.pageConfig == nil || fc.pageConfig.PageID == "" {
		fc.reportError("NO_PAGE_CONFIG", "page config missing or PageID empty", "error")
		return nil
	}

	if err := fc.switchToPage(page, fc.pageConfig.PageName); err != nil {
		fc.reportError("PAGE_SWITCH_FAILED", err.Error(), "error")
		return nil
	}

	var out []*Notification
	if fc.config != nil && fc.config.ListenMessages {
		msgs, err := fc.collectPageMessages(ctx, page)
		if err != nil {
			fc.reportError("PAGE_MSG_FAILED", err.Error(), "warning")
		} else {
			out = append(out, msgs...)
		}
	}
	if fc.config != nil && fc.config.ListenComments {
		fc.jitter(1000, 2000)
		cmts, err := fc.collectPageComments(ctx, page)
		if err != nil {
			fc.reportError("PAGE_COMMENT_FAILED", err.Error(), "warning")
		} else {
			out = append(out, cmts...)
		}
	}
	return out
}

func (fc *FacebookCollector) switchToPage(page context.Context, pageName string) error {
	log.Printf("[Facebook:Page:%s] switching to page context: %s", fc.subtypeID, pageName)
	if err := waitVisible(page, `div[aria-label="Account"]`, actionTimeout); err != nil {
		return fmt.Errorf("account menu not visible: %w", err)
	}
	if err := clickSel(page, `div[aria-label="Account"]`); err != nil {
		return fmt.Errorf("open account menu: %w", err)
	}
	time.Sleep(2 * time.Second)

	sel := fmt.Sprintf(`//span[text()="%s"]`, pageName)
	if err := waitVisible(page, sel, actionTimeout); err != nil {
		return fmt.Errorf("page name not visible: %w", err)
	}
	if err := clickSel(page, sel); err != nil {
		return fmt.Errorf("click page name: %w", err)
	}
	time.Sleep(3 * time.Second)

	indicator := fmt.Sprintf(`//div[@aria-label="Account"]//span[contains(text(),"%s")]`, pageName)
	if err := waitVisible(page, indicator, actionTimeout); err != nil {
		return fmt.Errorf("page switch verification failed: %w", err)
	}
	return nil
}

func (fc *FacebookCollector) collectPageMessages(ctx context.Context, page context.Context) ([]*Notification, error) {
	inboxURL := fmt.Sprintf("https://www.facebook.com/%s/inbox", fc.pageConfig.PageID)
	if err := fc.navigateTo(page, inboxURL); err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second)

	fc.clickUnreadFilter(page)

	for i := 0; i < 5; i++ {
		_, _ = evalJS(page, `(function(){var c=document.querySelector('[role="grid"]')||document.body;c.scrollTop=c.scrollHeight;})()`)
		time.Sleep(1000 * time.Millisecond)
	}

	raw, err := evalJS(page, `
		(function(){var l=[];document.querySelectorAll('a[href*="/messages/t/"]').forEach(function(a){if(a.href)l.push(a.href);});return JSON.stringify(l);})()
	`)
	if err != nil {
		return nil, fmt.Errorf("extract page message URLs: %w", err)
	}
	var urls []string
	if s, ok := raw.(string); ok {
		json.Unmarshal([]byte(s), &urls)
	}

	var out []*Notification
	for _, url := range urls {
		if _, seen := fc.seenMessages.LoadOrStore(url, true); seen {
			continue
		}
		notif, err := fc.extractMessageDetails(ctx, page, url)
		if err != nil {
			log.Printf("[Facebook:Page:%s] extract failed: %v", fc.subtypeID, err)
			continue
		}
		if notif != nil {
			notif.SubtypeID = fc.subtypeID
			out = append(out, notif)
		}
		if err := fc.navigateTo(page, inboxURL); err != nil {
			break
		}
		fc.clickUnreadFilter(page)
		fc.jitter(1000, 2000)
	}
	return out, nil
}

func (fc *FacebookCollector) collectPageComments(ctx context.Context, page context.Context) ([]*Notification, error) {
	commentsURL := fmt.Sprintf("https://www.facebook.com/%s/inbox/comments", fc.pageConfig.PageID)
	if err := fc.navigateTo(page, commentsURL); err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second)

	fc.clickUnreadFilter(page)

	for i := 0; i < 8; i++ {
		_, _ = evalJS(page, `(function(){var f=document.querySelector('[role="feed"]')||document.body;f.scrollTop=f.scrollHeight;})()`)
		time.Sleep(1000 * time.Millisecond)
	}

	var items []struct {
		Text    string `json:"comment_text"`
		Author  string `json:"author"`
		PostURL string `json:"post_url"`
	}
	raw, err := evalJS(page, `
		(function(){
			var out=[];
			document.querySelectorAll('[role="article"]').forEach(function(el){
				var tEl=el.querySelector('[dir="auto"]');
				var aEl=el.querySelector('a[role="link"]');
				var pEl=el.querySelector('a[href*="/posts/"]');
				var t=tEl?tEl.innerText.trim():'';
				var a=aEl?aEl.innerText.trim():'';
				var p=pEl?pEl.href:'';
				if(t&&a)out.push({comment_text:t,author:a,post_url:p});
			});
			return JSON.stringify(out);
		})()
	`)
	if err != nil {
		return nil, fmt.Errorf("extract page comments: %w", err)
	}
	if s, ok := raw.(string); ok {
		json.Unmarshal([]byte(s), &items)
	}

	var out []*Notification
	for _, c := range items {
		key := hex.EncodeToString(sha256.New().Sum([]byte(c.PostURL + ":" + c.Text)))
		if _, seen := fc.seenPageComments.LoadOrStore(key, true); seen {
			continue
		}
		uid := fc.genUserID(c.Author)
		userData, recentMsgs, drop, _ := fc.buildEnrichment(uid)
		if drop {
			continue
		}
		productID := fc.extractProductID(c.Text)
		var productData map[string]interface{}
		if productID != "" {
			productData, _ = fc.getProductData(productID)
		}
		raw := map[string]interface{}{
			"comment_text": c.Text,
			"post_url":     c.PostURL,
			"platform":     "facebook",
			"subtype":      "page",
			"page_id":      fc.pageConfig.PageID,
			"collected_at": time.Now().Format(time.RFC3339),
		}
		if productData != nil {
			raw["product_id"] = productID
			raw["product_data"] = productData
		}
		if userData != nil {
			raw["user_data"] = userData
			raw["recent_messages"] = recentMsgs
		} else {
			raw["is_new_user"] = true
			raw["user_data"] = nil
		}
		out = append(out, &Notification{
			ID:         fmt.Sprintf("fb_page_cmt_%s_%d", fc.subtypeID, time.Now().UnixNano()),
			PlatformID: fc.platformID,
			SubtypeID:  fc.subtypeID,
			AccountID:  fc.accountID,
			Type:       NotificationTypeComment,
			Timestamp:  time.Now(),
			Comment: &CommentData{
				PostURL:     c.PostURL,
				PostID:      fc.extractPostID(c.PostURL),
				CommentText: c.Text,
				CommentAuthor: UserInfo{
					Username:    c.Author,
					DisplayName: c.Author,
					UserID:      uid,
				},
				Timestamp: time.Now(),
			},
			RawData:     raw,
			CollectedAt: time.Now(),
		})
	}
	return out, nil
}

func (fc *FacebookCollector) collectGroupData(ctx context.Context, page context.Context) []*Notification {
	if fc.groupConfig == nil {
		fc.reportError("NO_GROUP_CONFIG", "group config not loaded", "error")
		return nil
	}
	log.Printf("[Facebook:Group:%s] collect START (%s)", fc.subtypeID, fc.groupConfig.GroupName)
	if err := fc.navigateTo(page, fmt.Sprintf("https://facebook.com/groups/%s", fc.groupConfig.GroupID)); err != nil {
		fc.reportError("GROUP_NAV_FAILED", err.Error(), "error")
		return nil
	}
	fc.jitter(2000, 3000)
	return nil
}

// ---------- Instruction handling ----------
func (fc *FacebookCollector) ReceiveInstructions(instruction *shared.AutomationInstruction) error {
	if instruction.Platform != "facebook" && instruction.Platform != fc.platformID {
		return fmt.Errorf("wrong platform: %s", instruction.Platform)
	}
	if instruction.TicketID == "" {
		return fmt.Errorf("empty ticket ID")
	}
	if instruction.CreatedAt.IsZero() {
		instruction.CreatedAt = time.Now()
	}
	select {
	case fc.instructionQueue <- instruction:
		log.Printf("[Facebook:%s] instruction queued: %s (%d steps)", fc.accountID, instruction.TicketID, len(instruction.Steps))
		return nil
	case <-time.After(2 * time.Second):
		fc.reportError("INSTRUCTION_QUEUE_FULL", fmt.Sprintf("dropped: %s", instruction.TicketID), "warning")
		return fmt.Errorf("instruction queue full")
	}
}

func (fc *FacebookCollector) ProcessPendingInstructions() {
	fc.executionMu.Lock()
	defer fc.executionMu.Unlock()
	for {
		select {
		case instr := <-fc.instructionQueue:
			fc.executeInstruction(instr)
		default:
			return
		}
	}
}

func (fc *FacebookCollector) executeInstruction(instr *shared.AutomationInstruction) {
	log.Printf("[Facebook:%s] executing instruction %s (%s, %d steps)",
		fc.accountID, instr.TicketID, instr.Action, len(instr.Steps))

	if err := fc.pauseCollection(); err != nil {
		log.Printf("[Facebook:%s] pause note: %v", fc.accountID, err)
	}
	defer fc.resumeCollection()
	time.Sleep(800 * time.Millisecond)

	page, err := fc.getValidPage()
	if err != nil {
		log.Printf("[Facebook:%s] instruction aborted — no browser: %v", fc.accountID, err)
		return
	}

	timeout := instr.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(page, timeout)
	defer cancel()

	start := time.Now()
	var lastErr error
	for i, step := range instr.Steps {
		if ctx.Err() != nil {
			lastErr = fmt.Errorf("instruction timed out before step %d/%d: %w", i+1, len(instr.Steps), ctx.Err())
			break
		}
		log.Printf("[Facebook:%s] step %d/%d: %s (%s)", fc.accountID, i+1, len(instr.Steps), step.Type, step.Description)
		if step.DelayBefore > 0 {
			time.Sleep(time.Duration(step.DelayBefore) * time.Millisecond)
		}

		maxAttempts := step.RetryCount
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		var stepErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			stepErr = fc.executeStep(ctx, step)
			if stepErr == nil {
				break
			}
			log.Printf("[Facebook:%s] step %d/%d attempt %d/%d failed: %v",
				fc.accountID, i+1, len(instr.Steps), attempt, maxAttempts, stepErr)
			if attempt < maxAttempts {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if stepErr != nil {
			lastErr = stepErr
			if len(instr.FallbackSteps) > 0 {
				log.Printf("[Facebook:%s] step %d/%d failed after %d attempt(s), running fallback steps",
					fc.accountID, i+1, len(instr.Steps), maxAttempts)
				lastErr = fc.runFallbackSteps(ctx, instr.FallbackSteps)
			}
			break
		}

		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	log.Printf("[Facebook:%s] instruction %s done in %v (err=%v)",
		fc.accountID, instr.TicketID, time.Since(start), lastErr)
}

// runFallbackSteps runs an instruction's FallbackSteps after its main steps
// failed; its result replaces the main loop's error.
func (fc *FacebookCollector) runFallbackSteps(page context.Context, steps []shared.InstructionStep) error {
	var lastErr error
	for i, step := range steps {
		if step.DelayBefore > 0 {
			time.Sleep(time.Duration(step.DelayBefore) * time.Millisecond)
		}
		if err := fc.executeStep(page, step); err != nil {
			log.Printf("[Facebook:%s] fallback step %d/%d failed: %v", fc.accountID, i+1, len(steps), err)
			lastErr = err
		}
		if step.DelayAfter > 0 {
			time.Sleep(time.Duration(step.DelayAfter) * time.Millisecond)
		}
	}
	return lastErr
}

func (fc *FacebookCollector) executeStep(page context.Context, step shared.InstructionStep) error {
	switch step.Type {

	case shared.StepTypeNavigate:
		if step.Value == "" {
			return fmt.Errorf("navigate: no URL")
		}
		return fc.navigateTo(page, step.Value)

	case shared.StepTypeClick:
		if step.Selector == "" {
			return fmt.Errorf("click: no selector")
		}
		fc.jitter(150, 400)
		if err := waitVisible(page, step.Selector, actionTimeout); err != nil {
			return err
		}
		return clickSel(page, step.Selector)

	case shared.StepTypeType:
		if step.Selector == "" || step.Value == "" {
			return fmt.Errorf("type: missing selector or value")
		}
		if err := waitVisible(page, step.Selector, actionTimeout); err != nil {
			return err
		}
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
		return fillSel(page, step.Selector, step.Value)

	case shared.StepTypeWait:
		if step.Condition == "selector" && step.Value != "" {
			return waitVisible(page, step.Value, actionTimeout)
		}
		delay := 2000
		if step.DelayAfter > 0 {
			delay = step.DelayAfter
		}
		time.Sleep(time.Duration(delay) * time.Millisecond)
		return nil

	case shared.StepTypeScroll:
		amount := "500"
		if step.Value != "" {
			amount = step.Value
		}
		sel := step.Selector
		var err error
		if sel != "" {
			_, err = evalJS(page, fmt.Sprintf(
				`(function(){var el=document.querySelector(%q);if(el)el.scrollTop+=parseInt(%s);else window.scrollBy(0,%s);})()`,
				sel, amount, amount))
		} else {
			_, err = evalJS(page, fmt.Sprintf(`window.scrollBy(0,%s);`, amount))
		}
		return err

	case shared.StepTypeJavaScript:
		if step.Value == "" {
			return fmt.Errorf("js: no code")
		}
		_, err := evalJS(page, step.Value)
		return err

	case shared.StepTypePress:
		if step.Selector == "" {
			return fmt.Errorf("press: no selector")
		}
		key := step.Value
		if key == "" {
			key = "Return"
		}
		if err := waitVisible(page, step.Selector, actionTimeout); err != nil {
			return err
		}
		if key == "Return" || key == "Enter" {
			key = "\r"
		}
		return pressKeyOn(page, step.Selector, key)

	case shared.StepTypeSendMessage:
		return fc.stepSendMessage(page, step)

	case shared.StepTypeReply:
		return fc.stepReply(page, step)

	case shared.StepTypeLike:
		return fc.stepLike(page, step)

	case shared.StepTypeReact:
		return fc.stepReact(page, step)

	case shared.StepTypeFollow:
		return fc.stepFollow(page, step)

	case shared.StepTypeUnfollow:
		return fc.stepUnfollow(page, step)

	case shared.StepTypeBlock:
		return fc.stepBlock(page, step)

	case shared.StepTypePost:
		return fc.stepPost(page, step)

	case shared.StepTypeShare:
		return fc.stepShare(page, step)

	case shared.StepTypeSearch:
		return fc.stepSearch(page, step)

	case shared.StepTypeUpload:
		return fc.stepUpload(page, step)

	case shared.StepTypeDownload:
		return fc.stepDownload(page, step)

	case shared.StepTypeSave:
		return fc.stepSave(page, step)

	case shared.StepTypeAPICall:
		return fc.stepAPICall(page, step)

	case shared.StepTypeDBUpdate, shared.StepTypeDBRecord, shared.StepTypeAIGenerate:
		log.Printf("[Facebook:%s] step %s handled by orchestrator", fc.accountID, step.Type)
		return nil

	case shared.StepTypeRateLimitCheck:
		log.Printf("[Facebook:%s] rate-limit check: OK", fc.accountID)
		return nil

	case shared.StepTypeLog:
		log.Printf("[Facebook:%s] [STEP LOG] %s", fc.accountID, step.Value)
		return nil

	default:
		return fmt.Errorf("unknown step type: %s", step.Type)
	}
}

// ---------- Step handlers ----------

func (fc *FacebookCollector) stepSendMessage(page context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepSendMessage: no message text")
	}

	recipient, _ := step.Options["to"].(string)
	if recipient == "" {
		recipient, _ = step.Options["recipient"].(string)
	}

	if recipient != "" {
		convURL := recipient
		if !strings.HasPrefix(recipient, "http") {
			convURL = fmt.Sprintf("https://www.facebook.com/messages/t/%s", recipient)
		}
		if err := fc.navigateTo(page, convURL); err != nil {
			return fmt.Errorf("stepSendMessage navigate: %w", err)
		}
		fc.jitter(1500, 2500)
	}

	imagePath, _ := step.Options["image_path"].(string)
	imageURL, _ := step.Options["image_url"].(string)

	if imageURL != "" {
		localPath, err := fc.downloadExternalImage(imageURL)
		if err != nil {
			return fmt.Errorf("stepSendMessage download image: %w", err)
		}
		imagePath = localPath
	}

	if imagePath != "" {
		if err := fc.attachImageToConversation(page, imagePath); err != nil {
			log.Printf("[Facebook:%s] image attach failed (continuing with text): %v", fc.accountID, err)
		}
	}

	return fc.sendMessageInContext(page, step.Value)
}

func (fc *FacebookCollector) stepReply(page context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepReply: no reply text")
	}

	quotedSelector, _ := step.Options["quote_selector"].(string)
	if quotedSelector != "" {
		_, err := evalJS(page, fmt.Sprintf(`
			(function(){
				var el=document.querySelector(%q);
				if(!el)return false;
				el.dispatchEvent(new MouseEvent('mouseover',{bubbles:true}));
				var btn=el.querySelector('[aria-label*="Reply"],[aria-label*="reply"]');
				if(btn){btn.click();return true;}
				return false;
			})()
		`, quotedSelector))
		if err != nil {
			log.Printf("[Facebook:%s] stepReply quote hover error: %v", fc.accountID, err)
		}
		fc.jitter(800, 1500)
	}

	return fc.sendMessageInContext(page, step.Value)
}

func (fc *FacebookCollector) stepLike(page context.Context, step shared.InstructionStep) error {
	if step.Value != "" && strings.HasPrefix(step.Value, "http") {
		if err := fc.navigateTo(page, step.Value); err != nil {
			return fmt.Errorf("stepLike navigate: %w", err)
		}
	}

	sel := step.Selector
	if sel == "" {
		sel = `[aria-label="Like"],[aria-label*="Like this"],[data-testid="like_button"]`
	}

	fc.jitter(300, 700)

	isLikedRaw, err := evalJS(page, fmt.Sprintf(`
		(function(){
			var el=document.querySelector(%q);
			if(!el)return false;
			var label=(el.getAttribute('aria-label')||el.textContent||'').toLowerCase();
			return label.includes('unlike')||el.getAttribute('aria-pressed')==='true';
		})()
	`, sel))
	if err == nil {
		if isLiked, _ := isLikedRaw.(bool); isLiked {
			log.Printf("[Facebook:%s] like: already liked, skipping", fc.accountID)
			return nil
		}
	}

	if err := waitVisible(page, sel, actionTimeout); err != nil {
		return err
	}
	return clickSel(page, sel)
}

func (fc *FacebookCollector) stepReact(page context.Context, step shared.InstructionStep) error {
	if step.Value != "" && strings.HasPrefix(step.Value, "http") {
		if err := fc.navigateTo(page, step.Value); err != nil {
			return fmt.Errorf("stepReact navigate: %w", err)
		}
	}

	emoji, _ := step.Options["emoji"].(string)
	if emoji == "" {
		emoji = "like"
	}

	likeSel := `[aria-label="Like"],[aria-label*="Like this"],[data-testid="like_button"]`
	fc.jitter(300, 600)

	jsTemplate := `
		(function(){
			var likeBtn = document.querySelector(%q);
			if (!likeBtn) return false;
			likeBtn.dispatchEvent(new MouseEvent('mouseover', {bubbles: true}));
			setTimeout(function(){
				var picker = document.querySelector('[aria-label*="reaction"],[data-testid*="reaction"]');
				if (!picker) return;
				var emojiMap = {like:'👍', love:'❤️', haha:'😆', wow:'😮', sad:'😢', angry:'😠'};
				var target = %q;
				var btns = picker.querySelectorAll('div[role="button"], button');
				for (var b of btns) {
					var lbl = (b.getAttribute('aria-label') || '').toLowerCase();
					if (lbl.includes(target)) { b.click(); return; }
				}
			}, 1200);
			return true;
		})()`
	_, err := evalJS(page, fmt.Sprintf(jsTemplate, likeSel, strings.ToLower(emoji)))
	if err != nil {
		log.Printf("[Facebook:%s] stepReact error: %v", fc.accountID, err)
	}
	time.Sleep(2 * time.Second)
	return nil
}

func (fc *FacebookCollector) stepFollow(page context.Context, step shared.InstructionStep) error {
	profileURL, _ := step.Options["profile_url"].(string)
	if profileURL == "" {
		profileURL = step.Value
	}
	if profileURL != "" && strings.HasPrefix(profileURL, "http") {
		if err := fc.navigateTo(page, profileURL); err != nil {
			return fmt.Errorf("stepFollow navigate: %w", err)
		}
	}

	sel := step.Selector
	if sel == "" {
		sel = `[aria-label*="Follow"],[data-testid*="follow"]`
	}

	isFollowingRaw, err := evalJS(page, fmt.Sprintf(`
		(function(){
			var el=document.querySelector(%q);
			if(!el)return false;
			var lbl=(el.getAttribute('aria-label')||el.textContent||'').toLowerCase();
			return lbl.includes('following')||lbl.includes('unfollow');
		})()
	`, sel))
	if err == nil {
		if isFollowing, _ := isFollowingRaw.(bool); isFollowing {
			log.Printf("[Facebook:%s] follow: already following, skipping", fc.accountID)
			return nil
		}
	}

	if err := waitVisible(page, sel, actionTimeout); err != nil {
		return err
	}
	return clickSel(page, sel)
}

func (fc *FacebookCollector) stepUnfollow(page context.Context, step shared.InstructionStep) error {
	profileURL, _ := step.Options["profile_url"].(string)
	if profileURL == "" {
		profileURL = step.Value
	}
	if profileURL != "" && strings.HasPrefix(profileURL, "http") {
		if err := fc.navigateTo(page, profileURL); err != nil {
			return fmt.Errorf("stepUnfollow navigate: %w", err)
		}
	}

	sel := step.Selector
	if sel == "" {
		sel = `[aria-label*="Following"],[aria-label*="Unfollow"]`
	}

	if err := waitVisible(page, sel, actionTimeout); err != nil {
		return err
	}
	if err := clickSel(page, sel); err != nil {
		return err
	}

	fc.jitter(400, 700)

	_, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('unfollow')){b.click();return true;}
			}
			return false;
		})()
	`)
	return err
}

func (fc *FacebookCollector) stepBlock(page context.Context, step shared.InstructionStep) error {
	profileURL, _ := step.Options["profile_url"].(string)
	if profileURL == "" {
		profileURL = step.Value
	}
	if profileURL != "" && strings.HasPrefix(profileURL, "http") {
		if err := fc.navigateTo(page, profileURL); err != nil {
			return fmt.Errorf("stepBlock navigate: %w", err)
		}
		fc.jitter(1000, 2000)
	}

	_, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('more')||lbl.includes('...')||lbl==='…'){b.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(1 * time.Second)

	sel := step.Selector
	if sel == "" {
		sel = `[aria-label*="Block"],[data-testid*="block"]`
	}
	if err := waitVisible(page, sel, actionTimeout); err != nil {
		return fmt.Errorf("stepBlock click: %w", err)
	}
	if err := clickSel(page, sel); err != nil {
		return fmt.Errorf("stepBlock click: %w", err)
	}

	fc.jitter(500, 1000)
	_, err = evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('confirm')||lbl==='block'){b.click();return true;}
			}
			return false;
		})()
	`)
	log.Printf("[Facebook:%s] stepBlock: block action completed", fc.accountID)
	return err
}

func (fc *FacebookCollector) stepPost(page context.Context, step shared.InstructionStep) error {
	if step.Value == "" {
		return fmt.Errorf("stepPost: no post text")
	}

	postType, _ := step.Options["post_type"].(string)
	imagePath, _ := step.Options["image_path"].(string)
	imageURL, _ := step.Options["image_url"].(string)
	groupID, _ := step.Options["group_id"].(string)
	pageID, _ := step.Options["page_id"].(string)
	linkURL, _ := step.Options["link_url"].(string)

	if imageURL != "" && imagePath == "" {
		local, err := fc.downloadExternalImage(imageURL)
		if err != nil {
			log.Printf("[Facebook:%s] stepPost: image download failed (continuing without image): %v", fc.accountID, err)
		} else {
			imagePath = local
		}
	}

	switch postType {
	case "group":
		targetGroupID := groupID
		if targetGroupID == "" && fc.groupConfig != nil {
			targetGroupID = fc.groupConfig.GroupID
		}
		if targetGroupID == "" {
			return fmt.Errorf("stepPost group: no group_id specified")
		}
		return fc.createGroupPost(page, targetGroupID, step.Value, imagePath)

	case "page":
		targetPageID := pageID
		if targetPageID == "" && fc.pageConfig != nil {
			targetPageID = fc.pageConfig.PageID
		}
		if targetPageID == "" {
			return fmt.Errorf("stepPost page: no page_id specified")
		}
		return fc.createPagePost(page, targetPageID, step.Value, imagePath)

	case "story":
		return fc.createStory(page, step.Value, imagePath)

	case "reel":
		if imagePath == "" {
			return fmt.Errorf("stepPost reel: image_path or image_url required")
		}
		return fc.createReel(page, imagePath, step.Value)

	default:
		if linkURL != "" {
			return fc.createPostWithLink(page, step.Value, linkURL)
		}
		return fc.createPost(page, step.Value, imagePath)
	}
}

// ---------- Post creation helpers ----------

func (fc *FacebookCollector) createPost(page context.Context, text, imagePath string) error {
	if err := fc.navigateTo(page, "https://www.facebook.com"); err != nil {
		return fmt.Errorf("createPost navigate home: %w", err)
	}

	composerSel := `div[aria-label="Create a post"]`
	if err := waitVisible(page, composerSel, 30*time.Second); err != nil {
		return fmt.Errorf("open post dialog: %w", err)
	}
	if err := clickSel(page, composerSel); err != nil {
		return fmt.Errorf("open post dialog: %w", err)
	}
	time.Sleep(2 * time.Second)

	if imagePath != "" {
		if err := fc.attachImageToPostDialog(page, imagePath); err != nil {
			log.Printf("[Facebook:%s] createPost image attach failed (continuing): %v", fc.accountID, err)
		}
	}

	textField := `div[aria-label="What's on your mind?"][contenteditable="true"]`
	if err := waitVisible(page, textField, actionTimeout); err != nil {
		return fmt.Errorf("type post text: %w", err)
	}
	if err := fillSel(page, textField, text); err != nil {
		return fmt.Errorf("type post text: %w", err)
	}
	time.Sleep(800 * time.Millisecond)

	submitSel := `div[aria-label="Post"]`
	if err := waitVisible(page, submitSel, 30*time.Second); err != nil {
		return fmt.Errorf("post submit: %w", err)
	}
	if err := clickSel(page, submitSel); err != nil {
		return fmt.Errorf("post submit: %w", err)
	}
	time.Sleep(3 * time.Second)
	return nil
}

func (fc *FacebookCollector) createPostWithLink(page context.Context, text, linkURL string) error {
	if err := fc.navigateTo(page, "https://www.facebook.com"); err != nil {
		return err
	}

	composerSel := `div[aria-label="Create a post"]`
	if err := waitVisible(page, composerSel, 30*time.Second); err != nil {
		return fmt.Errorf("open post dialog: %w", err)
	}
	if err := clickSel(page, composerSel); err != nil {
		return fmt.Errorf("open post dialog: %w", err)
	}
	time.Sleep(2 * time.Second)

	combined := text + "\n" + linkURL
	textField := `div[aria-label="What's on your mind?"][contenteditable="true"]`
	if err := waitVisible(page, textField, actionTimeout); err != nil {
		return fmt.Errorf("type post with link: %w", err)
	}
	if err := fillSel(page, textField, combined); err != nil {
		return fmt.Errorf("type post with link: %w", err)
	}
	time.Sleep(3 * time.Second)

	submitSel := `div[aria-label="Post"]`
	if err := waitVisible(page, submitSel, 30*time.Second); err != nil {
		return fmt.Errorf("post submit: %w", err)
	}
	if err := clickSel(page, submitSel); err != nil {
		return fmt.Errorf("post submit: %w", err)
	}
	time.Sleep(3 * time.Second)
	return nil
}

func (fc *FacebookCollector) createGroupPost(page context.Context, groupID, text, imagePath string) error {
	groupURL := fmt.Sprintf("https://www.facebook.com/groups/%s", groupID)
	if err := fc.navigateTo(page, groupURL); err != nil {
		return fmt.Errorf("createGroupPost navigate: %w", err)
	}

	postBoxSels := []string{
		`div[aria-label*="Write something"]`,
		`div[aria-label*="Create a post"]`,
		`div[data-pagelet*="GroupFeed"] div[role="button"]`,
	}
	var clicked bool
	for _, sel := range postBoxSels {
		raw, err := evalJS(page, fmt.Sprintf(`
			(function(){var el=document.querySelector(%q);if(el){el.click();return true;}return false;})()
		`, sel))
		if err == nil {
			if ok, _ := raw.(bool); ok {
				clicked = true
				break
			}
		}
	}
	if !clicked {
		return fmt.Errorf("createGroupPost: could not open post dialog")
	}
	time.Sleep(2 * time.Second)

	if imagePath != "" {
		if err := fc.attachImageToPostDialog(page, imagePath); err != nil {
			log.Printf("[Facebook:%s] createGroupPost image attach failed (continuing): %v", fc.accountID, err)
		}
	}

	textField := `div[contenteditable="true"][aria-label]`
	if err := waitVisible(page, textField, actionTimeout); err != nil {
		return fmt.Errorf("createGroupPost type text: %w", err)
	}
	if err := fillSel(page, textField, text); err != nil {
		return fmt.Errorf("createGroupPost type text: %w", err)
	}
	time.Sleep(800 * time.Millisecond)

	_, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').trim();
				if(lbl==='Post'||lbl==='Share'){b.click();return true;}
			}
			return false;
		})()
	`)
	if err != nil {
		return fmt.Errorf("createGroupPost: could not click Post button")
	}
	time.Sleep(3 * time.Second)
	log.Printf("[Facebook:%s] group post created in group %s", fc.accountID, groupID)
	return nil
}

func (fc *FacebookCollector) createPagePost(page context.Context, pageID, text, imagePath string) error {
	if fc.pageConfig != nil {
		if err := fc.switchToPage(page, fc.pageConfig.PageName); err != nil {
			log.Printf("[Facebook:%s] createPagePost switch failed, posting directly: %v", fc.accountID, err)
		}
	}

	pageURL := fmt.Sprintf("https://www.facebook.com/%s", pageID)
	if err := fc.navigateTo(page, pageURL); err != nil {
		return fmt.Errorf("createPagePost navigate: %w", err)
	}

	clickedRaw, err := evalJS(page, `
		(function(){
			var sels=['div[aria-label*="Write something"]','div[aria-label*="Create a post"]','div[role="button"][tabindex="0"]'];
			for(var s of sels){var el=document.querySelector(s);if(el){el.click();return true;}}
			return false;
		})()
	`)
	if err != nil {
		return fmt.Errorf("createPagePost evaluate composer: %w", err)
	}
	clicked, ok := clickedRaw.(bool)
	if !ok || !clicked {
		return fmt.Errorf("createPagePost: could not open post dialog")
	}
	time.Sleep(2 * time.Second)

	if imagePath != "" {
		if err := fc.attachImageToPostDialog(page, imagePath); err != nil {
			log.Printf("[Facebook:%s] createPagePost image attach failed (continuing): %v", fc.accountID, err)
		}
	}

	textField := `div[contenteditable="true"][aria-label]`
	if err := waitVisible(page, textField, actionTimeout); err != nil {
		return fmt.Errorf("createPagePost type text: %w", err)
	}
	if err := fillSel(page, textField, text); err != nil {
		return fmt.Errorf("createPagePost type text: %w", err)
	}
	time.Sleep(800 * time.Millisecond)

	_, err = evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').trim();
				if(lbl==='Post'||lbl==='Share Now'||lbl==='Publish'){b.click();return true;}
			}
			return false;
		})()
	`)
	if err != nil {
		return fmt.Errorf("createPagePost: could not click Post/Publish button: %w", err)
	}
	time.Sleep(3 * time.Second)
	log.Printf("[Facebook:%s] page post created on page %s", fc.accountID, pageID)
	return nil
}

func (fc *FacebookCollector) createStory(page context.Context, text, imagePath string) error {
	if err := fc.navigateTo(page, "https://www.facebook.com"); err != nil {
		return err
	}

	clickedRaw, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],a');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('create story')||lbl.includes('add to story')){b.click();return true;}
			}
			return false;
		})()
	`)
	if err != nil {
		return fmt.Errorf("createStory evaluate: %w", err)
	}
	clicked, ok := clickedRaw.(bool)
	if !ok || !clicked {
		return fmt.Errorf("createStory: could not open story creator")
	}
	time.Sleep(2 * time.Second)

	if imagePath != "" {
		fileSel := `input[type="file"]`
		if err := waitVisible(page, fileSel, 10*time.Second); err == nil {
			_ = setInputFilesSel(page, fileSel, []string{imagePath})
			time.Sleep(2 * time.Second)
		}
	}

	if text != "" {
		textAddedRaw, _ := evalJS(page, `
			(function(){
				var btns=document.querySelectorAll('div[role="button"],button');
				for(var b of btns){
					var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
					if(lbl.includes('add text')||lbl.includes('text')){b.click();return true;}
				}
				return false;
			})()
		`)
		if added, _ := textAddedRaw.(bool); added {
			time.Sleep(1 * time.Second)
			textFieldSel := `div[contenteditable="true"],textarea`
			if err := waitVisible(page, textFieldSel, 30*time.Second); err == nil {
				_ = fillSel(page, textFieldSel, text)
			}
		}
	}

	_, err = evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('share to story')||lbl==='share'){b.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(3 * time.Second)
	log.Printf("[Facebook:%s] story created", fc.accountID)
	return err
}

func (fc *FacebookCollector) createReel(page context.Context, videoPath, caption string) error {
	if err := fc.navigateTo(page, "https://www.facebook.com/reels/create"); err != nil {
		return fmt.Errorf("createReel navigate: %w", err)
	}
	time.Sleep(2 * time.Second)

	fileSel := `input[type="file"]`
	if err := waitVisible(page, fileSel, 15*time.Second); err != nil {
		return fmt.Errorf("createReel upload: %w", err)
	}
	if err := setInputFilesSel(page, fileSel, []string{videoPath}); err != nil {
		return fmt.Errorf("createReel upload: %w", err)
	}
	time.Sleep(3 * time.Second)

	if caption != "" {
		captionSel := `div[aria-label*="caption"],div[contenteditable="true"],textarea`
		if err := waitVisible(page, captionSel, 30*time.Second); err == nil {
			_ = fillSel(page, captionSel, caption)
		}
	}

	_, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('publish')||lbl.includes('share')||lbl==='post'){b.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(3 * time.Second)
	log.Printf("[Facebook:%s] reel published", fc.accountID)
	return err
}

func (fc *FacebookCollector) stepShare(page context.Context, step shared.InstructionStep) error {
	postURL, _ := step.Options["post_url"].(string)
	if postURL == "" {
		postURL = step.Value
	}

	if postURL != "" && strings.HasPrefix(postURL, "http") {
		if err := fc.navigateTo(page, postURL); err != nil {
			return fmt.Errorf("stepShare navigate: %w", err)
		}
	}

	shareTarget, _ := step.Options["share_to"].(string)

	shareClickedRaw, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button,span');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase().trim();
				if(lbl==='share'||lbl.includes('share')){b.click();return true;}
			}
			return false;
		})()
	`)
	if err != nil || shareClickedRaw == nil {
		return fmt.Errorf("stepShare: could not find share button")
	}
	if ok, _ := shareClickedRaw.(bool); !ok {
		return fmt.Errorf("stepShare: could not find share button")
	}
	time.Sleep(1500 * time.Millisecond)

	if shareTarget != "" {
		_, _ = evalJS(page, fmt.Sprintf(`
			(function(){
				var opts=document.querySelectorAll('div[role="menuitem"],div[role="option"]');
				for(var o of opts){
					var lbl=(o.getAttribute('aria-label')||o.textContent||'').toLowerCase();
					if(lbl.includes(%q)){o.click();return true;}
				}
				return false;
			})()
		`, strings.ToLower(shareTarget)))
		time.Sleep(1 * time.Second)
	}

	message, _ := step.Options["message"].(string)
	if message != "" {
		textFieldSel := `div[contenteditable="true"],textarea`
		if err := waitVisible(page, textFieldSel, 30*time.Second); err == nil {
			_ = fillSel(page, textFieldSel, message)
		}
	}

	_, err = evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').trim();
				if(lbl==='Post'||lbl==='Share'||lbl==='Share Now'){b.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(2 * time.Second)
	log.Printf("[Facebook:%s] stepShare: shared successfully", fc.accountID)
	return err
}

func (fc *FacebookCollector) stepSearch(page context.Context, step shared.InstructionStep) error {
	query, _ := step.Options["query"].(string)
	if query == "" {
		query = step.Value
	}
	if query == "" {
		return fmt.Errorf("stepSearch: query required")
	}

	searchType, _ := step.Options["search_type"].(string)
	searchURL := fmt.Sprintf("https://www.facebook.com/search/%s?q=%s", searchType, query)
	if searchType == "" {
		searchURL = fmt.Sprintf("https://www.facebook.com/search/top?q=%s", query)
	}

	if err := fc.navigateTo(page, searchURL); err != nil {
		return fmt.Errorf("stepSearch navigate: %w", err)
	}
	time.Sleep(2 * time.Second)

	raw, err := evalJS(page, `
		(function(){
			var out=[];
			document.querySelectorAll('[role="article"],[data-testid*="result"]').forEach(function(el){
				var titleEl=el.querySelector('h2,h3,strong,a[role="link"]');
				var descEl=el.querySelector('[dir="auto"],span');
				var linkEl=el.querySelector('a[href*="facebook.com"]');
				out.push({
					title:titleEl?titleEl.innerText.trim():'',
					description:descEl?descEl.innerText.trim():'',
					url:linkEl?linkEl.href:''
				});
			});
			return JSON.stringify(out.slice(0,20));
		})()
	`)
	if err != nil {
		return err
	}
	if s, ok := raw.(string); ok {
		if step.Options == nil {
			step.Options = make(map[string]interface{})
		}
		step.Options["result"] = s
	}
	log.Printf("[Facebook:%s] stepSearch: query=%q results stored", fc.accountID, query)
	return nil
}

func (fc *FacebookCollector) stepUpload(page context.Context, step shared.InstructionStep) error {
	filePath, _ := step.Options["file_path"].(string)
	imageURL, _ := step.Options["image_url"].(string)
	caption, _ := step.Options["caption"].(string)
	if caption == "" {
		caption = step.Value
	}

	if imageURL != "" && filePath == "" {
		local, err := fc.downloadExternalImage(imageURL)
		if err != nil {
			return fmt.Errorf("stepUpload download: %w", err)
		}
		filePath = local
	}
	if filePath == "" {
		return fmt.Errorf("stepUpload: file_path or image_url required")
	}

	recipient, _ := step.Options["to"].(string)
	if recipient != "" {
		convURL := recipient
		if !strings.HasPrefix(recipient, "http") {
			convURL = fmt.Sprintf("https://www.facebook.com/messages/t/%s", recipient)
		}
		if err := fc.navigateTo(page, convURL); err != nil {
			return fmt.Errorf("stepUpload navigate: %w", err)
		}
		fc.jitter(1500, 2500)
		return fc.attachImageToConversation(page, filePath)
	}

	return fc.createPost(page, caption, filePath)
}

func (fc *FacebookCollector) stepDownload(page context.Context, step shared.InstructionStep) error {
	url, _ := step.Options["url"].(string)
	if url == "" {
		url = step.Value
	}
	if url == "" {
		return fmt.Errorf("stepDownload: url required")
	}

	savePath, _ := step.Options["save_path"].(string)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Get(url)
	if err != nil {
		return fmt.Errorf("stepDownload fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stepDownload: HTTP %d", resp.StatusCode)
	}

	if savePath == "" {
		h := sha256.Sum256([]byte(url))
		savePath = filepath.Join(fc.mediaDir(), hex.EncodeToString(h[:])[:16]+".bin")
		os.MkdirAll(fc.mediaDir(), 0755)
	}

	f, err := os.Create(savePath)
	if err != nil {
		return fmt.Errorf("stepDownload create file: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("stepDownload write: %w", err)
	}

	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	step.Options["local_path"] = savePath
	log.Printf("[Facebook:%s] stepDownload: %s → %s", fc.accountID, url, savePath)
	return nil
}

func (fc *FacebookCollector) stepSave(page context.Context, step shared.InstructionStep) error {
	postURL, _ := step.Options["post_url"].(string)
	if postURL == "" {
		postURL = step.Value
	}

	if postURL != "" && strings.HasPrefix(postURL, "http") {
		if err := fc.navigateTo(page, postURL); err != nil {
			return fmt.Errorf("stepSave navigate: %w", err)
		}
	}

	savedRaw, err := evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button,span[role="button"]');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase().trim();
				if(lbl.includes('save post')||lbl.includes('save')){b.click();return true;}
			}
			var menuBtns=document.querySelectorAll('[aria-label="More"],[aria-label*="options"]');
			if(menuBtns.length>0){menuBtns[0].click();return false;}
			return false;
		})()
	`)
	if err == nil {
		if saved, _ := savedRaw.(bool); !saved {
			time.Sleep(1 * time.Second)
			_, _ = evalJS(page, `
				(function(){
					var items=document.querySelectorAll('div[role="menuitem"],li');
					for(var i of items){
						var lbl=(i.getAttribute('aria-label')||i.textContent||'').toLowerCase();
						if(lbl.includes('save')){i.click();return true;}
					}
					return false;
				})()
			`)
		}
	}
	log.Printf("[Facebook:%s] stepSave: save attempted", fc.accountID)
	return nil
}

func (fc *FacebookCollector) stepAPICall(page context.Context, step shared.InstructionStep) error {
	targetURL := step.Value
	if u, ok := step.Options["url"].(string); ok && u != "" {
		targetURL = u
	}
	if targetURL == "" {
		return fmt.Errorf("stepAPICall: url required")
	}

	method, _ := step.Options["method"].(string)
	if method == "" {
		method = http.MethodGet
	}
	method = strings.ToUpper(method)

	var bodyReader io.Reader
	if body, ok := step.Options["body"].(string); ok && body != "" {
		bodyReader = strings.NewReader(body)
	}

	req, err := http.NewRequestWithContext(context.Background(), method, targetURL, bodyReader)
	if err != nil {
		return fmt.Errorf("stepAPICall build request: %w", err)
	}

	if headers, ok := step.Options["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			if vs, ok := v.(string); ok {
				req.Header.Set(k, vs)
			}
		}
	}
	if req.Header.Get("Content-Type") == "" && bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("stepAPICall request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("stepAPICall read response: %w", err)
	}

	log.Printf("[Facebook:%s] stepAPICall %s %s → HTTP %d (%d bytes)",
		fc.accountID, method, targetURL, resp.StatusCode, len(respBytes))

	if step.Options == nil {
		step.Options = make(map[string]interface{})
	}
	step.Options["result"] = string(respBytes)
	step.Options["status_code"] = resp.StatusCode

	if resp.StatusCode >= 400 {
		return fmt.Errorf("stepAPICall: server returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	return nil
}

// ---------- UI helpers ----------

func (fc *FacebookCollector) sendMessageInContext(page context.Context, msg string) error {
	var contextType string
	raw, _ := evalJS(page, `
		(function(){
			if(document.querySelector('[aria-label="Message"]'))return"messages";
			if(document.querySelector('[aria-label*="comment"],[aria-label*="Comment"]'))return"comments";
			return"unknown";
		})()
	`)
	if s, ok := raw.(string); ok {
		contextType = s
	}

	var sel, sendSel string
	switch contextType {
	case "messages":
		sel = `div[aria-label="Message"],textarea[aria-label*="message"]`
		sendSel = `div[aria-label="Send"],[aria-label*="Send"]`
	case "comments":
		sel = `[aria-label*="comment"],textarea[aria-label*="comment"]`
		sendSel = `[aria-label="Comment"],button[type="submit"]`
	default:
		sel = `div[aria-label="Message"],textarea`
		sendSel = `div[aria-label="Send"],button[type="submit"]`
	}

	if err := waitVisible(page, sel, actionTimeout); err != nil {
		return err
	}
	if err := fillSel(page, sel, msg); err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond)

	if err := waitVisible(page, sendSel, actionTimeout); err != nil {
		return err
	}
	return clickSel(page, sendSel)
}

func (fc *FacebookCollector) attachImageToPostDialog(page context.Context, imagePath string) error {
	_, _ = evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button,label');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.textContent||'').toLowerCase();
				if(lbl.includes('photo')||lbl.includes('video')||lbl.includes('image')){b.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(1 * time.Second)

	fileSel := `input[type="file"]`
	if err := waitVisible(page, fileSel, 10*time.Second); err != nil {
		return err
	}
	if err := setInputFilesSel(page, fileSel, []string{imagePath}); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	return nil
}

func (fc *FacebookCollector) attachImageToConversation(page context.Context, imagePath string) error {
	_, _ = evalJS(page, `
		(function(){
			var btns=document.querySelectorAll('div[role="button"],button,label,i');
			for(var b of btns){
				var lbl=(b.getAttribute('aria-label')||b.title||'').toLowerCase();
				if(lbl.includes('photo')||lbl.includes('attach')||lbl.includes('image')){b.click();return true;}
			}
			return false;
		})()
	`)
	time.Sleep(500 * time.Millisecond)

	fileSel := `input[type="file"]`
	if err := waitVisible(page, fileSel, 10*time.Second); err != nil {
		return err
	}
	if err := setInputFilesSel(page, fileSel, []string{imagePath}); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	return nil
}

func (fc *FacebookCollector) downloadExternalImage(imageURL string) (string, error) {
	if imageURL == "" {
		return "", fmt.Errorf("empty URL")
	}
	dir := fc.mediaDir()
	os.MkdirAll(dir, 0755)
	h := sha256.Sum256([]byte(imageURL))
	path := filepath.Join(dir, hex.EncodeToString(h[:])[:16]+".jpg")

	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return path, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if os.IsExist(err) {
		return path, nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Get(imageURL)
	if err != nil {
		os.Remove(path)
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Remove(path)
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

// ---------- Setters ----------

func (fc *FacebookCollector) SetPageConfig(p *config.FacebookPage)   { fc.pageConfig = p }
func (fc *FacebookCollector) SetGroupConfig(g *config.FacebookGroup) { fc.groupConfig = g }

// ---------- Enrichment helpers ----------

func (fc *FacebookCollector) genUserID(name string) string {
	return "fb_" + strings.ToLower(strings.NewReplacer(" ", "_", ".", "_").Replace(name))
}

func (fc *FacebookCollector) extractPostID(url string) string {
	parts := strings.Split(url, "/")
	for i, p := range parts {
		if p == "posts" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return fmt.Sprintf("post_%d", time.Now().UnixNano())
}

var fbProductPatterns = []*regexp.Regexp{
	regexp.MustCompile(`SKU\s*=\s*["']([^"']+)["']`),
	regexp.MustCompile(`SKU\s*=\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)product\s*id\s*:\s*([^\s<>"']+)`),
	regexp.MustCompile(`(?i)sku\s*:\s*([^\s<>"']+)`),
}

func (fc *FacebookCollector) extractProductID(text string) string {
	for _, p := range fbProductPatterns {
		if m := p.FindStringSubmatch(text); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}

func (fc *FacebookCollector) buildEnrichment(platformUserID string) (
	userData map[string]interface{}, recentMsgs []string, drop bool, err error,
) {
	pu, err := fc.lookupPlatformUser(platformUserID)
	if err != nil {
		return nil, nil, false, nil
	}
	if !pu.Found {
		return nil, nil, false, nil
	}
	if pu.IsBlocked {
		return nil, nil, true, nil
	}
	userData = map[string]interface{}{
		"last_intent":  pu.LastIntent,
		"is_blocked":   false,
		"display_name": pu.DisplayName,
	}
	recentMsgs, _ = fc.getRecentMessages(pu.ID)
	return userData, recentMsgs, false, nil
}

func (fc *FacebookCollector) lookupPlatformUser(platformUserID string) (*platformUserData, error) {
	if fc.db == nil {
		return &platformUserData{}, nil
	}
	var id, displayName, lastIntent sql.NullString
	var isBlocked sql.NullInt64
	err := fc.db.QueryRow(`
		SELECT id, display_name, is_blocked, last_intent
		FROM platform_users
		WHERE platform = ? AND platform_user_id = ?
	`, "facebook", platformUserID).Scan(&id, &displayName, &isBlocked, &lastIntent)
	if err == sql.ErrNoRows {
		return &platformUserData{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &platformUserData{
		ID:          id.String,
		DisplayName: displayName.String,
		IsBlocked:   isBlocked.Int64 == 1,
		LastIntent:  lastIntent.String,
		Found:       true,
	}, nil
}

func (fc *FacebookCollector) getRecentMessages(userID string) ([]string, error) {
	if fc.db == nil || userID == "" {
		return nil, nil
	}
	rows, err := fc.db.Query(`
		SELECT message_text FROM (
			SELECT message_text, received_at
			FROM messages
			WHERE user_id = ? AND direction = 'incoming'
			ORDER BY received_at DESC LIMIT 3
		) sub ORDER BY received_at ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t sql.NullString
		if rows.Scan(&t) == nil && t.Valid {
			out = append(out, t.String)
		}
	}
	return out, nil
}

func (fc *FacebookCollector) getProductData(productID string) (map[string]interface{}, error) {
	if fc.db == nil || productID == "" {
		return nil, nil
	}
	const exactQ = `
		SELECT id,sku,name,description,category,subcategory,tags,
		       price,price_per_pack,quantity_per_pack,currency,
		       stock,reserved_stock,low_stock_threshold,
		       image_url,thumbnail_url,weight_kg,dimensions,
		       is_active,is_featured,metadata,created_at,updated_at
		FROM products WHERE sku=? AND is_active=1 LIMIT 1`
	const fuzzyQ = `
		SELECT id,sku,name,description,category,subcategory,tags,
		       price,price_per_pack,quantity_per_pack,currency,
		       stock,reserved_stock,low_stock_threshold,
		       image_url,thumbnail_url,weight_kg,dimensions,
		       is_active,is_featured,metadata,created_at,updated_at
		FROM products WHERE name LIKE ? AND is_active=1 LIMIT 1`

	var (
		id, sku, name, desc, cat, subcat, tags sql.NullString
		price, pricePerPack, weightKg          sql.NullFloat64
		qtyPerPack                             sql.NullInt64
		currency                               sql.NullString
		stock, rStock, lowStock                sql.NullInt64
		imgURL, thumbURL, dims                 sql.NullString
		isActive, isFeatured                   sql.NullBool
		meta                                   sql.NullString
		createdAt, updatedAt                   sql.NullTime
	)
	scanInto := func(row *sql.Row) error {
		return row.Scan(
			&id, &sku, &name, &desc, &cat, &subcat, &tags,
			&price, &pricePerPack, &qtyPerPack, &currency,
			&stock, &rStock, &lowStock,
			&imgURL, &thumbURL, &weightKg, &dims,
			&isActive, &isFeatured, &meta, &createdAt, &updatedAt,
		)
	}

	err := scanInto(fc.db.QueryRow(exactQ, productID))
	if err == sql.ErrNoRows && len(productID) >= 4 {
		err = scanInto(fc.db.QueryRow(fuzzyQ, "%"+productID+"%"))
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
		"stock": stock.Int64, "reserved_stock": rStock.Int64, "low_stock_threshold": lowStock.Int64,
		"image_url": imgURL.String, "thumbnail_url": thumbURL.String,
		"weight_kg": weightKg.Float64, "dimensions": dims.String,
		"is_active": isActive.Bool, "is_featured": isFeatured.Bool,
		"metadata": unmarshal(meta), "created_at": createdAt.Time, "updated_at": updatedAt.Time,
	}, nil
}

// ---------- Media helpers ----------

func (fc *FacebookCollector) mediaDir() string {
	cfg := fc.configMgr.GetConfig()
	if cfg != nil && cfg.Paths.PostImages != "" {
		return cfg.Paths.PostImages
	}
	return "./media/images"
}

func (fc *FacebookCollector) imageRecognitionEnabled() bool {
	cfg := fc.configMgr.GetConfig()
	return cfg != nil && cfg.ImageRecognition.Enabled
}

func (fc *FacebookCollector) downloadImages(urls []string, t NotificationType) []string {
	if !fc.imageRecognitionEnabled() {
		return nil
	}
	wantTypes := map[NotificationType]bool{
		NotificationTypeMessage: true,
		NotificationTypeComment: true,
		NotificationTypeMention: true,
	}
	if !wantTypes[t] {
		return nil
	}
	dir := fc.mediaDir()
	os.MkdirAll(dir, 0755)
	var out []string
	for _, u := range urls {
		if u == "" {
			continue
		}
		hash := sha256.Sum256([]byte(u))
		path := filepath.Join(dir, hex.EncodeToString(hash[:])[:16]+".jpg")
		if _, err := os.Stat(path); err == nil {
			out = append(out, path)
			continue
		}
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(u)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		f, err := os.Create(path)
		if err != nil {
			resp.Body.Close()
			continue
		}
		io.Copy(f, resp.Body)
		f.Close()
		resp.Body.Close()
		out = append(out, path)
	}
	return out
}

func (fc *FacebookCollector) buildMediaAttachments(urls, localPaths []string, t NotificationType) []MediaAttachment {
	if !fc.imageRecognitionEnabled() {
		return nil
	}
	var out []MediaAttachment
	for i, u := range urls {
		if u == "" {
			continue
		}
		thumb := ""
		if i < len(localPaths) {
			thumb = localPaths[i]
		}
		out = append(out, MediaAttachment{
			ID:        fmt.Sprintf("img_%d_%d", time.Now().UnixNano(), i),
			Type:      "image",
			URL:       u,
			Thumbnail: thumb,
		})
	}
	return out
}
