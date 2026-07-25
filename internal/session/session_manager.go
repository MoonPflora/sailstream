package session

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/mileusna/viber"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"

	"sailstream/internal/config"
	"sailstream/internal/database"
	"sailstream/internal/enviroment"
)

const (
	StateAuthenticated = "authenticated"
	StateUnauthentic   = "unauthenticated"
)

// ------------------------------------------------------------
// Cookie, Session, and Request types
// ------------------------------------------------------------

type Cookie struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Domain   string `json:"domain"`
	Path     string `json:"path"`
	Expires  int64  `json:"expires"`
	Secure   bool   `json:"secure"`
	HTTPOnly bool   `json:"httpOnly"`
}

type Session struct {
	PlatformID string            `json:"platform_id"`
	Subtype    string            `json:"subtype"`
	AccountID  string            `json:"account_id"`
	State      string            `json:"state"`
	Cookies    []Cookie          `json:"cookies"`
	CreatedAt  time.Time         `json:"created_at"`
	LastUsed   time.Time         `json:"last_used"`
	ExpiresAt  time.Time         `json:"expires_at"`
	Metadata   map[string]string `json:"metadata"`
	Corrupted  bool              `json:"corrupted,omitempty"`
}

type SessionRequest struct {
	PlatformID string `json:"platform_id"`
	Subtype    string `json:"subtype"`
	AccountID  string `json:"account_id"`
	ForceNew   bool   `json:"force_new"`
}

type PlatformError struct {
	PlatformID string    `json:"platform_id"`
	Subtype    string    `json:"subtype"`
	AccountID  string    `json:"account_id"`
	Error      string    `json:"error"`
	Type       string    `json:"type"`
	Timestamp  time.Time `json:"timestamp"`
	Severity   string    `json:"severity"`
}

// ------------------------------------------------------------
// Storage
// ------------------------------------------------------------

type Storage struct {
	cacheDir    string
	sessionsDir string
	profilesDir string
	mu          sync.RWMutex
}

func NewStorage(cacheDir, sessionsDir, profilesDir string) *Storage {
	os.MkdirAll(sessionsDir, 0750)
	os.MkdirAll(profilesDir, 0750)
	return &Storage{
		cacheDir:    cacheDir,
		sessionsDir: sessionsDir,
		profilesDir: profilesDir,
	}
}

func (s *Storage) sessionPath(platformID, subtype string) string {
	return filepath.Join(s.sessionsDir, fmt.Sprintf("%s_%s.json", platformID, subtype))
}

func (s *Storage) ProfilePathByID(profileID string) string {
	rel := filepath.Join(s.profilesDir, profileID)
	abs, err := filepath.Abs(rel)
	if err != nil {
		log.Printf("[Session:Storage] WARNING: cannot resolve abs path for %q: %v", profileID, err)
		return rel
	}
	return abs
}

func (s *Storage) Save(sess *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(sess, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal session: %w", err)
	}
	return os.WriteFile(s.sessionPath(sess.PlatformID, sess.Subtype), data, 0640)
}

func (s *Storage) GetSessionsDir() string { return s.sessionsDir }

func (s *Storage) Load(platformID, subtype string) (*Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := os.ReadFile(s.sessionPath(platformID, subtype))
	if err != nil {
		return nil, errors.New("session not found")
	}
	var sess Session
	if err := json.Unmarshal(data, &sess); err != nil {
		return nil, fmt.Errorf("session file corrupted: %w", err)
	}
	return &sess, nil
}

func (s *Storage) Delete(platformID, subtype string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.sessionPath(platformID, subtype)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("[Session:Storage] WARNING: could not delete session file %s: %v", path, err)
	}
}

func (s *Storage) DeleteProfile(profileID string) {
	path := s.ProfilePathByID(profileID)
	if err := os.RemoveAll(path); err != nil {
		log.Printf("[Session:Storage] WARNING: could not delete profile %s: %v", path, err)
	}
}

// ------------------------------------------------------------
// Helper functions (ID generation, profile management, credential extraction)
// ------------------------------------------------------------

func generateNumericID() string {
	n, err := rand.Int(rand.Reader, big.NewInt(90_000_000))
	if err != nil {
		return fmt.Sprintf("%08d", time.Now().UnixNano()%90_000_000+10_000_000)
	}
	return fmt.Sprintf("%08d", n.Int64()+10_000_000)
}

func (m *Manager) getOrCreateProfileID(platformID, subtype string) string {
	if existing, err := m.storage.Load(platformID, subtype); err == nil &&
		existing.Metadata != nil &&
		existing.Metadata["profile_id"] != "" {
		return existing.Metadata["profile_id"]
	}

	m.profileIDMu.Lock()
	defer m.profileIDMu.Unlock()

	prefix := fmt.Sprintf("%s_%s_", platformID, subtype)
	if entries, err := os.ReadDir(m.storage.profilesDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
				continue
			}
			profilePath := m.storage.ProfilePathByID(entry.Name())
			subEntries, readErr := os.ReadDir(profilePath)
			if readErr == nil && len(subEntries) > 0 {
				log.Printf("[Session] Found existing profile dir %q for %s:%s — reusing", entry.Name(), platformID, subtype)
				return entry.Name()
			}
		}
	}
	id := fmt.Sprintf("%s_%s_%s", platformID, subtype, generateNumericID())
	log.Printf("[Session] Minted new profile ID %q for %s:%s", id, platformID, subtype)
	return id
}

func (m *Manager) GetProfilePath(sess *Session) string {
	if sess != nil && sess.Metadata != nil && sess.Metadata["profile_id"] != "" {
		return m.storage.ProfilePathByID(sess.Metadata["profile_id"])
	}
	return ""
}

func (m *Manager) GetProfilePathForAccount(platformID, subtype, _ string) string {
	sess, err := m.storage.Load(platformID, subtype)
	if err == nil && sess != nil &&
		sess.Metadata != nil &&
		sess.Metadata["profile_id"] != "" {
		return m.storage.ProfilePathByID(sess.Metadata["profile_id"])
	}
	return ""
}

func (m *Manager) GetProfileID(platformID, subtype string) string {
	sess, err := m.storage.Load(platformID, subtype)
	if err == nil && sess != nil &&
		sess.Metadata != nil &&
		sess.Metadata["profile_id"] != "" {
		return sess.Metadata["profile_id"]
	}
	return ""
}

func (m *Manager) GetStorage() *Storage { return m.storage }

// EnsureIsolatedProfile makes sure destDir contains a usable, app-owned
// copy of the environment's real default browser profile (duplicated once,
// lock artifacts skipped, reused afterwards). Exported so callers outside
// this package — e.g. platform collectors driving their own chromedp
// session — never launch a browser directly against the live profile,
// which trips Chrome's singleton-instance guard ("Opening in existing
// browser session.") whenever the real browser is already open.
func (m *Manager) EnsureIsolatedProfile(destDir string) error {
	return ensureIsolatedProfile(m.env, destDir)
}

// GetOrCreateIsolatedProfilePath returns the isolated, app-owned profile
// directory for platformID:subtype, minting a profile ID (and its on-disk
// directory) if one doesn't exist yet. Unlike GetProfilePathForAccount /
// GetProfileID, this never returns "" — it's the right call for anything
// about to launch its own chromedp session against this account.
func (m *Manager) GetOrCreateIsolatedProfilePath(platformID, subtype string) string {
	profileID := m.getOrCreateProfileID(platformID, subtype)
	return m.storage.ProfilePathByID(profileID)
}

func (m *Manager) ResolveAccountID(platformID, subtype, accountID string) string {
	return m.resolveAccountID(platformID, subtype, accountID)
}

var cfgMu sync.RWMutex

// getCreds extracts credentials from config (unchanged)
func getCreds(cfg *config.Config, platformID, subtype string) (user, pass string) {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	platform, ok := cfg.Platforms[platformID]
	if !ok || !platform.Enabled {
		return "", ""
	}
	for _, st := range platform.Subtypes {
		if st.ID == subtype && st.Enabled && st.Auth != nil {
			for _, key := range []string{
				"username", "phone", "bot_token", "access_token", "bearer_token",
				"acct_email", "email",
			} {
				if v, ok2 := st.Auth[key]; ok2 && fmt.Sprintf("%v", v) != "" {
					user = fmt.Sprintf("%v", v)
					break
				}
			}
			for _, key := range []string{"password", "acct_password"} {
				if v, ok2 := st.Auth[key]; ok2 && fmt.Sprintf("%v", v) != "" {
					pass = fmt.Sprintf("%v", v)
					break
				}
			}
			if user != "" || pass != "" {
				return
			}
		}
	}
	switch platformID {
	case "facebook":
		if platform.Facebook != nil && platform.Facebook.Account != nil {
			return strings.TrimSpace(platform.Facebook.Account.Email),
				strings.TrimSpace(platform.Facebook.Account.Password)
		}
	case "instagram":
		if platform.Instagram != nil && platform.Instagram.Account != nil {
			return strings.TrimSpace(platform.Instagram.Account.Username),
				strings.TrimSpace(platform.Instagram.Account.Password)
		}
	case "twitter":
		if platform.Twitter != nil {
			if platform.Twitter.BearerToken != "" {
				return platform.Twitter.BearerToken, ""
			}
			return platform.Twitter.AccessToken, platform.Twitter.AccessSecret
		}
	case "whatsapp":
		if platform.WhatsApp != nil {
			return strings.TrimSpace(platform.WhatsApp.Phone),
				strings.TrimSpace(platform.WhatsApp.Password)
		}
	case "viber":
		if platform.Viber != nil && platform.Viber.BotToken != "" {
			return platform.Viber.BotToken, ""
		}
		if tok := os.Getenv("VIBER_BOT_TOKEN"); tok != "" {
			return tok, ""
		}
	case "telegram":
		// handled separately
	case "tiktok":
		if platform.TikTok != nil {
			return strings.TrimSpace(platform.TikTok.Username),
				strings.TrimSpace(platform.TikTok.Password)
		}
	}
	return "", ""
}

func browserFlags(headless bool, profilePath string, env *enviroment.Environment) []chromedp.ExecAllocatorOption {
	flags := []chromedp.ExecAllocatorOption{
		chromedp.ExecPath(env.GetBrowserPath()),
		chromedp.UserDataDir(profilePath),
		chromedp.Flag("profile-directory", "Default"),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("disable-dev-shm-usage", true),
	}
	if headless {
		flags = append(flags,
			chromedp.Flag("headless", true),
			chromedp.Flag("window-size", "1920,1080"),
		)
	} else {
		flags = append(flags,
			chromedp.Flag("headless", false),
			chromedp.Flag("start-maximized", true),
		)
	}
	return flags
}

func detectSecurityChallenge(ctx context.Context) (bool, string) {
	var currentURL, pageText string
	chromedp.Run(ctx, chromedp.Location(&currentURL))
	chromedp.Run(ctx, chromedp.Evaluate(
		`document.body ? (document.body.innerText || document.body.textContent || '') : ''`,
		&pageText,
	))
	urlLow := strings.ToLower(currentURL)
	txtLow := strings.ToLower(pageText)
	type sig struct{ token, label string }
	for _, s := range []sig{
		{"checkpoint", "account checkpoint"},
		{"two_step", "two-step verification"},
		{"two-step", "two-step verification"},
		{"2fa", "two-factor authentication"},
		{"captcha", "captcha challenge"},
		{"challenge", "security challenge"},
		{"security", "security check"},
		{"verify", "identity verification"},
		{"unusual", "unusual activity"},
	} {
		if strings.Contains(urlLow, s.token) {
			return true, fmt.Sprintf("%s (URL: %s)", s.label, currentURL)
		}
	}
	for _, s := range []sig{
		{"checkpoint", "account checkpoint"},
		{"two-factor", "two-factor authentication"},
		{"two factor", "two-factor authentication"},
		{"2-step", "two-step verification"},
		{"captcha", "captcha challenge"},
		{"security check", "security check"},
		{"verify your identity", "identity verification"},
		{"confirm your identity", "identity confirmation"},
		{"unusual activity", "unusual activity"},
		{"suspicious activity", "suspicious activity"},
		{"enter the code", "verification code"},
		{"verification code", "verification code"},
		{"authenticate your account", "account authentication"},
	} {
		if strings.Contains(txtLow, s.token) {
			return true, fmt.Sprintf("%s (page text)", s.label)
		}
	}
	return false, ""
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ------------------------------------------------------------
// Cookie import — manual fallback for when automated login fails
// (CAPTCHA/2FA/checkpoint). The user exports cookies from their own
// logged-in browser session (e.g. via a cookie-export extension) and
// pastes/uploads the JSON here.
// ------------------------------------------------------------

// ImportedCookie mirrors the common JSON shape produced by browser
// cookie-export extensions (e.g. Cookie-Editor / EditThisCookie).
type ImportedCookie struct {
	Name           string  `json:"name"`
	Value          string  `json:"value"`
	Domain         string  `json:"domain"`
	Path           string  `json:"path"`
	ExpirationDate float64 `json:"expirationDate"`
	Expires        float64 `json:"expires"`
	Secure         bool    `json:"secure"`
	HTTPOnly       bool    `json:"httpOnly"`
}

func hasAuthCookies(platformID string, cookies []Cookie) bool {
	switch platformID {
	case "facebook":
		c, x := false, false
		for _, ck := range cookies {
			if ck.Name == "c_user" && ck.Value != "" {
				c = true
			}
			if ck.Name == "xs" && ck.Value != "" {
				x = true
			}
		}
		return c && x
	case "instagram":
		s, d := false, false
		for _, ck := range cookies {
			if ck.Name == "sessionid" && ck.Value != "" {
				s = true
			}
			if ck.Name == "ds_user_id" && ck.Value != "" {
				d = true
			}
		}
		return s && d
	case "twitter":
		for _, ck := range cookies {
			if ck.Name == "auth_token" || ck.Name == "ct0" {
				return true
			}
		}
	case "tiktok":
		for _, ck := range cookies {
			if ck.Name == "sessionid" && ck.Value != "" {
				return true
			}
		}
	default:
		return len(cookies) > 2
	}
	return false
}

// ImportCookiesFromJSON builds an authenticated session for platformID:subtype
// from a raw cookie-export JSON blob (an array of cookie objects). This is the
// standard onboarding/renewal flow for platforms that block browser automation:
// the user logs in once in their own browser, exports cookies with an extension,
// and pastes the JSON here.
func (m *Manager) ImportCookiesFromJSON(platformID, subtype, accountID string, rawJSON []byte) (*Session, error) {
	log.Printf("[Session] Importing cookies for %s:%s (%d bytes of JSON)", platformID, subtype, len(rawJSON))
	var imported []ImportedCookie
	if err := json.Unmarshal(rawJSON, &imported); err != nil {
		log.Printf("[Session] Cookie import FAILED for %s:%s: invalid JSON: %v", platformID, subtype, err)
		return nil, fmt.Errorf("invalid cookie JSON: %w", err)
	}
	if len(imported) == 0 {
		log.Printf("[Session] Cookie import FAILED for %s:%s: JSON parsed but contained 0 cookies", platformID, subtype)
		return nil, errors.New("no cookies found in JSON")
	}
	log.Printf("[Session] Parsed %d cookie(s) from import for %s:%s", len(imported), platformID, subtype)

	cookies := make([]Cookie, 0, len(imported))
	var minExpiry float64
	for _, c := range imported {
		exp := c.ExpirationDate
		if exp == 0 {
			exp = c.Expires
		}
		if exp > 0 && (minExpiry == 0 || exp < minExpiry) {
			minExpiry = exp
		}
		cookies = append(cookies, Cookie{
			Name:     c.Name,
			Value:    c.Value,
			Domain:   c.Domain,
			Path:     c.Path,
			Expires:  int64(exp),
			Secure:   c.Secure,
			HTTPOnly: c.HTTPOnly,
		})
	}

	if !hasAuthCookies(platformID, cookies) {
		log.Printf("[Session] Cookie import FAILED for %s:%s: %d cookie(s) imported but required auth cookies are missing",
			platformID, subtype, len(cookies))
		return nil, fmt.Errorf("imported cookies for %s do not include the required auth cookies", platformID)
	}

	resolvedAccountID := m.resolveAccountID(platformID, subtype, accountID)
	if resolvedAccountID == "" {
		resolvedAccountID = generateID()
	}

	profileID := m.getOrCreateProfileID(platformID, subtype)

	expiresAt := time.Now().Add(7 * 24 * time.Hour)
	if minExpiry > 0 {
		if t := time.Unix(int64(minExpiry), 0); t.After(time.Now()) {
			expiresAt = t
		}
	}

	sess := &Session{
		PlatformID: platformID,
		Subtype:    subtype,
		AccountID:  resolvedAccountID,
		State:      StateAuthenticated,
		Cookies:    cookies,
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		ExpiresAt:  expiresAt,
		Metadata: map[string]string{
			"login_type": "cookie_import",
			"profile_id": profileID,
		},
	}
	m.persistSession(sess)
	log.Printf("[Session] Cookie import SUCCESS for %s:%s — account=%s, %d cookie(s) stored, session authenticated (expires %s)",
		platformID, subtype, resolvedAccountID, len(cookies), expiresAt.Format(time.RFC3339))
	return sess, nil
}

// ------------------------------------------------------------
// Real-profile browser login (Facebook/Instagram)
//
// The isolated profile directory is duplicated from the real browser
// profile once (reused on later runs). Launch happens against that copy,
// visibly. If not already logged in, auto-fill email/password and submit;
// handle a "Continue" confirmation prompt if shown, then check cookies.
// If still not authenticated (CAPTCHA/2FA/checkpoint), the window stays
// open for the user to finish manually while cookies are polled.
// ------------------------------------------------------------

// killBrowserProcesses forcefully kills every process for the given
// browser executable, including child/helper processes, so its profile
// lock is actually released.
func killBrowserProcesses(browserName string) {
	procNames := browserProcessNames(browserName)
	if len(procNames) == 0 {
		log.Printf("[Profile Copy] killBrowserProcesses: unknown browser name %q, nothing to kill", browserName)
		return
	}

	// Browsers like Edge/Chrome ship a "background apps" / "startup boost"
	// feature that respawns a process within seconds of being closed, to
	// make the next real launch faster. A single kill + wait can pass its
	// check right before that respawn grabs the profile's LevelDB/shader
	// caches again (this is what happened: fresh locks on
	// DawnGraphiteCache/Extension State appeared ~3s after taskkill).
	// So: kill, confirm clear, wait, confirm it's *still* clear — and if
	// something respawned, kill again. Repeat up to maxRounds.
	const maxRounds = 5
	const pollInterval = 300 * time.Millisecond
	const maxWaitPerRound = 6 * time.Second
	const stabilityCheckDelay = 1500 * time.Millisecond

	for round := 1; round <= maxRounds; round++ {
		killed := killBrowserOnce(browserName, procNames)
		log.Printf("[Profile Copy] Round %d/%d: kill command(s) issued for %s (%d succeeded). Waiting for exit...",
			round, maxRounds, browserName, killed)

		waited := time.Duration(0)
		for waited < maxWaitPerRound {
			if !isBrowserProcessRunning(procNames) {
				break
			}
			time.Sleep(pollInterval)
			waited += pollInterval
		}
		if isBrowserProcessRunning(procNames) {
			log.Printf("[Profile Copy] Round %d/%d: %s still running after %s — retrying kill", round, maxRounds, browserName, maxWaitPerRound)
			continue
		}
		log.Printf("[Profile Copy] Round %d/%d: %s exited after %s — checking it doesn't respawn...", round, maxRounds, browserName, waited.Round(time.Millisecond))

		// The respawn window: wait a bit longer, then check again. If it's
		// still gone, we're actually clear. If it's back, this was a
		// background-relaunch and we need another kill round.
		time.Sleep(stabilityCheckDelay)
		if isBrowserProcessRunning(procNames) {
			log.Printf("[Profile Copy] Round %d/%d: %s respawned in the background (startup boost?) — killing again", round, maxRounds, browserName)
			continue
		}

		log.Printf("[Profile Copy] Confirmed %s stayed closed for %s — proceeding to copy", browserName, stabilityCheckDelay)
		// Extra settle buffer: even with the process confirmed gone,
		// Windows/AV can briefly keep a handle open on files it was
		// scanning/flushing.
		settleBuffer := 800 * time.Millisecond
		time.Sleep(settleBuffer)
		return
	}
	log.Printf("[Profile Copy] WARNING: %s kept respawning after %d kill rounds — proceeding anyway, some files may fail to copy", browserName, maxRounds)
}

// killBrowserOnce issues a single kill pass (taskkill/pkill) for the given
// process names and returns how many kill commands reported success.
func killBrowserOnce(browserName string, procNames []string) int {
	killed := 0
	switch runtime.GOOS {
	case "windows":
		for _, name := range procNames {
			if !strings.HasSuffix(name, ".exe") {
				continue
			}
			out, err := exec.Command("taskkill", "/F", "/IM", name, "/T").CombinedOutput()
			if err != nil {
				log.Printf("[Profile Copy] taskkill %s: %v (%s) — likely wasn't running", name, err, strings.TrimSpace(string(out)))
				continue
			}
			log.Printf("[Profile Copy] taskkill %s succeeded: %s", name, strings.TrimSpace(string(out)))
			killed++
		}
	default:
		for _, name := range procNames {
			if strings.HasSuffix(name, ".exe") {
				continue
			}
			err := exec.Command("pkill", "-9", "-f", name).Run()
			if err != nil {
				log.Printf("[Profile Copy] pkill -f %s: %v — likely wasn't running", name, err)
				continue
			}
			log.Printf("[Profile Copy] pkill -f %s succeeded", name)
			killed++
		}
	}
	return killed
}

// isBrowserProcessRunning checks whether any of the given executable names
// are still present in the process list, so we can wait on an actual
// condition instead of guessing with a fixed sleep.
func isBrowserProcessRunning(procNames []string) bool {
	switch runtime.GOOS {
	case "windows":
		for _, name := range procNames {
			if !strings.HasSuffix(name, ".exe") {
				continue
			}
			out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("IMAGENAME eq %s", name)).CombinedOutput()
			if err != nil {
				continue
			}
			if strings.Contains(strings.ToLower(string(out)), strings.ToLower(name)) {
				return true
			}
		}
	default:
		for _, name := range procNames {
			if strings.HasSuffix(name, ".exe") {
				continue
			}
			if err := exec.Command("pgrep", "-f", name).Run(); err == nil {
				return true // pgrep exits 0 when it finds a match
			}
		}
	}
	return false
}

// clearProfileLocks removes the Chromium lock artifacts that survive a
// process kill and cause subsequent launches to silently use a blank
// throwaway profile instead of the real one.
func clearProfileLocks(profilePath string) {
	if profilePath == "" {
		return
	}
	lockNames := []string{"SingletonLock", "SingletonSocket", "SingletonCookie", "lockfile"}
	for _, name := range lockNames {
		path := filepath.Join(profilePath, name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("[Login] Could not remove lock file %s: %v", path, err)
		}
	}
}

// heavyProfileDirs are cache/large directories inside a Chromium "Default"
// profile folder that are safe to skip when duplicating a profile — they're
// unnecessary for auth cookies and can be large enough to make the copy slow.
var heavyProfileDirs = map[string]bool{
	"Cache":               true,
	"Code Cache":          true,
	"GPUCache":            true,
	"Service Worker":      true,
	"blob_storage":        true,
	"GrShaderCache":       true,
	"ShaderCache":         true,
	"component_crx_cache": true,
}

// profileLockFiles are Chromium's own "another instance is using this
// profile" markers — never copy these, or the duplicate will look locked too.
var profileLockFiles = map[string]bool{
	"SingletonLock":   true,
	"SingletonSocket": true,
	"SingletonCookie": true,
	"lockfile":        true,
}

type copyStats struct {
	filesCopied  int
	filesSkipped int
	bytesCopied  int64
	dirsSkipped  int
}

// copyFileBestEffort copies a single file, retrying with backoff if it's
// still locked by another process.
func copyFileBestEffort(src, dst string, stats *copyStats) {
	const maxAttempts = 5
	backoff := 250 * time.Millisecond

	var data []byte
	var readErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		data, readErr = os.ReadFile(src)
		if readErr == nil {
			break
		}
		if !isLockedFileErr(readErr) {
			break
		}
		log.Printf("[Profile Copy] %s locked (attempt %d/%d): %v — retrying in %s", src, attempt, maxAttempts, readErr, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
	if readErr != nil {
		log.Printf("[Profile Copy] skip (unreadable after %d attempt(s)) %s: %v", maxAttempts, src, readErr)
		stats.filesSkipped++
		return
	}

	var writeErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		writeErr = os.WriteFile(dst, data, 0600)
		if writeErr == nil {
			break
		}
		if !isLockedFileErr(writeErr) {
			break
		}
		log.Printf("[Profile Copy] %s locked (attempt %d/%d): %v — retrying in %s", dst, attempt, maxAttempts, writeErr, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
	if writeErr != nil {
		log.Printf("[Profile Copy] skip (unwritable after %d attempt(s)) %s: %v", maxAttempts, dst, writeErr)
		stats.filesSkipped++
		return
	}
	stats.filesCopied++
	stats.bytesCopied += int64(len(data))
}

// isLockedFileErr reports whether err looks like a "file is in use by
// another process" condition.
func isLockedFileErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "used by another process") ||
		strings.Contains(msg, "being used by another process") ||
		strings.Contains(msg, "sharing violation") ||
		strings.Contains(msg, "resource busy") ||
		strings.Contains(msg, "text file busy") ||
		os.IsPermission(err)
}

func copyDirFiltered(src, dst string, stats *copyStats) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("[Profile Copy] skip %s: %v", path, err)
			stats.filesSkipped++
			return nil
		}
		rel, relErr := filepath.Rel(src, path)
		if relErr != nil {
			stats.filesSkipped++
			return nil
		}
		target := filepath.Join(dst, rel)

		if info.IsDir() {
			if rel != "." && heavyProfileDirs[info.Name()] {
				log.Printf("[Profile Copy] skipping cache dir: %s", rel)
				stats.dirsSkipped++
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0750)
		}
		if profileLockFiles[info.Name()] {
			log.Printf("[Profile Copy] skipping lock artifact: %s", rel)
			stats.filesSkipped++
			return nil
		}
		copyFileBestEffort(path, target, stats)
		return nil
	})
}

// isolatedProfileExists reports whether destDir already has a usable
// duplicated profile, so we don't re-close the real browser and redo a
// (slow) full copy on every single login attempt — only the first time,
// or if it's missing/incomplete.
func isolatedProfileExists(destDir string) bool {
	marker := filepath.Join(destDir, "Default", "Preferences")
	info, err := os.Stat(marker)
	return err == nil && info.Size() > 0
}

// duplicateRealProfile closes the user's real browser (to get a consistent,
// unlocked snapshot) and copies its "Default" profile folder + "Local State"
// into an isolated directory our app owns exclusively, so launching against
// it never fights the real browser's profile lock.
func duplicateRealProfile(env *enviroment.Environment, destDir string) error {
	start := time.Now()
	srcRoot := env.GetProfilePath()
	if srcRoot == "" {
		return fmt.Errorf("could not locate the browser's real profile directory — " +
			"set BROWSER_PROFILE_PATH or use cookie import instead")
	}
	browserName := env.GetBrowserName()

	log.Printf("[Profile Copy] Starting duplication: browser=%s source=%s dest=%s", browserName, srcRoot, destDir)
	log.Printf("[Profile Copy] Closing %s to safely duplicate its profile (please save your work first).", browserName)
	killBrowserProcesses(browserName)

	if err := os.MkdirAll(destDir, 0750); err != nil {
		return fmt.Errorf("cannot create isolated profile dir: %w", err)
	}
	clearProfileLocks(destDir)

	stats := &copyStats{}

	localStateSrc := filepath.Join(srcRoot, "Local State")
	localStateDst := filepath.Join(destDir, "Local State")
	if _, err := os.Stat(localStateSrc); err != nil {
		log.Printf("[Profile Copy] WARNING: Local State not found at %s (%v)", localStateSrc, err)
	} else {
		copyFileBestEffort(localStateSrc, localStateDst, stats)
	}

	srcDefault := filepath.Join(srcRoot, "Default")
	destDefault := filepath.Join(destDir, "Default")
	if _, err := os.Stat(srcDefault); err != nil {
		return fmt.Errorf("source profile folder %s not found: %w", srcDefault, err)
	}
	if err := copyDirFiltered(srcDefault, destDefault, stats); err != nil {
		return fmt.Errorf("copy profile: %w", err)
	}

	log.Printf("[Profile Copy] Done in %s — files copied: %d (%.1f MB), files skipped: %d, cache dirs skipped: %d",
		time.Since(start).Round(time.Millisecond), stats.filesCopied, float64(stats.bytesCopied)/1024/1024,
		stats.filesSkipped, stats.dirsSkipped)
	return nil
}

// ensureIsolatedProfile duplicates the real profile into destDir only if a
// usable copy doesn't already exist there.
func ensureIsolatedProfile(env *enviroment.Environment, destDir string) error {
	if isolatedProfileExists(destDir) {
		log.Printf("[Profile Copy] Isolated profile already exists at %s — skipping duplication", destDir)
		return nil
	}
	log.Printf("[Profile Copy] No isolated profile found at %s yet — duplicating from the real browser", destDir)
	return duplicateRealProfile(env, destDir)
}

// clickIfPresent tries to click an element matched by an XPath-ish search
// string within a short timeout, and simply returns false (no error) if
// nothing matched — used for optional prompts like Facebook's "Continue"
// confirmation, which may or may not appear.
func clickIfPresent(parentCtx context.Context, taskCtx context.Context, xpathQuery string, wait time.Duration) bool {
	clickCtx, cancel := context.WithTimeout(taskCtx, wait)
	defer cancel()
	err := chromedp.Run(clickCtx, chromedp.Click(xpathQuery, chromedp.BySearch))
	_ = parentCtx
	return err == nil
}

// loginWithRealProfile duplicates the user's real browser profile (once —
// reused on subsequent calls) into isolatedProfileDir, launches the browser
// visibly against that copy, navigates to loginURL, and:
//  1. checks if already logged in (cookies present) — returns immediately if so
//  2. otherwise auto-fills email/password (if provided) and submits
//  3. clicks through an optional "Continue" confirmation prompt if present
//  4. re-checks cookies — returns if now authenticated
//  5. otherwise leaves the window open for the user to finish manually,
//     polling cookies until waitTimeout elapses
func loginWithRealProfile(ctx context.Context, platformID, loginURL, email, password string, env *enviroment.Environment, isolatedProfileDir string, waitTimeout time.Duration) ([]*network.Cookie, error) {
	overallStart := time.Now()
	log.Printf("[Login] === Starting real-profile login for %s ===", platformID)

	browserPath := env.GetBrowserPath()
	if browserPath == "" {
		return nil, fmt.Errorf("no CDP-compatible browser found")
	}
	browserName := env.GetBrowserName()
	log.Printf("[Login] Step 1/5: browser=%s path=%s isolatedProfileDir=%s", browserName, browserPath, isolatedProfileDir)

	if err := ensureIsolatedProfile(env, isolatedProfileDir); err != nil {
		log.Printf("[Login] Step 1/5 FAILED: %v", err)
		return nil, err
	}

	opts := []chromedp.ExecAllocatorOption{
		chromedp.ExecPath(browserPath),
		chromedp.UserDataDir(isolatedProfileDir),
		chromedp.Flag("profile-directory", "Default"),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("restore-last-session", false),
		chromedp.Flag("hide-crash-restore-bubble", true),
		chromedp.Flag("start-maximized", true),
		chromedp.Flag("headless", false),
		chromedp.Flag("window-size", "1280,900"),
	}

	allocCtx, allocCancel := chromedp.NewExecAllocator(ctx, opts...)
	defer allocCancel()
	taskCtx, taskCancel := chromedp.NewContext(allocCtx)
	defer taskCancel()

	log.Printf("[Login] Step 2/5: launching %s and navigating to %s", browserName, loginURL)
	navStart := time.Now()
	if err := chromedp.Run(taskCtx,
		chromedp.Navigate(loginURL),
		chromedp.WaitReady("body", chromedp.ByQuery),
	); err != nil {
		log.Printf("[Login] Step 2/5 FAILED after %s: %v", time.Since(navStart).Round(time.Millisecond), err)
		return nil, fmt.Errorf("navigation failed: %w", err)
	}
	log.Printf("[Login] Step 2/5 done in %s: page loaded", time.Since(navStart).Round(time.Millisecond))

	getCookies := func() ([]*network.Cookie, error) {
		var cookies []*network.Cookie
		err := chromedp.Run(taskCtx, chromedp.ActionFunc(func(ctx context.Context) error {
			var err error
			cookies, err = network.GetCookies().Do(ctx)
			return err
		}))
		return cookies, err
	}

	log.Printf("[Login] Step 3/5: checking for existing auth cookies")
	cookies, err := getCookies()
	if err != nil {
		log.Printf("[Login] Step 3/5: could not read cookies: %v", err)
	} else {
		log.Printf("[Login] Step 3/5: read %d cookie(s)", len(cookies))
		if hasAuth(platformID, cookies) {
			log.Printf("[Login] Already logged in — auth cookies carried over from the duplicated profile. Total time: %s",
				time.Since(overallStart).Round(time.Millisecond))
			return cookies, nil
		}
	}

	if email != "" && password != "" {
		// Give the page a moment to finish rendering before we start
		// looking for form fields — on a busy machine the page can report
		// "loaded" before the login form has actually finished mounting,
		// which made the field search fail spuriously.
		time.Sleep(3 * time.Second)
		log.Printf("[Login] Step 4/5: looking for a login form to auto-fill")
		emailSels, passSels, buttonSels := loginFieldSelectors(platformID)

		const perFieldTimeout = 4 * time.Second
		emailSel, foundEmail := firstVisibleSelector(taskCtx, emailSels, perFieldTimeout)
		if !foundEmail {
			log.Printf("[Login] Step 4/5: no email/username field matched %v — no login form present "+
				"(you may already be logged in, or this is a different flow); checking cookies now", emailSels)
			cookies, err = getCookies()
			if err == nil && hasAuth(platformID, cookies) {
				log.Printf("[Login] Step 4/5: already authenticated (%d cookies). Total time: %s",
					len(cookies), time.Since(overallStart).Round(time.Millisecond))
				return cookies, nil
			}
			log.Printf("[Login] Step 4/5: not authenticated yet — falling through to manual login")
		} else {
			passSel, foundPass := firstVisibleSelector(taskCtx, passSels, perFieldTimeout)
			if !foundPass {
				log.Printf("[Login] Step 4/5: found email field (%s) but no password field matched %v — falling through to manual login",
					emailSel, passSels)
			} else {
				// Use real keystroke simulation (SendKeys), not SetValue.
				// SetValue pokes the DOM value directly via JS without firing
				// input/change events, so a React-controlled form (like
				// Facebook's login page) never updates its own state — the
				// next re-render then snaps the field back to empty, which is
				// exactly the "cleared after ~2 seconds" symptom seen before.
				// SendKeys dispatches real key events, so onChange fires and
				// React's state actually reflects what got typed.
				if err := chromedp.Run(taskCtx,
					chromedp.Click(emailSel, chromedp.ByQuery),
					chromedp.SendKeys(emailSel, email, chromedp.ByQuery),
					chromedp.Click(passSel, chromedp.ByQuery),
					chromedp.SendKeys(passSel, password, chromedp.ByQuery),
				); err != nil {
					log.Printf("[Login] Step 4/5: found both fields (%s, %s) but failed to type into them: %v — falling through to manual login",
						emailSel, passSel, err)
				} else {
					log.Printf("[Login] Step 4/5: typed email (%d chars) into %s and password (%d chars) into %s",
						len(email), emailSel, len(password), passSel)

					// Submit immediately via Enter in the password field —
					// this is the fast, reliable path for a standard HTML
					// form and avoids burning several seconds hunting for a
					// submit button while the typed value sits there at risk
					// of being wiped by a re-render. Only fall back to
					// searching for a button if Enter itself fails to send.
					if err := chromedp.Run(taskCtx, chromedp.SendKeys(passSel, "\r", chromedp.ByQuery)); err != nil {
						log.Printf("[Login] Step 4/5: Enter key submission failed (%v) — searching for a login button instead", err)
						buttonSel, foundButton := firstVisibleSelector(taskCtx, buttonSels, 1*time.Second)
						if foundButton {
							if clickErr := chromedp.Run(taskCtx, chromedp.Click(buttonSel, chromedp.ByQuery)); clickErr != nil {
								log.Printf("[Login] Step 4/5: found login button (%s) but click also failed: %v", buttonSel, clickErr)
							} else {
								log.Printf("[Login] Step 4/5: clicked login button (%s)", buttonSel)
							}
						} else {
							log.Printf("[Login] Step 4/5: no login button matched %v either — form was not submitted", buttonSels)
						}
					} else {
						log.Printf("[Login] Step 4/5: submitted via Enter key in the password field")
					}

					log.Printf("[Login] Step 4/5: waiting for response...")
					time.Sleep(3 * time.Second)

					// Facebook sometimes shows a "Continue as <name>" / "Continue"
					// confirmation (e.g. on a recognized device). Click it if present;
					// harmless no-op if it isn't there.
					if clickIfPresent(ctx, taskCtx, `//*[self::button or @role='button'][contains(., 'Continue')]`, 4*time.Second) {
						log.Printf("[Login] Step 4/5: clicked a 'Continue' confirmation prompt")
						time.Sleep(2 * time.Second)
					}

					cookies, err = getCookies()
					if err == nil && hasAuth(platformID, cookies) {
						log.Printf("[Login] Step 4/5: auto-fill login succeeded (%d cookies). Total time: %s",
							len(cookies), time.Since(overallStart).Round(time.Millisecond))
						return cookies, nil
					}

					if found, reason := detectSecurityChallenge(taskCtx); found {
						log.Printf("[Login] Step 4/5: 2FA/CAPTCHA challenge detected (%s) — this needs a human, "+
							"handing off to manual login now", reason)
					} else {
						log.Printf("[Login] Step 4/5: auto-fill login did not complete authentication — falling through to manual login")
					}
				}
			}
		}
	} else {
		log.Printf("[Login] Step 4/5: no credentials configured — skipping auto-fill, going straight to manual login")
	}

	log.Printf("[Login] Step 5/5: not logged in yet — please log in manually in the window that just opened for %s (waiting up to %s)",
		platformID, waitTimeout)
	loopStart := time.Now()
	deadline := loopStart.Add(waitTimeout)
	pollCount := 0
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			log.Printf("[Login] Step 5/5 canceled after %d poll(s): %v", pollCount, ctx.Err())
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
		pollCount++
		cookies, err := getCookies()
		if err != nil {
			log.Printf("[Login] Step 5/5 poll #%d: could not read cookies: %v", pollCount, err)
			continue
		}
		if hasAuth(platformID, cookies) {
			log.Printf("[Login] Step 5/5: manual login detected after %d poll(s) (%s waiting). Total time: %s",
				pollCount, time.Since(loopStart).Round(time.Second), time.Since(overallStart).Round(time.Millisecond))
			return cookies, nil
		}
	}
	log.Printf("[Login] Step 5/5 TIMED OUT after %d poll(s) / %s", pollCount, waitTimeout)
	return nil, fmt.Errorf("timed out waiting for manual login to %s (%s)", platformID, waitTimeout)
}

// loginFieldSelectors returns the email/password/submit selectors for a
// platform's login form.
// loginFieldSelectors returns several candidate CSS selectors per field, in
// priority order, since login page markup varies (different flows,
// checkpoint pages, A/B tests) and a single hardcoded selector silently
// finding nothing was exactly what made auto-fill look like it ran but
// never actually typed anything.
func loginFieldSelectors(platformID string) (emailSels, passSels, buttonSels []string) {
	switch platformID {
	case "instagram":
		return []string{`input[name="username"]`, `input#loginForm input[type="text"]`},
			[]string{`input[name="password"]`, `input[type="password"]`},
			[]string{`button[type="submit"]`, `button[data-testid="loginForm"] button`}
	default: // facebook
		return []string{`input[name="email"]`, `input#email`, `input[type="email"]`, `input[type="text"][autocomplete="username"]`},
			[]string{`input[name="pass"]`, `input#pass`, `input[type="password"]`},
			[]string{`button[name="login"]`, `button[data-testid="royal_login_button"]`, `button[type="submit"]`}
	}
}

// firstVisibleSelector tries each selector in order (each bounded by its own
// short timeout) and returns the first one that actually becomes visible.
// Returns ("", false) if none matched — the caller must not assume a field
// was found just because we attempted to look for it.
func firstVisibleSelector(taskCtx context.Context, selectors []string, perSelectorTimeout time.Duration) (string, bool) {
	for _, sel := range selectors {
		checkCtx, cancel := context.WithTimeout(taskCtx, perSelectorTimeout)
		err := chromedp.Run(checkCtx, chromedp.WaitVisible(sel, chromedp.ByQuery))
		cancel()
		if err == nil {
			return sel, true
		}
	}
	return "", false
}

func browserProcessNames(browserName string) []string {
	switch browserName {
	case "edge":
		return []string{"msedge.exe", "msedge"}
	case "chrome":
		return []string{"chrome.exe", "google-chrome", "chrome"}
	case "brave":
		return []string{"brave.exe", "brave-browser", "brave"}
	case "chromium":
		return []string{"chromium.exe", "chromium-browser", "chromium"}
	default:
		return nil
	}
}

// ------------------------------------------------------------
// Authentication helpers
// ------------------------------------------------------------

// hasAuth validates chromedp/CDP-style cookies. It delegates to
// hasAuthCookies (the same check used by ImportCookiesFromJSON) via
// convertCookies, so the two entry points — real-profile login and
// manual cookie import — can never silently drift apart on what counts
// as "authenticated" for a given platform.
func hasAuth(platformID string, cookies []*network.Cookie) bool {
	return hasAuthCookies(platformID, convertCookies(cookies))
}

func validateAPI(url, token string) (bool, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", token)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200, nil
}

func validateTelegram(token string) (bool, error) {
	resp, err := (&http.Client{Timeout: 10 * time.Second}).
		Get(fmt.Sprintf("https://api.telegram.org/bot%s/getMe", token))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return strings.Contains(string(body), `"ok":true`), nil
}

// ------------------------------------------------------------
// WhatsApp and Viber client structs
// ------------------------------------------------------------

type WhatsAppClient struct {
	PlatformID string
	SubtypeID  string
	AccountID  string

	client *whatsmeow.Client
	store  *sqlstore.Container
	qrDone chan struct{}
	qrSrv  *http.Server
	env    *enviroment.Environment
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

type ViberBotClient struct {
	PlatformID string
	SubtypeID  string
	AccountID  string

	client *viber.Viber
	mu     sync.Mutex
}

// ------------------------------------------------------------
// Manager struct
// ------------------------------------------------------------

type Manager struct {
	config  *config.Config
	env     *enviroment.Environment
	storage *Storage
	db      *sql.DB

	mu       sync.RWMutex
	sessions map[string]*Session

	profileIDMu sync.Mutex

	errorChan chan PlatformError
	shutdown  chan struct{}
	wg        sync.WaitGroup

	activeLogins sync.Map

	pendingTelegramAuth   map[string]chan string
	pendingTelegramAuthMu sync.Mutex

	whatsappMu      sync.Mutex
	whatsappClients map[string]*WhatsAppClient

	viberMu      sync.Mutex
	viberClients map[string]*ViberBotClient
}

func NewManager(cfg *config.Config, env *enviroment.Environment) (*Manager, error) {
	db := database.GetDB()
	if db == nil {
		return nil, errors.New("database not connected")
	}
	cacheDir := "./cache"
	if cfg != nil && cfg.Paths.Cache != "" {
		cacheDir = cfg.Paths.Cache
	}
	sessionsDir := filepath.Join(cacheDir, "sessions")
	if cfg != nil && cfg.Paths.Sessions != "" {
		if filepath.IsAbs(cfg.Paths.Sessions) {
			sessionsDir = cfg.Paths.Sessions
		} else {
			sessionsDir = filepath.Join(cacheDir, cfg.Paths.Sessions)
		}
	}
	profilesDir := filepath.Join(cacheDir, "profiles")

	mgr := &Manager{
		config:              cfg,
		env:                 env,
		storage:             NewStorage(cacheDir, sessionsDir, profilesDir),
		db:                  db,
		sessions:            make(map[string]*Session),
		errorChan:           make(chan PlatformError, 1000),
		shutdown:            make(chan struct{}),
		whatsappClients:     make(map[string]*WhatsAppClient),
		viberClients:        make(map[string]*ViberBotClient),
		pendingTelegramAuth: make(map[string]chan string),
	}

	mgr.killOrphanedChrome()

	mgr.wg.Add(1)
	go mgr.cleanupWorker()
	return mgr, nil
}

func (m *Manager) killOrphanedChrome() {
	pidFile := filepath.Join(m.storage.cacheDir, "session_chrome.pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		os.Remove(pidFile)
		return
	}
	log.Printf("[Session] Found orphaned Chrome PID %d — killing", pid)
	proc, err := os.FindProcess(pid)
	if err == nil {
		proc.Kill()
	}
	os.Remove(pidFile)
}

func (m *Manager) GetErrorChannel() <-chan PlatformError { return m.errorChan }

func (m *Manager) sendError(platformID, subtype, accountID, errorType, errorMsg, severity string) {
	select {
	case m.errorChan <- PlatformError{
		PlatformID: platformID,
		Subtype:    subtype,
		AccountID:  accountID,
		Error:      errorMsg,
		Type:       errorType,
		Timestamp:  time.Now(),
		Severity:   severity,
	}:
	case <-m.shutdown:
	}
}

func (m *Manager) resolveAccountID(platformID, subtype, accountID string) string {
	if accountID != "" {
		return accountID
	}
	user, _ := getCreds(m.config, platformID, subtype)
	if user == "" {
		return ""
	}
	safe := strings.NewReplacer(
		"@", "_at_", "/", "_", "\\", "_", ":", "_", " ", "_",
	).Replace(user)
	if len(safe) > 40 {
		safe = safe[:40]
	}
	return safe
}

func (m *Manager) normalizeTelegramAccountID() string {
	phone, _, _ := m.getTelegramPhoneAndCreds()
	if phone == "" {
		return ""
	}
	id := strings.TrimPrefix(phone, "+")
	id = strings.ReplaceAll(id, " ", "")
	return id
}

// ------------------------------------------------------------
// GetSession – main session retrieval
// ------------------------------------------------------------

func (m *Manager) GetSession(ctx context.Context, req SessionRequest) (*Session, error) {
	req.AccountID = m.resolveAccountID(req.PlatformID, req.Subtype, req.AccountID)
	if req.AccountID == "" {
		return nil, fmt.Errorf("no credentials configured for %s:%s", req.PlatformID, req.Subtype)
	}
	key := fmt.Sprintf("%s:%s", req.PlatformID, req.Subtype)

	if req.PlatformID == "whatsapp" {
		phone, _ := getCreds(m.config, req.PlatformID, req.Subtype)
		if phone == "" {
			return nil, fmt.Errorf("no WhatsApp phone number configured for %s:%s", req.PlatformID, req.Subtype)
		}
		if req.ForceNew {
			m.MarkCorrupted(req.PlatformID, req.Subtype, true)
		} else {
			m.cleanupStaleWhatsAppSession(req.PlatformID, req.Subtype, req.AccountID)
		}
		return m.syntheticWhatsAppSession(req), nil
	}

	if req.PlatformID == "viber" {
		token, _ := getCreds(m.config, req.PlatformID, req.Subtype)
		if token == "" {
			return nil, fmt.Errorf("no Viber bot token configured for %s:%s", req.PlatformID, req.Subtype)
		}
		if req.ForceNew {
			m.MarkCorrupted(req.PlatformID, req.Subtype, true)
		}
		return m.syntheticViberSession(req), nil
	}

	if req.PlatformID == "twitter" {
		user, _ := getCreds(m.config, req.PlatformID, req.Subtype)
		if user == "" {
			return nil, fmt.Errorf("no Twitter credentials configured for %s:%s", req.PlatformID, req.Subtype)
		}
		if strings.HasPrefix(user, "AAAA") || strings.HasPrefix(user, "Bearer ") {
			if req.ForceNew {
				m.MarkCorrupted(req.PlatformID, req.Subtype, true)
			}
			return m.syntheticTokenSession(req, "twitter_bearer"), nil
		}
	}

	if req.PlatformID == "telegram" {
		normalized := m.normalizeTelegramAccountID()
		if normalized == "" {
			return nil, fmt.Errorf("no Telegram phone number configured")
		}
		req.AccountID = normalized

		tgDir := filepath.Join(m.storage.GetSessionsDir(), "telegram")
		if err := os.MkdirAll(tgDir, 0750); err != nil {
			return nil, fmt.Errorf("cannot create telegram session dir: %w", err)
		}
		sessionFile := filepath.Join(tgDir, fmt.Sprintf("tg_%s_%s.session", req.Subtype, req.AccountID))

		if req.ForceNew {
			m.MarkCorrupted(req.PlatformID, req.Subtype, true)
		}

		if _, err := os.Stat(sessionFile); err == nil && !req.ForceNew {
			m.mu.RLock()
			existing, inMem := m.sessions[key]
			m.mu.RUnlock()
			if inMem && m.valid(existing) {
				existing.LastUsed = time.Now()
				return existing, nil
			}
			profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)
			sess := &Session{
				PlatformID: req.PlatformID,
				Subtype:    req.Subtype,
				AccountID:  req.AccountID,
				State:      StateAuthenticated,
				Cookies:    []Cookie{},
				CreatedAt:  time.Now(),
				LastUsed:   time.Now(),
				ExpiresAt:  time.Now().Add(30 * 24 * time.Hour),
				Metadata: map[string]string{
					"profile_id":   profileID,
					"session_type": "gogram_mtproto",
					"session_file": sessionFile,
				},
			}
			m.persistSession(sess)
			return sess, nil
		}

		return m.authenticateTelegram(ctx, req)
	}

	if req.ForceNew {
		m.MarkCorrupted(req.PlatformID, req.Subtype, true)
	} else {
		m.mu.RLock()
		sess, exists := m.sessions[key]
		m.mu.RUnlock()
		if exists && m.valid(sess) {
			if sess.AccountID != req.AccountID {
				log.Printf("[Session] Credential change detected for %s:%s (old=%s new=%s) – purging",
					req.PlatformID, req.Subtype, sess.AccountID, req.AccountID)
				m.MarkCorrupted(req.PlatformID, req.Subtype, true)
			} else {
				sess.LastUsed = time.Now()
				return sess, nil
			}
		}

		if diskSess, err := m.storage.Load(req.PlatformID, req.Subtype); err == nil {
			if diskSess.Corrupted {
				log.Printf("[Session] Corrupted session on disk for %s:%s – deleting",
					req.PlatformID, req.Subtype)
				m.storage.Delete(req.PlatformID, req.Subtype)
			} else if m.valid(diskSess) {
				if diskSess.AccountID != req.AccountID {
					log.Printf("[Session] Credential change detected for %s:%s (disk old=%s new=%s) – purging",
						req.PlatformID, req.Subtype, diskSess.AccountID, req.AccountID)
					m.MarkCorrupted(req.PlatformID, req.Subtype, true)
				} else {
					m.mu.Lock()
					m.sessions[key] = diskSess
					m.mu.Unlock()
					diskSess.LastUsed = time.Now()
					return diskSess, nil
				}
			}
		} else if strings.Contains(err.Error(), "corrupted") {
			log.Printf("[Session] Disk session file corrupted for %s:%s – deleting",
				req.PlatformID, req.Subtype)
			m.storage.Delete(req.PlatformID, req.Subtype)
		}
	}

	return m.createSession(ctx, req)
}

// ------------------------------------------------------------
// Synthetic sessions
// ------------------------------------------------------------

func (m *Manager) cleanupStaleWhatsAppSession(platformID, subtype, expectedAccountID string) {
	key := fmt.Sprintf("%s:%s", platformID, subtype)
	m.mu.RLock()
	existing, exists := m.sessions[key]
	m.mu.RUnlock()
	if !exists {
		return
	}
	if existing.AccountID != "" && existing.AccountID != expectedAccountID {
		log.Printf("[Session] WhatsApp credential mismatch: old=%s new=%s – cleaning up",
			existing.AccountID, expectedAccountID)
		m.MarkCorrupted(platformID, subtype, true)
	}
}

func (m *Manager) syntheticWhatsAppSession(req SessionRequest) *Session {
	profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)
	sess := &Session{
		PlatformID: req.PlatformID,
		Subtype:    req.Subtype,
		AccountID:  req.AccountID,
		State:      StateAuthenticated,
		Cookies:    []Cookie{},
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		ExpiresAt:  time.Now().Add(30 * 24 * time.Hour),
		Metadata: map[string]string{
			"profile_id":   profileID,
			"session_type": "whatsmeow",
		},
	}
	m.persistSession(sess)
	log.Printf("[Session] WhatsApp synthetic session for %s:%s (profile: %s)",
		req.PlatformID, req.Subtype, profileID)
	return sess
}

func (m *Manager) syntheticViberSession(req SessionRequest) *Session {
	profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)
	sess := &Session{
		PlatformID: req.PlatformID,
		Subtype:    req.Subtype,
		AccountID:  req.AccountID,
		State:      StateAuthenticated,
		Cookies:    []Cookie{},
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		ExpiresAt:  time.Now().Add(30 * 24 * time.Hour),
		Metadata: map[string]string{
			"profile_id":   profileID,
			"session_type": "viber_bot",
		},
	}
	m.persistSession(sess)
	log.Printf("[Session] Viber synthetic session for %s:%s (profile: %s)",
		req.PlatformID, req.Subtype, profileID)
	return sess
}

func (m *Manager) syntheticTokenSession(req SessionRequest, sessionType string) *Session {
	profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)
	sess := &Session{
		PlatformID: req.PlatformID,
		Subtype:    req.Subtype,
		AccountID:  req.AccountID,
		State:      StateAuthenticated,
		Cookies:    []Cookie{},
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		ExpiresAt:  time.Now().Add(30 * 24 * time.Hour),
		Metadata: map[string]string{
			"profile_id":   profileID,
			"session_type": sessionType,
		},
	}
	m.persistSession(sess)
	log.Printf("[Session] Synthetic token session (%s) for %s:%s",
		sessionType, req.PlatformID, req.Subtype)
	return sess
}

// ------------------------------------------------------------
// Session validity
// ------------------------------------------------------------

func (m *Manager) valid(sess *Session) bool {
	if sess == nil || sess.State != StateAuthenticated || sess.Corrupted {
		return false
	}
	if time.Now().After(sess.ExpiresAt) {
		return false
	}
	if sess.Metadata != nil {
		switch sess.Metadata["session_type"] {
		case "gogram_mtproto":
			sessionFile := sess.Metadata["session_file"]
			if sessionFile == "" {
				return true
			}
			if _, err := os.Stat(sessionFile); err != nil {
				return false
			}
			return true
		case "whatsmeow", "viber_bot", "twitter_bearer":
			return true
		}
	}
	if len(sess.Cookies) == 0 {
		return false
	}
	for _, c := range sess.Cookies {
		if c.Expires > 0 && time.Now().After(time.Unix(c.Expires, 0)) {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------
// createSession – real-profile CDP login for Facebook/Instagram, headless chromedp for others
// ------------------------------------------------------------

func (m *Manager) createSession(ctx context.Context, req SessionRequest) (*Session, error) {
	key := fmt.Sprintf("%s:%s", req.PlatformID, req.Subtype)

	inflight := make(chan struct{})
	actual, loaded := m.activeLogins.LoadOrStore(key, inflight)
	if loaded {
		existingCh := actual.(chan struct{})
		log.Printf("[Session] Login already in progress for %s — waiting", key)
		select {
		case <-existingCh:
			m.mu.RLock()
			sess, exists := m.sessions[key]
			m.mu.RUnlock()
			if exists && m.valid(sess) {
				return sess, nil
			}
			return nil, fmt.Errorf("session still unavailable after concurrent login for %s", key)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	defer func() {
		recover()
		close(inflight)
		m.activeLogins.Delete(key)
	}()

	if req.PlatformID == "facebook" || req.PlatformID == "instagram" {
		loginURL := "https://www.facebook.com/"
		if req.PlatformID == "instagram" {
			loginURL = "https://www.instagram.com/accounts/login/"
		}
		profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)
		isolatedProfileDir := m.storage.ProfilePathByID(profileID)
		email, password := getCreds(m.config, req.PlatformID, req.Subtype)
		log.Printf("[Session] %s:%s has no valid session — attempting real-profile login (profileID=%s, isolatedDir=%s)",
			req.PlatformID, req.Subtype, profileID, isolatedProfileDir)

		// Give the user up to 5 minutes to log in manually if auto-fill
		// doesn't get all the way through (CAPTCHA/2FA/checkpoint).
		loginCtx, loginCancel := context.WithTimeout(ctx, 5*time.Minute+30*time.Second)
		defer loginCancel()
		cookies, err := loginWithRealProfile(loginCtx, req.PlatformID, loginURL, email, password, m.env, isolatedProfileDir, 5*time.Minute)
		if err != nil {
			msg := fmt.Sprintf(
				"%s real-profile login failed (%v) — export cookies from a logged-in browser "+
					"session and call ImportCookiesFromJSON(%q, %q, accountID, cookieJSON) instead",
				req.PlatformID, err, req.PlatformID, req.Subtype)
			log.Printf("[Session] %s", msg)
			m.sendError(req.PlatformID, req.Subtype, req.AccountID,
				"REAL_PROFILE_LOGIN_FAILED", msg, "critical")
			return nil, errors.New(msg)
		}
		if !hasAuth(req.PlatformID, cookies) {
			log.Printf("[Session] %s:%s login completed but final auth check failed (%d cookies present)",
				req.PlatformID, req.Subtype, len(cookies))
			m.sendError(req.PlatformID, req.Subtype, req.AccountID,
				"NO_AUTH_COOKIES", "Login completed but auth cookies absent", "error")
			return nil, errors.New("no auth cookies after login")
		}
		log.Printf("[Session] %s:%s real-profile login SUCCESS (%d cookies)", req.PlatformID, req.Subtype, len(cookies))
		sess := m.buildAuthSession(req, profileID, "real_profile", cookies)
		m.persistSession(sess)
		return sess, nil
	}

	user, pass := getCreds(m.config, req.PlatformID, req.Subtype)
	if user == "" && pass == "" {
		m.sendError(req.PlatformID, req.Subtype, req.AccountID,
			"NO_CREDENTIALS", "No credentials configured", "critical")
		return nil, errors.New("no credentials")
	}

	profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)

	log.Printf("[Session] Login attempt for %s:%s (profile: %s)",
		req.PlatformID, req.Subtype, profileID)

	// Other platforms: chromedp
	profilePath := m.storage.ProfilePathByID(profileID)
	if err := os.MkdirAll(profilePath, 0755); err != nil {
		return nil, fmt.Errorf("cannot create profile dir %s: %w", profilePath, err)
	}

	opts := browserFlags(true, profilePath, m.env)
	allocCtx, allocCancel := chromedp.NewExecAllocator(ctx, opts...)
	defer allocCancel()
	taskCtx, taskCancel := chromedp.NewContext(allocCtx)
	defer taskCancel()

	if err := chromedp.Run(taskCtx, chromedp.Navigate("about:blank")); err != nil {
		m.sendError(req.PlatformID, req.Subtype, req.AccountID,
			"CHROME_INIT_FAILED", err.Error(), "error")
		return nil, fmt.Errorf("chrome init failed: %w", err)
	}

	cookies, loginErr := platformLogin(taskCtx, req.PlatformID, user, pass)

	if loginErr != nil || !hasAuth(req.PlatformID, cookies) {
		if detected, reason := detectSecurityChallenge(taskCtx); detected {
			m.sendError(req.PlatformID, req.Subtype, req.AccountID,
				"CAPTCHA_2FA_DETECTED", reason, "warning")
			return nil, fmt.Errorf("security_challenge:%s", reason)
		}
	}
	if loginErr != nil {
		m.sendError(req.PlatformID, req.Subtype, req.AccountID,
			"LOGIN_FAILED", loginErr.Error(), "warning")
		return nil, loginErr
	}
	if !hasAuth(req.PlatformID, cookies) {
		m.sendError(req.PlatformID, req.Subtype, req.AccountID,
			"NO_AUTH_COOKIES",
			"Login completed but auth cookies absent — possible silent challenge",
			"error")
		return nil, errors.New("no auth cookies after login")
	}

	sess := m.buildAuthSession(req, profileID, "headless", cookies)
	m.persistSession(sess)
	return sess, nil
}

func platformLogin(ctx context.Context, platformID, user, pass string) ([]*network.Cookie, error) {
	switch platformID {
	case "twitter":
		return twitterLogin(ctx, user, pass)
	case "tiktok":
		return tiktokLogin(ctx, user, pass)
	default:
		return nil, fmt.Errorf("platform %s does not use browser-based login", platformID)
	}
}

func twitterLogin(ctx context.Context, user, pass string) ([]*network.Cookie, error) {
	if strings.HasPrefix(user, "Bearer ") || strings.HasPrefix(user, "AAAA") {
		if ok, err := validateAPI("https://api.twitter.com/2/users/me", "Bearer "+strings.TrimPrefix(user, "Bearer ")); ok {
			return []*network.Cookie{}, nil
		} else if err != nil {
			return nil, fmt.Errorf("twitter API validation: %w", err)
		}
		return nil, errors.New("twitter bearer token rejected by API")
	}
	if err := chromedp.Run(ctx, chromedp.Navigate("https://twitter.com/i/flow/login")); err != nil {
		return nil, fmt.Errorf("navigate: %w", err)
	}
	if err := sleepCtx(ctx, 3*time.Second); err != nil {
		return nil, err
	}
	if err := chromedp.Run(ctx, chromedp.SendKeys(`input[autocomplete="username"]`, user)); err != nil {
		return nil, fmt.Errorf("type username: %w", err)
	}
	if err := sleepCtx(ctx, 1*time.Second); err != nil {
		return nil, err
	}
	if err := chromedp.Run(ctx, chromedp.Click(`div[role="button"][data-testid]`)); err != nil {
		return nil, fmt.Errorf("click next: %w", err)
	}
	if err := sleepCtx(ctx, 2*time.Second); err != nil {
		return nil, err
	}
	if err := chromedp.Run(ctx, chromedp.SendKeys(`input[name="password"]`, pass)); err != nil {
		return nil, fmt.Errorf("type password: %w", err)
	}
	if err := sleepCtx(ctx, 1*time.Second); err != nil {
		return nil, err
	}
	if err := chromedp.Run(ctx, chromedp.Click(`div[role="button"][data-testid="LoginForm_Login_Button"]`)); err != nil {
		return nil, fmt.Errorf("click login: %w", err)
	}
	for i := 0; i < 15; i++ {
		if err := sleepCtx(ctx, 2*time.Second); err != nil {
			return nil, err
		}
		var cookies []*network.Cookie
		chromedp.Run(ctx, chromedp.ActionFunc(func(ctx context.Context) error {
			var e error
			cookies, e = network.GetCookies().Do(ctx)
			return e
		}))
		if hasAuth("twitter", cookies) {
			return cookies, nil
		}
	}
	return nil, errors.New("timeout waiting for twitter auth cookies")
}

func tiktokLogin(ctx context.Context, user, pass string) ([]*network.Cookie, error) {
	if err := chromedp.Run(ctx, chromedp.Navigate("https://tiktok.com/login")); err != nil {
		return nil, fmt.Errorf("navigate: %w", err)
	}
	if err := sleepCtx(ctx, 3*time.Second); err != nil {
		return nil, err
	}
	var cookies []*network.Cookie
	chromedp.Run(ctx, chromedp.ActionFunc(func(ctx context.Context) error {
		var e error
		cookies, e = network.GetCookies().Do(ctx)
		return e
	}))
	if !hasAuth("tiktok", cookies) {
		return nil, errors.New("no tiktok auth cookies — manual login required")
	}
	return cookies, nil
}

// ------------------------------------------------------------
// buildAuthSession, persistSession, MarkCorrupted, etc.
// ------------------------------------------------------------

func (m *Manager) buildAuthSession(
	req SessionRequest,
	profileID, loginType string,
	rawCookies []*network.Cookie,
) *Session {
	return &Session{
		PlatformID: req.PlatformID,
		Subtype:    req.Subtype,
		AccountID:  req.AccountID,
		State:      StateAuthenticated,
		Cookies:    convertCookies(rawCookies),
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		ExpiresAt:  time.Now().Add(7 * 24 * time.Hour),
		Metadata: map[string]string{
			"login_type": loginType,
			"profile_id": profileID,
		},
	}
}

func (m *Manager) persistSession(sess *Session) {
	if err := m.storage.Save(sess); err != nil {
		log.Printf("[Session] WARNING: could not persist session for %s:%s: %v",
			sess.PlatformID, sess.Subtype, err)
	}
	key := fmt.Sprintf("%s:%s", sess.PlatformID, sess.Subtype)
	m.mu.Lock()
	m.sessions[key] = sess
	m.mu.Unlock()
}

func (m *Manager) MarkCorrupted(platformID, subtype string, deleteProfile bool) {
	key := fmt.Sprintf("%s:%s", platformID, subtype)

	m.mu.Lock()
	delete(m.sessions, key)
	m.mu.Unlock()
	m.storage.Delete(platformID, subtype)

	if platformID == "whatsapp" {
		accountID := m.resolveAccountID(platformID, subtype, "")
		dbPath := m.whatsappSessionDBPath(accountID)
		if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
			log.Printf("[Session] Failed to delete WhatsApp DB %s: %v", dbPath, err)
		} else {
			log.Printf("[Session] Deleted WhatsApp DB for %s:%s", platformID, subtype)
		}
		m.StopWhatsApp(platformID, subtype)
	}

	if platformID == "viber" {
		m.StopViber(platformID, subtype)
	}

	if platformID == "telegram" {
		telegramSessionsDir := filepath.Join(m.storage.GetSessionsDir(), "telegram")
		if _, err := os.Stat(telegramSessionsDir); err == nil {
			pattern := fmt.Sprintf("tg_%s_*.session", subtype)
			matches, _ := filepath.Glob(filepath.Join(telegramSessionsDir, pattern))
			for _, match := range matches {
				if err := os.Remove(match); err != nil {
					log.Printf("[Session] Failed to delete Telegram session %s: %v", match, err)
				} else {
					log.Printf("[Session] Deleted Telegram session %s", match)
				}
			}
		}
	}

	if deleteProfile {
		profileID := m.GetProfileID(platformID, subtype)
		if profileID != "" {
			log.Printf("[Session] Deleting browser profile %s for %s:%s",
				profileID, platformID, subtype)
			m.storage.DeleteProfile(profileID)
		}
	}

	log.Printf("[Session] Session for %s:%s marked corrupted (profile deleted: %v)",
		platformID, subtype, deleteProfile)
}

func (m *Manager) whatsappSessionDBPath(accountID string) string {
	waDir := filepath.Join(m.storage.sessionsDir, "whatsapp")
	os.MkdirAll(waDir, 0700)
	return filepath.Join(waDir, accountID+".db")
}

// ------------------------------------------------------------
// Cleanup and shutdown
// ------------------------------------------------------------

func (m *Manager) cleanupWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-m.shutdown:
			return
		}
	}
}

func (m *Manager) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, sess := range m.sessions {
		if time.Now().After(sess.ExpiresAt) {
			delete(m.sessions, key)
			m.storage.Delete(sess.PlatformID, sess.Subtype)
			log.Printf("[Session] Cleaned up expired session for %s:%s",
				sess.PlatformID, sess.Subtype)
		}
	}
}

func (m *Manager) Close() {
	close(m.shutdown)

	m.activeLogins.Range(func(_, value interface{}) bool {
		return true
	})

	m.whatsappMu.Lock()
	waClients := make([]*WhatsAppClient, 0, len(m.whatsappClients))
	for _, wc := range m.whatsappClients {
		waClients = append(waClients, wc)
	}
	m.whatsappClients = make(map[string]*WhatsAppClient)
	m.whatsappMu.Unlock()

	for _, wc := range waClients {
		wc.Disconnect()
	}

	m.viberMu.Lock()
	m.viberClients = make(map[string]*ViberBotClient)
	m.viberMu.Unlock()

	m.wg.Wait()
	close(m.errorChan)
}

// ------------------------------------------------------------
// WhatsAppClient methods
// ------------------------------------------------------------

func (wc *WhatsAppClient) GetClient() *whatsmeow.Client {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	return wc.client
}

func (wc *WhatsAppClient) CheckConnection() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.client == nil || !wc.client.IsConnected() {
		return errors.New("not connected")
	}
	return nil
}

func (wc *WhatsAppClient) Disconnect() {
	wc.cancel()
	wc.mu.Lock()
	c := wc.client
	wc.client = nil
	wc.mu.Unlock()
	if c != nil {
		c.Disconnect()
	}
	if wc.qrSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		wc.qrSrv.Shutdown(ctx)
	}
}

func openBrowser(env *enviroment.Environment, url string) {
	if browserPath := env.GetBrowserPath(); browserPath != "" {
		if err := exec.Command(browserPath, url).Start(); err == nil {
			return
		}
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		log.Printf("[QR] could not open browser automatically: %v — visit %s manually", err, url)
	}
}

func (wc *WhatsAppClient) startQRServer(qrChan <-chan whatsmeow.QRChannelItem) {
	defer wc.cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Printf("[WhatsApp] QR server listen error: %v", err)
		return
	}

	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || tcpAddr == nil {
		log.Printf("[WhatsApp] QR server: could not determine listen port")
		ln.Close()
		return
	}
	addr := fmt.Sprintf("http://127.0.0.1:%d", tcpAddr.Port)

	var clientsMu sync.Mutex
	clients := make(map[chan string]struct{})

	broadcast := func(msg string) {
		clientsMu.Lock()
		defer clientsMu.Unlock()
		for ch := range clients {
			select {
			case ch <- msg:
			default:
			}
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, qrPageHTML)
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "SSE not supported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		ch := make(chan string, 8)
		clientsMu.Lock()
		clients[ch] = struct{}{}
		clientsMu.Unlock()
		defer func() {
			clientsMu.Lock()
			delete(clients, ch)
			clientsMu.Unlock()
		}()
		for {
			select {
			case msg := <-ch:
				fmt.Fprint(w, msg)
				flusher.Flush()
			case <-r.Context().Done():
				return
			case <-wc.ctx.Done():
				return
			}
		}
	})

	srv := &http.Server{Handler: mux}
	wc.qrSrv = srv
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[WhatsApp] QR server error: %v", err)
		}
	}()

	log.Printf("[WhatsApp] QR server at %s — opening browser", addr)
	openBrowser(wc.env, addr)

	for {
		select {
		case qrItem, ok := <-qrChan:
			if !ok {
				broadcast("event: timeout\ndata: channel closed\n\n")
				return
			}
			switch qrItem.Event {
			case "code":
				log.Printf("[WhatsApp] New QR code — scan at %s", addr)
				broadcast(fmt.Sprintf("event: qr\ndata: %s\n\n", qrItem.Code))
			case "success":
				broadcast("event: success\ndata: paired\n\n")
				close(wc.qrDone)
				return
			case "timeout":
				broadcast("event: timeout\ndata: expired\n\n")
				log.Printf("[WhatsApp] QR timeout")
				return
			default:
				log.Printf("[WhatsApp] unexpected QR event: %s", qrItem.Event)
			}
		case <-wc.ctx.Done():
			return
		}
	}
}

func (vc *ViberBotClient) GetClient() *viber.Viber {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return vc.client
}

// ------------------------------------------------------------
// Viber client management
// ------------------------------------------------------------

func (m *Manager) StartViber(req SessionRequest) (*ViberBotClient, error) {
	key := fmt.Sprintf("%s:%s", req.PlatformID, req.Subtype)

	if req.ForceNew {
		log.Printf("[Viber] ForceNew requested – cleaning up old client")
		m.MarkCorrupted(req.PlatformID, req.Subtype, true)
	}

	m.viberMu.Lock()
	existing := m.viberClients[key]
	m.viberMu.Unlock()

	if existing != nil && existing.GetClient() != nil {
		log.Printf("[Viber] %s/%s reusing existing client", req.PlatformID, req.Subtype)
		return existing, nil
	}

	botToken, _ := getCreds(m.config, req.PlatformID, req.Subtype)
	if botToken == "" {
		return nil, fmt.Errorf("Viber BotToken not configured for %s:%s — "+
			"set platforms.viber.bot_token or VIBER_BOT_TOKEN",
			req.PlatformID, req.Subtype)
	}

	senderName := os.Getenv("VIBER_SENDER_NAME")
	senderAvatar := os.Getenv("VIBER_SENDER_AVATAR")
	if senderName == "" {
		senderName = "Bot"
	}

	log.Printf("[Viber] %s/%s creating viber.Viber client (sender=%q)",
		req.PlatformID, req.Subtype, senderName)

	vClient := viber.New(botToken, senderName, senderAvatar)

	vc := &ViberBotClient{
		PlatformID: req.PlatformID,
		SubtypeID:  req.Subtype,
		AccountID:  req.AccountID,
		client:     vClient,
	}

	m.viberMu.Lock()
	m.viberClients[key] = vc
	m.viberMu.Unlock()

	log.Printf("[Viber] %s/%s ✓ client created", req.PlatformID, req.Subtype)
	return vc, nil
}

func (m *Manager) GetViberClient(platformID, subtype string) (*viber.Viber, error) {
	key := fmt.Sprintf("%s:%s", platformID, subtype)
	m.viberMu.Lock()
	vc := m.viberClients[key]
	m.viberMu.Unlock()
	if vc == nil {
		return nil, fmt.Errorf("Viber client not started for %s:%s", platformID, subtype)
	}
	client := vc.GetClient()
	if client == nil {
		return nil, fmt.Errorf("Viber client is nil for %s:%s", platformID, subtype)
	}
	return client, nil
}

func (m *Manager) StopViber(platformID, subtype string) {
	key := fmt.Sprintf("%s:%s", platformID, subtype)
	m.viberMu.Lock()
	delete(m.viberClients, key)
	m.viberMu.Unlock()
	log.Printf("[Viber] %s/%s stopped", platformID, subtype)
}

// ------------------------------------------------------------
// WhatsApp client management
// ------------------------------------------------------------

func (m *Manager) StartWhatsApp(req SessionRequest) (*WhatsAppClient, error) {
	key := fmt.Sprintf("%s:%s", req.PlatformID, req.Subtype)

	if req.ForceNew {
		log.Printf("[WhatsApp] ForceNew requested – cleaning up old client and DB")
		m.MarkCorrupted(req.PlatformID, req.Subtype, true)
	}

	m.whatsappMu.Lock()
	existing := m.whatsappClients[key]
	m.whatsappMu.Unlock()

	if existing != nil {
		if err := existing.CheckConnection(); err == nil {
			return existing, nil
		}
		existing.Disconnect()
		m.whatsappMu.Lock()
		if m.whatsappClients[key] == existing {
			delete(m.whatsappClients, key)
		}
		m.whatsappMu.Unlock()
	}

	accountID := m.resolveAccountID(req.PlatformID, req.Subtype, req.AccountID)
	if accountID == "" {
		return nil, fmt.Errorf("no WhatsApp phone number configured")
	}
	wc := &WhatsAppClient{
		PlatformID: req.PlatformID,
		SubtypeID:  req.Subtype,
		AccountID:  accountID,
		qrDone:     make(chan struct{}),
		env:        m.env,
	}
	wc.ctx, wc.cancel = context.WithCancel(context.Background())

	dbPath := m.whatsappSessionDBPath(accountID)
	dsn := fmt.Sprintf(
		"file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"+
			"&_pragma=busy_timeout(10000)&_pragma=synchronous(NORMAL)"+
			"&_pragma=cache_size(-8000)", dbPath)
	dbLog := waLog.Stdout("WAStore:"+accountID, "INFO", true)
	container, err := sqlstore.New(wc.ctx, "sqlite", dsn, dbLog)
	if err != nil {
		wc.cancel()
		return nil, fmt.Errorf("whatsmeow store: %w", err)
	}
	deviceStore, err := container.GetFirstDevice(wc.ctx)
	if err != nil {
		wc.cancel()
		return nil, fmt.Errorf("device store: %w", err)
	}
	clientLog := waLog.Stdout("WAClient:"+accountID, "INFO", true)
	wc.client = whatsmeow.NewClient(deviceStore, clientLog)
	wc.store = container

	if wc.client.Store.ID != nil {
		if err := wc.client.Connect(); err != nil {
			wc.cancel()
			return nil, fmt.Errorf("connect: %w", err)
		}
		m.whatsappMu.Lock()
		m.whatsappClients[key] = wc
		m.whatsappMu.Unlock()
		log.Printf("[WhatsApp] %s/%s connected with existing session", req.PlatformID, req.Subtype)
		return wc, nil
	}

	qrChan, err := wc.client.GetQRChannel(wc.ctx)
	if err != nil {
		wc.cancel()
		return nil, fmt.Errorf("QR channel: %w", err)
	}
	go wc.startQRServer(qrChan)
	if err := wc.client.Connect(); err != nil {
		wc.cancel()
		return nil, fmt.Errorf("connect for QR: %w", err)
	}
	select {
	case <-wc.qrDone:
		m.whatsappMu.Lock()
		m.whatsappClients[key] = wc
		m.whatsappMu.Unlock()
		log.Printf("[WhatsApp] %s/%s QR pairing successful", req.PlatformID, req.Subtype)
		return wc, nil
	case <-wc.ctx.Done():
		wc.client.Disconnect()
		return nil, fmt.Errorf("QR pairing cancelled")
	}
}

func (m *Manager) GetWhatsAppClient(platformID, subtype string) (*whatsmeow.Client, error) {
	key := fmt.Sprintf("%s:%s", platformID, subtype)
	m.whatsappMu.Lock()
	wc := m.whatsappClients[key]
	m.whatsappMu.Unlock()
	if wc == nil {
		return nil, fmt.Errorf("WhatsApp client not started for %s:%s", platformID, subtype)
	}
	client := wc.GetClient()
	if client == nil {
		return nil, fmt.Errorf("WhatsApp client disconnected for %s:%s", platformID, subtype)
	}
	return client, nil
}

func (m *Manager) StopWhatsApp(platformID, subtype string) {
	key := fmt.Sprintf("%s:%s", platformID, subtype)
	m.whatsappMu.Lock()
	wc := m.whatsappClients[key]
	if wc != nil {
		delete(m.whatsappClients, key)
	}
	m.whatsappMu.Unlock()
	if wc != nil {
		wc.Disconnect()
	}
}

// ------------------------------------------------------------
// Telegram authentication
// ------------------------------------------------------------

func (m *Manager) authenticateTelegram(ctx context.Context, req SessionRequest) (*Session, error) {
	phone, apiID, apiHash := m.getTelegramPhoneAndCreds()
	if phone == "" {
		return nil, fmt.Errorf("missing TELEGRAM_PHONE")
	}
	if apiID == 0 {
		return nil, fmt.Errorf("missing TELEGRAM_API_ID / TELEGRAM_API_HASH")
	}

	req.AccountID = m.normalizeTelegramAccountID()

	client, err := telegram.NewClient(telegram.ClientConfig{
		AppID:   apiID,
		AppHash: apiHash,
	})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	defer client.Disconnect()

	connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
	connectDone := make(chan error, 1)
	go func() { _, connErr := client.Conn(); connectDone <- connErr }()
	select {
	case connErr := <-connectDone:
		connectCancel()
		if connErr != nil {
			return nil, fmt.Errorf("connect: %w", connErr)
		}
	case <-connectCtx.Done():
		connectCancel()
		return nil, fmt.Errorf("connect timed out after 30s")
	}

	sentCode, err := client.AuthSendCode(phone, apiID, apiHash, &telegram.CodeSettings{})
	if err != nil {
		return nil, fmt.Errorf("send code: %w", err)
	}

	rawToken := make([]byte, 8)
	if _, err := rand.Read(rawToken); err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}
	token := hex.EncodeToString(rawToken)
	codeCh := make(chan string, 1)
	m.pendingTelegramAuthMu.Lock()
	m.pendingTelegramAuth[token] = codeCh
	m.pendingTelegramAuthMu.Unlock()
	defer func() {
		m.pendingTelegramAuthMu.Lock()
		delete(m.pendingTelegramAuth, token)
		m.pendingTelegramAuthMu.Unlock()
	}()

	m.launchTerminalForCode(token, phone, req.Subtype)
	url := fmt.Sprintf(
		"http://127.0.0.1:8086/submit_auth_code?token=%s&platform_id=%s&subtype_id=%s&code=<YOUR_CODE>",
		token, req.PlatformID, req.Subtype)
	log.Printf("[TelegramAuth] Submit the code: curl -X POST %q", url)

	var code string
	select {
	case code = <-codeCh:
		log.Printf("[TelegramAuth] Code received, signing in...")
	case <-time.After(5 * time.Minute):
		return nil, fmt.Errorf("timeout waiting for auth code")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	sentCodeObj, ok := sentCode.(*telegram.AuthSentCodeObj)
	if !ok {
		return nil, fmt.Errorf("unexpected AuthSentCode type: %T", sentCode)
	}

	signDone := make(chan error, 1)
	go func() {
		_, e := client.AuthSignIn(phone, sentCodeObj.PhoneCodeHash, code, nil)
		signDone <- e
	}()
	select {
	case err = <-signDone:
	case <-time.After(30 * time.Second):
		err = fmt.Errorf("sign in timed out")
	}

	if err != nil {
		if strings.Contains(err.Error(), "SESSION_PASSWORD_NEEDED") {
			password := m.getTelegram2FAPassword()
			accountPassword, pErr := client.AccountGetPassword()
			if pErr != nil {
				return nil, fmt.Errorf("get password info: %w", pErr)
			}
			input, pErr := telegram.GetInputCheckPassword(password, accountPassword)
			if pErr != nil {
				return nil, fmt.Errorf("generate SRP: %w", pErr)
			}
			if _, pErr := client.AuthCheckPassword(input); pErr != nil {
				return nil, fmt.Errorf("2FA failed: %w", pErr)
			}
		} else {
			return nil, fmt.Errorf("sign in: %w", err)
		}
	}

	sessionString := client.ExportSession()
	if sessionString == "" {
		return nil, fmt.Errorf("exported session string is empty")
	}

	tgDir := filepath.Join(m.storage.GetSessionsDir(), "telegram")
	os.MkdirAll(tgDir, 0750)
	sessionFile := filepath.Join(tgDir, fmt.Sprintf("tg_%s_%s.session", req.Subtype, req.AccountID))
	if err := os.WriteFile(sessionFile, []byte(sessionString), 0600); err != nil {
		return nil, fmt.Errorf("write session: %w", err)
	}
	log.Printf("[TelegramAuth] Session saved to %s", sessionFile)

	profileID := m.getOrCreateProfileID(req.PlatformID, req.Subtype)
	sess := &Session{
		PlatformID: req.PlatformID,
		Subtype:    req.Subtype,
		AccountID:  req.AccountID,
		State:      StateAuthenticated,
		Cookies:    []Cookie{},
		CreatedAt:  time.Now(),
		LastUsed:   time.Now(),
		ExpiresAt:  time.Now().Add(30 * 24 * time.Hour),
		Metadata: map[string]string{
			"profile_id":   profileID,
			"session_type": "gogram_mtproto",
			"session_file": sessionFile,
		},
	}
	m.persistSession(sess)
	log.Printf("[TelegramAuth] ✓ Authenticated (account: %s)", req.AccountID)
	return sess, nil
}

func (m *Manager) StartTelegram(req SessionRequest) error {
	phone, apiID, apiHash := m.getTelegramPhoneAndCreds()
	if phone == "" {
		return fmt.Errorf("no Telegram phone number configured")
	}
	if apiID == 0 || apiHash == "" {
		return fmt.Errorf("TELEGRAM_API_ID / TELEGRAM_API_HASH not configured")
	}

	normalized := m.normalizeTelegramAccountID()
	if normalized == "" {
		return fmt.Errorf("cannot normalize Telegram account ID from phone %s", phone)
	}
	req.AccountID = normalized

	tgDir := filepath.Join(m.storage.GetSessionsDir(), "telegram")
	if err := os.MkdirAll(tgDir, 0750); err != nil {
		return fmt.Errorf("cannot create telegram session dir: %w", err)
	}
	sessionFile := filepath.Join(tgDir, fmt.Sprintf("tg_%s_%s.session", req.Subtype, req.AccountID))

	if _, err := os.Stat(sessionFile); err == nil && !req.ForceNew {
		log.Printf("[TelegramAuth] Session file already exists for %s:%s, skipping auth", req.PlatformID, req.Subtype)
		return nil
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		log.Printf("[TelegramAuth] Starting background authentication for %s:%s", req.PlatformID, req.Subtype)
		if _, err := m.authenticateTelegram(ctx, req); err != nil {
			log.Printf("[TelegramAuth] Authentication failed for %s:%s: %v", req.PlatformID, req.Subtype, err)
		} else {
			log.Printf("[TelegramAuth] ✓ Authentication succeeded for %s:%s", req.PlatformID, req.Subtype)
		}
	}()

	return nil
}

func (m *Manager) SubmitTelegramAuthCode(token, code string) {
	m.pendingTelegramAuthMu.Lock()
	ch, ok := m.pendingTelegramAuth[token]
	if ok {
		delete(m.pendingTelegramAuth, token)
	}
	m.pendingTelegramAuthMu.Unlock()
	if ok {
		ch <- code
	}
}

// ------------------------------------------------------------
// GetTelegramClient
// ------------------------------------------------------------

func (m *Manager) GetTelegramClient(ctx context.Context, req SessionRequest) (*telegram.Client, error) {
	sess, err := m.GetSession(ctx, req)
	if err != nil {
		return nil, err
	}
	if sess.Metadata == nil || sess.Metadata["session_file"] == "" {
		return nil, fmt.Errorf("telegram session file not found")
	}
	sessionFile := sess.Metadata["session_file"]
	data, err := os.ReadFile(sessionFile)
	if err != nil {
		return nil, fmt.Errorf("read session file: %w", err)
	}
	sessionString := strings.TrimSpace(string(data))

	_, apiID, apiHash := m.getTelegramPhoneAndCreds()
	if apiID == 0 {
		return nil, fmt.Errorf("telegram API ID not configured")
	}
	cfg := telegram.ClientConfig{
		AppID:         apiID,
		AppHash:       apiHash,
		StringSession: sessionString,
		MemorySession: true,
	}
	client, err := telegram.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Conn()
		done <- err
	}()
	select {
	case err = <-done:
		if err != nil {
			return nil, fmt.Errorf("connect: %w", err)
		}
	case <-connectCtx.Done():
		return nil, fmt.Errorf("connect timeout")
	}
	return client, nil
}

// ------------------------------------------------------------
// Telegram credential helpers
// ------------------------------------------------------------

func (m *Manager) getTelegramPhoneAndCreds() (phone string, apiID int32, apiHash string) {
	if cfg := m.config; cfg != nil {
		if plat, ok := cfg.Platforms["telegram"]; ok {
			for _, sub := range plat.Subtypes {
				if !sub.Enabled || sub.Auth == nil {
					continue
				}
				if phone == "" {
					if v, ok2 := sub.Auth["phone_number"]; ok2 {
						phone = strings.TrimSpace(fmt.Sprintf("%v", v))
					}
					if phone == "" {
						if v, ok2 := sub.Auth["phone"]; ok2 {
							phone = strings.TrimSpace(fmt.Sprintf("%v", v))
						}
					}
				}
				if apiID == 0 {
					if v, ok2 := sub.Auth["api_id"]; ok2 {
						id, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprintf("%v", v)), 10, 32)
						apiID = int32(id)
					}
				}
				if apiHash == "" {
					if v, ok2 := sub.Auth["api_hash"]; ok2 {
						apiHash = strings.TrimSpace(fmt.Sprintf("%v", v))
					}
				}
				if phone != "" && apiID != 0 && apiHash != "" {
					break
				}
			}
			if plat.Telegram != nil {
				if phone == "" && plat.Telegram.Account != nil {
					phone = strings.TrimSpace(plat.Telegram.Account.PhoneNumber)
				}
				if apiID == 0 && plat.Telegram.APIID != "" {
					id, _ := strconv.ParseInt(strings.TrimSpace(plat.Telegram.APIID), 10, 32)
					apiID = int32(id)
				}
				if apiHash == "" {
					apiHash = plat.Telegram.APIHash
				}
			}
		}
	}
	if phone == "" {
		phone = os.Getenv("TELEGRAM_PHONE")
	}
	if !strings.HasPrefix(phone, "+") && phone != "" {
		phone = "+" + phone
	}
	if apiID == 0 {
		if v := os.Getenv("TELEGRAM_API_ID"); v != "" {
			id, _ := strconv.ParseInt(v, 10, 32)
			apiID = int32(id)
		}
	}
	if apiHash == "" {
		apiHash = os.Getenv("TELEGRAM_API_HASH")
	}
	return
}

func (m *Manager) getTelegram2FAPassword() string {
	if cfg := m.config; cfg != nil {
		if plat, ok := cfg.Platforms["telegram"]; ok {
			for _, sub := range plat.Subtypes {
				if !sub.Enabled || sub.Auth == nil {
					continue
				}
				if v, ok2 := sub.Auth["password"]; ok2 {
					if pw := strings.TrimSpace(fmt.Sprintf("%v", v)); pw != "" {
						return pw
					}
				}
			}
			if plat.Telegram != nil && plat.Telegram.Account != nil {
				if pw := strings.TrimSpace(plat.Telegram.Account.Password); pw != "" {
					return pw
				}
			}
		}
	}
	return os.Getenv("TELEGRAM_2FA_PASSWORD")
}

func (m *Manager) launchTerminalForCode(token, phone, subtype string) {
	url := fmt.Sprintf(
		"http://127.0.0.1:8086/submit_auth_code?token=%s&platform_id=telegram&subtype_id=%s",
		token, subtype,
	)
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		script := fmt.Sprintf(
			"@echo off\necho Telegram login code was sent to %s.\n"+
				"set /p CODE=\"Enter code: \"\ncurl -X POST \"%s&code=%%CODE%%\"\n"+
				"echo Code submitted.\npause", phone, url)
		tmpFile := filepath.Join(os.TempDir(), "tg_auth.bat")
		os.WriteFile(tmpFile, []byte(script), 0644)
		cmd = exec.Command("cmd", "/c", "start", "cmd", "/k", tmpFile)
	default:
		script := fmt.Sprintf(
			"#!/bin/bash\necho \"Telegram login code sent to %s.\"\n"+
				"read -p \"Enter code: \" CODE\ncurl -X POST \"%s&code=$CODE\"\n"+
				"echo \"Code submitted.\"", phone, url)
		tmpFile := filepath.Join(os.TempDir(), "tg_auth.sh")
		os.WriteFile(tmpFile, []byte(script), 0755)
		cmd = exec.Command("x-terminal-emulator", "-e", "bash", tmpFile)
		if _, err := exec.LookPath("x-terminal-emulator"); err != nil {
			cmd = exec.Command("gnome-terminal", "--", "bash", tmpFile)
		}
	}
	if cmd != nil {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			log.Printf("[TelegramAuth] Failed to launch terminal: %v", err)
		}
	}
}

// ------------------------------------------------------------
// Utility helpers
// ------------------------------------------------------------

func convertCookies(cookies []*network.Cookie) []Cookie {
	result := make([]Cookie, len(cookies))
	for i, c := range cookies {
		result[i] = Cookie{
			Name:     c.Name,
			Value:    c.Value,
			Domain:   c.Domain,
			Path:     c.Path,
			Expires:  int64(c.Expires),
			Secure:   c.Secure,
			HTTPOnly: c.HTTPOnly,
		}
	}
	return result
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ------------------------------------------------------------
// QR page HTML
// ------------------------------------------------------------

const qrPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>WhatsApp QR Login</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
         background:#111b21;color:#e9edef;display:flex;flex-direction:column;
         align-items:center;justify-content:center;min-height:100vh;gap:24px;padding:24px}
    h1{font-size:1.4rem;font-weight:600}
    p{font-size:.9rem;color:#8696a0;text-align:center;max-width:320px}
    #qr-wrap{background:#fff;border-radius:12px;padding:20px;display:flex;
             align-items:center;justify-content:center;min-width:260px;min-height:260px}
    #status{font-size:.85rem;color:#00a884;min-height:1.2em}
    .spinner{width:32px;height:32px;border:3px solid #2a3942;border-top-color:#00a884;
             border-radius:50%;animation:spin .8s linear infinite}
    @keyframes spin{to{transform:rotate(360deg)}}
  </style>
</head>
<body>
  <h1>Scan with WhatsApp</h1>
  <p>Open WhatsApp → Linked Devices → Link a device, then scan.</p>
  <div id="qr-wrap"><div class="spinner"></div></div>
  <div id="status">Waiting for QR code…</div>
  <script>
    function render(text){
      var wrap=document.getElementById('qr-wrap');
      wrap.innerHTML='';
      new QRCode(wrap,{text:text,width:240,height:240,
        colorDark:'#000000',colorLight:'#ffffff',
        correctLevel:QRCode.CorrectLevel.L});
      document.getElementById('status').textContent='Ready — scan now';
    }
    var es=new EventSource('/events');
    es.addEventListener('qr',function(e){render(e.data)});
    es.addEventListener('success',function(){
      document.getElementById('status').textContent='✅ Paired! You can close this tab.';
      document.getElementById('qr-wrap').innerHTML='<div style="font-size:4rem">✅</div>';
      es.close();
    });
    es.addEventListener('timeout',function(){
      document.getElementById('status').textContent='⏰ QR expired — restart to get a new code.';
      es.close();
    });
    es.onerror=function(){
      document.getElementById('status').textContent='Connection lost.';
    };
  </script>
</body>
</html>`
