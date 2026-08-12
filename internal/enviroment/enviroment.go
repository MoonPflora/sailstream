package enviroment

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sailstream/internal/config"
	"strings"
)

type Environment struct {
	isTermux       bool
	isMobile       bool
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	BrowserPath    string `json:"browser_path"` // CDP‑compatible browser (Edge/Chrome/Brave/Chromium)
	BrowserName    string `json:"browser_name"` // "chrome", "edge", "brave", "chromium"
	ProfilePath    string `json:"profile_path"` // path to the user's real default browser profile
	DataDir        string `json:"data_dir"`
	TempDir        string `json:"temp_dir"`
	DeviceModel    string `json:"device_model"`
	AndroidVersion string `json:"android_version"`
	BrowserVersion string `json:"browser_version"`

	BaseDir string
	errors  []string
}

// Getters
func (e *Environment) GetBrowserPath() string    { return e.BrowserPath }
func (e *Environment) GetBrowserName() string    { return e.BrowserName }
func (e *Environment) GetProfilePath() string    { return e.ProfilePath }
func (e *Environment) GetDataDir() string        { return e.DataDir }
func (e *Environment) GetTempDir() string        { return e.TempDir }
func (e *Environment) IsTermux() bool            { return e.isTermux }
func (e *Environment) IsMobile() bool            { return e.isMobile }
func (e *Environment) GetErrors() []string       { return e.errors }
func (e *Environment) HasErrors() bool           { return len(e.errors) > 0 }
func (e *Environment) GetDeviceModel() string    { return e.DeviceModel }
func (e *Environment) GetAndroidVersion() string { return e.AndroidVersion }
func (e *Environment) GetBrowserVersion() string { return e.BrowserVersion }

func (e *Environment) SetBaseDir(dir string) {
	e.BaseDir = dir
}

func NewEnvironment(cfg *config.Config) *Environment {
	env := &Environment{
		OS:   runtime.GOOS,
		Arch: runtime.GOARCH,
	}

	env.isTermux = env.detectTermuxSafe()
	env.isMobile = env.detectMobileSafe(cfg)
	if env.isTermux || env.isMobile {
		env.detectDeviceInfo()
	}
	env.BrowserPath = env.getBrowserPathSafe()
	env.ProfilePath = env.getProfilePathSafe()
	env.DataDir = env.getDataDirSafe(cfg)
	env.TempDir = env.getTempDirSafe(cfg)
	env.detectBrowserVersion()
	env.logEnvironment()
	return env
}

// detectDeviceInfo (unchanged)
func (e *Environment) detectDeviceInfo() {
	if e.isTermux {
		e.detectViaTermux()
	} else if e.isMobile && !e.isTermux {
		e.detectViaAndroid()
	}
}

func (e *Environment) detectViaTermux() {
	props := map[string]string{
		"ro.product.model":         "DeviceModel",
		"ro.product.manufacturer":  "Manufacturer",
		"ro.product.brand":         "Brand",
		"ro.build.version.release": "AndroidVersion",
		"ro.build.version.sdk":     "SDK",
		"ro.product.name":          "ProductName",
		"ro.build.id":              "BuildID",
	}
	for prop, field := range props {
		if value := e.getTermuxProperty(prop); value != "" {
			switch field {
			case "DeviceModel":
				e.DeviceModel = value
			case "AndroidVersion":
				e.AndroidVersion = value
			}
			log.Printf("[Env] Detected %s: %s", prop, value)
		}
	}
	if e.DeviceModel == "" {
		if model := e.getTermuxCommand("uname", "-m"); model != "" {
			e.DeviceModel = model
		}
	}
	if res := e.getTermuxCommand("termux-window-size"); res != "" {
		log.Printf("[Env] Screen: %s", res)
	}
	if battery := e.getTermuxCommand("termux-battery-status"); battery != "" {
		log.Printf("[Env] Battery info available")
	}
}

func (e *Environment) detectViaAndroid() {
	cmd := exec.Command("getprop", "ro.product.model")
	if output, err := cmd.Output(); err == nil {
		e.DeviceModel = strings.TrimSpace(string(output))
	}
	cmd = exec.Command("getprop", "ro.build.version.release")
	if output, err := cmd.Output(); err == nil {
		e.AndroidVersion = strings.TrimSpace(string(output))
	}
}

// detectBrowserVersion – tries to read version from the browser executable
func (e *Environment) detectBrowserVersion() {
	if e.BrowserPath == "" {
		return
	}
	log.Printf("[Env] Checking browser version...")
	if e.OS == "windows" {
		e.detectBrowserVersionWindows()
	} else {
		e.detectBrowserVersionOther()
	}
}

func (e *Environment) detectBrowserVersionWindows() {
	version := e.getBrowserVersionFromFile()
	if version != "" {
		e.BrowserVersion = version
		log.Printf("[Env] Browser version (from file): %s", version)
		return
	}
	cmd := exec.Command("wmic", "datafile", "where",
		fmt.Sprintf("name='%s'", strings.ReplaceAll(e.BrowserPath, "\\", "\\\\")),
		"get", "Version", "/value")
	if output, err := cmd.Output(); err == nil {
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "Version=") {
				version = strings.TrimPrefix(line, "Version=")
				version = strings.TrimSpace(version)
				if version != "" {
					e.BrowserVersion = version
					log.Printf("[Env] Browser version (wmic): %s", version)
					return
				}
			}
		}
	}
	log.Printf("[Env] Could not detect browser version via file properties")
}

func (e *Environment) detectBrowserVersionOther() {
	cmd := exec.Command(e.BrowserPath, "--version")
	if output, err := cmd.Output(); err == nil {
		version := strings.TrimSpace(string(output))
		e.BrowserVersion = version
		log.Printf("[Env] Browser version: %s", version)
		if strings.Contains(version, " ") {
			parts := strings.Fields(version)
			for _, part := range parts {
				if strings.Contains(part, ".") {
					e.BrowserVersion = part
					break
				}
			}
		}
	}
}

func (e *Environment) getBrowserVersionFromFile() string {
	if e.OS != "windows" {
		return ""
	}
	psCmd := `(Get-Item "%s").VersionInfo.FileVersion`
	cmd := exec.Command("powershell", "-Command", fmt.Sprintf(psCmd, e.BrowserPath))
	if output, err := cmd.Output(); err == nil {
		version := strings.TrimSpace(string(output))
		if version != "" {
			return version
		}
	}
	return ""
}

// getTermuxProperty, getTermuxCommand (unchanged)
func (e *Environment) getTermuxProperty(prop string) string {
	cmd := exec.Command("getprop", prop)
	if output, err := cmd.Output(); err == nil {
		return strings.TrimSpace(string(output))
	}
	if e.pathExists("/system/build.prop") {
		file, err := os.Open("/system/build.prop")
		if err == nil {
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, prop+"=") {
					return strings.TrimPrefix(line, prop+"=")
				}
			}
		}
	}
	return ""
}

func (e *Environment) getTermuxCommand(command string, args ...string) string {
	cmd := exec.Command(command, args...)
	if output, err := cmd.Output(); err == nil {
		return strings.TrimSpace(string(output))
	}
	return ""
}

// EnsureDirectories (unchanged)
func (e *Environment) EnsureDirectories() error {
	dirs := []string{e.DataDir, e.TempDir}
	subdirs := []string{
		filepath.Join(e.DataDir, "profiles"),
		filepath.Join(e.DataDir, "cache"),
		filepath.Join(e.TempDir, "screenshots"),
		filepath.Join(e.TempDir, "downloads"),
	}
	dirs = append(dirs, subdirs...)
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}
	return nil
}

// CheckBrowserInstallation – verifies a CDP‑compatible browser exists on this system
func (e *Environment) CheckBrowserInstallation() error {
	if e.BrowserPath == "" {
		return fmt.Errorf("no CDP‑compatible browser found (Edge/Chrome/Brave/Chromium). " +
			"Please install one, or set BROWSER_PATH to point to your browser executable")
	}
	if _, err := os.Stat(e.BrowserPath); err != nil {
		return fmt.Errorf("browser not accessible at %s: %v", e.BrowserPath, err)
	}
	return nil
}

// GetBrowserFlags – returns flags for launching the user's own default browser
// with remote debugging enabled, using their real profile (no synthetic user-agent).
func (e *Environment) GetBrowserFlags() []string {
	flags := []string{
		"--remote-debugging-port=0", // let the OS pick a free port; caller reads it from stdout/stderr
	}
	if e.ProfilePath != "" {
		flags = append(flags, fmt.Sprintf("--user-data-dir=%s", e.ProfilePath))
	}
	if e.isMobile || e.isTermux {
		flags = append(flags, "--window-size=320,240")
	}
	return flags
}

// detectTermuxSafe, detectMobileSafe (unchanged)
func (e *Environment) detectTermuxSafe() bool {
	defer func() {
		if r := recover(); r != nil {
			e.addError(fmt.Sprintf("Termux detection panic: %v", r))
		}
	}()
	return e.isTermuxEnvironment()
}

func (e *Environment) detectMobileSafe(cfg *config.Config) bool {
	defer func() {
		if r := recover(); r != nil {
			e.addError(fmt.Sprintf("Mobile detection panic: %v", r))
		}
	}()
	return e.isMobileEnvironment(cfg, e.isTermux)
}

// getBrowserPathSafe – wrapper with panic recovery
func (e *Environment) getBrowserPathSafe() string {
	defer func() {
		if r := recover(); r != nil {
			e.addError(fmt.Sprintf("Browser path detection panic: %v", r))
		}
	}()
	path := e.getBrowserPathInner()
	if path != "" {
		if _, err := os.Stat(path); err != nil {
			e.addError(fmt.Sprintf("Browser path exists but not accessible: %s - %v", path, err))
		}
	}
	return path
}

// getDataDirSafe, getTempDirSafe (unchanged)
func (e *Environment) getDataDirSafe(cfg *config.Config) string {
	defer func() {
		if r := recover(); r != nil {
			e.addError(fmt.Sprintf("Data dir detection panic: %v", r))
		}
	}()
	return e.getDataDirInner(cfg)
}

func (e *Environment) getTempDirSafe(cfg *config.Config) string {
	defer func() {
		if r := recover(); r != nil {
			e.addError(fmt.Sprintf("Temp dir detection panic: %v", r))
		}
	}()
	return e.getTempDirInner(cfg)
}

// isTermuxEnvironment, isMobileEnvironment (unchanged)
func (e *Environment) isTermuxEnvironment() bool {
	if _, err := os.Stat("/data/data/com.termux/files/usr"); err == nil {
		log.Printf("[Env] Termux detected: /data/data/com.termux/files/usr exists")
		return true
	}
	if e.commandExists("termux-setup-storage") {
		log.Printf("[Env] Termux detected: termux-setup-storage exists")
		return true
	}
	if prefix := os.Getenv("PREFIX"); strings.Contains(prefix, "com.termux") {
		log.Printf("[Env] Termux detected via PREFIX: %s", prefix)
		return true
	}
	return false
}

func (e *Environment) isMobileEnvironment(cfg *config.Config, isTermux bool) bool {
	if runtime.GOOS == "android" || runtime.GOOS == "ios" {
		log.Printf("[Env] Mobile detected via runtime.GOOS: %s", runtime.GOOS)
		return true
	}
	if isTermux {
		log.Printf("[Env] Mobile-like environment (Termux)")
		return true
	}
	if _, err := os.Stat("/system"); err == nil {
		log.Printf("[Env] Android detected via /system")
		return true
	}
	return false
}

// getBrowserPathInner – finds a CDP‑compatible browser (Edge, Chrome, Brave)
func (e *Environment) getBrowserPathInner() string {
	// 1. Environment variable (highest priority) – user can override with BROWSER_PATH
	if path := os.Getenv("BROWSER_PATH"); path != "" {
		if e.pathExists(path) || e.commandExists(path) {
			log.Printf("[Env] Browser from BROWSER_PATH: %s", path)
			return path
		}
	}

	if e.isTermux {
		// In Termux, Chromium is often available; we can check its path.
		paths := []string{
			"/data/data/com.termux/files/usr/bin/chromium",
			"/data/data/com.termux/files/usr/bin/chromium-browser",
			"chromium",
		}
		for _, path := range paths {
			if e.commandExists(path) || e.pathExists(path) {
				log.Printf("[Env] Found Chromium in Termux: %s", path)
				return path
			}
		}
		e.addError("No Chromium found in Termux. Install with: pkg install chromium")
		return ""
	}

	switch e.OS {
	case "windows":
		// Common installation paths for Edge, Chrome, Brave (checked in this order,
		// since Edge ships with Windows and is the most likely default)
		candidates := []struct {
			name string
			path string
		}{
			{"edge", `C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`},
			{"edge", `C:\Program Files\Microsoft\Edge\Application\msedge.exe`},
			{"chrome", `C:\Program Files\Google\Chrome\Application\chrome.exe`},
			{"chrome", `C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`},
			{"brave", `C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe`},
			{"brave", `C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe`},
		}
		for _, c := range candidates {
			if e.pathExists(c.path) {
				e.BrowserName = c.name
				return c.path
			}
		}
	case "darwin":
		candidates := []struct {
			name string
			path string
		}{
			{"chrome", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"},
			{"brave", "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"},
			{"edge", "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"},
		}
		for _, c := range candidates {
			if e.pathExists(c.path) {
				e.BrowserName = c.name
				return c.path
			}
		}
	case "linux":
		candidates := []struct {
			name string
			path string
		}{
			{"chrome", "/usr/bin/google-chrome"},
			{"chromium", "/usr/bin/chromium"},
			{"chromium", "/usr/bin/chromium-browser"},
			{"brave", "/usr/bin/brave-browser"},
		}
		for _, c := range candidates {
			if e.pathExists(c.path) || e.commandExists(c.path) {
				e.BrowserName = c.name
				return c.path
			}
		}
	}

	// Fallback: try looking in system PATH for these executables (excluding Firefox)
	pathCandidates := []struct {
		name string
		bin  string
	}{
		{"edge", "msedge"},
		{"chrome", "chrome"},
		{"brave", "brave"},
		{"chromium", "chromium"},
	}
	for _, c := range pathCandidates {
		if path, err := exec.LookPath(c.bin); err == nil {
			log.Printf("[Env] Found browser in PATH: %s", path)
			e.BrowserName = c.name
			return path
		}
	}

	e.addError("No CDP‑compatible browser (Edge, Chrome, Brave, Chromium) found on system. " +
		"Please install one or set BROWSER_PATH environment variable.")
	return ""
}

// getProfilePathSafe – wrapper with panic recovery
func (e *Environment) getProfilePathSafe() string {
	defer func() {
		if r := recover(); r != nil {
			e.addError(fmt.Sprintf("Profile path detection panic: %v", r))
		}
	}()
	return e.getProfilePathInner()
}

// getProfilePathInner – locates the user's own real default profile directory
// for the browser that was detected, so we launch against their existing
// session/cookies/extensions instead of a fresh automation profile.
func (e *Environment) getProfilePathInner() string {
	if path := os.Getenv("BROWSER_PROFILE_PATH"); path != "" {
		if e.pathExists(path) {
			log.Printf("[Env] Profile from BROWSER_PROFILE_PATH: %s", path)
			return path
		}
	}

	if e.BrowserName == "" {
		return ""
	}

	if e.isTermux {
		// Termux Chromium doesn't expose a conventional profile dir.
		return ""
	}

	home := os.Getenv("HOME")
	if home == "" {
		home = os.Getenv("USERPROFILE")
	}

	var path string
	switch e.OS {
	case "windows":
		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData == "" {
			localAppData = filepath.Join(home, "AppData", "Local")
		}
		switch e.BrowserName {
		case "edge":
			path = filepath.Join(localAppData, "Microsoft", "Edge", "User Data")
		case "chrome":
			path = filepath.Join(localAppData, "Google", "Chrome", "User Data")
		case "brave":
			path = filepath.Join(localAppData, "BraveSoftware", "Brave-Browser", "User Data")
		}
	case "darwin":
		switch e.BrowserName {
		case "chrome":
			path = filepath.Join(home, "Library", "Application Support", "Google", "Chrome")
		case "brave":
			path = filepath.Join(home, "Library", "Application Support", "BraveSoftware", "Brave-Browser")
		case "edge":
			path = filepath.Join(home, "Library", "Application Support", "Microsoft Edge")
		}
	default: // linux
		switch e.BrowserName {
		case "chrome":
			path = filepath.Join(home, ".config", "google-chrome")
		case "chromium":
			path = filepath.Join(home, ".config", "chromium")
		case "brave":
			path = filepath.Join(home, ".config", "BraveSoftware", "Brave-Browser")
		case "edge":
			path = filepath.Join(home, ".config", "microsoft-edge")
		}
	}

	if path == "" {
		return ""
	}
	if !e.pathExists(path) {
		e.addError(fmt.Sprintf("Default profile directory for %s not found at %s", e.BrowserName, path))
		return ""
	}
	log.Printf("[Env] Using %s profile: %s", e.BrowserName, path)
	return path
}

// getDataDirInner, getTempDirInner (unchanged)
func (e *Environment) getDataDirInner(cfg *config.Config) string {
	if cfg != nil && cfg.Paths.Cache != "" {
		if err := os.MkdirAll(cfg.Paths.Cache, 0750); err == nil {
			return cfg.Paths.Cache
		}
	}
	if e.isTermux {
		return "/data/data/com.termux/files/home/.sailstream"
	}
	home := os.Getenv("HOME")
	if home == "" {
		home = "."
	}
	switch e.OS {
	case "windows":
		appData := os.Getenv("APPDATA")
		if appData == "" {
			appData = filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming")
		}
		return filepath.Join(appData, "SailStream")
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "SailStream")
	default:
		return filepath.Join(home, ".sailstream")
	}
}

func (e *Environment) getTempDirInner(cfg *config.Config) string {
	if cfg != nil && cfg.Paths.Temp != "" {
		if err := os.MkdirAll(cfg.Paths.Temp, 0750); err == nil {
			return cfg.Paths.Temp
		}
	}
	if e.isTermux {
		return "/data/data/com.termux/files/usr/tmp"
	}
	return filepath.Join(os.TempDir(), "sailstream")
}

// addError, logEnvironment (unchanged)
func (e *Environment) addError(msg string) {
	e.errors = append(e.errors, msg)
	log.Printf("[Env Error] %s", msg)
}

func (e *Environment) logEnvironment() {
	log.Printf("[Environment] === Detection Results ===")
	log.Printf("[Environment] OS: %s, Arch: %s", e.OS, e.Arch)
	log.Printf("[Environment] IsTermux: %v, IsMobile: %v", e.isTermux, e.isMobile)
	if e.DeviceModel != "" {
		log.Printf("[Environment] Device: %s", e.DeviceModel)
	}
	if e.AndroidVersion != "" {
		log.Printf("[Environment] Android: %s", e.AndroidVersion)
	}
	if e.BrowserVersion != "" {
		log.Printf("[Environment] Browser: %s", e.BrowserVersion)
	}
	if e.BrowserName != "" {
		log.Printf("[Environment] Browser Name: %s", e.BrowserName)
	}
	log.Printf("[Environment] Browser Path: %s", e.BrowserPath)
	log.Printf("[Environment] Profile Path: %s", e.ProfilePath)
	log.Printf("[Environment] Data Dir: %s", e.DataDir)
	log.Printf("[Environment] Temp Dir: %s", e.TempDir)
	if len(e.errors) > 0 {
		log.Printf("[Environment] === Issues Found (%d) ===", len(e.errors))
		for i, err := range e.errors {
			log.Printf("[Environment] %d. %s", i+1, err)
		}
		log.Printf("[Environment] === End Issues ===")
	} else {
		log.Printf("[Environment] No issues detected ✓")
	}
}

// pathExists, commandExists (unchanged)
func (e *Environment) pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (e *Environment) commandExists(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

// GetPythonPath, EnsurePythonDependencies, etc. (unchanged)
func (e *Environment) GetPythonPath() string {
	return e.resolvePython()
}

func (e *Environment) EnsurePythonDependencies() error {
	python := e.resolvePython()
	if python == "" {
		return fmt.Errorf("python interpreter not found")
	}
	log.Printf("[Environment] Python found at: %s", python)
	reqFile := e.findRequirementsFile()
	if reqFile == "" {
		return fmt.Errorf("python_req.txt not found in executable dir, working dir, data dir, or base dir")
	}
	log.Printf("[Environment] Using requirements file: %s", reqFile)
	cmd := exec.Command(python, "-m", "pip", "install", "-r", reqFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		e.addError(fmt.Sprintf("pip install failed: %v\nOutput: %s", err, output))
		return fmt.Errorf("failed to install Python dependencies: %v", err)
	}
	log.Printf("[Environment] Python dependencies installed successfully")
	return nil
}

func (e *Environment) resolvePython() string {
	if py := os.Getenv("SAILSTREAM_PYTHON"); py != "" {
		if e.commandExists(py) || e.pathExists(py) {
			return py
		}
	}
	if e.isTermux {
		termuxPy := "/data/data/com.termux/files/usr/bin/python3"
		if e.pathExists(termuxPy) {
			return termuxPy
		}
	}
	for _, name := range []string{"python3", "python"} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return ""
}

func (e *Environment) findRequirementsFile() string {
	exe, err := os.Executable()
	if err == nil {
		dir := filepath.Dir(exe)
		req := filepath.Join(dir, "python_req.txt")
		if e.pathExists(req) {
			return req
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		req := filepath.Join(cwd, "python_req.txt")
		if e.pathExists(req) {
			return req
		}
	}
	if e.DataDir != "" {
		req := filepath.Join(e.DataDir, "python_req.txt")
		if e.pathExists(req) {
			return req
		}
	}
	if e.BaseDir != "" {
		req := filepath.Join(e.BaseDir, "internal", "enviroment", "python_req.txt")
		if e.pathExists(req) {
			return req
		}
		req = filepath.Join(e.BaseDir, "python_req.txt")
		if e.pathExists(req) {
			return req
		}
	}
	return ""
}
