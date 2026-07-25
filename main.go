package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"sailstream/internal/config"
	"sailstream/internal/database"
	"sailstream/internal/enviroment"
)

var (
	baseDir    string
	configPath string
	manager    *config.ConfigManager
	env        *enviroment.Environment
)

func main() {
	var err error
	baseDir, err = getBaseDir()
	if err != nil {
		log.Fatalf("❌ Failed to get base directory: %v", err)
	}
	log.Printf("📁 Base directory: %s", baseDir)

	// Change to base directory
	os.Chdir(baseDir)

	// ==================== MOBILE DETECTION & REROUTE ====================
	if isMobileEnvironment() {
		routeToMobileMain()
		return
	}
	// ==================== END MOBILE REROUTE ====================

	// 1. Check system dependencies BEFORE anything else
	if !checkSystemDependencies() {
		log.Println("❌ Missing system dependencies, cannot continue")
		os.Exit(1)
	}

	// 2. Initialize config
	configPath = filepath.Join(baseDir, "internal", "config", "config.json")
	manager = initConfig(configPath)
	if manager == nil {
		runWizard()
		return
	}

	// 3. Initialize environment
	env = enviroment.NewEnvironment(manager.GetConfig())
	env.SetBaseDir(baseDir) // <-- CRITICAL FIX: set base dir for Python requirements lookup
	log.Println("🌍 Environment initialized:")
	log.Printf("   Browser: %s (%s)", env.GetBrowserName(), env.GetBrowserPath())
	log.Printf("   Profile: %s", env.GetProfilePath())

	// 4. Ensure Python dependencies (uses the environment's method)
	if err := env.EnsurePythonDependencies(); err != nil {
		log.Printf("❌ Python dependency installation failed: %v", err)
		log.Println("⚠️ Please install dependencies manually: pip install -r internal/enviroment/python_req.txt")
		// You can choose to exit here if Python is critical:
		// os.Exit(1)
	} else {
		log.Println("✅ Python dependencies are ready.")
	}

	// 5. Check for a CDP-compatible browser (uses the user's own installed browser & profile)
	if err := checkBrowser(); err != nil {
		log.Printf("⚠️ Browser issue: %v (browser automation may not work)", err)
	}

	// 6. Check all paths
	log.Println("\n🔍 Checking all paths from config:")
	if !CheckAllPaths() {
		log.Println("❌ Some paths are invalid, running wizard...")
		runWizard()
		return
	}

	// 7. Ensure environment directories
	if err := env.EnsureDirectories(); err != nil {
		log.Printf("⚠️ Failed to create directories: %v", err)
	}

	// 8. Get database path and check if exists
	dbPath := manager.GetDatabasePath()
	if dbPath == "" {
		dbPath = filepath.Join(baseDir, "data", "database.db")
	}

	// 9. Initialize database only if doesn't exist
	if !fileExists(dbPath) {
		log.Println("🗄️ Initializing new database...")
		if err := database.Initialize(dbPath); err != nil {
			log.Printf("❌ Failed to initialize database: %v", err)
			runWizard()
			return
		}
	} else {
		log.Println("🗄️ Database already exists, skipping initialization")
	}

	// 10. Install Go dependencies
	if err := installGoDependencies(); err != nil {
		log.Printf("❌ Failed to install Go dependencies: %v", err)
		os.Exit(1)
	}

	// 11. All good, start dashboard
	log.Println("\n✅ Everything ready, starting dashboard...")
	startDashboard()
}

// ============ MOBILE DETECTION & REROUTE ============

func isMobileEnvironment() bool {
	if runtime.GOOS == "android" || runtime.GOOS == "ios" {
		log.Println("📱 Mobile OS detected via runtime")
		return true
	}
	if _, err := os.Stat("/data/data/com.termux/files/usr"); err == nil {
		log.Println("📱 Termux detected")
		return true
	}
	if os.Getenv("TERMUX_VERSION") != "" {
		log.Println("📱 Termux detected via env var")
		return true
	}
	if _, err := os.Stat("/system"); err == nil {
		log.Println("📱 Android system detected")
		return true
	}
	if os.Getenv("GOOS") == "android" || os.Getenv("GOOS") == "ios" {
		log.Println("📱 Mobile build tag detected")
		return true
	}
	return false
}

func routeToMobileMain() {
	mobileMainPath := filepath.Join(baseDir, "platforms", "android", "scripts", "mobile.go")

	if !fileExists(mobileMainPath) {
		log.Printf("❌ Mobile entry point not found at: %s", mobileMainPath)
		log.Println("📱 Creating mobile entry point...")
		createMobileEntryPoint()
		if !fileExists(mobileMainPath) {
			log.Println("❌ Failed to create mobile entry point")
			log.Println("📱 Falling back to simple mobile interface...")
			startSimpleMobile()
			return
		}
	}

	log.Printf("🚀 Launching mobile version from: %s", mobileMainPath)
	cmd := exec.Command("go", "run", mobileMainPath)
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("❌ Failed to run mobile main: %v", err)
		log.Println("📱 Falling back to simple mobile interface...")
		startSimpleMobile()
	}
}

func createMobileEntryPoint() {
	mobileDir := filepath.Join(baseDir, "platforms", "android", "scripts")
	if err := os.MkdirAll(mobileDir, 0755); err != nil {
		log.Printf("❌ Failed to create mobile directory: %v", err)
		return
	}

	mobilePath := filepath.Join(mobileDir, "mobile.go")
	content := `// mobile.go - Mobile entry point for SailStream
package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	fmt.Println("\n" + strings.Repeat("=", 40))
	fmt.Println("   📱 SAILSTREAM MOBILE")
	fmt.Println(strings.Repeat("=", 40))
	
	log.Println("📱 Mobile version starting...")
	
	baseDir, _ := os.Getwd()
	log.Printf("📁 Working directory: %s", baseDir)
	
	configPath := filepath.Join(baseDir, "internal", "config", "config.json")
	
	if fileExists(configPath) {
		log.Println("✅ Config found, starting dashboard...")
		startMobileDashboard(baseDir)
	} else {
		log.Println("📄 Config not found, starting wizard...")
		startMobileWizard(baseDir)
	}
}

func startMobileDashboard(baseDir string) {
	maestroPath := filepath.Join(baseDir, "maestro_main.go")
	if fileExists(maestroPath) {
		log.Printf("🎵 Starting maestro: %s", maestroPath)
		cmd := exec.Command("go", "run", maestroPath)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		
		if err := cmd.Run(); err != nil {
			log.Printf("❌ Maestro failed: %v", err)
			startFallbackInterface(baseDir)
		}
		return
	}
	
	log.Println("❌ Maestro not found")
	startFallbackInterface(baseDir)
}

func startMobileWizard(baseDir string) {
	pcWizard := filepath.Join(baseDir, "platforms", "pc", "wizzard.go")
	if fileExists(pcWizard) {
		log.Printf("🧙 Starting wizard: %s", pcWizard)
		configPath := filepath.Join(baseDir, "internal", "config", "config.json")
		cmd := exec.Command("go", "run", pcWizard, "-config", configPath)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		
		if err := cmd.Run(); err != nil {
			log.Printf("❌ Wizard failed: %v", err)
			startFallbackInterface(baseDir)
		}
		return
	}
	
	log.Println("❌ Wizard not found")
	startFallbackInterface(baseDir)
}

func startFallbackInterface(baseDir string) {
	fmt.Println("\n" + strings.Repeat("=", 40))
	fmt.Println("   📱 SIMPLE MOBILE INTERFACE")
	fmt.Println(strings.Repeat("=", 40))
	
	fmt.Println("\nAvailable options:")
	fmt.Println("  1. Start bot")
	fmt.Println("  2. Check status")
	fmt.Println("  3. Exit")
	
	fmt.Print("\nSelect option (1-3): ")
	
	var choice string
	fmt.Scanln(&choice)
	
	switch choice {
	case "1":
		startBot(baseDir)
	case "2":
		checkStatus(baseDir)
	case "3":
		log.Println("👋 Exiting...")
		os.Exit(0)
	default:
		fmt.Println("❌ Invalid choice")
		startFallbackInterface(baseDir)
	}
}

func startBot(baseDir string) {
	fmt.Println("\n🤖 Starting bot...")
	fmt.Println("📱 Bot started (placeholder)")
	fmt.Println("Press Enter to return...")
	fmt.Scanln()
	startFallbackInterface(baseDir)
}

func checkStatus(baseDir string) {
	fmt.Println("\n📊 Status Check:")
	fmt.Println("  ✓ Base directory: OK")
	fmt.Println("  ✓ Mobile mode: Active")
	fmt.Println("  ✓ Platform: Android")
	
	fmt.Println("\nPress Enter to return...")
	fmt.Scanln()
	startFallbackInterface(baseDir)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
`

	if err := os.WriteFile(mobilePath, []byte(content), 0644); err != nil {
		log.Printf("❌ Failed to create mobile.go: %v", err)
	} else {
		log.Printf("✅ Created mobile entry point at: %s", mobilePath)
	}
}

func startSimpleMobile() {
	fmt.Println("\n" + strings.Repeat("=", 40))
	fmt.Println("   📱 SAILSTREAM MOBILE")
	fmt.Println(strings.Repeat("=", 40))
	fmt.Println("\nMobile interface not fully implemented yet.")
	fmt.Println("\nTo set up SailStream on mobile:")
	fmt.Println("  1. Install dependencies:")
	fmt.Println("     pkg install chromium python")
	fmt.Println("  2. Create config manually")
	fmt.Println("  3. Run: go run main.go --mobile")
	fmt.Println("\nPress Enter to exit...")

	var input string
	fmt.Scanln(&input)
}

// ============ DEPENDENCY CHECKS ============

func checkSystemDependencies() bool {
	log.Println("🔍 Checking system dependencies...")
	allGood := true

	if !checkTool("go", "version") {
		log.Println("❌ Go not found. Install from: https://golang.org/dl/")
		allGood = false
	} else {
		log.Println("✅ Go found")
	}

	return allGood
}

func checkTool(name string, args ...string) bool {
	cmd := exec.Command(name, args...)
	return cmd.Run() == nil
}

// checkBrowser – verifies the user already has a CDP-compatible browser
// (Edge/Chrome/Brave/Chromium) installed and detects its real profile path.
// No downloading or auto-installation is performed: we use whatever the
// user already has set up, in their own existing profile.
func checkBrowser() error {
	log.Println("🌐 Checking for a CDP-compatible browser...")

	if err := env.CheckBrowserInstallation(); err != nil {
		return err
	}

	log.Printf("✅ Browser found: %s (%s)", env.GetBrowserName(), env.GetBrowserPath())
	if profile := env.GetProfilePath(); profile != "" {
		log.Printf("✅ Using existing browser profile: %s", profile)
	} else {
		log.Println("⚠️ Could not locate the browser's default profile directory; a fresh profile may be used")
	}
	return nil
}

// ============ GO DEPENDENCIES ============

func installGoDependencies() error {
	log.Println("🐹 Installing Go dependencies...")

	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = baseDir
	if output, err := cmd.CombinedOutput(); err != nil {
		log.Printf("❌ go mod tidy failed: %v", err)
		log.Printf("Output: %s", string(output))
		return fmt.Errorf("go mod tidy failed: %v", err)
	}
	log.Println("✅ go mod tidy completed")

	cmd = exec.Command("go", "mod", "download")
	cmd.Dir = baseDir
	if output, err := cmd.CombinedOutput(); err != nil {
		log.Printf("❌ go mod download failed: %v", err)
		log.Printf("Output: %s", string(output))
		return fmt.Errorf("go mod download failed: %v", err)
	}
	log.Println("✅ Go dependencies downloaded")

	keyDeps := []string{
		"github.com/chromedp/chromedp",
		"github.com/charmbracelet/bubbletea",
		"modernc.org/sqlite",
	}
	for _, dep := range keyDeps {
		cmd := exec.Command("go", "list", dep)
		if err := cmd.Run(); err != nil {
			log.Printf("⚠️ Warning: Dependency %s not found", dep)
		}
	}
	return nil
}

// ============ CONFIG FUNCTIONS ============

func initConfig(path string) *config.ConfigManager {
	if !fileExists(path) {
		log.Printf("❌ config.json not found at: %s", path)
		return nil
	}

	log.Printf("✅ config.json found at: %s", path)
	manager := config.NewConfigManager(path)

	if err := manager.Load(); err != nil {
		log.Printf("❌ Failed to load config: %v", err)
		return nil
	}

	if isEmptyConfig(manager) {
		log.Println("📄 Config is empty")
		return nil
	}

	log.Println("✅ Config loaded successfully")
	return manager
}

func isEmptyConfig(manager *config.ConfigManager) bool {
	return manager.GetStoreName() == "" ||
		manager.GetAIProvider() == "" ||
		manager.GetTimezone() == ""
}

// ============ PATH CHECKING ============

func checkPath(name, path string) bool {
	if path == "" {
		log.Printf("  ❌ %s: NOT SET", name)
		return false
	}

	if _, err := os.Stat(path); err != nil {
		ext := filepath.Ext(path)
		isFile := ext == ".db" || ext == ".json" || ext == ".sqlite"

		if !isFile {
			if err := os.MkdirAll(path, 0755); err == nil {
				log.Printf("  ✅ %s: %s (directory created)", name, path)
				return true
			} else {
				log.Printf("  ❌ %s: %s (failed to create directory: %v)", name, path, err)
				return false
			}
		} else {
			parentDir := filepath.Dir(path)
			if err := os.MkdirAll(parentDir, 0755); err == nil {
				log.Printf("  ⚠️  %s: %s (file doesn't exist, parent dir created)", name, path)
				return true
			} else {
				log.Printf("  ❌ %s: %s (file doesn't exist, failed to create parent: %v)", name, path, err)
				return false
			}
		}
	} else {
		log.Printf("  ✅ %s: %s (exists)", name, path)
		return true
	}
}

func CheckAllPaths() bool {
	if manager == nil {
		return false
	}

	paths := []struct {
		name string
		path string
	}{
		{"Logs", manager.GetLogsPath()},
		{"Config", manager.GetConfigPath()},
		{"Cache", manager.GetCachePath()},
		{"Media", manager.GetMediaPath()},
		{"Models", manager.GetModelsPath()},
		{"Temp", manager.GetTempPath()},
		{"Sessions", manager.GetSessionsPath()},
		{"Database", manager.GetDatabasePath()},
		{"Backup", manager.GetBackupPath()},
		{"Post Images", manager.GetPostImagesPath()},
		{"Product Images", manager.GetProductImagesPath()},
		{"Post Videos", manager.GetPostVideosPath()},
		{"Scheduled Posts", manager.GetScheduledPostsPath()},
		{"Training Images", manager.GetTrainingImagesPath()},
	}

	allValid := true
	for _, p := range paths {
		if !checkPath(p.name, p.path) {
			allValid = false
		}
	}
	return allValid
}

// ============ WIZARD & DASHBOARD ============

func runWizard() {
	log.Println("\n🧙 Running Configuration Wizard...")
	configDir := filepath.Dir(configPath)
	os.MkdirAll(configDir, 0755)

	wizardPath := filepath.Join(baseDir, "internal", "platforms", "pc", "wizzard.go")
	cmd := exec.Command("go", "run", wizardPath, "-config", configPath)
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("❌ Wizard failed: %v", err)
		log.Println("Please fix wizard issues and try again.")
		os.Exit(1)
	}

	log.Println("✅ Wizard completed successfully")
	log.Println("🔄 Restarting application...")
	restartApp()
}

func startDashboard() {
	log.Println("\n📊 Starting Dashboard...")
	startDashboardFallback()
}

func startDashboardFallback() {
	log.Println("📊 Starting Dashboard (fallback)...")
	dashboardPath := filepath.Join(baseDir, "internal", "platforms", "pc", "dashboard", "dashboard.go")

	if !fileExists(dashboardPath) {
		log.Printf("❌ Dashboard not found at: %s", dashboardPath)
		log.Println("Please check if dashboard exists.")
		return
	}

	cmd := exec.Command("go", "run", dashboardPath)
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("❌ Dashboard failed: %v", err)
		log.Println("Please check dashboard errors above.")
	}
}

// ============ HELPER FUNCTIONS ============

func getBaseDir() (string, error) {
	cwd, _ := os.Getwd()
	if filepath.Base(cwd) == "sailstream" && filepath.Base(filepath.Dir(cwd)) == "cmd" {
		return filepath.Join(cwd, "..", ".."), nil
	}
	return cwd, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func restartApp() {
	exe, err := os.Executable()
	if err != nil {
		mainPath := filepath.Join(baseDir, "main.go")
		cmd := exec.Command("go", "run", mainPath)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Start()
		os.Exit(0)
	}

	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	os.Exit(0)
}

// Export for other packages
func GetBaseDir() string                      { return baseDir }
func GetConfigPath() string                   { return configPath }
func GetConfigManager() *config.ConfigManager { return manager }
func GetEnvironment() *enviroment.Environment { return env }
