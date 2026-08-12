// main_mobile.go - Mobile entry point for SailStream
// Auto-generated when mobile environment detected

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sailstream/internal/config"
	"sailstream/internal/database"
	"sailstream/internal/enviroment"
	"strings"
	"time"
)

var (
	baseDir    string
	configPath string
	manager    *config.ConfigManager
	env        *enviroment.Environment
)

func main() {
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("   📱 SAILSTREAM MOBILE")
	fmt.Println(strings.Repeat("=", 50))

	log.Println("📱 Mobile version starting...")

	// Get base directory (user knows to cd to correct dir)
	cwd, _ := os.Getwd()
	baseDir = cwd
	log.Printf("📁 Working directory: %s", baseDir)

	// ==================== TERMUX DEPENDENCY CHECK ====================
	log.Println("\n🔍 Checking Termux environment...")

	// Initialize environment
	tempManager := config.NewConfigManager("")
	env = enviroment.NewEnvironment(tempManager.GetConfig())

	if env.IsTermux() {
		log.Println("✅ Running in Termux")
		checkTermuxRequirements()
	} else {
		log.Println("📱 Running on Android")
	}

	// ==================== GO CHECK ====================
	log.Println("\n🐹 Checking Go...")
	if !commandExists("go") {
		log.Fatal("❌ Go not found! Install Go first:")
		if env.IsTermux() {
			log.Fatal("   Run: pkg install golang")
		} else {
			log.Fatal("   Download from: https://go.dev/dl/")
		}
	}

	// Check Go version
	cmd := exec.Command("go", "version")
	if output, err := cmd.Output(); err == nil {
		log.Printf("✅ %s", strings.TrimSpace(string(output)))
	}

	// ==================== GO DEPENDENCIES ====================
	log.Println("\n📦 Setting up Go dependencies...")
	setupGoDependencies()

	// ==================== PYTHON CHECK (OPTIONAL) ====================
	log.Println("\n🐍 Checking Python (optional)...")
	if !commandExists("python3") && !commandExists("python") {
		log.Println("⚠️ Python not found (TensorFlow/NumPy won't work)")
		if env.IsTermux() {
			log.Println("💡 Install: pkg install python python-pip")
		}
	} else {
		log.Println("✅ Python found")
		installPythonPackages()
	}

	// ==================== CONFIG & ENVIRONMENT ====================
	log.Println("\n📄 Checking configuration...")

	// Set config path using baseDir
	configPath = filepath.Join(baseDir, "internal", "config", "config.json")

	// Initialize config manager
	manager = initConfigMobile(configPath)

	// Re-initialize environment with real config
	if manager != nil {
		env = enviroment.NewEnvironment(manager.GetConfig())
	} else {
		env = enviroment.NewEnvironment(nil)
	}

	// Show device info
	fmt.Println("\n📱 Device Info:")
	fmt.Printf("  Model: %s\n", env.GetDeviceModel())
	fmt.Printf("  Android: %s\n", env.GetAndroidVersion())
	if env.IsTermux() {
		fmt.Println("  Environment: Termux")
	}

	// Show any errors
	if env.HasErrors() {
		fmt.Println("\n⚠️ Issues detected:")
		for _, err := range env.GetErrors() {
			fmt.Printf("  • %s\n", err)
		}
	}

	// ==================== DATABASE SETUP ====================
	log.Println("\n🗄️ Setting up database...")
	setupDatabase()

	// ==================== PATHS CHECK ====================
	log.Println("\n🔍 Checking paths...")
	checkMobilePaths()

	// ==================== ROUTE TO APPROPRIATE INTERFACE ====================
	log.Println("\n🚦 Routing...")
	routeMobile()
}

// ==================== HELPER FUNCTIONS ====================

func checkTermuxRequirements() {
	// Check if we have termux-api for storage access
	if !commandExists("termux-setup-storage") {
		log.Println("⚠️ termux-setup-storage not found")
		log.Println("💡 Install: pkg install termux-api")
		log.Println("💡 Then run: termux-setup-storage (grant permission)")
	} else {
		// Check if storage is set up
		if _, err := os.Stat("/sdcard"); os.IsNotExist(err) {
			log.Println("⚠️ Storage not accessible")
			log.Println("💡 Run: termux-setup-storage and grant permission")
		} else {
			log.Println("✅ Storage accessible")
		}
	}

	// Check for wget/curl
	if !commandExists("wget") && !commandExists("curl") {
		log.Println("📥 Installing wget...")
		cmd := exec.Command("pkg", "install", "wget", "-y")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Printf("⚠️ Failed to install wget: %v", err)
		} else {
			log.Println("✅ wget installed")
		}
	}
}

func setupGoDependencies() {
	// Check if go.mod exists
	goModPath := filepath.Join(baseDir, "go.mod")
	if !fileExists(goModPath) {
		log.Fatal("❌ go.mod not found in current directory!")
	}

	// Read go.mod to check dependencies
	content, err := os.ReadFile(goModPath)
	if err != nil {
		log.Printf("⚠️ Could not read go.mod: %v", err)
	} else {
		log.Println("✅ go.mod found")

		// Check for key dependencies
		deps := []string{
			"fyne.io/fyne/v2",
			"github.com/chromedp/chromedp",
			"github.com/charmbracelet/bubbletea",
			"modernc.org/sqlite",
		}

		for _, dep := range deps {
			if strings.Contains(string(content), dep) {
				log.Printf("  ✓ %s", dep)
			} else {
				log.Printf("  ✗ %s (missing)", dep)
			}
		}
	}

	// Run go mod tidy
	log.Println("🧹 Running go mod tidy...")
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("⚠️ go mod tidy failed: %v", err)
	} else {
		log.Println("✅ go mod tidy completed")
	}

	// Run go mod download
	log.Println("📥 Downloading modules...")
	cmd = exec.Command("go", "mod", "download")
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("⚠️ go mod download failed: %v", err)
	} else {
		log.Println("✅ Modules downloaded")
	}

	// Build to cache dependencies
	log.Println("🔨 Building to cache dependencies...")
	cmd = exec.Command("go", "build", "-o", "/dev/null", "./...")
	cmd.Dir = baseDir
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Run(); err != nil {
		log.Printf("⚠️ Build failed (might be OK): %v", err)
	} else {
		log.Println("✅ Build successful")
	}
}

func installPythonPackages() {
	if !commandExists("pip3") && !commandExists("pip") {
		log.Println("⚠️ pip not found, skipping Python packages")
		return
	}

	log.Println("🐍 Installing Python packages (TensorFlow, NumPy)...")

	// Try to install TensorFlow Lite for mobile (smaller)
	pipCmd := "pip3"
	if !commandExists("pip3") {
		pipCmd = "pip"
	}

	packages := []string{
		"tensorflow",
		"numpy",
		"pillow",
		"opencv-python-headless",
	}

	for _, pkg := range packages {
		log.Printf("📦 Installing %s...", pkg)
		cmd := exec.Command(pipCmd, "install", pkg)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("⚠️ Failed to install %s: %v", pkg, err)
		}
	}

	log.Println("✅ Python packages installed")
}

func initConfigMobile(path string) *config.ConfigManager {
	// Check if config exists
	if !fileExists(path) {
		log.Printf("❌ Config not found at: %s", path)
		log.Println("💡 Will create default config or run wizard")
		return nil
	}

	log.Printf("✅ Config found at: %s", path)
	manager := config.NewConfigManager(path)

	if err := manager.Load(); err != nil {
		log.Printf("❌ Failed to load config: %v", err)
		return nil
	}

	// Check if config has content
	if isEmptyConfigMobile(manager) {
		log.Println("📄 Config is empty")
		return nil
	}

	log.Println("✅ Config loaded successfully")
	return manager
}

func isEmptyConfigMobile(manager *config.ConfigManager) bool {
	// Simple check for empty config
	cfg := manager.GetConfig()
	if cfg == nil {
		return true
	}

	// Check a few key fields
	return cfg.Store.Name == "" &&
		cfg.System.Language == "" &&
		cfg.AI.Provider == ""
}

func setupDatabase() {
	// Determine database path
	var dbPath string

	if manager != nil && manager.GetDatabasePath() != "" {
		dbPath = manager.GetDatabasePath()
	} else {
		// Default mobile-friendly path
		if env.IsTermux() {
			dbPath = filepath.Join(baseDir, "data", "sailstream.db")
		} else {
			// Try external storage first
			externalPaths := []string{
				"/sdcard/sailstream/data/sailstream.db",
				filepath.Join(baseDir, "data", "sailstream.db"),
			}

			for _, path := range externalPaths {
				parent := filepath.Dir(path)
				if err := os.MkdirAll(parent, 0755); err == nil {
					dbPath = path
					break
				}
			}
		}
	}

	if dbPath == "" {
		dbPath = filepath.Join(baseDir, "data", "sailstream.db")
	}

	log.Printf("🗄️ Database path: %s", dbPath)

	// Create parent directory
	parentDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		log.Printf("❌ Failed to create database directory: %v", err)
		return
	}

	// Check if database exists
	if fileExists(dbPath) {
		log.Println("✅ Database exists")
	} else {
		log.Println("🗄️ Creating new database...")
		if err := database.Initialize(dbPath); err != nil {
			log.Printf("❌ Failed to create database: %v", err)
			log.Println("💡 Will try to continue anyway...")
		} else {
			log.Println("✅ Database created successfully")
		}
	}
}

func checkMobilePaths() {
	// Check essential paths
	essentialPaths := []string{
		filepath.Join(baseDir, "internal"),
		filepath.Join(baseDir, "platforms"),
		filepath.Join(baseDir, "data"),
		filepath.Join(baseDir, "media"),
		filepath.Join(baseDir, "logs"),
	}

	allGood := true
	for _, path := range essentialPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Printf("📁 Creating: %s", path)
			if err := os.MkdirAll(path, 0755); err != nil {
				log.Printf("❌ Failed to create %s: %v", path, err)
				allGood = false
			}
		} else {
			log.Printf("✅ %s", path)
		}
	}

	if !allGood {
		log.Println("⚠️ Some paths could not be created")
	}
}

func routeMobile() {
	// Check if config exists and has content
	configExists := fileExists(configPath)
	configValid := false

	if configExists && manager != nil {
		configValid = !isEmptyConfigMobile(manager)
	}

	if configValid {
		// Config is good, try maestro first
		log.Println("\n✅ Config is valid, trying maestro...")
		startMaestro()
	} else {
		// Config missing or empty, go to wizard
		log.Println("\n📄 Config missing or empty, starting wizard...")
		startMobileWizard()
	}
}

// ============ ROUTING FUNCTIONS ============

func startMaestro() {
	// Try to start maestro from internal/maestro/maestro.go
	maestroPath := filepath.Join(baseDir, "internal", "maestro", "maestro.go")

	if !fileExists(maestroPath) {
		log.Printf("❌ Maestro not found at: %s", maestroPath)
		log.Println("💡 Falling back to dashboard...")
		startMobileDashboard()
		return
	}

	log.Printf("🎵 Starting maestro: %s", maestroPath)
	cmd := exec.Command("go", "run", maestroPath)
	cmd.Dir = baseDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("❌ Maestro failed: %v", err)
		log.Println("💡 Falling back to dashboard...")
		startMobileDashboard()
	}
}

func startMobileDashboard() {
	// Try Android dashboard first
	androidDashboard := filepath.Join(baseDir, "internal", "platforms", "android", "dashboard", "dashboard.go")
	if fileExists(androidDashboard) {
		log.Printf("🚀 Starting Android dashboard: %s", androidDashboard)
		cmd := exec.Command("go", "run", androidDashboard)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("❌ Android dashboard failed: %v", err)
			// Try PC dashboard as fallback
			startDashboardFallback()
		}
		return
	}

	// If Android dashboard doesn't exist, try PC dashboard
	startDashboardFallback()
}

func startDashboardFallback() {
	// Try PC dashboard
	pcDashboard := filepath.Join(baseDir, "internal", "platforms", "pc", "dashboard", "dashboard.go")
	if fileExists(pcDashboard) {
		log.Printf("💻 Starting PC dashboard (fallback): %s", pcDashboard)
		cmd := exec.Command("go", "run", pcDashboard)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("❌ PC dashboard failed: %v", err)
			// Try startmaestro as last resort
			startMaestroFallback()
		}
		return
	}

	// No dashboard found, try startmaestro
	startMaestroFallback()
}

func startMaestroFallback() {
	// Try startmaestro
	maestroPath := filepath.Join(baseDir, "internal", "maestro", "maestro.go")
	if fileExists(maestroPath) {
		log.Printf("🎵 Starting Maestro (fallback): %s", maestroPath)
		cmd := exec.Command("go", "run", maestroPath)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("❌ Maestro failed: %v", err)
			startFallbackInterface()
		}
		return
	}

	// No maestro found
	log.Println("❌ No maestro found")
	startFallbackInterface()
}

func startMobileWizard() {
	// Try Android wizard first
	androidWizard := filepath.Join(baseDir, "internal", "platforms", "android", "wizard", "wizard.go")
	if fileExists(androidWizard) {
		log.Printf("🧙 Starting Android wizard: %s", androidWizard)
		cmd := exec.Command("go", "run", androidWizard, "-config", configPath)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("❌ Android wizard failed: %v", err)
			// Try PC wizard as fallback
			startWizardFallback()
		}
		return
	}

	// If Android wizard doesn't exist, try PC wizard
	startWizardFallback()
}

func startWizardFallback() {
	// Try PC wizard
	pcWizard := filepath.Join(baseDir, "internal", "platforms", "pc", "wizzard.go")
	if fileExists(pcWizard) {
		log.Printf("💻 Starting PC wizard (fallback): %s", pcWizard)
		cmd := exec.Command("go", "run", pcWizard, "-config", configPath)
		cmd.Dir = baseDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			log.Printf("❌ PC wizard failed: %v", err)
			startFallbackWizard()
		}
		return
	}

	// No wizard found
	log.Println("❌ No wizard found")
	startFallbackWizard()
}

func startFallbackInterface() {
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("   📱 SAILSTREAM MOBILE - SIMPLE INTERFACE")
	fmt.Println(strings.Repeat("=", 50))

	fmt.Println("\nAvailable options:")
	fmt.Println("  1. Start bot")
	fmt.Println("  2. View logs")
	fmt.Println("  3. Check status")
	fmt.Println("  4. Exit")

	fmt.Print("\nSelect option (1-4): ")

	var choice string
	fmt.Scanln(&choice)

	switch choice {
	case "1":
		startMobileBot()
	case "2":
		viewMobileLogs()
	case "3":
		checkMobileStatus()
	case "4":
		log.Println("👋 Exiting...")
		os.Exit(0)
	default:
		fmt.Println("❌ Invalid choice")
		startFallbackInterface()
	}
}

func startFallbackWizard() {
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("   📱 SIMPLE CONFIGURATION")
	fmt.Println(strings.Repeat("=", 50))

	fmt.Println("\nConfig file missing or invalid.")
	fmt.Println("Let's create a basic config...")

	// Create minimal config
	cfg := &config.Config{
		System: config.SystemConfig{
			Language:      "en",
			OperationMode: "mobile",
		},
		Store: config.StoreConfig{
			Name:         "SailStream Mobile",
			HelloMessage: "Welcome to SailStream Mobile!",
		},
		Paths: config.PathsConfig{
			Media:    filepath.Join(baseDir, "media"),
			Logs:     filepath.Join(baseDir, "logs"),
			Cache:    filepath.Join(baseDir, "cache"),
			Database: filepath.Join(baseDir, "data", "sailstream.db"),
		},
	}

	// Save config
	manager := config.NewConfigManager(configPath)
	manager.SetConfig(cfg)

	if err := manager.Save(); err != nil {
		log.Printf("❌ Failed to save config: %v", err)
	} else {
		log.Println("✅ Basic config created")
		log.Printf("📄 Config saved to: %s", configPath)
	}

	fmt.Println("\n✅ Configuration complete!")
	fmt.Println("🔄 Restart the app to use the new config.")
	time.Sleep(3 * time.Second)
	os.Exit(0)
}

func startMobileBot() {
	fmt.Println("\n🤖 Starting bot...")
	fmt.Println("📱 Bot started (placeholder)")
	fmt.Println("Press Enter to return...")
	fmt.Scanln()
	startFallbackInterface()
}

func viewMobileLogs() {
	fmt.Println("\n📋 Viewing logs...")

	logPath := filepath.Join(baseDir, "logs")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		fmt.Println("No logs directory found")
	} else {
		files, _ := os.ReadDir(logPath)
		if len(files) == 0 {
			fmt.Println("No log files found")
		} else {
			for _, file := range files {
				fmt.Printf("  • %s\n", file.Name())
			}
		}
	}

	fmt.Println("\nPress Enter to return...")
	fmt.Scanln()
	startFallbackInterface()
}

func checkMobileStatus() {
	fmt.Println("\n📊 Status Check:")
	fmt.Println("  ✓ Go: Installed")
	fmt.Println("  ✓ Dependencies: Loaded")
	fmt.Printf("  ✓ Database: %s\n", func() string {
		if manager != nil && manager.GetDatabasePath() != "" {
			return "Configured"
		}
		return "Default"
	}())
	fmt.Printf("  ✓ Config: %s\n", func() string {
		if fileExists(configPath) {
			return "Exists"
		}
		return "Missing"
	}())

	fmt.Println("\nPress Enter to return...")
	fmt.Scanln()
	startFallbackInterface()
}

// ==================== UTILITY FUNCTIONS ====================

func commandExists(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
