package database

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var DB *sql.DB

func Initialize(dbPath string) error {
	if !strings.HasSuffix(dbPath, ".db") {
		if dbPath == "./data" {
			dbPath = "./data/database.db"
		} else {
			dbPath = dbPath + ".db"
		}
	}

	log.Printf("Initializing database at: %s", dbPath)

	dbDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dbDir, 0750); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	var err error
	DB, err = sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := DB.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Printf("✅ Database connected: %s", dbPath)

	if _, err := DB.Exec("PRAGMA journal_mode = WAL"); err != nil {
		log.Printf("⚠️ Warning: Failed to set WAL mode: %v", err)
	}
	if _, err := DB.Exec("PRAGMA foreign_keys = ON"); err != nil {
		log.Printf("⚠️ Warning: Failed to enable foreign keys: %v", err)
	}

	schemaPath := findFile("schema.sql")
	if schemaPath == "" {
		return fmt.Errorf("schema.sql not found")
	}

	log.Printf("Found schema at: %s", schemaPath)
	if err := executeSQLFile(schemaPath); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	triggersPath := findFile("triggers_and_views.sql")
	if triggersPath != "" {
		log.Printf("Found triggers at: %s", triggersPath)
		if err := executeSQLFile(triggersPath); err != nil {
			log.Printf("⚠️ Warning: Failed to create triggers/views: %v", err)
		}
	} else {
		log.Printf("⚠️ triggers_and_views.sql not found, skipping triggers/views")
	}

	log.Println("✅ Database initialized successfully")
	return nil
}

func findFile(filename string) string {
	possiblePaths := []string{
		filename,
		"./" + filename,
		"../" + filename,
		"../../" + filename,
		"../../../" + filename,
		"database/" + filename,
		"./database/" + filename,
		"../database/" + filename,
		"../../database/" + filename,
		"../../../database/" + filename,
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

func executeSQLFile(filePath string) error {
	schemaBytes, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	schema := string(schemaBytes)
	log.Printf("✅ Loaded SQL from: %s", filePath)

	return executeSQL(schema)
}

func executeSQL(sqlText string) error {
	statements := []string{}
	lines := strings.Split(sqlText, "\n")
	currentStatement := ""

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		currentStatement += line + "\n"

		if strings.Contains(line, ";") {
			statements = append(statements, strings.TrimSpace(currentStatement))
			currentStatement = ""
		}
	}

	if stmt := strings.TrimSpace(currentStatement); stmt != "" {
		statements = append(statements, stmt)
	}

	log.Printf("Found %d statements to execute", len(statements))

	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if _, err := DB.Exec(stmt); err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "already exists") ||
				strings.Contains(errStr, "duplicate") {
				log.Printf("✅ Statement %d skipped (already exists)", i+1)
			} else {
				log.Printf("❌ Statement %d failed: %v", i+1, err)
				log.Printf("   Statement: %.80s...", stmt)
			}
		} else {
			log.Printf("✅ Statement %d executed", i+1)
		}
	}

	return nil
}

func Close() error {
	if DB != nil {
		DB.Exec("PRAGMA journal_mode = DELETE")
		err := DB.Close()
		DB = nil
		if err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		log.Println("✅ Database connection closed")
	}
	return nil
}

func GetDB() *sql.DB {
	if DB == nil {
		panic("database not initialized - call Initialize() first")
	}
	return DB
}

func GenerateUserID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return "USER-" + strings.ToLower(hex.EncodeToString(b))
}

func GenerateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return strings.ToLower(hex.EncodeToString(b))
}

func GenerateOrderID() string {
	date := time.Now().Format("20060102")
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("ORD-%s-%s", date, strings.ToLower(hex.EncodeToString(b)))
}

func RunInTransaction(fn func(*sql.Tx) error) error {
	tx, err := DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func Connect(dbPath string) error {
	if !strings.HasSuffix(dbPath, ".db") {
		if dbPath == "./data" {
			dbPath = "./data/database.db"
		} else {
			dbPath = dbPath + ".db"
		}
	}

	log.Printf("Connecting to existing database: %s", dbPath)

	if _, err := os.Stat(dbPath); err != nil {
		return fmt.Errorf("database file does not exist: %w", err)
	}

	var err error
	DB, err = sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := DB.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Printf("✅ Database connected: %s", dbPath)

	if _, err := DB.Exec("PRAGMA journal_mode = WAL"); err != nil {
		log.Printf("⚠️ Warning: Failed to set WAL mode: %v", err)
	}
	if _, err := DB.Exec("PRAGMA foreign_keys = ON"); err != nil {
		log.Printf("⚠️ Warning: Failed to enable foreign keys: %v", err)
	}

	return nil
}
