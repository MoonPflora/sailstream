// internal/config/loader.go
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// ConfigManager manages loading and saving configuration.
type ConfigManager struct {
	mu         sync.RWMutex
	config     *Config
	configPath string
}

// NewConfigManager creates a new configuration manager.
func NewConfigManager(configPath string) *ConfigManager {
	return &ConfigManager{
		configPath: configPath,
	}
}

// ============ CORE METHODS ============

func (cm *ConfigManager) Load() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	data, err := os.ReadFile(cm.configPath)
	if err != nil {
		return err
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}

	cm.config = &config
	return nil
}

func (cm *ConfigManager) Save() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return fmt.Errorf("no config loaded")
	}

	cm.config.Meta.LastUpdated = time.Now().Format(time.RFC3339)

	data, err := json.MarshalIndent(cm.config, "", "  ")
	if err != nil {
		return err
	}

	tempPath := cm.configPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	if err := os.Rename(tempPath, cm.configPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	return nil
}

func (cm *ConfigManager) GetConfig() *Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config
}

func (cm *ConfigManager) SetConfig(cfg *Config) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.config = cfg
}

// ============ META GETTERS/SETTERS ============

func (cm *ConfigManager) GetMeta() MetaConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return MetaConfig{}
	}
	return cm.config.Meta
}

func (cm *ConfigManager) SetMeta(meta MetaConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta = meta
	}
}

func (cm *ConfigManager) GetAppVersion() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Meta.AppVersion
}

func (cm *ConfigManager) SetAppVersion(version string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta.AppVersion = version
	}
}

func (cm *ConfigManager) GetLastUpdated() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Meta.LastUpdated
}

func (cm *ConfigManager) GetDatabaseConfig() DatabaseConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return DatabaseConfig{}
	}
	return cm.config.Meta.Database
}

func (cm *ConfigManager) SetDatabaseConfig(db DatabaseConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta.Database = db
	}
}

func (cm *ConfigManager) GetDetectedOS() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Meta.DetectedOS
}

func (cm *ConfigManager) SetDetectedOS(os string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta.DetectedOS = os
	}
}

func (cm *ConfigManager) GetDetectedArch() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Meta.DetectedArch
}

func (cm *ConfigManager) SetDetectedArch(arch string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta.DetectedArch = arch
	}
}

func (cm *ConfigManager) GetDetectedEnv() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Meta.DetectedEnv
}

func (cm *ConfigManager) SetDetectedEnv(env string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta.DetectedEnv = env
	}
}

func (cm *ConfigManager) GetInstalledAt() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Meta.InstalledAt
}

func (cm *ConfigManager) SetInstalledAt(installed string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Meta.InstalledAt = installed
	}
}

// ============ SYSTEM GETTERS/SETTERS ============

func (cm *ConfigManager) GetSystem() SystemConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return SystemConfig{}
	}
	return cm.config.System
}

func (cm *ConfigManager) SetSystem(system SystemConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.System = system
	}
}

func (cm *ConfigManager) GetLanguage() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.System.Language
}

func (cm *ConfigManager) SetLanguage(lang string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.System.Language = lang
	}
}

func (cm *ConfigManager) GetOperationMode() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.System.OperationMode
}

func (cm *ConfigManager) SetOperationMode(mode string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.System.OperationMode = mode
	}
}

func (cm *ConfigManager) GetWakePolicy() WakePolicy {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return WakePolicy{}
	}
	return cm.config.System.WakePolicy
}

func (cm *ConfigManager) SetWakePolicy(policy WakePolicy) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.System.WakePolicy = policy
	}
}

func (cm *ConfigManager) GetWakePolicyIntervalMinutes() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.System.WakePolicy.IntervalMinutes
}

func (cm *ConfigManager) SetWakePolicyIntervalMinutes(minutes int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.System.WakePolicy.IntervalMinutes = minutes
	}
}

func (cm *ConfigManager) GetWakePolicyIdleSleepMinutes() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.System.WakePolicy.IdleSleepMinutes
}

func (cm *ConfigManager) SetWakePolicyIdleSleepMinutes(minutes int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.System.WakePolicy.IdleSleepMinutes = minutes
	}
}

// ============ AI GETTERS/SETTERS ============

func (cm *ConfigManager) GetAI() AIConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return AIConfig{}
	}
	return cm.config.AI
}

func (cm *ConfigManager) SetAI(ai AIConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI = ai
	}
}

func (cm *ConfigManager) GetAIProvider() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.AI.Provider
}

func (cm *ConfigManager) SetAIProvider(provider string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Provider = provider
	}
}

func (cm *ConfigManager) GetAIModel() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.AI.Model
}

func (cm *ConfigManager) SetAIModel(model string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Model = model
	}
}

func (cm *ConfigManager) GetAIAPIKey() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.AI.APIKey
}

func (cm *ConfigManager) SetAIAPIKey(key string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.APIKey = key
	}
}

func (cm *ConfigManager) GetAIBaseURL() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.AI.BaseURL
}

func (cm *ConfigManager) SetAIBaseURL(url string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.BaseURL = url
	}
}

func (cm *ConfigManager) GetAIGeneration() GenerationSettings {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return GenerationSettings{}
	}
	return cm.config.AI.Generation
}

func (cm *ConfigManager) SetAIGeneration(gen GenerationSettings) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Generation = gen
	}
}

// GetAIImageRecognition proxies to the top-level Config.ImageRecognition field.
// AIConfig.ImageRecognition has been removed to eliminate divergence between the
// two copies; all listeners use cfg.ImageRecognition (top-level) directly.
func (cm *ConfigManager) GetAIImageRecognition() ImageRecognition {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ImageRecognition{}
	}
	return cm.config.ImageRecognition
}

// SetAIImageRecognition proxies to the top-level Config.ImageRecognition field.
func (cm *ConfigManager) SetAIImageRecognition(ir ImageRecognition) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition = ir
	}
}

func (cm *ConfigManager) GetAIInstructions() AIInstructions {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return AIInstructions{}
	}
	return cm.config.AI.Instructions
}

func (cm *ConfigManager) SetAIInstructions(inst AIInstructions) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Instructions = inst
	}
}

func (cm *ConfigManager) GetAIMaxTokens() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.AI.Generation.MaxTokens
}

func (cm *ConfigManager) SetAIMaxTokens(tokens int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Generation.MaxTokens = tokens
	}
}

func (cm *ConfigManager) GetAITemperature() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.AI.Generation.Temperature
}

func (cm *ConfigManager) SetAITemperature(temp float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Generation.Temperature = temp
	}
}

func (cm *ConfigManager) GetAITopP() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.AI.Generation.TopP
}

func (cm *ConfigManager) SetAITopP(topP float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Generation.TopP = topP
	}
}

func (cm *ConfigManager) GetAIPresencePenalty() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.AI.Generation.PresencePenalty
}

func (cm *ConfigManager) SetAIPresencePenalty(penalty float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Generation.PresencePenalty = penalty
	}
}

func (cm *ConfigManager) GetAIFrequencyPenalty() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.AI.Generation.FrequencyPenalty
}

func (cm *ConfigManager) SetAIFrequencyPenalty(penalty float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.AI.Generation.FrequencyPenalty = penalty
	}
}

// GetAIImgRecognitionEnabled proxies to cfg.ImageRecognition (top-level).
func (cm *ConfigManager) GetAIImgRecognitionEnabled() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return false
	}
	return cm.config.ImageRecognition.Enabled
}

// SetAIImgRecognitionEnabled proxies to cfg.ImageRecognition (top-level).
func (cm *ConfigManager) SetAIImgRecognitionEnabled(enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.Enabled = enabled
	}
}

// GetAIImgRecognitionModelPath proxies to cfg.ImageRecognition (top-level).
func (cm *ConfigManager) GetAIImgRecognitionModelPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.ImageRecognition.ModelPath
}

// SetAIImgRecognitionModelPath proxies to cfg.ImageRecognition (top-level).
func (cm *ConfigManager) SetAIImgRecognitionModelPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.ModelPath = path
	}
}

// GetAIImgRecognitionConfidence proxies to cfg.ImageRecognition (top-level).
func (cm *ConfigManager) GetAIImgRecognitionConfidence() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.ImageRecognition.ConfidenceThreshold
}

// SetAIImgRecognitionConfidence proxies to cfg.ImageRecognition (top-level).
func (cm *ConfigManager) SetAIImgRecognitionConfidence(confidence float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.ConfidenceThreshold = confidence
	}
}

// ============ SCHEDULER GETTERS/SETTERS ============

func (cm *ConfigManager) GetScheduler() SchedulerConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return SchedulerConfig{}
	}
	return cm.config.Scheduler
}

func (cm *ConfigManager) SetScheduler(sched SchedulerConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler = sched
	}
}

func (cm *ConfigManager) GetTimezone() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Scheduler.Timezone
}

func (cm *ConfigManager) SetTimezone(tz string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.Timezone = tz
	}
}

func (cm *ConfigManager) GetCheckIntervalMinutes() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.Scheduler.CheckIntervalMinutes
}

func (cm *ConfigManager) SetCheckIntervalMinutes(minutes int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.CheckIntervalMinutes = minutes
	}
}

func (cm *ConfigManager) GetQuietHours() QuietHours {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return QuietHours{}
	}
	return cm.config.Scheduler.QuietHours
}

func (cm *ConfigManager) SetQuietHours(qh QuietHours) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.QuietHours = qh
	}
}

func (cm *ConfigManager) GetRateLimits() RateLimits {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return RateLimits{}
	}
	return cm.config.Scheduler.RateLimits
}

func (cm *ConfigManager) SetRateLimits(rl RateLimits) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.RateLimits = rl
	}
}

func (cm *ConfigManager) GetQuietHoursEnabled() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return false
	}
	return cm.config.Scheduler.QuietHours.Enabled
}

func (cm *ConfigManager) SetQuietHoursEnabled(enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.QuietHours.Enabled = enabled
	}
}

func (cm *ConfigManager) GetQuietHoursFrom() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Scheduler.QuietHours.From
}

func (cm *ConfigManager) SetQuietHoursFrom(from string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.QuietHours.From = from
	}
}

func (cm *ConfigManager) GetQuietHoursTo() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Scheduler.QuietHours.To
}

func (cm *ConfigManager) SetQuietHoursTo(to string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.QuietHours.To = to
	}
}

func (cm *ConfigManager) GetRateLimitMessagesPerMinute() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.Scheduler.RateLimits.MessagesPerMinute
}

func (cm *ConfigManager) SetRateLimitMessagesPerMinute(limit int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.RateLimits.MessagesPerMinute = limit
	}
}

func (cm *ConfigManager) GetRateLimitPostsPerHour() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.Scheduler.RateLimits.PostsPerHour
}

func (cm *ConfigManager) SetRateLimitPostsPerHour(limit int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.RateLimits.PostsPerHour = limit
	}
}

func (cm *ConfigManager) GetRateLimitPostsPerDay() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.Scheduler.RateLimits.PostsPerDay
}

func (cm *ConfigManager) SetRateLimitPostsPerDay(limit int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Scheduler.RateLimits.PostsPerDay = limit
	}
}

// ============ STORE GETTERS/SETTERS ============

// Validate on Config is a no-op; all field-level validates are also stubs.
func (c *Config) Validate() error { return nil }

func (cm *ConfigManager) GetStore() StoreConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return StoreConfig{}
	}
	return cm.config.Store
}

func (cm *ConfigManager) SetStore(store StoreConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store = store
	}
}

func (cm *ConfigManager) GetStoreName() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.Name
}

func (cm *ConfigManager) SetStoreName(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Name = name
	}
}

func (cm *ConfigManager) GetStoreDescription() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.Description
}

func (cm *ConfigManager) SetStoreDescription(desc string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Description = desc
	}
}

func (cm *ConfigManager) GetStoreHelloMessage() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.HelloMessage
}

func (cm *ConfigManager) SetStoreHelloMessage(msg string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.HelloMessage = msg
	}
}

func (cm *ConfigManager) GetStoreAddress() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.Address
}

func (cm *ConfigManager) SetStoreAddress(addr string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Address = addr
	}
}

func (cm *ConfigManager) GetStoreContact() ContactInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ContactInfo{}
	}
	return cm.config.Store.Contact
}

func (cm *ConfigManager) SetStoreContact(contact ContactInfo) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Contact = contact
	}
}

func (cm *ConfigManager) GetStoreBusinessHours() map[string]string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Store.BusinessHours == nil {
		return make(map[string]string)
	}
	return cm.config.Store.BusinessHours
}

func (cm *ConfigManager) SetStoreBusinessHours(hours map[string]string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.BusinessHours = hours
	}
}

func (cm *ConfigManager) GetStoreCurrency() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.Currency
}

func (cm *ConfigManager) SetStoreCurrency(currency string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Currency = currency
	}
}

func (cm *ConfigManager) GetStoreEmail() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.Contact.Email
}

func (cm *ConfigManager) SetStoreEmail(email string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Contact.Email = email
	}
}

func (cm *ConfigManager) GetStorePhone() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Store.Contact.Phone
}

func (cm *ConfigManager) SetStorePhone(phone string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Store.Contact.Phone = phone
	}
}

// ============ PLATFORMS GETTERS/SETTERS ============

func (cm *ConfigManager) GetAllPlatforms() map[string]PlatformConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return make(map[string]PlatformConfig)
	}
	return cm.config.Platforms
}

func (cm *ConfigManager) SetAllPlatforms(platforms map[string]PlatformConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Platforms = platforms
	}
}

func (cm *ConfigManager) GetPlatform(name string) (PlatformConfig, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return PlatformConfig{}, false
	}
	platform, exists := cm.config.Platforms[name]
	return platform, exists
}

func (cm *ConfigManager) SetPlatform(name string, platform PlatformConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		if cm.config.Platforms == nil {
			cm.config.Platforms = make(map[string]PlatformConfig)
		}
		cm.config.Platforms[name] = platform
	}
}

func (cm *ConfigManager) RemovePlatform(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		delete(cm.config.Platforms, name)
	}
}

func (cm *ConfigManager) GetPlatformEnabled(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return false
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return false
	}
	return platform.Enabled
}

func (cm *ConfigManager) SetPlatformEnabled(name string, enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Enabled = enabled
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformType(name string) PlatformType {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return PlatformType{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return PlatformType{}
	}
	return platform.Platform
}

func (cm *ConfigManager) SetPlatformType(name string, pt PlatformType) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Platform = pt
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformSubtypes(name string) []PlatformSubtype {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return []PlatformSubtype{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return []PlatformSubtype{}
	}
	return platform.Subtypes
}

func (cm *ConfigManager) SetPlatformSubtypes(name string, subtypes []PlatformSubtype) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Subtypes = subtypes
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformInstagram(name string) *InstagramConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.Instagram
}

func (cm *ConfigManager) SetPlatformInstagram(name string, insta *InstagramConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Instagram = insta
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformFacebook(name string) *FacebookConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.Facebook
}

func (cm *ConfigManager) SetPlatformFacebook(name string, fb *FacebookConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Facebook = fb
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformTelegram(name string) *TelegramConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.Telegram
}

func (cm *ConfigManager) SetPlatformTelegram(name string, tg *TelegramConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Telegram = tg
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformWhatsApp(name string) *WhatsAppConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.WhatsApp
}

func (cm *ConfigManager) SetPlatformWhatsApp(name string, wa *WhatsAppConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.WhatsApp = wa
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformTikTok(name string) *TikTokConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.TikTok
}

func (cm *ConfigManager) SetPlatformTikTok(name string, tt *TikTokConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.TikTok = tt
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformTwitter(name string) *TwitterConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.Twitter
}

func (cm *ConfigManager) SetPlatformTwitter(name string, tw *TwitterConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Twitter = tw
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformViber(name string) *ViberConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return nil
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return nil
	}
	return platform.Viber
}

func (cm *ConfigManager) SetPlatformViber(name string, vb *ViberConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Viber = vb
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformAutomation(name string) AutomationConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AutomationConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AutomationConfig{}
	}
	return platform.Automation
}

func (cm *ConfigManager) SetPlatformAutomation(name string, auto AutomationConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformPosting(name string) PlatformPostingConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return PlatformPostingConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return PlatformPostingConfig{}
	}
	return platform.Posting
}

func (cm *ConfigManager) SetPlatformPosting(name string, posting PlatformPostingConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Posting = posting
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformLimits(name string) PlatformLimits {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return PlatformLimits{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return PlatformLimits{}
	}
	return platform.Limits
}

func (cm *ConfigManager) SetPlatformLimits(name string, limits PlatformLimits) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Limits = limits
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformMetadata(name string) PlatformMetadata {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return PlatformMetadata{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return PlatformMetadata{}
	}
	return platform.Metadata
}

func (cm *ConfigManager) SetPlatformMetadata(name string, meta PlatformMetadata) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Metadata = meta
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformSettings(name string) PlatformSettings {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return PlatformSettings{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return PlatformSettings{}
	}
	return platform.Settings
}

func (cm *ConfigManager) SetPlatformSettings(name string, settings PlatformSettings) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Settings = settings
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformMessages(name string) MessageTemplates {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return MessageTemplates{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return MessageTemplates{}
	}
	return platform.Messages
}

func (cm *ConfigManager) SetPlatformMessages(name string, messages MessageTemplates) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Messages = messages
			cm.config.Platforms[name] = platform
		}
	}
}

// ============ PLATFORM AUTOMATION SUB-GETTERS/SETTERS ============

func (cm *ConfigManager) GetPlatformAutoReply(name string) AutoReplyConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AutoReplyConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AutoReplyConfig{}
	}
	return platform.Automation.AutoReply
}

func (cm *ConfigManager) SetPlatformAutoReply(name string, auto AutoReplyConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.AutoReply = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformAutoHeart(name string) AutoHeartConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AutoHeartConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AutoHeartConfig{}
	}
	return platform.Automation.AutoHeart
}

func (cm *ConfigManager) SetPlatformAutoHeart(name string, auto AutoHeartConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.AutoHeart = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformAutoFollow(name string) AutoFollowConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AutoFollowConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AutoFollowConfig{}
	}
	return platform.Automation.AutoFollow
}

func (cm *ConfigManager) SetPlatformAutoFollow(name string, auto AutoFollowConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.AutoFollow = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformAutoRepost(name string) AutoRepostConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AutoRepostConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AutoRepostConfig{}
	}
	return platform.Automation.AutoRepost
}

func (cm *ConfigManager) SetPlatformAutoRepost(name string, auto AutoRepostConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.AutoRepost = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformAnswerDM(name string) AnswerDMConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AnswerDMConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AnswerDMConfig{}
	}
	return platform.Automation.AnswerDM
}

func (cm *ConfigManager) SetPlatformAnswerDM(name string, auto AnswerDMConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.AnswerDM = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformAnswerComments(name string) AnswerCommentsConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return AnswerCommentsConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return AnswerCommentsConfig{}
	}
	return platform.Automation.AnswerComments
}

func (cm *ConfigManager) SetPlatformAnswerComments(name string, auto AnswerCommentsConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.AnswerComments = auto
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformWelcomeMessage(name string) WelcomeMessageConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return WelcomeMessageConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return WelcomeMessageConfig{}
	}
	return platform.Automation.WelcomeMessage
}

func (cm *ConfigManager) SetPlatformWelcomeMessage(name string, welcome WelcomeMessageConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.WelcomeMessage = welcome
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformMessageFilters(name string) MessageFilters {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return MessageFilters{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return MessageFilters{}
	}
	return platform.Automation.Filters
}

func (cm *ConfigManager) SetPlatformMessageFilters(name string, filters MessageFilters) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Automation.Filters = filters
			cm.config.Platforms[name] = platform
		}
	}
}

// ============ PLATFORM POSTING SUB-GETTERS/SETTERS ============

func (cm *ConfigManager) GetPlatformPostingRandom(name string) RandomPostingConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return RandomPostingConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return RandomPostingConfig{}
	}
	return platform.Posting.Random
}

func (cm *ConfigManager) SetPlatformPostingRandom(name string, random RandomPostingConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Posting.Random = random
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformPostingManual(name string) ManualPostingConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return ManualPostingConfig{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return ManualPostingConfig{}
	}
	return platform.Posting.Manual
}

func (cm *ConfigManager) SetPlatformPostingManual(name string, manual ManualPostingConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Posting.Manual = manual
			cm.config.Platforms[name] = platform
		}
	}
}

func (cm *ConfigManager) GetPlatformScheduleTimes(name string) []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return []string{}
	}
	platform, exists := cm.config.Platforms[name]
	if !exists {
		return []string{}
	}
	return platform.Posting.ScheduleTimes
}

func (cm *ConfigManager) SetPlatformScheduleTimes(name string, times []string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil && cm.config.Platforms != nil {
		if platform, exists := cm.config.Platforms[name]; exists {
			platform.Posting.ScheduleTimes = times
			cm.config.Platforms[name] = platform
		}
	}
}

// ============ POSTING GETTERS/SETTERS ============

func (cm *ConfigManager) GetPosting() PostingConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return PostingConfig{}
	}
	return cm.config.Posting
}

func (cm *ConfigManager) SetPosting(posting PostingConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Posting = posting
	}
}

func (cm *ConfigManager) GetRotationMode() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Posting.RotationMode
}

func (cm *ConfigManager) SetRotationMode(mode string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Posting.RotationMode = mode
	}
}

func (cm *ConfigManager) GetFallbackPosting() FallbackPosting {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return FallbackPosting{}
	}
	return cm.config.Posting.Fallback
}

func (cm *ConfigManager) SetFallbackPosting(fallback FallbackPosting) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Posting.Fallback = fallback
	}
}

func (cm *ConfigManager) GetScheduledPostsSummary() map[string]int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Posting.ScheduledPostsSummary == nil {
		return make(map[string]int)
	}
	return cm.config.Posting.ScheduledPostsSummary
}

func (cm *ConfigManager) SetScheduledPostsSummary(summary map[string]int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Posting.ScheduledPostsSummary = summary
	}
}

// ============ PATHS GETTERS/SETTERS ============

func (cm *ConfigManager) GetPaths() PathsConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return PathsConfig{}
	}
	return cm.config.Paths
}

func (cm *ConfigManager) SetPaths(paths PathsConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths = paths
	}
}

func (cm *ConfigManager) GetLogsPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Logs
}

func (cm *ConfigManager) SetLogsPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Logs = path
	}
}

func (cm *ConfigManager) GetConfigPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Config
}

func (cm *ConfigManager) SetConfigPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Config = path
	}
}

func (cm *ConfigManager) GetCachePath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Cache
}

func (cm *ConfigManager) SetCachePath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Cache = path
	}
}

func (cm *ConfigManager) GetMediaPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Media
}

func (cm *ConfigManager) SetMediaPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Media = path
	}
}

func (cm *ConfigManager) GetModelsPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Models
}

func (cm *ConfigManager) SetModelsPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Models = path
	}
}

func (cm *ConfigManager) GetTempPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Temp
}

func (cm *ConfigManager) SetTempPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Temp = path
	}
}

func (cm *ConfigManager) GetSessionsPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Sessions
}

func (cm *ConfigManager) SetSessionsPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Sessions = path
	}
}

func (cm *ConfigManager) GetDatabasePath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Database
}

func (cm *ConfigManager) SetDatabasePath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Database = path
	}
}

func (cm *ConfigManager) GetBackupPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.Backup
}

func (cm *ConfigManager) SetBackupPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.Backup = path
	}
}

func (cm *ConfigManager) GetPostImagesPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.PostImages
}

func (cm *ConfigManager) SetPostImagesPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.PostImages = path
	}
}

func (cm *ConfigManager) GetProductImagesPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.ProductImages
}

func (cm *ConfigManager) SetProductImagesPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.ProductImages = path
	}
}

func (cm *ConfigManager) GetPostVideosPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.PostVideos
}

func (cm *ConfigManager) SetPostVideosPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.PostVideos = path
	}
}

func (cm *ConfigManager) GetScheduledPostsPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.ScheduledPosts
}

func (cm *ConfigManager) SetScheduledPostsPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.ScheduledPosts = path
	}
}

func (cm *ConfigManager) GetTrainingImagesPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.Paths.TrainingImages
}

func (cm *ConfigManager) SetTrainingImagesPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Paths.TrainingImages = path
	}
}

// ============ CONTENT GETTERS/SETTERS ============

func (cm *ConfigManager) GetContent() ContentPool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ContentPool{}
	}
	return cm.config.Content
}

func (cm *ConfigManager) SetContent(content ContentPool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Content = content
	}
}

func (cm *ConfigManager) GetContentPosts() []PostContent {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Content.Posts == nil {
		return []PostContent{}
	}
	return cm.config.Content.Posts
}

func (cm *ConfigManager) SetContentPosts(posts []PostContent) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Content.Posts = posts
	}
}

func (cm *ConfigManager) GetContentMedia() []MediaItem {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Content.Media == nil {
		return []MediaItem{}
	}
	return cm.config.Content.Media
}

func (cm *ConfigManager) SetContentMedia(media []MediaItem) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Content.Media = media
	}
}

func (cm *ConfigManager) GetContentHashtags() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Content.Hashtags == nil {
		return []string{}
	}
	return cm.config.Content.Hashtags
}

func (cm *ConfigManager) SetContentHashtags(hashtags []string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Content.Hashtags = hashtags
	}
}

func (cm *ConfigManager) GetContentCategories() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil || cm.config.Content.Categories == nil {
		return []string{}
	}
	return cm.config.Content.Categories
}

func (cm *ConfigManager) SetContentCategories(categories []string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Content.Categories = categories
	}
}

func (cm *ConfigManager) GetContentLastUsedIndex() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.Content.LastUsedIndex
}

func (cm *ConfigManager) SetContentLastUsedIndex(index int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.Content.LastUsedIndex = index
	}
}

// ============ SCHEDULED POSTS GETTERS/SETTERS ============

func (cm *ConfigManager) GetScheduledPosts() []ScheduledPost {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return []ScheduledPost{}
	}
	return cm.config.ScheduledPosts
}

func (cm *ConfigManager) SetScheduledPosts(posts []ScheduledPost) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ScheduledPosts = posts
	}
}

// ============ IMAGE RECOGNITION GETTERS/SETTERS (top-level) ============

func (cm *ConfigManager) GetImageRecognition() ImageRecognition {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ImageRecognition{}
	}
	return cm.config.ImageRecognition
}

func (cm *ConfigManager) SetImageRecognition(ir ImageRecognition) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition = ir
	}
}

func (cm *ConfigManager) GetImageRecognitionEnabled() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return false
	}
	return cm.config.ImageRecognition.Enabled
}

func (cm *ConfigManager) SetImageRecognitionEnabled(enabled bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.Enabled = enabled
	}
}

func (cm *ConfigManager) GetImageRecognitionModelPath() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return ""
	}
	return cm.config.ImageRecognition.ModelPath
}

func (cm *ConfigManager) SetImageRecognitionModelPath(path string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.ModelPath = path
	}
}

func (cm *ConfigManager) GetImageRecognitionConfidence() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.ImageRecognition.ConfidenceThreshold
}

func (cm *ConfigManager) SetImageRecognitionConfidence(confidence float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.ConfidenceThreshold = confidence
	}
}

func (cm *ConfigManager) GetImageRecognitionMaxImageSize() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.config == nil {
		return 0
	}
	return cm.config.ImageRecognition.MaxImageSizePx
}

func (cm *ConfigManager) SetImageRecognitionMaxImageSize(size int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config != nil {
		cm.config.ImageRecognition.MaxImageSizePx = size
	}
}

// ============ HELPER METHODS ============

func (cm *ConfigManager) AddScheduledPost(post ScheduledPost) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return
	}

	now := time.Now().Format(time.RFC3339)
	if post.CreatedAt == "" {
		post.CreatedAt = now
	}
	post.UpdatedAt = now

	if post.Status == "" {
		post.Status = "pending"
	}

	cm.config.ScheduledPosts = append(cm.config.ScheduledPosts, post)

	if cm.config.Posting.ScheduledPostsSummary == nil {
		cm.config.Posting.ScheduledPostsSummary = make(map[string]int)
	}
	cm.config.Posting.ScheduledPostsSummary[post.Status]++
}

func (cm *ConfigManager) UpdateScheduledPost(id string, post ScheduledPost) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return false
	}

	for i, p := range cm.config.ScheduledPosts {
		if p.ID == id {
			if post.CreatedAt == "" {
				post.CreatedAt = p.CreatedAt
			}
			post.UpdatedAt = time.Now().Format(time.RFC3339)

			if p.Status != post.Status {
				if cm.config.Posting.ScheduledPostsSummary == nil {
					cm.config.Posting.ScheduledPostsSummary = make(map[string]int)
				}
				if cm.config.Posting.ScheduledPostsSummary[p.Status] > 0 {
					cm.config.Posting.ScheduledPostsSummary[p.Status]--
				}
				cm.config.Posting.ScheduledPostsSummary[post.Status]++
			}

			cm.config.ScheduledPosts[i] = post
			return true
		}
	}
	return false
}

func (cm *ConfigManager) RemoveScheduledPost(id string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return false
	}

	for i, post := range cm.config.ScheduledPosts {
		if post.ID == id {
			if cm.config.Posting.ScheduledPostsSummary != nil &&
				cm.config.Posting.ScheduledPostsSummary[post.Status] > 0 {
				cm.config.Posting.ScheduledPostsSummary[post.Status]--
			}
			cm.config.ScheduledPosts = append(
				cm.config.ScheduledPosts[:i],
				cm.config.ScheduledPosts[i+1:]...,
			)
			return true
		}
	}
	return false
}

func (cm *ConfigManager) GetPendingScheduledPosts() []ScheduledPost {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var pending []ScheduledPost
	if cm.config == nil {
		return pending
	}

	for _, post := range cm.config.ScheduledPosts {
		if post.Status == "pending" || post.Status == "scheduled" {
			pending = append(pending, post)
		}
	}
	return pending
}

func (cm *ConfigManager) AddContentPost(post PostContent) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return
	}

	if post.LastUsed == "" {
		post.LastUsed = time.Now().Format(time.RFC3339)
	}

	if cm.config.Content.Posts == nil {
		cm.config.Content.Posts = []PostContent{}
	}
	cm.config.Content.Posts = append(cm.config.Content.Posts, post)
}

func (cm *ConfigManager) AddContentMedia(media MediaItem) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil {
		return
	}

	if cm.config.Content.Media == nil {
		cm.config.Content.Media = []MediaItem{}
	}
	cm.config.Content.Media = append(cm.config.Content.Media, media)
}

func (cm *ConfigManager) FindContentPostByID(id string) (PostContent, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.config == nil {
		return PostContent{}, false
	}

	for _, post := range cm.config.Content.Posts {
		if post.ID == id {
			return post, true
		}
	}
	return PostContent{}, false
}

func (cm *ConfigManager) FindScheduledPostByID(id string) (ScheduledPost, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.config == nil {
		return ScheduledPost{}, false
	}

	for _, post := range cm.config.ScheduledPosts {
		if post.ID == id {
			return post, true
		}
	}
	return ScheduledPost{}, false
}

func (cm *ConfigManager) AddPlatformSubtype(platformName string, subtype PlatformSubtype) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil || cm.config.Platforms == nil {
		return
	}

	platform, exists := cm.config.Platforms[platformName]
	if !exists {
		return
	}

	if platform.Subtypes == nil {
		platform.Subtypes = []PlatformSubtype{}
	}
	platform.Subtypes = append(platform.Subtypes, subtype)
	cm.config.Platforms[platformName] = platform
}

func (cm *ConfigManager) RemovePlatformSubtype(platformName string, subtypeID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.config == nil || cm.config.Platforms == nil {
		return false
	}

	platform, exists := cm.config.Platforms[platformName]
	if !exists {
		return false
	}

	for i, subtype := range platform.Subtypes {
		if subtype.ID == subtypeID {
			platform.Subtypes = append(platform.Subtypes[:i], platform.Subtypes[i+1:]...)
			cm.config.Platforms[platformName] = platform
			return true
		}
	}
	return false
}

// ============ VALIDATION ============

func (cm *ConfigManager) Validate() error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.config == nil {
		return fmt.Errorf("no configuration loaded")
	}

	return nil
}

// ============ TELEGRAM API CREDENTIALS ============

func (cm *ConfigManager) GetTelegramAPIID(platformName string) string {
	tg := cm.GetPlatformTelegram(platformName)
	if tg == nil {
		return ""
	}
	return tg.APIID
}

func (cm *ConfigManager) SetTelegramAPIID(platformName, apiID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return
	}
	if platform, exists := cm.config.Platforms[platformName]; exists {
		if platform.Telegram == nil {
			platform.Telegram = &TelegramConfig{}
		}
		platform.Telegram.APIID = apiID
		cm.config.Platforms[platformName] = platform
	}
}

func (cm *ConfigManager) GetTelegramAPIHash(platformName string) string {
	tg := cm.GetPlatformTelegram(platformName)
	if tg == nil {
		return ""
	}
	return tg.APIHash
}

func (cm *ConfigManager) SetTelegramAPIHash(platformName, apiHash string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.config == nil || cm.config.Platforms == nil {
		return
	}
	if platform, exists := cm.config.Platforms[platformName]; exists {
		if platform.Telegram == nil {
			platform.Telegram = &TelegramConfig{}
		}
		platform.Telegram.APIHash = apiHash
		cm.config.Platforms[platformName] = platform
	}
}

// ============ RELOAD ============

func (cm *ConfigManager) Reload() error {
	return cm.Load()
}