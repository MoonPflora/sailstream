package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sailstream/internal/comms"
	"sailstream/internal/config"
	"sailstream/internal/database"
	"sailstream/internal/enviroment"
	"sailstream/internal/shared"
)

type Product struct {
	ID           string
	SKU          string
	Name         string
	Price        float64
	Currency     string
	ImageURL     sql.NullString
	ThumbnailURL sql.NullString
	AliasesEn    []string
	AliasesAr    []string
	AliasesKu    []string
	UsesEn       []string
	UsesAr       []string
	UsesKu       []string
}

type Poster struct {
	db              *sql.DB
	env             *enviroment.Environment
	configManager   *config.ConfigManager
	llmClient       *comms.Client
	recentProducts  map[string]time.Time
	mu              sync.RWMutex
	instructionChan chan *shared.AutomationInstruction
}

type ProductPostData struct {
	ProductID     string
	SKU           string
	Name          string
	Price         float64
	Currency      string
	ImageURL      string
	ImagePath     string
	ImageExists   bool
	MediaType     string
	FirstAliasEn  string
	FirstAliasAr  string
	FirstAliasKu  string
	FirstUseEn    string
	FirstUseAr    string
	FirstUseKu    string
	StoreName     string
	StorePhone    string
	StoreEmail    string
	StoreAddress  string
	StoreWhatsApp string
	Hashtags      []string
	MaxLength     int
}

func NewPoster(env *enviroment.Environment, cfgMgr *config.ConfigManager, ch chan *shared.AutomationInstruction) *Poster {
	return &Poster{
		db:              database.GetDB(),
		env:             env,
		configManager:   cfgMgr,
		llmClient:       comms.NewClient(cfgMgr),
		recentProducts:  make(map[string]time.Time),
		instructionChan: ch,
	}
}

func (p *Poster) PostRandom() {
	cfg := p.configManager.GetConfig()
	product, err := p.getRandomProduct()
	if err != nil {
		log.Printf("[Poster] getRandomProduct error: %v", err)
		return
	}
	if p.wasRecentlyPosted(product.ID) {
		log.Printf("[Poster] product %s posted recently, skipping", product.SKU)
		return
	}
	storeCfg := p.configManager.GetStore()
	for platformID, platformCfg := range cfg.Platforms {
		if !platformCfg.Enabled || !platformCfg.Posting.Random.Enabled {
			continue
		}
		targets := p.resolveTargets(platformID, platformCfg)
		for _, tgt := range targets {
			postData := p.buildProductPostData(product, storeCfg, platformCfg)
			postText := p.generatePostText(postData)
			if aiText, ok := p.generateAIPostText(postData, platformCfg); ok {
				postText = aiText
				log.Printf("[Poster] AI-generated caption for product %s", product.SKU)
			}
			instruction := p.buildProductInstruction(platformID, tgt.subtypeID, tgt.accountID, postData, postText, "random")
			p.dispatch(instruction)
		}
	}
	p.trackProductPosted(product.ID)
	storeCfg = p.configManager.GetStore()
	go p.logPostToDB("random", product.ID, "all", "", "pending", p.generatePostText(p.buildProductPostData(product, storeCfg, config.PlatformConfig{})), "")
	log.Printf("[Poster] PostRandom dispatched for product %s (%s)", product.Name, product.SKU)
}

func (p *Poster) PostRandomToPlatform(platformID, subtypeID, accountID string) {
	cfg := p.configManager.GetConfig()
	platformCfg, exists := cfg.Platforms[platformID]
	if !exists || !platformCfg.Enabled {
		log.Printf("[Poster] platform %s not enabled", platformID)
		return
	}
	product, err := p.getRandomProduct()
	if err != nil {
		log.Printf("[Poster] getRandomProduct error: %v", err)
		return
	}
	if p.wasRecentlyPosted(product.ID) {
		log.Printf("[Poster] product %s posted recently, skipping", product.SKU)
		return
	}
	storeCfg := p.configManager.GetStore()
	postData := p.buildProductPostData(product, storeCfg, platformCfg)
	postText := p.generatePostText(postData)
	if aiText, ok := p.generateAIPostText(postData, platformCfg); ok {
		postText = aiText
		log.Printf("[Poster] AI-generated caption for product %s", product.SKU)
	}
	instruction := p.buildProductInstruction(platformID, subtypeID, accountID, postData, postText, "random")
	p.dispatch(instruction)
	p.trackProductPosted(product.ID)
	go p.logPostToDB("random", product.ID, platformID, subtypeID, "pending", postText, "")
	log.Printf("[Poster] PostRandomToPlatform dispatched for product %s → %s:%s", product.SKU, platformID, subtypeID)
}

func (p *Poster) generateAIPostText(data *ProductPostData, platformCfg config.PlatformConfig) (string, bool) {
	if p.llmClient == nil || !p.llmClient.Enabled() {
		return "", false
	}
	req := comms.PostRequest{
		ProductName:     data.Name,
		ProductSKU:      data.SKU,
		ProductPrice:    data.Price,
		ProductCurrency: data.Currency,
		AliasEn:         data.FirstAliasEn,
		AliasAr:         data.FirstAliasAr,
		AliasKu:         data.FirstAliasKu,
		UseEn:           data.FirstUseEn,
		UseAr:           data.FirstUseAr,
		UseKu:           data.FirstUseKu,
		PlatformID:      "",
		MaxLength:       data.MaxLength,
		Hashtags:        data.Hashtags,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := p.llmClient.GeneratePost(ctx, req)
	if err != nil {
		log.Printf("[Poster] AI generation failed: %v, falling back to static", err)
		return "", false
	}
	return result.Caption, true
}

func (p *Poster) CheckAndRunScheduledFromDB() {
	now := time.Now()
	rows, err := p.db.Query(`
		SELECT id, title, content, schedule_type,
		       scheduled_time, target_platforms, media_paths
		FROM scheduled_posts
		WHERE status IN ('pending', 'scheduled')
		  AND schedule_type IN ('once', 'immediate')
		  AND (scheduled_time IS NULL OR (scheduled_time <= ? AND scheduled_time >= ?))
		ORDER BY scheduled_time ASC
	`, now, now.Add(-5*time.Minute))
	if err != nil {
		log.Printf("[Poster] CheckAndRunScheduledFromDB query error: %v", err)
		return
	}
	defer rows.Close()

	type dbPost struct {
		id              string
		title           string
		content         string
		scheduleType    string
		scheduledTime   sql.NullTime
		targetPlatforms sql.NullString
		mediaPaths      sql.NullString
	}
	var due []dbPost
	for rows.Next() {
		var dp dbPost
		if err := rows.Scan(&dp.id, &dp.title, &dp.content, &dp.scheduleType, &dp.scheduledTime, &dp.targetPlatforms, &dp.mediaPaths); err != nil {
			log.Printf("[Poster] CheckAndRunScheduledFromDB scan error: %v", err)
			continue
		}
		due = append(due, dp)
	}
	if err := rows.Err(); err != nil {
		log.Printf("[Poster] CheckAndRunScheduledFromDB rows error: %v", err)
	}
	if len(due) == 0 {
		return
	}
	cfg := p.configManager.GetConfig()
	storeCfg := p.configManager.GetStore()

	for _, dp := range due {
		type platformEntry struct {
			PlatformID string `json:"platform_id"`
			SubtypeID  string `json:"subtype_id"`
		}
		var platformEntries []platformEntry
		if dp.targetPlatforms.Valid && dp.targetPlatforms.String != "" {
			_ = json.Unmarshal([]byte(dp.targetPlatforms.String), &platformEntries)
		}
		if len(platformEntries) == 0 {
			for pid, pcfg := range cfg.Platforms {
				if pcfg.Enabled {
					platformEntries = append(platformEntries, platformEntry{PlatformID: pid, SubtypeID: "account"})
				}
			}
		}
		var mediaPaths []string
		if dp.mediaPaths.Valid && dp.mediaPaths.String != "" {
			_ = json.Unmarshal([]byte(dp.mediaPaths.String), &mediaPaths)
		}
		synth := config.ScheduledPost{
			ID:          dp.id,
			Title:       dp.title,
			Description: dp.content,
		}
		for _, mp := range mediaPaths {
			synth.Media = append(synth.Media, config.ScheduledMedia{FilePath: mp, Type: "image"})
		}
		postText := p.buildScheduledPostText(synth, storeCfg)
		dispatched := false
		for _, pe := range platformEntries {
			platformCfg, ok := cfg.Platforms[pe.PlatformID]
			if !ok || !platformCfg.Enabled {
				continue
			}
			subtypeID := pe.SubtypeID
			if subtypeID == "" {
				subtypeID = "account"
			}
			var mediaURLs []string
			for _, mp := range mediaPaths {
				if isPublicURL(mp) {
					mediaURLs = append(mediaURLs, mp)
				}
			}
			instruction := p.buildScheduledInstruction(pe.PlatformID, subtypeID, "", synth, platformCfg, postText)
			if len(mediaURLs) > 0 {
				for i := range instruction.Steps {
					if instruction.Steps[i].Options == nil {
						instruction.Steps[i].Options = make(map[string]interface{})
					}
					if _, already := instruction.Steps[i].Options["image_url"]; !already {
						if _, already := instruction.Steps[i].Options["image_path"]; !already {
							instruction.Steps[i].Options["image_url"] = mediaURLs[0]
						}
					}
				}
			}
			p.dispatch(instruction)
			dispatched = true
		}
		if dispatched {
			_, dbErr := p.db.Exec(`UPDATE scheduled_posts SET status='posted', posted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?`, dp.id)
			if dbErr != nil {
				log.Printf("[Poster] failed to mark post %s as posted: %v", dp.id, dbErr)
			}
		}
	}
}

func (p *Poster) PostScheduled(post config.ScheduledPost) {
	cfg := p.configManager.GetConfig()
	storeCfg := p.configManager.GetStore()
	postText := p.buildScheduledPostText(post, storeCfg)

	type platformTarget struct {
		platformID string
		subtypeID  string
		accountID  string
	}
	var targets []platformTarget
	for _, pp := range post.Platforms {
		if !pp.Enabled || pp.PlatformID == "" {
			continue
		}
		subtypeID := pp.SubtypeID
		if subtypeID == "" {
			subtypeID = "account"
		}
		targets = append(targets, platformTarget{pp.PlatformID, subtypeID, ""})
	}
	if len(targets) == 0 {
		for platformID, platformCfg := range cfg.Platforms {
			if !platformCfg.Enabled {
				continue
			}
			for _, tgt := range p.resolveTargets(platformID, platformCfg) {
				targets = append(targets, platformTarget{platformID, tgt.subtypeID, tgt.accountID})
			}
		}
	}
	dispatched := false
	for _, tgt := range targets {
		platformCfg, exists := cfg.Platforms[tgt.platformID]
		if !exists || !platformCfg.Enabled {
			continue
		}
		text := postText
		for _, pp := range post.Platforms {
			if pp.PlatformID == tgt.platformID && pp.CustomText != "" {
				text = pp.CustomText
			}
		}
		instruction := p.buildScheduledInstruction(tgt.platformID, tgt.subtypeID, tgt.accountID, post, platformCfg, text)
		p.dispatch(instruction)
		dispatched = true
	}
	if dispatched {
		go p.logPostToDB("scheduled", post.ID, "all", "", "posted", postText, "")
	}
	log.Printf("[Poster] PostScheduled dispatched: %s (dispatched=%v)", post.Title, dispatched)
}

func (p *Poster) buildProductInstruction(platformID, subtypeID, accountID string, data *ProductPostData, postText, postType string) *shared.AutomationInstruction {
	ticketID := "POST-" + strings.ToUpper(shared.GenerateRandomID(8))
	if len(data.ProductID) >= 8 {
		ticketID += "-" + data.ProductID[:8]
	}
	steps := p.buildStepsForPlatform(platformID, data, postText)
	return &shared.AutomationInstruction{
		Platform:     platformID,
		SubtypeID:    subtypeID,
		AccountID:    accountID,
		TicketID:     ticketID,
		Action:       shared.ActionSendProduct,
		Intent:       shared.IntentProductInquiry,
		Steps:        steps,
		MaxRetries:   3,
		Timeout:      3 * time.Minute,
		Priority:     50,
		OriginalText: postText,
		Data: map[string]interface{}{
			"product_id":  data.ProductID,
			"product_sku": data.SKU,
			"image_path":  data.ImagePath,
			"image_url":   data.ImageURL,
			"hashtags":    data.Hashtags,
			"store_name":  data.StoreName,
			"post_type":   postType,
			"media_type":  data.MediaType,
		},
		CreatedAt: time.Now(),
	}
}

func (p *Poster) buildScheduledInstruction(platformID, subtypeID, accountID string, post config.ScheduledPost, platformCfg config.PlatformConfig, postText string) *shared.AutomationInstruction {
	ticketID := "SCHED-" + strings.ToUpper(shared.GenerateRandomID(8))
	var mediaPaths, mediaURLs []string
	for _, m := range post.Media {
		if m.FilePath != "" {
			mediaPaths = append(mediaPaths, m.FilePath)
		}
		if m.URL != "" {
			mediaURLs = append(mediaURLs, m.URL)
		}
	}
	steps := p.buildScheduledStepsForPlatform(platformID, post, platformCfg, postText, mediaPaths, mediaURLs)
	return &shared.AutomationInstruction{
		Platform:     platformID,
		SubtypeID:    subtypeID,
		AccountID:    accountID,
		TicketID:     ticketID,
		Action:       shared.ActionSendProduct,
		Intent:       shared.IntentProductInquiry,
		Steps:        steps,
		MaxRetries:   3,
		Timeout:      3 * time.Minute,
		Priority:     60,
		OriginalText: postText,
		Data: map[string]interface{}{
			"post_id":       post.ID,
			"post_title":    post.Title,
			"media_paths":   mediaPaths,
			"media_urls":    mediaURLs,
			"hashtags":      post.Hashtags,
			"schedule_type": post.Schedule.Type,
		},
		CreatedAt: time.Now(),
	}
}

func (p *Poster) buildStepsForPlatform(platformID string, data *ProductPostData, postText string) []shared.InstructionStep {
	switch platformID {
	case shared.PlatformInstagram:
		return p.stepsInstagram(data, postText)
	case shared.PlatformFacebook:
		return p.stepsFacebook(data, postText)
	case shared.PlatformTwitter:
		return p.stepsTwitter(data, postText)
	case shared.PlatformTelegram:
		return p.stepsTelegram(data, postText)
	case shared.PlatformViber:
		return p.stepsViber(data, postText)
	case shared.PlatformWhatsApp:
		return p.stepsWhatsApp(data, postText)
	default:
		return p.stepsGeneric(data, postText)
	}
}

func (p *Poster) buildScheduledStepsForPlatform(platformID string, post config.ScheduledPost, platformCfg config.PlatformConfig, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	switch platformID {
	case shared.PlatformInstagram:
		return p.scheduledStepsInstagram(post, postText, mediaPaths, mediaURLs)
	case shared.PlatformFacebook:
		return p.scheduledStepsFacebook(post, postText, mediaPaths, mediaURLs)
	case shared.PlatformTwitter:
		return p.scheduledStepsTwitter(post, postText, mediaPaths, mediaURLs)
	case shared.PlatformTelegram:
		return p.scheduledStepsTelegram(post, postText, mediaPaths, mediaURLs)
	case shared.PlatformViber:
		return p.scheduledStepsViber(post, postText, mediaPaths, mediaURLs)
	case shared.PlatformWhatsApp:
		return p.scheduledStepsWhatsApp(post, postText, mediaPaths, mediaURLs)
	default:
		return p.scheduledStepsGeneric(post, postText, mediaPaths, mediaURLs)
	}
}

func (p *Poster) stepsInstagram(data *ProductPostData, postText string) []shared.InstructionStep {
	steps := []shared.InstructionStep{
		{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"},
		{Type: shared.StepTypeNavigate, Value: "https://www.instagram.com", DelayBefore: 1000, DelayAfter: 3000, Description: "Navigate to Instagram"},
		{Type: shared.StepTypeClick, Selector: `[aria-label="New post"], svg[aria-label="New post"], a[href="/create/style/"]`, DelayBefore: 800, DelayAfter: 2000, Description: "Open new post dialog"},
	}
	if data.ImageExists && data.ImagePath != "" {
		steps = append(steps, shared.InstructionStep{
			Type: shared.StepTypeUpload, Value: data.ImagePath, DelayBefore: 500, DelayAfter: 3000, Description: "Upload product image",
			Options: map[string]interface{}{"input_selector": `input[type="file"]`, "image_path": data.ImagePath},
		})
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeClick, Selector: `button:has-text("Next"), div[role="button"]:has-text("Next")`, DelayBefore: 1000, DelayAfter: 1500, Description: "Proceed to filters step"})
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeClick, Selector: `button:has-text("Next"), div[role="button"]:has-text("Next")`, DelayBefore: 500, DelayAfter: 1500, Description: "Proceed to caption step"})
	} else if data.ImageURL != "" {
		steps = append(steps, shared.InstructionStep{
			Type: shared.StepTypeUpload, Value: data.ImageURL, DelayBefore: 500, DelayAfter: 3000, Description: "Upload product image from URL",
			Options: map[string]interface{}{"input_selector": `input[type="file"]`, "image_url": data.ImageURL},
		})
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeClick, Selector: `button:has-text("Next"), div[role="button"]:has-text("Next")`, DelayBefore: 1000, DelayAfter: 1500, Description: "Proceed to filters step"})
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeClick, Selector: `button:has-text("Next"), div[role="button"]:has-text("Next")`, DelayBefore: 500, DelayAfter: 1500, Description: "Proceed to caption step"})
	}
	steps = append(steps,
		shared.InstructionStep{Type: shared.StepTypeType, Selector: `textarea[aria-label="Write a caption..."], div[aria-label="Write a caption..."]`, Value: postText, DelayBefore: 800, DelayAfter: 500, Description: "Type post caption"},
		shared.InstructionStep{Type: shared.StepTypeClick, Selector: `div[role="button"]:has-text("Share"), button[type="button"]:has-text("Share")`, DelayBefore: 1000, DelayAfter: 3000, Description: "Share the post"},
	)
	return steps
}

func (p *Poster) scheduledStepsInstagram(post config.ScheduledPost, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	fakeData := &ProductPostData{
		ImagePath:   firstString(mediaPaths),
		ImageURL:    firstString(mediaURLs),
		ImageExists: len(mediaPaths) > 0 && fileExists(firstString(mediaPaths)),
		Hashtags:    post.Hashtags,
	}
	return p.stepsInstagram(fakeData, postText)
}

func (p *Poster) stepsFacebook(data *ProductPostData, postText string) []shared.InstructionStep {
	postOpts := map[string]interface{}{"post_type": "account"}
	if data.ImageExists && data.ImagePath != "" {
		postOpts["image_path"] = data.ImagePath
	} else if data.ImageURL != "" {
		postOpts["image_url"] = data.ImageURL
	}
	return []shared.InstructionStep{
		{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"},
		{Type: shared.StepTypePost, Value: postText, DelayBefore: 1000, DelayAfter: 3000, Description: "Create Facebook post with product", Options: postOpts},
	}
}

func (p *Poster) scheduledStepsFacebook(post config.ScheduledPost, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	opts := map[string]interface{}{"post_type": "account"}
	if len(mediaPaths) > 0 && fileExists(mediaPaths[0]) {
		opts["image_path"] = mediaPaths[0]
	} else if len(mediaURLs) > 0 {
		opts["image_url"] = mediaURLs[0]
	}
	return []shared.InstructionStep{
		{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"},
		{Type: shared.StepTypePost, Value: postText, DelayBefore: 1000, DelayAfter: 3000, Description: "Create scheduled Facebook post", Options: opts},
	}
}

func (p *Poster) stepsTwitter(data *ProductPostData, postText string) []shared.InstructionStep {
	text := p.trimForTwitter(postText)
	steps := []shared.InstructionStep{
		{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"},
		{Type: shared.StepTypeNavigate, Value: "https://twitter.com/compose/tweet", DelayBefore: 1000, DelayAfter: 2500, Description: "Open tweet composer"},
		{Type: shared.StepTypeType, Selector: `div[data-testid="tweetTextarea_0"], div[aria-label="Tweet text"]`, Value: text, DelayBefore: 600, DelayAfter: 500, Description: "Type tweet text"},
	}
	if data.ImageExists && data.ImagePath != "" {
		steps = append(steps, shared.InstructionStep{
			Type: shared.StepTypeUpload, Value: data.ImagePath, DelayBefore: 400, DelayAfter: 2500, Description: "Attach product image",
			Options: map[string]interface{}{"input_selector": `input[data-testid="fileInput"]`, "image_path": data.ImagePath},
		})
	} else if data.ImageURL != "" {
		steps = append(steps, shared.InstructionStep{
			Type: shared.StepTypeUpload, Value: data.ImageURL, DelayBefore: 400, DelayAfter: 2500, Description: "Attach product image from URL",
			Options: map[string]interface{}{"input_selector": `input[data-testid="fileInput"]`, "image_url": data.ImageURL},
		})
	}
	steps = append(steps, shared.InstructionStep{Type: shared.StepTypeClick, Selector: `div[data-testid="tweetButton"], div[data-testid="tweetButtonInline"]`, DelayBefore: 800, DelayAfter: 3000, Description: "Tweet"})
	return steps
}

func (p *Poster) scheduledStepsTwitter(post config.ScheduledPost, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	fakeData := &ProductPostData{
		ImagePath:   firstString(mediaPaths),
		ImageURL:    firstString(mediaURLs),
		ImageExists: len(mediaPaths) > 0 && fileExists(firstString(mediaPaths)),
	}
	return p.stepsTwitter(fakeData, postText)
}

func (p *Poster) stepsTelegram(data *ProductPostData, postText string) []shared.InstructionStep {
	channelTarget := p.resolveTelegramChannel()
	steps := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 100, Description: "Check rate limits"}}
	if (data.ImageExists && data.ImagePath != "") || data.ImageURL != "" {
		uploadOpts := map[string]interface{}{"media_type": "image", "caption": postText}
		if channelTarget != "" {
			uploadOpts["to"] = channelTarget
		}
		if data.ImageExists && data.ImagePath != "" {
			uploadOpts["image_path"] = data.ImagePath
		} else {
			uploadOpts["image_url"] = data.ImageURL
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: data.ImagePath, DelayBefore: 500, DelayAfter: 2000, Description: "Send product image with caption to Telegram", Options: uploadOpts})
	} else {
		msgOpts := map[string]interface{}{}
		if channelTarget != "" {
			msgOpts["to"] = channelTarget
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeSendMessage, Value: postText, DelayBefore: 300, DelayAfter: 1000, Description: "Send product post to Telegram", Options: msgOpts})
	}
	return steps
}

func (p *Poster) scheduledStepsTelegram(post config.ScheduledPost, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	channelTarget := p.resolveTelegramChannel()
	steps := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 100, Description: "Check rate limits"}}
	if len(mediaPaths) > 0 && fileExists(mediaPaths[0]) {
		opts := map[string]interface{}{"media_type": "image", "caption": postText, "image_path": mediaPaths[0]}
		if channelTarget != "" {
			opts["to"] = channelTarget
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: mediaPaths[0], DelayBefore: 300, DelayAfter: 1500, Description: "Send scheduled media to Telegram", Options: opts})
	} else if len(mediaURLs) > 0 {
		opts := map[string]interface{}{"media_type": "image", "caption": postText, "image_url": mediaURLs[0]}
		if channelTarget != "" {
			opts["to"] = channelTarget
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: mediaURLs[0], DelayBefore: 300, DelayAfter: 1500, Description: "Send scheduled media (URL) to Telegram", Options: opts})
	} else {
		opts := map[string]interface{}{}
		if channelTarget != "" {
			opts["to"] = channelTarget
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeSendMessage, Value: postText, DelayBefore: 200, DelayAfter: 800, Description: "Send scheduled post text to Telegram", Options: opts})
	}
	if len(mediaPaths) > 1 {
		allExist := true
		for _, mp := range mediaPaths {
			if !fileExists(mp) {
				allExist = false
				break
			}
		}
		if allExist {
			steps = append(steps, shared.InstructionStep{Type: shared.StepTypeShare, Value: postText, DelayBefore: 500, DelayAfter: 2000, Description: "Send album to Telegram channel",
				Options: map[string]interface{}{"to": channelTarget, "file_paths": mediaPaths, "caption": postText, "album": true}})
		}
	}
	return steps
}

func (p *Poster) stepsViber(data *ProductPostData, postText string) []shared.InstructionStep {
	recipientID := p.resolveViberChannel()
	steps := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"}}
	publicImageURL := ""
	if isPublicURL(data.ImageURL) {
		publicImageURL = data.ImageURL
	}
	opts := map[string]interface{}{}
	if recipientID != "" {
		opts["to"] = recipientID
	}
	if publicImageURL != "" {
		sendOpts := map[string]interface{}{"image_url": publicImageURL, "caption": postText, "media_type": "image"}
		if recipientID != "" {
			sendOpts["to"] = recipientID
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: postText, DelayBefore: 500, DelayAfter: 2000, Description: "Send product image to Viber via Bot API", Options: sendOpts})
	} else {
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeSendMessage, Value: postText, DelayBefore: 300, DelayAfter: 1500, Description: "Send product text post to Viber", Options: opts})
	}
	return steps
}

func (p *Poster) scheduledStepsViber(post config.ScheduledPost, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	recipientID := p.resolveViberChannel()
	steps := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"}}
	publicURL := ""
	for _, u := range mediaURLs {
		if isPublicURL(u) {
			publicURL = u
			break
		}
	}
	opts := map[string]interface{}{}
	if recipientID != "" {
		opts["to"] = recipientID
	}
	if publicURL != "" {
		sendOpts := map[string]interface{}{"image_url": publicURL, "caption": postText, "media_type": "image"}
		if recipientID != "" {
			sendOpts["to"] = recipientID
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: postText, DelayBefore: 400, DelayAfter: 2000, Description: "Send scheduled media to Viber via Bot API", Options: sendOpts})
	} else {
		if len(mediaPaths) > 0 {
			log.Printf("[Poster] Viber scheduled post: media path %q is local, falling back to text-only", firstString(mediaPaths))
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeSendMessage, Value: postText, DelayBefore: 300, DelayAfter: 1500, Description: "Send scheduled post text to Viber", Options: opts})
	}
	return steps
}

func (p *Poster) stepsWhatsApp(data *ProductPostData, postText string) []shared.InstructionStep {
	channelJID := p.resolveWhatsAppChannel()
	steps := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"}}
	opts := map[string]interface{}{}
	if channelJID != "" {
		opts["to"] = channelJID
	}
	if (data.ImageExists && data.ImagePath != "") || data.ImageURL != "" {
		uploadOpts := map[string]interface{}{"media_type": "image", "caption": postText}
		if channelJID != "" {
			uploadOpts["to"] = channelJID
		}
		if data.ImageExists && data.ImagePath != "" {
			uploadOpts["image_path"] = data.ImagePath
			uploadOpts["file_path"] = data.ImagePath
		} else {
			uploadOpts["image_url"] = data.ImageURL
		}
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: data.ImagePath, DelayBefore: 1000, DelayAfter: 2000, Description: "Send product image to WhatsApp", Options: uploadOpts})
	} else {
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeSendMessage, Value: postText, DelayBefore: 1000, DelayAfter: 1500, Description: "Send product post to WhatsApp", Options: opts})
	}
	return steps
}

func (p *Poster) scheduledStepsWhatsApp(post config.ScheduledPost, postText string, mediaPaths, mediaURLs []string) []shared.InstructionStep {
	channelJID := p.resolveWhatsAppChannel()
	steps := []shared.InstructionStep{{Type: shared.StepTypeRateLimitCheck, DelayAfter: 200, Description: "Check rate limits"}}
	opts := map[string]interface{}{}
	if channelJID != "" {
		opts["to"] = channelJID
	}
	if len(mediaPaths) > 0 && fileExists(mediaPaths[0]) {
		opts["media_type"] = "image"
		opts["caption"] = postText
		opts["image_path"] = mediaPaths[0]
		opts["file_path"] = mediaPaths[0]
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: mediaPaths[0], DelayBefore: 800, DelayAfter: 2000, Description: "Send scheduled media to WhatsApp", Options: opts})
	} else if len(mediaURLs) > 0 {
		opts["media_type"] = "image"
		opts["caption"] = postText
		opts["image_url"] = mediaURLs[0]
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeUpload, Value: mediaURLs[0], DelayBefore: 800, DelayAfter: 2000, Description: "Send scheduled media URL to WhatsApp", Options: opts})
	} else {
		steps = append(steps, shared.InstructionStep{Type: shared.StepTypeSendMessage, Value: postText, DelayBefore: 500, DelayAfter: 1000, Description: "Send scheduled post to WhatsApp", Options: opts})
	}
	return steps
}

func (p *Poster) stepsGeneric(data *ProductPostData, postText string) []shared.InstructionStep {
	return []shared.InstructionStep{{Type: shared.StepTypeLog, Value: fmt.Sprintf("Generic post (no platform handler): %s", postText[:min(60, len(postText))]), Description: "Log – no platform handler registered"}}
}

func (p *Poster) scheduledStepsGeneric(post config.ScheduledPost, postText string, _, _ []string) []shared.InstructionStep {
	return p.stepsGeneric(nil, postText)
}

func (p *Poster) generatePostText(data *ProductPostData) string {
	var b strings.Builder
	if data.FirstAliasEn != "" {
		b.WriteString(data.FirstAliasEn)
		b.WriteString("\n\n")
	}
	if data.FirstUseAr != "" {
		b.WriteString(data.FirstUseAr)
		b.WriteString("\n\n")
	}
	if data.FirstUseKu != "" {
		b.WriteString(data.FirstUseKu)
		b.WriteString("\n\n")
	}
	p.writeContactBlock(&b, data.StoreName, data.StorePhone, data.StoreEmail, data.StoreAddress)
	if len(data.Hashtags) > 0 {
		b.WriteString(p.formatHashtags(data.Hashtags))
		b.WriteString("\n\n")
	}
	b.WriteString(fmt.Sprintf(`PID="%s"`, data.ProductID))
	return b.String()
}

func (p *Poster) buildScheduledPostText(post config.ScheduledPost, store config.StoreConfig) string {
	var b strings.Builder
	if post.Description != "" {
		b.WriteString(post.Description)
	} else {
		b.WriteString(post.Title)
	}
	b.WriteString("\n\n")
	p.writeContactBlock(&b, store.Name, store.Contact.Phone, store.Contact.Email, store.Address)
	if len(post.Hashtags) > 0 {
		b.WriteString(p.formatHashtags(post.Hashtags))
		b.WriteString("\n\n")
	}
	b.WriteString(fmt.Sprintf(`PID="%s"`, post.ID))
	return b.String()
}

func (p *Poster) writeContactBlock(b *strings.Builder, name, phone, email, address string) {
	b.WriteString("📱 Please contact ")
	b.WriteString(name)
	b.WriteString(" to inquire about prices, for both single and wholesale purchases.\n")
	b.WriteString("📱 يرجى الاتصال بـ ")
	b.WriteString(name)
	b.WriteString(" للاستفسار عن الأسعار، لكل من المشتريات الفردية والجملة.\n")
	b.WriteString("📱 تکایە پەیوەندی بە ")
	b.WriteString(name)
	b.WriteString(" بکە بۆ پرسیارکردن لەسەر نرخی، بۆ هەردووکی کڕینی تاک و فرۆشی گەورە.\n\n")
	b.WriteString("📞 Phone/WhatsApp: ")
	b.WriteString(phone)
	b.WriteString("\n")
	b.WriteString("✉️ Email: ")
	b.WriteString(email)
	b.WriteString("\n")
	b.WriteString("📍 Address: ")
	b.WriteString(address)
	b.WriteString("\n\n")
}

func (p *Poster) formatHashtags(tags []string) string {
	var parts []string
	for _, t := range tags {
		if !strings.HasPrefix(t, "#") {
			t = "#" + t
		}
		parts = append(parts, t)
	}
	return strings.Join(parts, " ")
}

func (p *Poster) trimForTwitter(text string) string {
	if len(text) <= 280 {
		return text
	}
	return text[:277] + "..."
}

func (p *Poster) getRandomProduct() (*Product, error) {
	query := `SELECT id, sku, name, price, currency, image_url, thumbnail_url, aliases_en, aliases_ar, aliases_ku, uses_en, uses_ar, uses_ku FROM products WHERE is_active = 1 AND stock > 0 AND image_url IS NOT NULL AND image_url != '' ORDER BY RANDOM() LIMIT 1`
	var product Product
	var aliasesEn, aliasesAr, aliasesKu sql.NullString
	var usesEn, usesAr, usesKu sql.NullString
	err := p.db.QueryRow(query).Scan(&product.ID, &product.SKU, &product.Name, &product.Price, &product.Currency, &product.ImageURL, &product.ThumbnailURL, &aliasesEn, &aliasesAr, &aliasesKu, &usesEn, &usesAr, &usesKu)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no active products with images in stock")
	}
	if err != nil {
		return nil, fmt.Errorf("getRandomProduct query: %w", err)
	}
	product.AliasesEn = parseCSV(aliasesEn.String)
	product.AliasesAr = parseCSV(aliasesAr.String)
	product.AliasesKu = parseCSV(aliasesKu.String)
	product.UsesEn = parseCSV(usesEn.String)
	product.UsesAr = parseCSV(usesAr.String)
	product.UsesKu = parseCSV(usesKu.String)
	return &product, nil
}

func (p *Poster) buildProductPostData(product *Product, store config.StoreConfig, platform config.PlatformConfig) *ProductPostData {
	imageURL := ""
	if product.ImageURL.Valid {
		imageURL = product.ImageURL.String
	}
	data := &ProductPostData{
		ProductID:     product.ID,
		SKU:           product.SKU,
		Name:          product.Name,
		Price:         product.Price,
		Currency:      product.Currency,
		ImageURL:      imageURL,
		MediaType:     "image",
		StoreName:     store.Name,
		StorePhone:    store.Contact.Phone,
		StoreEmail:    store.Contact.Email,
		StoreAddress:  store.Address,
		StoreWhatsApp: store.Contact.Phone,
		Hashtags:      platform.Settings.PostHashtags,
		MaxLength:     platform.Settings.MaxPostLength,
	}
	data.FirstAliasEn = firstString(product.AliasesEn)
	data.FirstAliasAr = firstString(product.AliasesAr)
	data.FirstAliasKu = firstString(product.AliasesKu)
	data.FirstUseEn = firstString(product.UsesEn)
	data.FirstUseAr = firstString(product.UsesAr)
	data.FirstUseKu = firstString(product.UsesKu)
	if imageURL != "" {
		data.ImagePath, data.ImageExists = p.resolveImagePath(imageURL)
	}
	return data
}

func (p *Poster) resolveImagePath(imageURL string) (string, bool) {
	if imageURL == "" {
		return "", false
	}
	if filepath.IsAbs(imageURL) {
		if _, err := os.Stat(imageURL); err == nil {
			return imageURL, true
		}
	}
	cfg := p.configManager.GetConfig()
	paths := cfg.Paths

	var candidates []string
	if paths.PostImages != "" {
		candidates = append(candidates,
			filepath.Join(paths.PostImages, imageURL),
			filepath.Join(paths.PostImages, filepath.Base(imageURL)),
		)
	}
	if paths.ProductImages != "" {
		candidates = append(candidates,
			filepath.Join(paths.ProductImages, imageURL),
			filepath.Join(paths.ProductImages, filepath.Base(imageURL)),
		)
	}
	if paths.Media != "" {
		candidates = append(candidates,
			filepath.Join(paths.Media, imageURL),
			filepath.Join(paths.Media, "products", imageURL),
			filepath.Join(paths.Media, "post_images", imageURL),
			filepath.Join(paths.Media, "product_images", imageURL),
			filepath.Join(paths.Media, filepath.Base(imageURL)),
		)
	}
	candidates = append(candidates, imageURL)
	for _, path := range candidates {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	log.Printf("[Poster] image not found locally for: %s", imageURL)
	return "", false
}

type postTarget struct{ subtypeID, accountID string }

func (p *Poster) resolveTargets(platformID string, platformCfg config.PlatformConfig) []postTarget {
	var targets []postTarget
	if len(platformCfg.Subtypes) > 0 {
		for _, sub := range platformCfg.Subtypes {
			if sub.Enabled {
				targets = append(targets, postTarget{sub.ID, sub.ID})
			}
		}
	}
	if len(targets) == 0 {
		subtype := platformCfg.Platform.Subtype
		if subtype == "" {
			subtype = "account"
		}
		targets = append(targets, postTarget{subtype, ""})
	}
	return targets
}

func (p *Poster) resolveTelegramChannel() string {
	return os.Getenv("TELEGRAM_CHANNEL")
}

func (p *Poster) resolveViberChannel() string {
	return os.Getenv("VIBER_CHANNEL")
}

func (p *Poster) resolveWhatsAppChannel() string {
	return os.Getenv("WHATSAPP_CHANNEL_JID")
}

func (p *Poster) dispatch(instruction *shared.AutomationInstruction) {
	select {
	case p.instructionChan <- instruction:
		log.Printf("[Poster] dispatched instruction %s → %s:%s action=%s", instruction.TicketID, instruction.Platform, instruction.SubtypeID, instruction.Action)
	default:
		log.Printf("[Poster] instruction channel full, dropping ticket %s for %s:%s", instruction.TicketID, instruction.Platform, instruction.SubtypeID)
	}
}

func (p *Poster) trackProductPosted(productID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recentProducts[productID] = time.Now()
	cutoff := time.Now().AddDate(0, 0, -7)
	for id, ts := range p.recentProducts {
		if ts.Before(cutoff) {
			delete(p.recentProducts, id)
		}
	}
}

func (p *Poster) wasRecentlyPosted(productID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ts, exists := p.recentProducts[productID]
	if !exists {
		return false
	}
	return time.Since(ts) < 72*time.Hour
}

func (p *Poster) logPostToDB(postType, postID, platformID, subtypeID, status, content, errorMsg string) {
	platformInfo := []map[string]string{{"platform_id": platformID, "subtype_id": subtypeID}}
	platformJSON, _ := json.Marshal(platformInfo)
	var postedAt interface{}
	if status == "posted" {
		postedAt = time.Now()
	}
	logID := fmt.Sprintf("log_%s_%s", postID, time.Now().Format("20060102150405"))
	_, err := p.db.Exec(`INSERT INTO scheduled_posts (id, title, content, schedule_type, status, posted_at, target_platforms, platform_post_ids, error_message, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, posted_at=excluded.posted_at, platform_post_ids=excluded.platform_post_ids, error_message=excluded.error_message, updated_at=CURRENT_TIMESTAMP`,
		logID, postType+" post", content, postType, status, postedAt, string(platformJSON), "", errorMsg, time.Now(), time.Now())
	if err != nil {
		log.Printf("[Poster] logPostToDB error: %v", err)
	}
}

func (p *Poster) UpdatePostStatus(postID, status, platformPostID, errorMsg string) {
	p.db.Exec(`UPDATE scheduled_posts SET status=?, platform_post_ids=COALESCE(platform_post_ids,?), error_message=?, posted_at=CASE WHEN ?='posted' THEN CURRENT_TIMESTAMP ELSE posted_at END, updated_at=CURRENT_TIMESTAMP WHERE id LIKE ?||'%'`,
		status, platformPostID, errorMsg, status, "log_"+postID)
}

func (p *Poster) GetStats() map[string]interface{} {
	p.mu.RLock()
	recentCount := len(p.recentProducts)
	p.mu.RUnlock()
	stats := map[string]interface{}{"total_posts": 0, "successful_posts": 0, "failed_posts": 0, "days_active": 0, "first_post_date": nil, "last_post_date": nil, "recent_products": recentCount}
	var total, success, fail, days int
	var firstDate, lastDate sql.NullString
	err := p.db.QueryRow(`SELECT COUNT(*), SUM(CASE WHEN status='posted' THEN 1 ELSE 0 END), SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), COUNT(DISTINCT DATE(posted_at)), MIN(posted_at), MAX(posted_at) FROM scheduled_posts WHERE id LIKE 'log_%'`).Scan(&total, &success, &fail, &days, &firstDate, &lastDate)
	if err == nil {
		stats["total_posts"] = total
		stats["successful_posts"] = success
		stats["failed_posts"] = fail
		stats["days_active"] = days
		if firstDate.Valid {
			stats["first_post_date"] = firstDate.String
		}
		if lastDate.Valid {
			stats["last_post_date"] = lastDate.String
		}
	}
	return stats
}

func (p *Poster) Cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recentProducts = make(map[string]time.Time)
}

func parseCSV(input string) []string {
	if input == "" {
		return nil
	}
	parts := strings.Split(strings.TrimSpace(input), ",")
	var out []string
	for _, part := range parts {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func firstString(ss []string) string {
	if len(ss) > 0 {
		return ss[0]
	}
	return ""
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

func isPublicURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}