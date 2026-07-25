// internal/comms/llm.go
//
// LLM is the single OpenAI-compatible HTTP client for the entire sailstream
// system.  Every prompt, tone, language, and generation parameter comes from
// config — nothing is hard-coded.
//
// Callers:
//   - nnlp/processor.go  → GenerateReply   (complex-query / ambiguous intent)
//   - scripts/poster.go  → GeneratePost    (random product post caption)
//
// Provider support (all OpenAI-compatible):
//   - Cloud: OpenAI, Anthropic (via proxy), Groq, Together, etc.
//   - Local: Ollama (http://localhost:11434/v1), LM Studio, llama.cpp server
//
// Ambiguity signal:
//   When the model cannot produce a confident, on-topic reply it returns the
//   sentinel string AmbiguousMarker inside the response text.  NNLP detects
//   this and routes the notification to compileFallback instead of ai_response.
//
// Config fields consumed (all read-only, never written back):
//   cfg.AI.Provider          — informational label ("openai", "ollama", …)
//   cfg.AI.Model             — model name sent to the API
//   cfg.AI.APIKey            — Bearer token (empty = local, no auth header)
//   cfg.AI.BaseURL           — base URL, e.g. "https://api.openai.com/v1" or
//                              "http://localhost:11434/v1"
//   cfg.AI.Generation.*      — MaxTokens, Temperature, TopP, Penalty fields
//   cfg.AI.Instructions.*    — SystemPrompt, PostInstructions,
//                              ReplyInstructions, ScheduledPostInstructions,
//                              Tone, MaxResponseLength
//   cfg.Store.*              — name, description, address, contact, hours, currency
//   cfg.System.Language      — default language when not inferred from text
//
// Thread safety: Client is safe for concurrent use from multiple goroutines.

package comms

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"sailstream/internal/config"
)

// =============================================================================
// SENTINEL — detected by NNLP to trigger fallback routing
// =============================================================================

// AmbiguousMarker is embedded in the model's reply when it cannot produce a
// confident, on-topic answer.  NNLP checks for this string and routes to
// compileFallback instead of compileAIResponse.
const AmbiguousMarker = "[[AMBIGUOUS]]"

// =============================================================================
// REQUEST / RESPONSE TYPES  (OpenAI chat-completions format)
// =============================================================================

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model            string        `json:"model"`
	Messages         []chatMessage `json:"messages"`
	MaxTokens        int           `json:"max_tokens,omitempty"`
	Temperature      float64       `json:"temperature,omitempty"`
	TopP             float64       `json:"top_p,omitempty"`
	PresencePenalty  float64       `json:"presence_penalty,omitempty"`
	FrequencyPenalty float64       `json:"frequency_penalty,omitempty"`
	Stream           bool          `json:"stream"`
}

type chatChoice struct {
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// =============================================================================
// CLIENT
// =============================================================================

// Client holds a shared HTTP client and a reference to the live ConfigManager.
// It reads config on every call so hot-reloads (e.g. after Maestro Resume) are
// picked up automatically without restarting the client.
type Client struct {
	http   *http.Client
	cfgMgr *config.ConfigManager
}

// NewClient constructs an LLM client.
// cfgMgr must already be loaded (NewMaestro calls Load before anything else).
func NewClient(cfgMgr *config.ConfigManager) *Client {
	return &Client{
		http: &http.Client{
			Timeout: 60 * time.Second,
		},
		cfgMgr: cfgMgr,
	}
}

// Enabled returns true when the config contains a non-empty model name.
// Callers should check this before calling GenerateReply / GeneratePost to
// avoid unnecessary work when AI is not configured.
func (c *Client) Enabled() bool {
	ai := c.cfgMgr.GetAI()
	return ai.Model != ""
}

// =============================================================================
// REPLY GENERATION  (used by NNLP compiler for ai_response action)
// =============================================================================

// ReplyRequest carries everything NNLP knows about the incoming message.
// All fields are optional — the client fills gaps from config defaults.
type ReplyRequest struct {
	// The raw customer message that triggered the AI path.
	UserMessage string

	// Detected language ("en", "ar", "ku"). Empty → cfg.System.Language.
	Language string

	// Recent conversation history in chronological order (oldest first).
	// Each entry is "user: <text>" or "assistant: <text>".
	History []string

	// ProductContext is the serialised product map if a product was identified
	// upstream (e.g. from alias search or image recognition).
	// Pass nil when no product context is available.
	ProductContext map[string]interface{}

	// PlatformID is used only for logging.
	PlatformID string
}

// ReplyResult is what NNLP receives back.
type ReplyResult struct {
	// Text is the model's reply, ready to send to the user.
	// If Text contains AmbiguousMarker the caller must route to fallback.
	Text string

	// Ambiguous is true when the model signalled it could not produce a
	// confident reply.  Callers should NOT send Text to the user in this case.
	Ambiguous bool
}

// GenerateReply calls the LLM to produce a customer-service reply.
// It builds the full prompt from config — callers only supply request-specific
// context.
func (c *Client) GenerateReply(ctx context.Context, req ReplyRequest) (*ReplyResult, error) {
	ai := c.cfgMgr.GetAI()
	if ai.Model == "" {
		return nil, fmt.Errorf("llm: no model configured")
	}

	store := c.cfgMgr.GetStore()
	sys := c.cfgMgr.GetSystem()

	lang := req.Language
	if lang == "" {
		lang = sys.Language
	}
	if lang == "" {
		lang = "en"
	}

	// ── System prompt ────────────────────────────────────────────────────────
	// Start from cfg.AI.Instructions.SystemPrompt (operator-supplied).
	// Append store context so the model knows who it is replying for.
	// Append reply instructions and tone.
	// Never hard-code any of these — if the config field is empty the section
	// is simply omitted, keeping the prompt minimal.
	var sysParts []string

	if ai.Instructions.SystemPrompt != "" {
		sysParts = append(sysParts, ai.Instructions.SystemPrompt)
	}

	// Store identity block — only non-empty fields are included.
	var storeLines []string
	if store.Name != "" {
		storeLines = append(storeLines, "Store: "+store.Name)
	}
	if store.Description != "" {
		storeLines = append(storeLines, "About: "+store.Description)
	}
	if store.Address != "" {
		storeLines = append(storeLines, "Address: "+store.Address)
	}
	if store.Contact.Phone != "" {
		storeLines = append(storeLines, "Phone/WhatsApp: "+store.Contact.Phone)
	}
	if store.Contact.Email != "" {
		storeLines = append(storeLines, "Email: "+store.Contact.Email)
	}
	if store.Currency != "" {
		storeLines = append(storeLines, "Currency: "+store.Currency)
	}
	if len(store.BusinessHours) > 0 {
		for day, hours := range store.BusinessHours {
			storeLines = append(storeLines, "Hours ("+day+"): "+hours)
		}
	}
	if len(storeLines) > 0 {
		sysParts = append(sysParts, strings.Join(storeLines, "\n"))
	}

	// Operator instructions for replies.
	if ai.Instructions.ReplyInstructions != "" {
		sysParts = append(sysParts, ai.Instructions.ReplyInstructions)
	}
	if ai.Instructions.Tone != "" {
		sysParts = append(sysParts, "Tone: "+ai.Instructions.Tone)
	}

	// Language directive.
	sysParts = append(sysParts, "Reply language: "+lang)

	// Response length cap from config.
	maxLen := ai.Instructions.MaxResponseLength
	if maxLen > 0 {
		sysParts = append(sysParts, fmt.Sprintf("Keep reply under %d characters.", maxLen))
	}

	// Ambiguity instruction — this is how we get the sentinel back.
	sysParts = append(sysParts,
		"If you cannot give a confident, on-topic answer relevant to the store or "+
			"its products, reply with exactly: "+AmbiguousMarker)

	systemContent := strings.Join(sysParts, "\n\n")

	// ── Message list ─────────────────────────────────────────────────────────
	messages := []chatMessage{
		{Role: "system", Content: systemContent},
	}

	// Inject conversation history.
	for _, h := range req.History {
		if strings.HasPrefix(h, "user:") {
			messages = append(messages, chatMessage{
				Role:    "user",
				Content: strings.TrimSpace(strings.TrimPrefix(h, "user:")),
			})
		} else if strings.HasPrefix(h, "assistant:") {
			messages = append(messages, chatMessage{
				Role:    "assistant",
				Content: strings.TrimSpace(strings.TrimPrefix(h, "assistant:")),
			})
		}
	}

	// If there is product context, add it as an assistant context note so the
	// model can reference it without the user having to repeat themselves.
	if len(req.ProductContext) > 0 {
		var productLines []string
		for _, key := range []string{"name", "sku", "price", "currency", "stock", "description"} {
			if v, ok := req.ProductContext[key]; ok && v != nil && fmt.Sprintf("%v", v) != "" {
				productLines = append(productLines, fmt.Sprintf("%s: %v", key, v))
			}
		}
		if len(productLines) > 0 {
			messages = append(messages, chatMessage{
				Role:    "assistant",
				Content: "[Context] Product identified: " + strings.Join(productLines, " | "),
			})
		}
	}

	// The user's actual message.
	messages = append(messages, chatMessage{
		Role:    "user",
		Content: req.UserMessage,
	})

	// ── Call the API ─────────────────────────────────────────────────────────
	text, err := c.call(ctx, ai, messages)
	if err != nil {
		return nil, err
	}

	ambiguous := strings.Contains(text, AmbiguousMarker)
	return &ReplyResult{
		Text:      strings.TrimSpace(text),
		Ambiguous: ambiguous,
	}, nil
}

// =============================================================================
// POST CAPTION GENERATION  (used by Poster for random product posts)
// =============================================================================

// PostRequest carries product and platform context for caption generation.
type PostRequest struct {
	// Product fields read from the DB row.
	ProductName    string
	ProductSKU     string
	ProductPrice   float64
	ProductCurrency string
	AliasEn        string // first English alias
	AliasAr        string // first Arabic alias
	AliasKu        string // first Kurdish alias
	UseEn          string // first English use-case
	UseAr          string // first Arabic use-case
	UseKu          string // first Kurdish use-case

	// Platform context — used to honour per-platform MaxPostLength.
	PlatformID string
	MaxLength  int // 0 = no limit (use cfg.AI.Instructions.MaxResponseLength)

	// Hashtags to append (already formatted with # prefix).
	Hashtags []string
}

// PostResult is the generated caption.
type PostResult struct {
	// Caption is the full multilingual post text ready to use.
	Caption string
}

// GeneratePost calls the LLM to produce a multilingual product post caption.
// The output replaces the static text produced by poster.generatePostText when
// AI is enabled — the caller decides which to use based on c.Enabled().
func (c *Client) GeneratePost(ctx context.Context, req PostRequest) (*PostResult, error) {
	ai := c.cfgMgr.GetAI()
	if ai.Model == "" {
		return nil, fmt.Errorf("llm: no model configured")
	}

	store := c.cfgMgr.GetStore()
	sys := c.cfgMgr.GetSystem()

	// ── System prompt ────────────────────────────────────────────────────────
	var sysParts []string

	if ai.Instructions.SystemPrompt != "" {
		sysParts = append(sysParts, ai.Instructions.SystemPrompt)
	}

	// Store identity.
	var storeLines []string
	if store.Name != "" {
		storeLines = append(storeLines, "Store: "+store.Name)
	}
	if store.Description != "" {
		storeLines = append(storeLines, "About: "+store.Description)
	}
	if store.Address != "" {
		storeLines = append(storeLines, "Address: "+store.Address)
	}
	if store.Contact.Phone != "" {
		storeLines = append(storeLines, "Phone/WhatsApp: "+store.Contact.Phone)
	}
	if store.Contact.Email != "" {
		storeLines = append(storeLines, "Email: "+store.Contact.Email)
	}
	if store.Currency != "" {
		storeLines = append(storeLines, "Currency: "+store.Currency)
	}
	if len(storeLines) > 0 {
		sysParts = append(sysParts, strings.Join(storeLines, "\n"))
	}

	// Post-specific instructions from config.
	if ai.Instructions.PostInstructions != "" {
		sysParts = append(sysParts, ai.Instructions.PostInstructions)
	}
	if ai.Instructions.Tone != "" {
		sysParts = append(sysParts, "Tone: "+ai.Instructions.Tone)
	}

	// Language directive — default to system language or trilingual.
	lang := sys.Language
	if lang == "" {
		lang = "en+ar+ku"
	}
	sysParts = append(sysParts, "Languages to include: "+lang)

	// Length cap: prefer platform MaxLength, then cfg MaxResponseLength.
	maxLen := req.MaxLength
	if maxLen == 0 {
		maxLen = ai.Instructions.MaxResponseLength
	}
	if maxLen > 0 {
		sysParts = append(sysParts, fmt.Sprintf("Keep the total post under %d characters.", maxLen))
	}

	// Instruct the model to always end with PID tag so the poster's ID
	// extraction still works downstream.
	sysParts = append(sysParts,
		`End the post with exactly one line: PID="<product_sku>" — use the SKU provided below.`)

	systemContent := strings.Join(sysParts, "\n\n")

	// ── User message — product details only, no prompt text ─────────────────
	// Build a compact product brief from the request fields.
	// Only non-empty fields are included to keep token count low.
	var briefLines []string
	if req.ProductName != "" {
		briefLines = append(briefLines, "Product name: "+req.ProductName)
	}
	if req.ProductSKU != "" {
		briefLines = append(briefLines, "SKU: "+req.ProductSKU)
	}
	if req.ProductPrice > 0 {
		cur := req.ProductCurrency
		if cur == "" {
			cur = store.Currency
		}
		briefLines = append(briefLines, fmt.Sprintf("Price: %.2f %s", req.ProductPrice, cur))
	}
	// Aliases — whichever languages are available.
	var aliases []string
	if req.AliasEn != "" {
		aliases = append(aliases, "EN: "+req.AliasEn)
	}
	if req.AliasAr != "" {
		aliases = append(aliases, "AR: "+req.AliasAr)
	}
	if req.AliasKu != "" {
		aliases = append(aliases, "KU: "+req.AliasKu)
	}
	if len(aliases) > 0 {
		briefLines = append(briefLines, "Aliases: "+strings.Join(aliases, " / "))
	}
	// Use-cases.
	var uses []string
	if req.UseEn != "" {
		uses = append(uses, "EN: "+req.UseEn)
	}
	if req.UseAr != "" {
		uses = append(uses, "AR: "+req.UseAr)
	}
	if req.UseKu != "" {
		uses = append(uses, "KU: "+req.UseKu)
	}
	if len(uses) > 0 {
		briefLines = append(briefLines, "Uses: "+strings.Join(uses, " / "))
	}
	// Hashtags — pass them so the model can weave them in naturally.
	if len(req.Hashtags) > 0 {
		briefLines = append(briefLines, "Hashtags: "+strings.Join(req.Hashtags, " "))
	}

	userContent := strings.Join(briefLines, "\n")

	messages := []chatMessage{
		{Role: "system", Content: systemContent},
		{Role: "user", Content: userContent},
	}

	// ── Call the API ─────────────────────────────────────────────────────────
	text, err := c.call(ctx, ai, messages)
	if err != nil {
		return nil, err
	}

	return &PostResult{Caption: strings.TrimSpace(text)}, nil
}

// =============================================================================
// SCHEDULED POST CAPTION GENERATION
// =============================================================================

// ScheduledPostRequest carries the context for a scheduled post.
type ScheduledPostRequest struct {
	Title       string
	Description string
	Hashtags    []string
	PlatformID  string
	MaxLength   int
}

// GenerateScheduledPost generates a caption for a manually-scheduled post.
// Uses cfg.AI.Instructions.ScheduledPostInstructions when available.
func (c *Client) GenerateScheduledPost(ctx context.Context, req ScheduledPostRequest) (*PostResult, error) {
	ai := c.cfgMgr.GetAI()
	if ai.Model == "" {
		return nil, fmt.Errorf("llm: no model configured")
	}

	store := c.cfgMgr.GetStore()
	sys := c.cfgMgr.GetSystem()

	var sysParts []string

	if ai.Instructions.SystemPrompt != "" {
		sysParts = append(sysParts, ai.Instructions.SystemPrompt)
	}

	var storeLines []string
	if store.Name != "" {
		storeLines = append(storeLines, "Store: "+store.Name)
	}
	if store.Contact.Phone != "" {
		storeLines = append(storeLines, "Phone/WhatsApp: "+store.Contact.Phone)
	}
	if store.Contact.Email != "" {
		storeLines = append(storeLines, "Email: "+store.Contact.Email)
	}
	if store.Address != "" {
		storeLines = append(storeLines, "Address: "+store.Address)
	}
	if len(storeLines) > 0 {
		sysParts = append(sysParts, strings.Join(storeLines, "\n"))
	}

	// Prefer the scheduled-post-specific instructions; fall back to post instructions.
	instructions := ai.Instructions.ScheduledPostInstructions
	if instructions == "" {
		instructions = ai.Instructions.PostInstructions
	}
	if instructions != "" {
		sysParts = append(sysParts, instructions)
	}
	if ai.Instructions.Tone != "" {
		sysParts = append(sysParts, "Tone: "+ai.Instructions.Tone)
	}

	lang := sys.Language
	if lang == "" {
		lang = "en+ar+ku"
	}
	sysParts = append(sysParts, "Languages to include: "+lang)

	maxLen := req.MaxLength
	if maxLen == 0 {
		maxLen = ai.Instructions.MaxResponseLength
	}
	if maxLen > 0 {
		sysParts = append(sysParts, fmt.Sprintf("Keep the total post under %d characters.", maxLen))
	}

	systemContent := strings.Join(sysParts, "\n\n")

	// User message: just the raw brief.
	var briefLines []string
	if req.Title != "" {
		briefLines = append(briefLines, "Title: "+req.Title)
	}
	if req.Description != "" {
		briefLines = append(briefLines, "Description: "+req.Description)
	}
	if len(req.Hashtags) > 0 {
		briefLines = append(briefLines, "Hashtags: "+strings.Join(req.Hashtags, " "))
	}

	messages := []chatMessage{
		{Role: "system", Content: systemContent},
		{Role: "user", Content: strings.Join(briefLines, "\n")},
	}

	text, err := c.call(ctx, ai, messages)
	if err != nil {
		return nil, err
	}

	return &PostResult{Caption: strings.TrimSpace(text)}, nil
}

// =============================================================================
// CORE HTTP CALL
// =============================================================================

// call sends a chat-completions request and returns the first choice text.
// All generation parameters come from cfg.AI.Generation.
func (c *Client) call(ctx context.Context, ai config.AIConfig, messages []chatMessage) (string, error) {
	// Resolve endpoint.
	baseURL := strings.TrimRight(ai.BaseURL, "/")
	if baseURL == "" {
		// Default to OpenAI.
		baseURL = "https://api.openai.com/v1"
	}
	endpoint := baseURL + "/chat/completions"

	// Generation settings — zero values are omitted by omitempty where
	// applicable; the API uses its own defaults for those fields.
	gen := ai.Generation
	maxTok := gen.MaxTokens
	if maxTok == 0 {
		maxTok = 1024 // safe default so we don't truncate unexpectedly
	}

	reqBody := chatRequest{
		Model:            ai.Model,
		Messages:         messages,
		MaxTokens:        maxTok,
		Temperature:      gen.Temperature,
		TopP:             gen.TopP,
		PresencePenalty:  gen.PresencePenalty,
		FrequencyPenalty: gen.FrequencyPenalty,
		Stream:           false,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("llm: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("llm: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Only add Authorization header when an API key is configured.
	// Local servers (Ollama, LM Studio) typically don't require one.
	if ai.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+ai.APIKey)
	}

	log.Printf("[LLM] → %s model=%s messages=%d maxTokens=%d",
		endpoint, ai.Model, len(messages), maxTok)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("llm: HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20)) // 2 MB cap
	if err != nil {
		return "", fmt.Errorf("llm: read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("llm: server returned HTTP %d: %s",
			resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBytes, &chatResp); err != nil {
		return "", fmt.Errorf("llm: unmarshal response: %w (body: %.200s)", err, respBytes)
	}
	if chatResp.Error != nil {
		return "", fmt.Errorf("llm: API error: %s", chatResp.Error.Message)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("llm: no choices in response")
	}

	text := chatResp.Choices[0].Message.Content
	log.Printf("[LLM] ← model=%s finish=%s len=%d",
		ai.Model, chatResp.Choices[0].FinishReason, len(text))
	return text, nil
}