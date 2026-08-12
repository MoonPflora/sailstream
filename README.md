<div align="center">

<img src="https://raw.githubusercontent.com/yourusername/yourrepo/main/assets/logo.png" alt="Sailstream logo" width="600"/>

# Sailstream

**A self-hosted, multi-platform social automation engine** — replies, takes orders, and posts products across WhatsApp, Facebook, Telegram, Twitter/X, and Viber.

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Python Version](https://img.shields.io/badge/Python-3.12+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](./LICENSE.md)
[![Status](https://img.shields.io/badge/Status-Active_Development-yellow)]()

</div>

---

## 💡 The Point

I built this so devs don't have to deal with expensive, paperwork-heavy official APIs.

**Sailstream is a power tool for solo devs and freelancers.** It uses your own logged-in consumer accounts to automate storefronts. No monthly fees, no begging for access, no SaaS lock-in. Just you, a Go binary, and your credentials.

---

## ⚡ What It Does

- **Listens** to DMs and comments across 5+ platforms (real browser sessions + native protocols).
- **Understands** customers using a hand-written rules engine (English/Arabic/Kurdish) for orders, stock checks, and complaints—LLM is just a fallback.
- **Acts** by replying, quoting, blocking, or placing orders while respecting rate limits.
- **Posts** on a fixed schedule or randomly, optionally with AI captions.
- **Debugs** locally via a sandbox UI that simulates the entire pipeline without touching live accounts.

> ⚠️ **Heads up:** This automates *consumer* accounts, not official business APIs. That's a grey area. Read the Disclaimer before running it.

---

## 🧭 Startup Flow & Architecture (High‑Level)

The app boots in this order: `main.go` → Environment setup → Maestro. Once running, messages flow through the pipeline: Listener → Processor (NLU) → Compiler (Tasker) → Listener (delivery).

The core components are:

- **`main.go`**: Bootstrapper. Checks dependencies, creates folders, loads/wizard-generates `config.json`, runs DB migrations, then hands off.
- **Environment**: Detects your OS, finds a CDP‑compatible browser (Edge/Chrome/Brave/Chromium), locates your real browser profile, and resolves the Python interpreter.
- **Maestro**: The conductor. Owns all goroutines, handles errors, hot‑reloads config, and wakes/sleeps platforms on a schedule.
- **Processor (NLU)**: The brains. Hand‑written rules engine (English/Arabic/Kurdish) that turns raw messages into structured intents.
- **Compiler (Tasker)**: Turns intents into concrete actions (send message, block user, create order) while enforcing rate limits.
- **Listeners**: One per platform. **Fully concurrent**—each runs in its own goroutine. They collect notifications, enrich them with user data, and execute instructions.
- **Session Manager**: Stores auth state. **Unique per platform + subtype** (e.g., Messenger vs Page comments have separate sessions).
- **Sandbox**: A web UI (`:8086/debug`) to inject fake messages and watch the pipeline process them in real‑time.

For a visual diagram of the full flow, see the [Architecture Diagram](#-architecture-diagram) at the end.

---

## 🗂️ Package Breakdown (Who Does What)

| Package | Purpose |
|---|---|
| **`main.go`** | Bootstrapper. Detects OS/browser/Python deps, creates directories, loads or wizard-generates `config.json`, runs DB migrations (`schema.sql` + triggers), and hands control to Maestro. |
| **`maestro`** | The Orchestrator. Owns every goroutine and channel. Starts/stops listeners, routes notifications to the processor, feeds compiler instructions back to the correct listener, handles self-healing, and exposes a control API. |
| **`internal/nnlp` (Processor)** | The NLU Engine. Turns raw messages into structured intent. Handles multi-turn order flow, product resolution (text search + CNN), cancellations, escalations, and falls back to an LLM only when nothing matches. Outputs `ProcessResult` tickets. |
| **`internal/tasker` (Compiler)** | The Action Compiler. Consumes `ProcessResult` tickets, enforces platform rate limits and LLM token/cost budgets, manages a priority-sorted retry queue, compiles intent into concrete `AutomationInstruction` structs, creates DB orders (with stock checks), and emits instructions to listeners. |
| **`shared`** (holds `instructs.go`) | The Unified Instruction Schema. Defines the contract between the compiler and listeners: `AutomationInstruction`, `InstructionStep`, `StepType`, and the `PlatformCollector` interface. |
| **`internal/listener`** | Notification Collectors + Executors. One per platform, each in its own goroutine. Collects messages/comments, enriches them with user data, and executes compiled instructions from a queue (holds collection while executing). |
| **`internal/session`** | Session Manager. Stores persistent authentication state **per platform + subtype**. Duplicates your real browser profile for Facebook/Instagram, handles WhatsApp QR-pairing, and manages Telegram OTP/2FA. |
| **`internal/database`** | Database Setup. Contains `schema.sql` and `triggers_and_views.sql`. Applied on first run. Ensures data consistency (stock decrement on confirm, order totals recalculation, etc.) at the DB layer. |
| **`internal/config`** | Configuration Hub. Defines the entire `config.json` structure. Provides a thread-safe `ConfigManager` with typed getters/setters and atomic `Load()`/`Save()`. |
| **`internal/platforms`** | UI + Platform Scripts. Holds the setup wizard, dashboard UI, and platform-specific automation scripts. Currently `pc` (Post Creator) handles scheduled/random posting with AI captions. |
| **`internal/comms`** | LLM Client. OpenAI-compatible HTTP client (OpenAI, Ollama, LM Studio). Generates replies (with `[[AMBIGUOUS]]` sentinel) and AI post captions. Has its own token/cost rate limiter. |
| **`internal/wrapper`** | CNN Bridge. Shells out to Python (`main_trainer.py`) for product-image recognition (`--predict`) and fine-training (`--train-new`). Parses JSON results. |
| **`internal/enviroment`** | Runtime Environment Detector. Detects OS/Arch/Termux/mobile, finds a CDP-compatible browser, locates the user's real default browser profile, resolves Python, and ensures Python dependencies. |
| **`internal/sandbox`** | Debug Harness. Isolated pipeline for testing NLU → Compiler → Delivery without touching live platforms. In-memory trace store, no-op listener, and web UI (`:8086/debug`) showing each pipeline stage as expandable JSON. |

---

## 🧠 NLU Layer (Processor Decision Tree)

This is where 80% of my active dev time goes. Every incoming message runs through this **first-match-wins** decision tree.

### 1. Process Notification (`processNotificationInternal`)

- **Check notification type** → Only `Message` / `Comment` proceed. Others → `noop`.
- **Skip flag check** → If user sent a fallback reply, skip to avoid double-answering.
- **Gatekeepers** (checks in order):
  - **Quiet Hours** → `block` (quiet_hours).
  - **Blocked User** → `block` (blocked_user).
  - **Auto-Heart enabled & positive words** → `auto_heart`.
  - **Auto-reply disabled** → `noop` (auto_reply_disabled).
  - **Empty content (no text & no images)** → `noop` (empty_content).

### 2. Load User Data & Check Previous Intent (`handlePreviousIntent`)

- **If `last_intent` is empty** → Skip to step 3 (Fresh Classification).
- **If `last_intent == "greeting"`**:
  - Pure greeting & cooldown active? → `noop` (duplicate_greeting).
- **If `last_intent` is `product_availability`, `product_price_query`, or `product_confirmation`**:
  - New availability query? → Fall through to Fresh Classification.
  - **Rejection response** ("no", "wrong", "not this"):
    - If state is `product_confirmation` → `browseNextProductMatch` (try next search result).
    - Else → `send_message` (product_rejected).
  - **Confirmation / Order intent / Acknowledgement** → `createOrderIntentTicket`.
- **If `last_intent == "order_intent"`**:
  - **Cancellation?** → `send_cancellation` + clear pending.
  - **Side query?** (delivery cost / order status) → Answer + `preserve_prior_state` (no state change).
  - **Try finalize delivery** (`tryFinalizeDelivery`):
    - Complete (phone found) → `ask_order_confirmation`.
    - Incomplete → `ask_delivery_details`.
- **If `last_intent == "awaiting_quantity"`**:
  - **Cancellation?** → `send_cancellation` + clear.
  - **Side query?** → Answer + preserve state.
  - **Extract explicit quantity**:
    - No quantity found → `ask_quantity` (retry).
    - **Mode == "pack"** → `buildPackResult`.
    - **Stock insufficient** → `send_stock_warning`.
    - **Stock OK** → `tryFinalizeDelivery` → Confirm or Ask details.
- **If `last_intent == "awaiting_order_details"`**:
  - **Cancellation?** → `send_cancellation` + clear.
  - **Side query?** → Answer + preserve state.
  - **Merge delivery details** (phone, raw text).
  - **Complete delivery?** → `ask_order_confirmation`.
  - **Incomplete** → `ask_delivery_details` (asks only missing fields).
- **If `last_intent == "awaiting_confirmation"`**:
  - **Side query?** → Answer + preserve state.
  - **Change details request?** → Restart order (clear pending, ask details).
  - **Confirmation ("yes", "ok")**:
    - `createOrderInDB` (re-check stock, reserve, insert).
    - Success → `order_created`.
    - Insufficient stock → `send_stock_warning`.
    - DB error → `order_failed`.
  - **Cancellation / Rejection**:
    - Fetch last order.
    - If shipped/delivered → `order_uncancelable`.
    - Else → Cancel order, update stock, `send_message` (order_cancelled).
  - **Unrecognized reply** → `rebuildConfirmationPrompt` (re-show the summary).
- **If `last_intent == "multiple_products_found"`**:
  - Re-run text search.
  - 1 match now? → Handle intent on that match.
  - Else → Fall through to Fresh Classification.

### 3. Fresh Classification (No active previous intent)

- **Pure Greeting** → `send_greeting` (sets cooldown).
- **Store Info Query** (address/hours) → `send_store_info`.
- **Delivery Cost Query** ("how much is shipping?") → `send_delivery_cost` (looks up `shipping` table).
- **Cancellation Intent** → `send_cancellation`.
- **Order Status Query** ("my orders") → `send_order_status` (looks up DB orders).
- **Escalation Intent** (complaint/return/payment/shipping/tech/feedback):
  - Log `urgent_messages`.
  - LLM enabled? → `createAITicket`.
  - Else → `send_fallback` or `noop`.

### 4. Product Resolution (`resolveProduct`)

- **Product attached to notification** (`product_data`) → Use it.
- **Text Search** (keyword extraction + weighted LIKE search):
  - 1 exact match → Use it.
  - Multiple matches, confident top (`score >= 3` & `>= second+2`) → Use it.
  - Multiple matches, ambiguous → `ask_clarify_product`.
- **Image Recognition** (CNN, if enabled):
  - 1 confident match → Use it.
  - Multiple matches → `ask_clarify_product`.
  - No match → `ask_product_name_or_image`.
- **Last product from conversation** (`last_product_sku`) → Use it.
- **No product found**:
  - **Price haggle?** → `price_haggle_reject`.
  - **Intent needs product?** (price/order/pack/availability) → `ask_product_name_or_image`.
  - **Else** → Continue without product (go to Fallbacks).

### 5. Product-Based Actions (If Product Resolved)

- **Compatibility Query** ("does this work with X?") → `send_compatibility_answer` (checks `uses` columns).
- **Pack Intent** ("box", "carton") → `handlePackIntent`:
  - Quantity stated? → `buildPackResult` (creates order immediately).
  - No quantity? → `ask_quantity` (mode: pack).
- **Order Intent**:
  - Product came from text search? → `createOrderProductConfirmation` (ask "Is this the product?" first).
  - Else → `createOrderIntentTicket` (extract quantity, check stock, send template).
- **Price Query** → `send_product_price`.
- **Availability Query** → `send_product_availability`.
- **Default (showing product)** → `createProductConfirmation`.

### 6. Final Fallbacks (No Product & No Intent)

- **Simple Acknowledgement** ("ok", "thanks") → `simple_ack`.
- **LLM Configured** → `createAITicket` (logs AI ticket).
- **Else** → `createFallbackTicket` (logs urgent message, sets `skipNext` flag for the next turn).

---

## ⚙️ Compiler Layer (Tasker Decision Tree)

Once the NLU figures out *what* to do, the compiler figures out *how* to do it—with rate limits, retries, and platform‑specific steps.

### 1. Compiler Initialization (`NewCompiler`)

- Sets up LLM rate limiter (tokens/min and cost/min from config).
- Starts background goroutines:
  - `runProcessQueue` (main queue processor)
  - `runReportStatus` (periodic status updates)
  - `runCleanupProcessedIDs` (prune completed ticket IDs)
  - `runCleanupQueue` (remove stale queue items)
  - `runCleanupExpiredOrders` (auto‑cancel pending orders older than 24h)

### 2. Queue Processing (`runProcessQueue`)

- Ticker fires every 5 seconds.
- Locks queue, sorts by priority (highest first).
- Batches up to 5 items, marks them as "processing", spawns a goroutine for each.
- Each goroutine calls `processQueuedInstruction(item)`.

### 3. Process Queued Instruction (`processQueuedInstruction`)

- Increment attempt count.
- Call `Compile(item.Ticket)`.
- On success → mark as `completed`.
- On error:
  - If non‑retryable (e.g., missing user ID) → mark `failed`.
  - Else if attempts < max attempts → requeue as `queued`.
  - Else → mark `failed`.

### 4. Main Compile Function (`Compile`)

- **Input**: `nnlp.ProcessResult` (ticket) from NLU.
- **Steps**:
  1. Extract `Notification` from ticket data.
  2. Extract user ID – return error if missing.
  3. Check if ticket already processed (cache) – if yes, return error.
  4. **Platform Rate Limit Check** (`checkRateLimits`):
     - If sandbox mode → skip.
     - Ask `RateLimiter.CanProceed(platform, subtype, action)`.
     - If denied → emit rate‑limit error, enqueue item for retry (max 3 attempts), return error.
  5. **Read user data** from DB (state, last intent, etc.).
  6. **Compile action** (`compileAction`) – see switch below.
  7. On compile error → release the rate‑limit slot (since nothing will be sent).
  8. On success:
     - Save outgoing message to DB.
     - Add platform‑specific pacing delays (base + jitter).
     - Record rate‑limit usage (update platform metadata).
     - Mark ticket as processed (cache for 24h).
     - Emit `CompiledInstruction` report.
  9. Return compiled instruction (or error).

### 5. Action Compilation (`compileAction` switch)

The ticket's `Action` field determines which compile function is called.

- **`block`** → `compileBlock`
  - If intent = `quiet_hours` → send ephemeral closure message (no DB side effects).
  - Else → block user (persisted in DB), build `block` instruction.
- **`unfollow`** → `compileUnfollow`
- **`noop`** / **`queued`** → log only.
- **`auto_heart`**, **`react`**, **`like`** → `compileAutoHeart` (file‑log only, ephemeral).
- **`send_greeting`** → `compileGreeting`
  - Uses configured template (list from config) or hardcoded multilingual greeting.
- **`send_store_info`** → `compileStoreInfo`
  - Displays store name, contact, address, hours, description.
- **`send_product`** → `compileSendProduct`
  - Shows product details (name, price, description, stock status). If stock <= 0, marks as "no longer available".
- **`send_order_template`** → `compileOrderTemplate`
  - **If intent = `order_intent`**: shows order summary with quantity, price, total, shipping rates, asks for delivery details. Sets user state to `order_intent`.
  - **If intent = `order_intent_confirmed`**: creates order (via `createConfirmedOrder`), returns confirmation message. (Used by pack orders; regular orders go through different path.)
- **`ask_quantity`** → `compileAskQuantity`
  - Asks how many of the product they want. Sets state to `awaiting_quantity`.
- **`send_delivery_cost`** → `compileDeliveryCost`
  - Answers shipping cost query: if city matched → show specific cost; else show full rate list.
- **`pack_price`** → `compilePackOrder`
  - Creates order for packs (quantity = pack_count × items_per_pack). Calls `createConfirmedOrder`.
- **`send_stock_warning`** → `compileStockWarning`
  - Shows requested vs available stock, suggests alternatives.
- **`send_cancellation`** → `compileCancellation`
  - Finds most recent order. If status is shipped/delivered → uncancelable. Else cancels (releases stock, updates user totals), sends confirmation.
- **`send_fallback`** → `compileFallback`
  - Uses configured fallback message or hardcoded "call us".
- **`send_message`** → `compileSendMessage`
  - Generic message sender (e.g., for simple acknowledgements).
- **`ask_product`**, **`ask_product_name`**, **`ask_product_name_or_image`** → ask user for product name.
- **`ask_clarify_product`** → lists ambiguous matches (with prices) for user to choose.
- **`ask_for_order`** → generic prompt to start order.
- **`ask_order_confirmation`** → `compileAskOrderConfirmation`
  - Shows full order summary (including shipping address from ticket), asks for CONFIRM/CHANGE/CANCEL. Sets state to `awaiting_confirmation`.
- **`ask_product_confirmation`** → `compileAskProductConfirmation`
  - Asks "Is this the product you meant?" (no price shown yet).
- **`send_compatibility_answer`** → `compileCompatibilityAnswer`
  - Answers "does this work for X?" with yes/no/unknown based on product uses data.
- **`send_order_status`** → `compileOrderStatus`
  - Shows recent orders (up to 3), with status labels and line items for the latest.
- **`ask_delivery_details`** → `compileAskDeliveryDetails`
  - Prompts for name, phone, address (or missing fields only). Sets state to `awaiting_order_details`.
- **`order_created`** → `compileOrderCreated`
  - Final receipt with order ID, product, quantity, shipping cost, total.
- **`ai_ticket`**, **`ai_response`** → `compileAITicket` / `compileAIResponse`
  - Calls LLM with conversation history and product context. If ambiguous → fallback. Else sends AI reply. Sets state to `support` or `ai_response`.
- **`share`**, **`save`**, **`follow`** → compile generic social actions (file‑log only).

### 6. Order Creation (`createConfirmedOrder`)

Called by `compilePackOrder` and (indirectly) by `compileOrderTemplate` for pack orders.

- **Steps**:
  1. Begin DB transaction.
  2. Re‑check stock and reserved stock for the product.
  3. If insufficient → return `errInsufficientStock`.
  4. Update product `reserved_stock += stockQty`.
  5. Insert order with status `'confirmed'` (this fires stock decrement trigger).
  6. Insert order item (lineQty, unitPrice, total).
  7. Update user totals (orders count, total spent).
  8. Commit transaction.
- Returns order ID.

### 7. Rate Limiting

- **Platform Rate Limiter** (`RateLimiter` interface):
  - `CanProceed(platform, subtype, action)` reserves a slot atomically.
  - If denied, the instruction is enqueued for retry.
  - On compile failure, `releaseRateLimitUsage` gives the slot back.
- **LLM Rate Limiter** (`LLMRateLimiter`):
  - Tracks tokens and cost per rolling minute.
  - `Allow(tokens, cost)` checks if limits allow; if not, returns retry-after.
  - `Record(tokens, cost)` records usage after successful LLM call.

### 8. Background Cleanups

- `runCleanupProcessedIDs`: removes processed ticket IDs older than 24h.
- `runCleanupQueue`: removes queue items with status `completed`/`failed` older than 5min, and `processing` items stuck >10min.
- `runCleanupExpiredOrders`: auto‑cancels `pending` orders older than 24h (releases reserved stock).

### 9. Instruction Emission

- `Compile` emits a `CompiledInstruction` via `instructionChan`.
- Errors are emitted via `errorChan` with severity/retryability flags.
- Status updates (queue size, counts) are emitted via `statusChan` every 30s.

### 10. Step Construction

- `steps(platform, action, notification, message, extra)` → returns list of `InstructionStep`.
- WhatsApp steps: base rate‑limit check + action‑specific step (send, react, block, etc.) with delay.
- Browser steps (Facebook, Instagram, others): base + reply/send step.
- Adds platform‑specific base delay + jitter to the first step.

---

## 📡 Platform Status

| Platform | Mechanism | Status |
|---|---|---|
| **WhatsApp** | Native protocol (`whatsmeow`) | ✅ Stable |
| **Facebook** | Browser automation (`chromedp`) | ✅ Stable |
| **Telegram** | Native MTProto (`gogram`) | ✅ Stable |
| **Viber** | Bot API | ✅ Stable |
| **Twitter/X** | Session-based | 🚧 In progress |
| **Instagram** | Browser automation | 🚧 In progress |

Each listener runs in its own goroutine—fully concurrent. If WhatsApp is slow delivering a message, Facebook keeps processing incoming DMs without waiting.

---

## 🔐 Session Manager

The session manager is the piece that makes browser automation actually usable. It:

- Stores persistent session files per platform in `cache/sessions/`.
- **Every session is unique per platform + subtype** – so you can have separate authenticated sessions for Facebook Messenger and Facebook Page comments running side-by-side.
- For Facebook: duplicates your real browser profile into an app-owned directory *once*, kills the real browser process briefly to copy it, then reuses that copy forever. No fighting for single-instance locks.
- Supports interactive login—launches a visible browser window, auto-fills credentials if you have them in config, or lets you manually log in for up to 5 minutes while it polls for cookies.
- Also supports importing cookies from a JSON export.
- Handles Telegram 2FA and WhatsApp QR-pairing via a local SSE-powered mini-page.

If a session gets corrupted (e.g., logged out), it nukes the cache and triggers a clean restart.

---

## 🧪 Sandbox (Debug Without Fear)

Run the app with the sandbox listener registered, open `http://localhost:8086/debug`.

Type a fake message, pick a platform, and watch each pipeline stage (`received` → `nlp_result` → `compiled` → `delivered`) expand as clickable JSON cards. Doesn't touch a real account. Great for tweaking the rules engine without annoying real customers.

---

## 🚀 Getting Started (Rough Guide)

1. **Prereqs**: Go (1.21+), Python 3 (only for image rec), and a Chromium browser (Edge/Chrome/Brave) logged into any account you want to automate.
2. **Clone & run**: `go run main.go`. First run generates `config.json` and launches a setup wizard.
3. **Configure**: Fill in store info, AI provider (optional), and credentials.
4. **Login**: Facebook opens a browser. Telegram prompts for OTP. WhatsApp shows a QR code.
5. **Test**: Use the sandbox UI *before* pointing it at real customers.

---

## 🗺️ Roadmap (What I'm Building Next)

1. **Enhance NLU** – More natural language patterns and tighter order/product logic. (Priority #1)
2. **Finish Twitter/X & Instagram** – Get both listeners production-ready.
3. **Better UI** – Replace the ugly sandbox with a clean dashboard for orders, messages, and platform status.

---

## 👥 Who This Is For

- **Solo devs, freelancers, and store owners** who want full control without API fees.
- **Tinkerers** who want to modify the automation logic themselves.

If you're a company wanting to white-label this as a closed-source SaaS—**AGPL says no**. Use it internally? Fine. Fork it and share improvements? Even better.

---

## ⚠️ Disclaimer

This automates real consumer accounts via unofficial channels because official business APIs are either too expensive or non-existent for small stores.

**Using this is very likely against the platform's ToS.** I provide it "as is," with no guarantee it'll keep working. You accept full responsibility for bans, data loss, or legal consequences. Don't run it if you can't accept that.

---

## 📄 License (AGPL-3.0)

Licensed under the **GNU Affero General Public License v3.0**. Full text in [`LICENSE.md`](./LICENSE.md).

**In plain English:**

- **Individuals & freelancers**: Use, modify, fork, run for your store, even offer it as a paid service. Just share your modifications if you distribute or run it as a public network service.
- **Organizations**: Use it internally without open-sourcing everything. But if you offer it as a SaaS (or distribute modified versions), AGPL kicks in—you must make your source available to your users.

I chose AGPL so solo devs have freedom, but nobody can wrap the core logic into a closed-source product without giving back.

---

## 📐 Architecture Diagram

```mermaid
graph TD
    A[main.go] -->|Initializes files/paths| B(Environment Setup);
    B -->|Detects OS, Browser, Python| C[Maestro Orchestrator];
    
    C -->|Starts| D[Platform Listener];
    D -->|Incoming Message| E[Processor NLU];
    E -->|Intent/Entities| F[Compiler Tasker];
    F -->|Platform Instructions| D;
    D -->|Delivers reply| G[Platform API/Browser];
    
    C -->|Manages lifecycle| H[Session Manager];
    H -->|Unique session per platform+subtype| I[(SQLite DB)];
