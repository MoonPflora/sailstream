# System_Flowchart.md — Audit Findings & Fix Plan

Audit basis: `System_Flowchart.md` compared against actual source code (`types.go`, `Processor.go`, `Compiler.go`, `Maestro_main.go`, `Main.go`, `Instruct.md`, `Issues.md`, `Shared/Instructs.go`, `Config.go`).

---

## 1. MISSING SUBSYSTEMS / NODES

| # | Missing Item | Source Evidence | Severity | Fix Action |
|---|-------------|----------------|----------|------------|
| M1 | **Debounce / Dedup queue** in Processor | `Processor.go` — `debounceNotification()` + duplicate check via `debounceMap` + `debounceTimer`. Enqueues to worker pool before main decision tree. | **HIGH** — 1st processing stage | Add subgraph "Debounce & Worker Pool" before `processNotificationInternal()`. Show debounce timer expiry → worker dequeue → processNotification |
| M2 | **`Notification` struct fields** | `types.go` — `struct Notification { ID, PlatformID, Type, Timestamp, Message *Message, Comment *Comment, Position, RawData, ... }` | **MEDIUM** — aids reader understanding | Add sidebar annotation in SHARED_ROUTER subgraph |
| M3 | **`MediaAttachment` types** | `types.go` — `struct MediaAttachment { MediaType, URL, FilePath, Caption }` — drives image recognition branch | **MEDIUM** | Add node in PROCESSOR for `hasImages()` → `collectImageURLs()` |
| M4 | **`ProcessResult` struct** | `Processor.go` — `struct ProcessResult { TicketID, Action, Intent, Data map[string]interface{} }` — the actual output object flowing to Compiler | **MEDIUM** | Add note in PROC_OUTPUT |
| M5 | **`InstructionStep` types** | `Compiler.go` — `StepTypeRateLimitCheck`, `StepTypeSendMessage`, `StepTypeReply`, `StepTypeReact`, `StepTypeBlock`, `StepTypeUpload`, `StepTypeLog` | **HIGH** | Add subgraph "Step Types" inside EXECUTION |
| M6 | **`runFallbackSteps()`** | `Compiler.go` — executed when primary instruction fails; runs compensatory actions (log, notify, rollback) | **HIGH** — core fallback mechanism | Add node after Q_DEAD |
| M7 | **`emitReport()` / `emitError()`** | `Compiler.go` — emitReport sends to StatusChan, emitError sends to ErrorChan; both write to DB audit log | **MEDIUM** | Add as output edges from queue execution |
| M8 | **`loadSessionContext()`** in Processor | `Processor.go` — loads `last_intent`, `conversation_state`, `total_orders` before `handlePreviousIntent()` | **HIGH** | Add node in PROCESSOR before P_PREV_INTENT |
| M9 | **`resolveProduct()` inner tree** | `Processor.go` — tries price match → name match → image match → category fallback. `needsClarification` branch shows matched candidates | **HIGH** — major decision node | Expand P_PRODUCT into sub-sub-decisions |
| M10 | **`processImages()` → CNN Python** | `Processor.go` — `processImages()` calls `Wrapper.go → CNNWrapper` which calls `Cnn/*.py` scripts | **MEDIUM** | Add edge from P_IMAGE_PIPE to CNN_PY in INFRA |
| M11 | **`Launch.bat` startup** | `Launch.bat` — calls `Go run Main.go` | **LOW** | Add node linked from BOOT |
| M12 | **`Cmd/sandbox-cli/`** | `Cmd/sandbox-cli/` — standalone sandbox runner | **LOW** | Add in SANDBOX section |
| M13 | **`DB/schema.sql` + triggers** | `DB/schema.sql` (`update_stock_on_order_confirmed` trigger), `triggers_and_views.sql` | **MEDIUM** | Add `DB Trigger: auto stock decrement` node in STORAGE |
| M14 | **Scheduler for scheduled posts** | `Media/scheduled_posts/` directory + `Issues.md` mentions scheduled posting | **MEDIUM** | Add "Scheduled Post Daemon" in INFRA |
| M15 | **Backup goroutines** | `Backup/` directory + `Issues.md` | **LOW** | Add "Periodic Backup" edge from BOOT |
| M16 | **Panic recovery (`recover()`)** | `Compiler.go` + `Session_manager.go` both have `defer function() { if r := recover(); r != nil { log } }` | **MEDIUM** — resilience | Add "Panic Recovery → log + continue" to Q_PROCESS and SESSION_MGR |

---

## 2. MISSING / BROKEN CONNECTIONS

| # | Missing Link | Source Evidence | Severity | Fix Action |
|---|-------------|----------------|----------|------------|
| L1 | **Maestro_main.go → each listener goroutine** | `Maestro_main.go` — `wg.Add(1)` → `go listener.RunTelegram()`, `go listener.RunFacebook()`, etc. | **HIGH** | Add edges: MAESTRO_ORCH → each LISTENER subgraph |
| L2 | **Compiler → Sandbox integration** | `Compiler.go` has `StepType` but sandbox execution is only in `Sandbox.go` as standalone | **MEDIUM** — unclear if compiler calls sandbox | Add dashed edge from EXECUTION → SANDBOX (conditional) |
| L3 | **Processor → Session Manager direct read** | `Processor.go` — calls `userData` which contains session state | **HIGH** | Add edge from P_ENTRY → SESSION_MGR for context load |
| L4 | **Queue → Session expiry refresh** | `Session_manager.go` — on every access, refresh TTL | **MEDIUM** | Add edge DB_SESS → SESSION_MGR (TTL check) |
| L5 | **ErrorChan → Dashboard / Log** | `Compiler.go` — `ErrorChan` consumed by `Main.go` event loop → writes to `Maestro.log` | **MEDIUM** | Add edge from Q_DEAD → LOGS → Dashboard |
| L6 | **Rate-limit counters → Cache** | `RateLimiter` in `Compiler.go` stores counters; `Cache/` directory holds LRU cache | **LOW** | Add RL_CHECK → CACHE |
| L7 | **Product data → product_images media** | `Processor.go` — `collectImageURLs()` pulls images from `Media/product_images/` | **LOW** | Add edge from P_IMAGE_PIPE → Media/ |
| L8 | **Config reload signal** | `Config.go` — no hot-reload, requires restart. Noted for documentation. | **LOW** | Add annotation to CFG subgraph |

---

## 3. CONDITION / BRANCH OMISSIONS

| # | Branch | Source Evidence | Severity | Fix Action |
|---|--------|----------------|----------|------------|
| B1 | **`isPureGreeting()` → multi-language check** | Returns true if text matches greeting patterns in en/ar/ku | **MEDIUM** | Annotate P_GREETING node |
| B2 | **`isCancellationIntent()` → checks keywords** | Matches "cancel", "إلغاء", "پشیمان" etc. | **MEDIUM** | Annotate P_CANCEL node |
| B3 | **`classifyEscalationIntent()` → 3 types** | Returns "speak_to_human", "complaint", "support" — each routes differently | **HIGH** | Split P_ESC_RES into 3 branches |
| B4 | **`isPriceHaggle()` → template reply** | Returns `action=ai_response` with template `price_haggle_reject` | **MEDIUM** | Annotate P_HAGGLE with template ID |
| B5 | **`shouldAutoHeart()` conditions** | Platform enabled + short comment + no negative keywords + cooldown | **HIGH** | Add decision details to P_AUTO_HEART |
| B6 | **`isInQuietHours()` config-driven** | Checks `Config.json → quiet_hours.start / end` | **MEDIUM** | Add config reference to P_QUIET_HOURS |
| B7 | **`isBlockedUser()` → checks userData** | Reads `userData["is_blocked"]` boolean from session | **MEDIUM** | Annotate P_BLOCKED_USER |
| B8 | **`createConfirmedOrder()` stock handling** | `errInsufficientStock` → routes to `compileStockWarning` instead of generic fallback | **HIGH** | Add explicit edge: insufficient stock → STOCK_WARNING |
| B9 | **Pack-order pricing branch** | `compilePackOrder` vs `compileOrderTemplate` — same transaction logic, different per-pack pricing | **MEDIUM** | Add "pack pricing" annotation |
| B10 | **Language detection fallback** | `detectLanguage()` → fasttext model .h5 → fails → default "en" | **HIGH** | Add node between language detection and template selection |

---

## 4. FLOW & LABEL ISSUES

| # | Issue | Location | Severity | Fix |
|---|-------|----------|----------|-----|
| I1 | **No feedback loop from Compiler back to Session** | After instruction completes, session state should update | **HIGH** | Add edge: WA_SEND / FB_SEND → SESSION_MGR (update last_intent) |
| I2 | **Dashboard UI not connected to system** | `wizzard.go` + `wizzard.html` shown in INFRA but no edges from data | **MEDIUM** | Add arrows: DB_STORE → DASHBOARD, LOGS → DASHBOARD |
| I3 | **Multiple outbound endpoints go to single generic "Platform API"** | EXECUTION sends to WA_SEND but not back to the specific endpoint nodes | **HIGH** | Add: WA_SEND → WA_API, FB_SEND → FB_API, etc. |
| I4 | **No `Maestro.Stop() / shutdown` path** | `Maestro_main.go` has signal handler for graceful shutdown | **MEDIUM** | Add "Signal → Stop → drain queues → close DB" node |
| I5 | **Legend incomplete** | Missing `sandbox`, `config`, `llm` class swatches | **LOW** | Add all 10 classDefs to legend |
| I6 | **Node `P_PRODUCT` overloaded** | Contains 6 exit paths without sub-decisions → hard to follow | **HIGH** | Break into nested decision subgraph |

---

## 5. RATE LIMIT & RESILIENCE GAPS

| # | Gap | Source | Fix |
|---|-----|--------|-----|
| R1 | **LLM provider rate limit** | `llm.go` has `retryCount`, `timeout` but no explicit per-model token-bucket | Add node: "LLM Provider Rate Limit (tokens/min)" |
| R2 | **DB connection pool pressure** | `database.go` uses `sql.DB` with default max_open_conns | Add: "DB connection pool mgmt" node |
| R3 | **Cache layer for rate counters** | Rate counter reads hit Cache/ SQLite on every Check | Add: "Cache TTL → stale counter cleanup" |
| R4 | **Dead-letter recovery** | Errors go to audit log but no re-drive mechanism | Add: "Dead Letter Queue → periodic retry" |

---

## Summary of Required Changes

| Category | Count |
|----------|-------|
| MISSING items (M1–M16) | 16 |
| BROKEN connections (L1–L8) | 8 |
| BRANCH omissions (B1–B10) | 10 |
| FLOW issues (I1–I6) | 6 |
| RESILIENCE gaps (R1–R4) | 4 |
| **Total** | **44** |

### Priority Actions
1. **HIGHEST** (immediate correctness): L1, B3, B8, I3, L6 — these cause the flowchart to misrepresent actual system behavior
2. **HIGH** (completeness): M1, M5, M6, M8, M9, M16, L2, L3, B5, B10, I1, I6, R1
3. **MEDIUM** (accuracy/detail): M2–M4, M7, M10, M13, M14, B1, B2, B4, B6, B7, B9, I2, I4
4. **LOW** (polish): M11, M12, M15, L7, L8, I5, R2–R4