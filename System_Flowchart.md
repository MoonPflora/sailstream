# SailStream — Complete System Flowchart (English)

```mermaid
flowchart TD
    %% ───── STYLES ─────
    classDef platform fill:#1a237e,color:#fff,stroke:#0d47a1,stroke-width:2px
    classDef listener fill:#004d40,color:#fff,stroke:#00695c
    classDef nlp fill:#4a148c,color:#fff,stroke:#6a1b9a
    classDef core fill:#b71c1c,color:#fff,stroke:#d32f2f
    classDef storage fill:#e65100,color:#fff,stroke:#ef6c00
    classDef sandbox fill:#33691e,color:#fff,stroke:#558b2f
    classDef llm fill:#880e4f,color:#fff,stroke:#ad1457
    classDef ui fill:#0d47a1,color:#fff,stroke:#1565c0
    classDef config fill:#4e342e,color:#fff,stroke:#5d4037
    classDef cache fill:#37474f,color:#fff,stroke:#455a64
    classDef decision fill:#e65100,color:#fff,stroke:#ef6c00,stroke-dasharray:4

    %% ═══════════════════════════════════════════════
    %%  1.  EXTERNAL INGRESS  (6 platform endpoints)
    %% ═══════════════════════════════════════════════
    subgraph EXTERNAL["🌐 External Endpoints"]
        FB_API["Facebook Graph API"]
        TG_API["Telegram Bot API"]
        WA_API["WhatsApp Cloud API"]
        IG_API["Instagram Graph API"]
        TW_API["Twitter v2 API"]
        VB_API["Viber REST API"]
    end

    subgraph LISTENERS["📥 Listeners — Notification IN"]
        L_FB["facebook.go\nWebhook → parse\nverify hub.challenge"]
        L_TG["telegram.go\nPolling loop\nUpdate handler"]
        L_WA["whatsapp.go\nWebhook payload\nparseMessage"]
        L_IG["instagram.go\nWebhook handler"]
        L_TW["twitter.go\nAccount activity\nDM webhook"]
        L_VB["viber.go\nWebhook handler"]
    end

    subgraph SHARED_ROUTER["🔄 Shared Router (types.go)"]
        INGRESS["IngressRouter"]
        VERIFY["Verify signature / source"]
        RL_INGRESS["Rate limit (per channel)\ntoken bucket / sliding window"]
        CLASSIFY_TYPE["Classify notification type"]
        FALLBACK_TYPE["Unsupported type → noop + log"]
    end

    %% connections: external → listener
    FB_API --> L_FB; TG_API --> L_TG; WA_API --> L_WA
    IG_API --> L_IG; TW_API --> L_TW; VB_API --> L_VB

    L_FB & L_TG & L_WA & L_IG & L_TW & L_VB --> INGRESS

    INGRESS --> VERIFY
    VERIFY -->|invalid| REJECT_403["🔐 403 Forbidden\nlog security event"]
    VERIFY -->|valid| RL_INGRESS
    RL_INGRESS -->|exceeded| REJECT_429["🚦 429 Rate Limited\nnotify user + retry-after"]
    RL_INGRESS -->|ok| CLASSIFY_TYPE

    REJECT_403 --> FB_API & TG_API & WA_API & IG_API & TW_API & VB_API
    REJECT_429 --> FB_API & TG_API & WA_API & IG_API & TW_API & VB_API

    CLASSIFY_TYPE -->|Message / Comment| PROCESSOR_ENTRY
    CLASSIFY_TYPE -->|unsupported| FALLBACK_TYPE
    FALLBACK_TYPE --> LOG["📋 Log to file + audit"]

    %% ═══════════════════════════════════════════════════════
    %%  2.  PROCESSOR DECISION TREE  (Processor.go)
    %% ═══════════════════════════════════════════════════════
    subgraph PROCESSOR["🧠 NLP Processor — processNotificationInternal()"]
        P_ENTRY["processNotificationInternal()
        notification, ctx"]

        %% ─── Level 1: type guard ───
        P_TYPE_GUARD{"notification.Type
        is Message or Comment?"}
        P_TYPE_GUARD -->|yes| P_SKIP_NEXT
        P_TYPE_GUARD -->|no → noop + log| P_NOOP_UNSUPPORTED["createResult()
        action=noop
        intent=unsupported_type"]

        %% ─── Level 2: skipNext ───
        P_SKIP_NEXT{"skipNext[userID] == true?
        (skip after fallback)"}
        P_SKIP_NEXT -->|yes, delete flag| P_SKIP_RESULT["createResult()
        action=noop
        skipped_after_fallback"]
        P_SKIP_NEXT -->|no| P_QUIET_HOURS

        %% ─── Level 3: quiet hours ───
        P_QUIET_HOURS{"isInQuietHours() → true?"}
        P_QUIET_HOURS -->|yes| P_BLOCK_QUIET["createResultNoImageDelete()
        action=block | intent=quiet_hours
        → route BLOCK"]
        P_QUIET_HOURS -->|no| P_BLOCKED_USER

        %% ─── Level 4: blocked user ───
        P_BLOCKED_USER{"isBlockedUser() → true?"}
        P_BLOCKED_USER -->|yes| P_BLOCK_USER["createResultNoImageDelete()
        action=block | intent=blocked_user
        stats.blocked++ → route BLOCK"]
        P_BLOCKED_USER -->|no| P_AUTO_HEART

        %% ─── Level 5a: auto-heart (comments only) ───
        P_AUTO_HEART{"Type == Comment &&
        shouldAutoHeart() → true?"}
        P_AUTO_HEART -->|yes| P_AUTO_HEART_RES["createResult()
        action=auto_heart
        intent=auto_engagement
        stats.autoHearts++
        → route AUTO_HEART"]
        P_AUTO_HEART -->|no| P_SHOULD_AUTO_REPLY

        %% ─── Level 5b: auto-reply enabled? ───
        P_SHOULD_AUTO_REPLY{"shouldAutoReply() → true?"}
        P_SHOULD_AUTO_REPLY -->|no| P_NOOP_AUTO_DISABLED["createResult()
        action=noop | auto_reply_disabled"]
        P_SHOULD_AUTO_REPLY -->|yes| P_CONTENT_CHECK

        %% ─── Level 6: empty content ───
        P_CONTENT_CHECK{"text == '' && !hasImages?"}
        P_CONTENT_CHECK -->|yes empty| P_NOOP_EMPTY["createResult()
        action=noop | empty_content"]
        P_CONTENT_CHECK -->|no has content| P_PREV_INTENT

        %% ─── Level 7: previous intent handler ───
        P_PREV_INTENT{"handlePreviousIntent()
        → returns result?"}
        P_PREV_INTENT -->|yes pending| P_PREV_RES["use previous intent result
        (route to specific compiler)"]
        P_PREV_INTENT -->|no| P_SIMPLE_ACK

        %% ─── Level 8: simple acknowledgement ───
        P_SIMPLE_ACK{"isSimpleAcknowledgement() → true?"}
        P_SIMPLE_ACK -->|yes| P_ACK_RES["createSimpleAck()
        action=ai_response
        message=template.ack"]
        P_SIMPLE_ACK -->|no| P_GREETING

        %% ─── Level 9: pure greeting ───
        P_GREETING{"isPureGreeting() → true?"}
        P_GREETING -->|yes| P_GREETING_RES["createGreetingTicket()
        → route GREETING"]
        P_GREETING -->|no| P_STORE_INFO

        %% ─── Level 10: store info query ───
        P_STORE_INFO{"isStoreInfoQuery() → true?"}
        P_STORE_INFO -->|yes| P_STORE_RES["createStoreInfoTicket()
        → route STORE_INFO"]
        P_STORE_INFO -->|no| P_CANCEL

        %% ─── Level 11: cancellation intent ───
        P_CANCEL{"isCancellationIntent() → true?"}
        P_CANCEL -->|yes| P_CANCEL_RES["createResult()
        action=send_cancellation"]
        P_CANCEL -->|no| P_ESCALATE

        %% ─── Level 12: escalation intent ───
        P_ESCALATE{"classifyEscalationIntent()
        → matched?"}
        P_ESCALATE -->|yes| P_ESC_RES["createIntentEscalationTicket()
        → route AI_TICKET with intent"]
        P_ESCALATE -->|no| P_PRODUCT

        %% ─── Level 13: product resolution ───
        P_PRODUCT["resolveProduct()
        → product, source, needsClarification, matches"]
        P_PRODUCT -->|needsClarification==true| P_CLARIFY["createClarificationTicket()
        → route CLARIFICATION"]
        P_PRODUCT -->|product==nil & image_no_match| P_ASK_PRODUCT["askForProductName()
        → route ASK_PRODUCT"]
        P_PRODUCT -->|product==nil & text != '' & isPriceHaggle| P_HAGGLE["createPriceHaggleTicket()
        → route AI_RESPONSE (haggle_reject)"]
        P_PRODUCT -->|product != nil found| P_ORDER_INTENT
        P_PRODUCT -->|nothing matched| P_AI_DEFAULT

        %% ─── Level 14a: make order intent ───
        P_ORDER_INTENT{"isMakeOrderIntent() → true?"}
        P_ORDER_INTENT -->|yes| P_MAKE_ORDER["isMakeOrderIntent()
        → intent=make_order"]
        P_ORDER_INTENT -->|no| P_ORDER_FOLLOWUP{"isMakeOrderFollowUp() → true?"}
        P_ORDER_FOLLOWUP -->|yes| P_ORDER_PROMPT["→ route ORDER_CONFIRMATION"]
        P_ORDER_FOLLOWUP -->|no| P_AI_DEFAULT

        %% ─── Level 14b: product → pricing variant ───
        P_MAKE_ORDER --> P_PRICE_QUERY{"isPriceQuery() → true?"}
        P_PRICE_QUERY -->|yes| P_PRICE_RESP["→ route SEND_PRODUCT (price)"]
        P_PRICE_QUERY -->|no| P_PACK{"→ pack variant?"}
        P_PACK -->|yes pack| P_ORDER["→ route ORDER_TEMPLATE / PACK_ORDER"]
        P_PACK -->|no simple| P_ORDER_SIMPLE["→ route ORDER_TEMPLATE"]

        %% ─── Level 15: AI default ticket ───
        P_AI_DEFAULT["createAITicket()
        → builds AITaskTicket
          includes: language, vision, productContext
          sessionContext with last_intent, orders"]
        P_AI_DEFAULT -->|fallback if no API key| P_FALLBACK_AI["createFallbackTicket()
          → route FALLBACK + set skipNext[userID]=true"]

        %% image processing sub-pipeline (called from createAITicket)
        P_AI_DEFAULT --> P_IMAGE_PIPE["processImages()
          → classify images via CNN wrapper
          → match products by image"]
        P_IMAGE_PIPE -->|product matched| P_IMAGE_RES["add matchedProductIDs
          to visionContext"]
        P_IMAGE_PIPE -->|no match| P_IMAGE_NONE["no product context"]
    end

    %% Connect CLASSIFY_TYPE to Processor entry
    CLASSIFY_TYPE -->|Message / Comment| P_ENTRY

    %% ────────────────────────────────────────────────────────────
    %%  3.  PROCESSOR OUTPUT ROUTER  (action dispatch)
    %% ────────────────────────────────────────────────────────────
    subgraph PROC_OUTPUT["Processor → Action Router"]
        direction TB
        ROUTE_NOOP["noop / queued / skipped_after_fallback / auto_reply_disabled / empty_content → Compiler.compileNoop"]
        ROUTE_BLOCK["block / quiet_hours / blocked_user → Compiler.compileBlock"]
        ROUTE_UNFOLLOW["→ Compiler.compileUnfollow"]
        ROUTE_AUTO_HEART["auto_heart / react / like → Compiler.compileAutoHeart"]
        ROUTE_GREETING["send_greeting → Compiler.compileGreeting"]
        ROUTE_STORE_INFO["send_store_info → Compiler.compileStoreInfo"]
        ROUTE_SEND_PRODUCT["send_product → Compiler.compileSendProduct"]
        ROUTE_ORDER["send_order_template → Compiler.compileOrderTemplate"]
        ROUTE_PACK_ORDER["pack_price → Compiler.compilePackOrder"]
        ROUTE_STOCK_WARN["send_stock_warning → Compiler.compileStockWarning"]
        ROUTE_CANCEL["send_cancellation → Compiler.compileCancellation"]
        ROUTE_FALLBACK["send_fallback → Compiler.compileFallback"]
        ROUTE_ASK_PRODUCT["ask_product → Compiler.compileAskProduct"]
        ROUTE_ASK_ORDER["ask_for_order → Compiler.compileAskForOrder"]
        ROUTE_ORDER_CONFIRM["ask_order_confirmation → Compiler.compileAskOrderConfirmation"]
        ROUTE_AI_TICKET["ai_ticket → Compiler.compileAITicket"]
        ROUTE_AI_RESPONSE["ai_response → Compiler.compileAIResponse"]
        ROUTE_SHARE["share → Compiler.compileShare"]
        ROUTE_SAVE["save → Compiler.compileSave"]
        ROUTE_FOLLOW["follow → Compiler.compileFollow"]
        ROUTE_CLARIFY["ask_product_name → Compiler.compileAskProduct"]
    end

    %% map processor results to routes
    P_NOOP_UNSUPPORTED --> ROUTE_NOOP
    P_SKIP_RESULT --> ROUTE_NOOP
    P_BLOCK_QUIET --> ROUTE_BLOCK
    P_BLOCK_USER --> ROUTE_BLOCK
    P_AUTO_HEART_RES --> ROUTE_AUTO_HEART
    P_NOOP_AUTO_DISABLED --> ROUTE_NOOP
    P_NOOP_EMPTY --> ROUTE_NOOP
    P_PREV_RES --> ROUTE_AI_RESPONSE
    P_ACK_RES --> ROUTE_AI_RESPONSE
    P_GREETING_RES --> ROUTE_GREETING
    P_STORE_RES --> ROUTE_STORE_INFO
    P_CANCEL_RES --> ROUTE_CANCEL
    P_ESC_RES --> ROUTE_AI_TICKET
    P_CLARIFY --> ROUTE_CLARIFY
    P_ASK_PRODUCT --> ROUTE_ASK_PRODUCT
    P_HAGGLE --> ROUTE_AI_RESPONSE
    P_PRICE_RESP --> ROUTE_SEND_PRODUCT
    P_ORDER --> ROUTE_ORDER
    P_ORDER_SIMPLE --> ROUTE_ORDER
    P_AI_DEFAULT --> ROUTE_AI_TICKET
    P_FALLBACK_AI --> ROUTE_FALLBACK

    %% ═════════════════════════════════════════════════════════
    %%  4.  COMPILER DECISION TREE  (Compiler.go)
    %% ═════════════════════════════════════════════════════════
    subgraph COMPILER_TREE["⚙️ Task Compiler — compileAction() dispatcher"]
        C_ENTRY["compileAction()
        ticket.Action dispatcher"]

        C_ENTRY --> C_SWITCH{"action switch"}

        %% Block
        C_SWITCH -->|block| C_BLOCK{"compileBlock:
        already blocked?"}
        C_BLOCK -->|yes| C_BLOCK_NOOP["→ compileNoop"]
        C_BLOCK -->|no| C_BLOCK_INSTR["Build instruction:
        action=block, priority=10
        maxRetries=2, timeout=20s"]

        %% Unfollow
        C_SWITCH -->|unfollow| C_UNFOLLOW["compileUnfollow:
        priority=5, maxRetries=2, timeout=20s"]

        %% Noop / Queued
        C_SWITCH -->|noop| C_NOOP["compileNoop:
        StepType=Log"]
        C_SWITCH -->|queued| C_QUEUED["compileQueued:
        StepType=Log (queued)"]

        %% Auto-heart
        C_SWITCH -->|auto_heart / react / like| C_AUTOHEART["compileAutoHeart:
        emoji, from_me=true
        priority=3"]

        %% Greeting
        C_SWITCH -->|send_greeting| C_GREETING["compileGreeting:
        detect language → template en/ar/ku
        priority=4"]

        %% Store info
        C_SWITCH -->|send_store_info| C_STOREINFO["compileStoreInfo:
        template with name, contact,
        address, hours
        priority=5"]

        %% Send product
        C_SWITCH -->|send_product| C_SENDPROD{"compileSendProduct:
        productData exists?"}
        C_SENDPROD -->|yes| C_SENDPROD_INSTR["Build product card:
        name, price, currency, desc,
        stock, sku, recordProductView()
        priority=7"]
        C_SENDPROD -->|no product data| C_SENDPROD_FALLBACK["→ compileFallback"]

        %% Order template
        C_SWITCH -->|send_order_template| C_ORDERTMPL["compileOrderTemplate:
        → createConfirmedOrder()
        → stock check → reserve → commit"]

        %% Pack order
        C_SWITCH -->|pack_price| C_PACK["compilePackOrder:
        → createConfirmedOrder(pack variant)
        → per-pack pricing, lineQty in packs"]

        %% Stock warning
        C_SWITCH -->|send_stock_warning| C_STOCKWARN["compileStockWarning:
        → low stock threshold check"]

        %% Cancellation
        C_SWITCH -->|send_cancellation| C_CANCEL["compileCancellation:
        → restore stock on cancel
        → DB rollback if fail"]

        %% Fallback
        C_SWITCH -->|send_fallback| C_FALLBACK["compileFallback:
        set skipNext[userID]=true
        → generic apology + log"]

        %% Ask product
        C_SWITCH -->|ask_product| C_ASKPROD["compileAskProduct:
        → prompt for name/image"]

        %% Ask for order
        C_SWITCH -->|ask_for_order| C_ASKORDER["compileAskForOrder"]

        %% Ask order confirmation
        C_SWITCH -->|ask_order_confirmation| C_ASKCONFIRM["compileAskOrderConfirmation"]

        %% AI ticket (LLM call)
        C_SWITCH -->|ai_ticket| C_AITICKET["compileAITicket:
        → calls LLM for reply
        → LLM prompt includes:
          system + conversation history
          product context + vision data
        → parse LLM response JSON
        → rateLimit check before call"]

        C_AITICKET --> C_LLM_CALL["LLM call (llm.go):
        retries=3, timeout=30s
        parse intent + payload"]
        C_LLM_CALL -->|success → extract| C_AI_PARSE["Parse reply:
        text, emoji, buttons,
        quick replies"]
        C_LLM_CALL -->|fail x3 → AMBIGUOUS| C_AI_FAIL["→ compileFallback
        set skipNext[userID]=true"]

        %% AI response
        C_SWITCH -->|ai_response| C_AIRESPONSE["compileAIResponse:
        uses pre-generated AI text
        build steps → send"]

        %% Share / Save / Follow
        C_SWITCH -->|share| C_SHARE["compileShare:
        share post/card"]
        C_SWITCH -->|save| C_SAVE["compileSave:
        save post URL to DB"]
        C_SWITCH -->|follow| C_FOLLOW["compileFollow:
        follow user"]

        %% Default unknown
        C_SWITCH -->|default| C_UNKNOWN["return error:
        unknown action: %s"]
    end

    %% ─────────────────────────────────────────────────────────
    %%  5.  QUEUE & RATE LIMITS  (Compiler)
    %% ─────────────────────────────────────────────────────────
    subgraph QUEUE["📊 Queue & Rate Limits"]
        Q_ENTRY["InstructionQueue
        maxSize=1000"]
        Q_ENQUEUE["→ enqueue (priority sort)"]
        Q_DEQUEUE["→ dequeue goroutine"]
        Q_PROCESS["processQueuedInstruction()"]
        Q_RETRY{"attempts >= maxAttempts?"}
        Q_RETRY -->|no → retry| Q_ENQUEUE
        Q_RETRY -->|yes| Q_DEAD["emitError → dead letter
        severity=critical"]

        RL_CHECK{"RateLimiter.CanProceed
        (platform, subtypeID, action)"}
        RL_CHECK -->|allowed| RL_OK["→ proceed + RecordUsage"]
        RL_CHECK -->|denied duration| RL_WAIT["→ retry after cooldown
        emitError retryable=true"]
    end

    %% Connect compiler instructions to queue/rate-limit
    C_BLOCK_NOOP --> Q_ENQUEUE
    C_BLOCK_INSTR --> RL_CHECK
    C_UNFOLLOW --> RL_CHECK
    C_NOOP --> Q_ENQUEUE
    C_QUEUED --> Q_ENQUEUE
    C_AUTOHEART --> RL_CHECK
    C_GREETING --> RL_CHECK
    C_STOREINFO --> RL_CHECK
    C_SENDPROD_INSTR --> RL_CHECK
    C_SENDPROD_FALLBACK --> RL_CHECK
    C_ORDERTMPL --> RL_CHECK
    C_PACK --> RL_CHECK
    C_STOCKWARN --> RL_CHECK
    C_CANCEL --> RL_CHECK
    C_FALLBACK --> RL_CHECK
    C_ASKPROD --> RL_CHECK
    C_ASKORDER --> RL_CHECK
    C_ASKCONFIRM --> RL_CHECK
    C_AITICKET --> RL_CHECK
    C_AI_PARSE --> RL_CHECK
    C_AIRESPONSE --> RL_CHECK
    C_SHARE --> RL_CHECK
    C_SAVE --> RL_CHECK
    C_FOLLOW --> RL_CHECK
    C_AI_FAIL --> Q_ENQUEUE

    RL_OK --> Q_ENQUEUE

    %% ═══════════════════════════════════════════════════════════
    %%  6.  STEP EXECUTION & PLATFORM DISPATCH
    %% ═══════════════════════════════════════════════════════════
    subgraph EXECUTION["📤 Step Execution → Notification OUT"]
        Q_PROCESS --> STEPS["steps() → per-platform
        whatsappSteps / browserSteps"]

        STEPS -->|whatsapp| WA_STEPS["whatsappSteps:
        action → react/block/unfollow/upload/sendMessage
        chat_jid, message_id
        rateLimitCheck(stepType)
        delayAfter=100-2000ms"]

        STEPS -->|browser| BR_STEPS["browserSteps:
        facebook/instagram → StepTypeReply
        default → StepTypeSendMessage
        options: conversationID/chat_id
        delayAfter=1000ms"]

        WA_STEPS -->|StepTypeSendMessage| WA_SEND["WhatsApp:
        waClient.SendMessage()
        Upload media → send"]
        WA_STEPS -->|StepTypeReact| WA_REACT["WhatsApp react:
        send reaction emoji"]
        WA_STEPS -->|StepTypeUpload| WA_UPLOAD["WhatsApp upload:
        Upload() → sendMessage"]
        WA_STEPS -->|StepTypeBlock| WA_BLOCK["WhatsApp block"]
        WA_STEPS -->|StepTypeLog| WA_LOG["WhatsApp log only"]

        BR_STEPS -->|StepTypeReply| FB_SEND["Facebook: reply to comment/msg"]
        BR_STEPS -->|StepTypeReply| IG_SEND["Instagram: reply"]
        BR_STEPS -->|StepTypeSendMessage| TG_SEND["Telegram/Viber: sendMessage"]
    end

    %% ─────────────────────────────────────────────────────────
    %%  7.  DATABASE WRITES & AUDIT
    %% ─────────────────────────────────────────────────────────
    subgraph STORAGE["💾 DB Writes & Audit"]
        DB_SESS["Sessions table
        → userID, platform, state, expiry"]
        DB_MSG["Messages table
        → messageID, platform, text, timestamp"]
        DB_USER["User profiles table
        → userID, displayName, isBlocked, lastIntent"]
        DB_RATE["Rate limits table
        → platform, subtype, action, hourCount, dayCount"]
        DB_AUDIT["Audit log table
        → ticketID, action, status, error, timestamps"]
        DB_ORDER["Orders / order_items
        → productID, qty, price, total, shipping"]
        DB_PRODUCT["Products table
        → stock, reserved_stock updates"]
        DB_CONV["Conversation history
        → userID, messages, intent"]
    end

    %% writes from compiler flow
    RL_OK --> DB_RATE
    C_BLOCK_INSTR --> DB_USER
    C_UNFOLLOW --> DB_USER
    C_AUTOHEART --> DB_AUDIT
    C_GREETING --> DB_SESS
    C_STOREINFO --> DB_AUDIT
    C_SENDPROD_INSTR --> DB_PRODUCT
    C_SENDPROD_INSTR --> DB_AUDIT
    C_ORDERTMPL --> DB_ORDER
    C_ORDERTMPL --> DB_PRODUCT
    C_PACK --> DB_ORDER
    C_PACK --> DB_PRODUCT
    C_STOCKWARN --> DB_PRODUCT
    C_CANCEL --> DB_ORDER
    C_CANCEL --> DB_PRODUCT
    C_FALLBACK --> DB_AUDIT
    C_AI_PARSE --> DB_CONV
    C_AIRESPONSE --> DB_CONV
    C_SHARE --> DB_AUDIT
    C_SAVE --> DB_AUDIT
    C_FOLLOW --> DB_USER
    Q_DEAD --> DB_AUDIT
    WA_SEND --> DB_MSG
    WA_REACT --> DB_AUDIT
    FB_SEND --> DB_MSG
    IG_SEND --> DB_MSG
    TG_SEND --> DB_MSG
    LOG --> DB_AUDIT
    REJECT_403 --> DB_AUDIT
    REJECT_429 --> DB_RATE
    REJECT_429 --> DB_AUDIT

    %% ═══════════════════════════════════════════════════════════
    %%  8.  LEGEND
    %% ═══════════════════════════════════════════════════════════
    subgraph LEGEND["📋 Legend"]
        L1["Platform / External"]:::platform
        L2["Listener / IN"]:::listener
        L3["NLP Processor / Decision"]:::nlp
        L4["Maestro / Compiler / Core"]:::core
        L5["Storage / DB"]:::storage
        L6["Rate Limit / Condition"]:::decision
        L7["LLM / Comms"]:::llm
    end

    %% ═══════════════════════════════════════════════════════════
    %%  9.  BACKGROUND INFRASTRUCTURE
    %% ═══════════════════════════════════════════════════════════
    subgraph INFRA["⚙️ Background Services"]
        BOOT["Main.go → Maestro_main.go
        - Load Config (Config.json + Loader.go)
        - Init DB (database.go)
        - Init Enviroment (enviroment.go)
        - Init Session Manager
        - Init Cache DB
        - Init Loggers
        - Spawn goroutines for each listener"]

        MAESTRO_ORCH["Maestro.go orchestration:
        - refreshPlatformStatuses()
        - GlobalRateLimits (hourly/daily)
        - CanProceed + RecordUsage
        - scheduled posting
        - error recovery
        - Dashboard API server"]

        SESSION_MGR["Session_manager.go:
        - load/create session
        - TTL expiry (5 min idle)
        - context carry-over
        - persist on update
        - panic rescue"]

        CNN_PY["CNN Python pipeline:
        - Cnn/main_trainer.py
        - Cnn/accuracy_booster.py
        - Cnn/multilingual_augmentation.py
        - Cnn/production_trainer.py
        Called via Wrapper.go → subprocess"]

        WRAPPER["Wrapper.go:
        - CNNWrapper (Train/Predict/ProductionTrain)
        - Multilingual augmentation
        - Accuracy booster
        - Model management"]
    end

    BOOT --> MAESTRO_ORCH
    MAESTRO_ORCH --> SESSION_MGR
    MAESTRO_ORCH --> WRAPPER
    WRAPPER --> CNN_PY
    SESSION_MGR --> DB_SESS
    SESSION_MGR --> DB_CONV
```

---

## Flowchart Coverage Summary

### Processor.go — Decision Tree (`processNotificationInternal`)
Every decision branch is modeled as diamond nodes:

| Level | Decision | Outcome route |
|-------|----------|---------------|
| 1 | Notification type is Message/Comment? | no → noop/unsupported |
| 2 | `skipNext[userID]` set? | yes → noop/skipped_after_fallback |
| 3 | `isInQuietHours()`? | yes → block/quiet_hours |
| 4 | `isBlockedUser()`? | yes → block/blocked_user |
| 5a | Comment + `shouldAutoHeart()`? | yes → auto_heart |
| 5b | `shouldAutoReply()` enabled? | no → noop |
| 6 | text empty + no images? | yes → noop/empty_content |
| 7 | `handlePreviousIntent()` → result? | yes → route to specific compiler |
| 8 | `isSimpleAcknowledgement()`? | yes → ai_response (template ack) |
| 9 | `isPureGreeting()`? | yes → greeting |
| 10 | `isStoreInfoQuery()`? | yes → store_info |
| 11 | `isCancellationIntent()`? | yes → cancellation |
| 12 | `classifyEscalationIntent()`? | yes → ai_ticket with intent |
| 13 | `resolveProduct()` → clarification? | yes → ask_product (clarification) |
| 13 | product=nil + image_no_match | → ask_for_product_name |
| 13 | `isPriceHaggle()` | → ai_response (haggle) |
| 14 | `isMakeOrderIntent()` → product matched | → order_template / pack_order |
| 15 | Default → `createAITicket()` | includes vision + product context |
| 15b | No API key? | → fallback + set skipNext[userID] |

### Compiler.go — Action Dispatcher (`compileAction`)
22 action cases mapped to specific compile methods, each with:
- **compileBlock**: check `already_blocked` → noop or block instruction
- **compileSendProduct**: check `productData` exists → product card or fallback
- **compileOrderTemplate/createConfirmedOrder**: stock check → reserve → commit, or `errInsufficientStock` → stock_warning
- **compileAITicket**: LLM call with retry (3x, 30s timeout) → parse or AMBIGUOUS→fallback
- **compileFallback**: sets `skipNext[userID]=true` for next message
- **All**: Rate-limit check via `RateLimiter.CanProceed` before execution
- **All**: Queue integration with priority sort, retry max 3, dead letter on exhaustion

### Rate Limits
- **Ingress**: per-channel token bucket (at listener level)
- **Egress**: `RateLimiter.CanProceed(platform, subtypeID, action)` at compiler level
- **Global**: hourly/daily post caps in `Maestro.GlobalRateLimits`
- **Steps**: per-step delay (100–2000ms) and rate-limit check type

### All DB Writes
Sessions, messages, user profiles, rate-limit counters, audit log, conversation history, orders/order_items, products (stock/reserved), error reports