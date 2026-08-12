<div align="center"><img src="./assets/logo.png" alt="Sailstream logo" width="220"/>
Sailstream
A self-hosted, multi-platform social automation engine — listens on WhatsApp, Facebook, Telegram, Twitter/X, and Viber, figures out what customers actually want using a rule-based NLU pipeline (with an LLM safety net), then replies, takes orders, and posts products on your behalf.

</div>
The point
I built this because dealing with official platform APIs is a pain in the ass. They're expensive, rate-limited to death, require endless paperwork, and half the platforms don't even offer one for small businesses.

Sailstream is a power tool for devs who just want to automate their storefront without jumping through hoops. It uses your own logged-in accounts—no business approval, no monthly API fees, no begging for access. Just you, your credentials, and a local Go server that does the heavy lifting.

It's not a hosted service. It's not a SaaS. It's a self-contained engine you run on your own machine, for your own accounts, on your own terms. You want to tweak the rules? Go dig into processor.go. You want to add a platform? Implement the listener interface. You want to use it for freelance client work? Go ahead—it's AGPL, not proprietary.

So, what does it actually do?
Listens to DMs and comments across platforms—some via native protocols, some by puppeting a real logged-in browser session (your own account).

Understands the message using a hand-written rules engine (English/Arabic/Kurdish) that handles product lookups, price/stock checks, multi-turn ordering (quantity → city → cost → confirm), order status, and complaints—it only calls an LLM as a last resort.

Compiles that understanding into a concrete list of platform-specific actions (reply, quote, block, etc.) while respecting per-platform rate limits and an independent LLM token budget.

Delivers those actions back through the same listener that caught the message, with retries and a fallback path if a step fails.

Posts on a fixed schedule or randomly, optionally with AI-generated captions.

Includes a local debug UI where you can inject a fake message and watch it flow through the whole pipeline without touching a live account.

A quick warning: this uses real consumer accounts (your WhatsApp number, your Facebook login, etc.), not official business APIs. That means it lives in a grey area. Read the disclaimer at the bottom before you point it at anything you actually care about.

How it's put together (the startup flow)
Let me walk you through the bootstrap first, because it matters:

main.go is the entry point—it checks your Go/Python/browser dependencies, creates the necessary cache and data directories if they don't exist, loads (or wizard-generates) config.json, and runs the SQLite migrations from schema.sql.

The environment package kicks in next—detects your OS, locates a Chromium-based browser (Edge/Chrome/Brave), finds your real browser profile directory, and resolves the Python interpreter for the image-recognition scripts.

Only once all that boring setup is green does main.go hand everything off to Maestro—the orchestrator that actually runs the show.

Once Maestro is alive, the message flow looks like this:

text
platform event → Listener → Maestro (debounce/rate-limit) 
              → NLU Processor (rules engine) 
              → Compiler (platform actions) 
              → Listener (delivery) → platform
The key components, stripped down:

Maestro – owns all the background workers, routes every message, handles errors, restarts crashed listeners, and provides a control API that hot-reloads config.

Config – credentials and toggles live in config.json. Schedules, rate limits, and shipping costs live in the database so you can tweak them without restarting.

Database – SQLite with WAL mode. Triggers keep stock, order totals, and product image counts consistent automatically.

Session Manager – stores persistent sessions. Each session is unique per platform + subtype (e.g., facebook_messenger vs facebook_page each get separate auth). Duplicates your real browser profile for Facebook, handles WhatsApp QR, and manages Telegram OTP via a local HTTP endpoint.

NLU Processor – the brains. Written entirely in Go. No external NLP services for the core logic.

Compiler – turns the processor's decision into actionable steps.

Listeners – one per platform, all implementing the same interface. Every listener runs concurrently—each platform gets its own goroutine, so WhatsApp doesn't block Facebook.

Sandbox – a self-contained web UI that records every pipeline stage so you can debug without spamming real users.

The NLU layer (how it actually understands people)
This is where 80% of my dev time goes. Every incoming message runs through a first-match-wins decision tree:

Gating – quiet hours? blocked user? empty message? Drop it.

Mid-flow continuation – if the user was in the middle of an order, that takes priority. Handles quantity extraction, city lookups, merging partial details, cancellations, and side-questions without losing state.

Simple stuff – greetings, "thanks", store-info queries get canned acks.

Order status – "where's my order?" gets a lookup.

Escalations – complaints, returns, payment issues get handed to the LLM or logged for a human.

Product resolution – finds the product via SKU regex, text search (multi-language), image recognition (CNN), or last discussed product.

Intent matching – once a product is found, checks if the user wants price, availability, compatibility, or to order.

LLM fallback – if nothing matched and an AI provider is configured, hands off with full context.

Final fallback – a polite "I didn't get that" with a flag to prevent double-answering the next message.

All keyword matching is whole-word, trilingual (English/Arabic/Kurdish). Messages are debounced (5s window) so fast typers don't trigger multiple runs.

Current focus: expanding the NLU coverage—more natural language patterns, better multi-turn handling, tighter order/product integration. Push more conversations through deterministic rules before they ever hit the LLM.

Compiler actions (what it actually does)
The compiler turns processor decisions into these instruction types:

Action	What it does
block	Blocks the user on-platform.
ai_response	Calls the LLM for a contextual reply.
pack_price / send_order_template	Handles multi-turn order flow—prompts, confirms, or finalizes.
send_product / send_compatibility / send_order_status	Info replies.
ask_product_name / ask_clarify	Asks the user for more info.
send_greeting / send_fallback	Canned responses.
auto_heart / react	Passive engagement.
Order creation runs as a single DB transaction—checks stock, reserves it, inserts the order as confirmed (triggers stock decrement), and logs shipping details. No half-baked orders.

Platform listeners (the messy part)
Platform	Mechanism	Status
WhatsApp	Native protocol (whatsmeow)	✅ Working
Facebook	Browser automation (chromedp)	✅ Working
Telegram	Native MTProto (gogram)	✅ Working
Viber	Bot API	✅ Working
Twitter/X	Session-based	🚧 In progress
Instagram	Browser automation (planned)	🚧 In progress
Every listener runs in its own goroutine—fully concurrent. If WhatsApp is slow delivering a message, Facebook keeps processing incoming DMs without waiting.

The sandbox (debug without fear)
Run the app with the sandbox listener registered, open http://localhost:8086/debug, and you get a live pipeline visualizer. Type a message, pick a platform, hit send—and watch each stage expand with raw JSON. Doesn't touch a live platform. Great for tweaking rules without annoying real customers.

Getting started (rough guide)
Prereqs: Go (recent), Python 3 (only for image recognition), and a Chromium-based browser logged into any account you want to automate.

Clone & run go run main.go. On first run, it generates config.json and launches a setup wizard.

Fill in your store info, AI provider (optional), and platform credentials.

First login – Facebook opens a browser. Telegram prompts for an OTP code. WhatsApp shows a QR code.

Test first – use the sandbox UI before pointing it at real accounts.

What I'm working on right now
Enhancing the NLU – more rules, better multi-turn handling, cleaner intent extraction. This is priority one.

Finishing Twitter/X and Instagram listeners – both partially implemented, need real-world testing and polish.

A proper UI – the sandbox is functional but ugly. I want a clean dashboard for orders, messages, and platform status at a glance.

Who this is for
Solo devs, freelancers, store owners, and tinkerers who want a self-hosted automation toolkit without the corporate API BS.

If you're a company looking to white-label this and sell it as a closed-source SaaS, AGPL says no. If you want to use it internally to run your own operations, go ahead. If you want to fork it, improve it, and share those improvements back, even better.

⚠️ Disclaimer
This automates real consumer accounts via unofficial channels because official business APIs are either too expensive or non-existent for small stores.

Automating a personal or business account this way is very likely against the platform's Terms of Service. I provide this "as is", with no guarantee it'll keep working as platforms change. You accept full responsibility for account bans, data loss, financial losses, or legal consequences. If you don't accept that, don't run it.
