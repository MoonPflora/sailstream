# SailStream — Session Hyper-Compact (7/30)

## Build Status: ✅ `go build ./...` PASS

## Order Flow Bugs Fixed (in `internal/nnlp/processor.go`)

### Fix 1: `isSimpleAcknowledgement` now rejects order text
- **Root cause**: `"yes, i want 2 please"` matched `HasPrefix("yes ")` → short-circuited to "You're welcome!"
- **Fix**: Added order keyword blacklist (want/buy/order/purchase etc) + digit detection; if present → return `false`
- **Also**: Removed `product_price_query`, `product_availability` from `isAckAppropriateState()` so those intents don't ack

### Fix 2: `handlePreviousIntent` now handles product states
- Added case for `lastIntent IN (product_availability, product_price_query, product_confirmation)`
- User says "yes" → `isOrderIntent` → `createOrderIntentTicket` (sends `send_order_template`)
- User says "no" → `isRejectionResponse` → rejection message
- Otherwise affirmative → also forwards to order template

### Fix 3: `resolveProduct` already falls back to `last_product_sku` (line 1285)
- If text search + image search return nil, it checks userData["last_product_sku"] → `getProductBySKU`
- **Bug**: On "yes, i want 2", `isOrderIntent(text)` returns true → `intentNeedsProduct = true` → but `resolveProduct` may return nil if text has no product name → `createProductRequestTicket` fires asking "what product?"
- **Remaining issue**: `processNotificationInternal` needs product fallback BEFORE the `intentNeedsProduct && product == nil` check

## Remaining Work (for next session)

```
1. In processNotificationInternal: when product == nil AND intentNeedsProduct AND userData.last_product_sku != "" 
   → fallback to getProductBySKU before asking "what product?" (not createProductRequestTicket)
2. Add awaiting_order_details state to DB schema CHECK (already added to schema.sql)
3. Implement handlePreviousIntent case for "awaiting_order_details" → extract name/address/phone  
4. Implement handlePreviousIntent case for "awaiting_confirmation" → create order in DB
5. Ensure new product query overrides old last_product_sku (reset context)
```

## Order Flow Wanted (user confirmed)

1. User: "do you have wick for turo" → system sends product info → `last_intent=product_availability`
2. User: "yes i want 2" → system extracts quantity, sends order template asking name/address/phone
3. User sends details → system captures, sends summary + "confirm to complete?"
4. User: "confirm" → system creates order + order_items in DB, sends tracking
5. User asks about new product → system resets old context, searches fresh