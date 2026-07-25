issue one : even when product is found compiler dioesnt send the correct stuff it falls back :
Hide JSON
{
  "platform": "telegram",
  "sender": "zuber",
  "subtype": "sandbox",
  "text": "Do you have wick",
  "timestamp": "2026-07-22T22:10:36.354183Z"
}
Hide JSON
{
  "action": "send_product",
  "intent": "product_availability",
  "products": null,
  "raw_text": "Do you have wick",
  "source": null,
  "ticket_id": "send_product-sandbox-1784758236354183000-1784758241410128200",
  "user_data": {
    "conversation_state": "browsing",
    "is_blocked": false,
    "last_intent": "product_confirmation",
    "last_product_sku": "12da",
    "pending_data": "",
    "total_orders": 0,
    "total_spent": 0
  }
}
Hide JSON
{
  "action": "send_fallback",
  "intent": "product_availability",
  "ticket": "send_product-sandbox-1784758236354183000-1784758241410128200"
}
Hide JSON
{
  "notification": {
    "id": "sandbox-1784758236354183000",
    "platform_id": "telegram",
    "subtype_id": "sandbox",
    "account_id": "sandbox-account",
    "type": "message",
    "timestamp": "2026-07-22T15:10:36.354183-07:00",
    "urgent": false,
    "raw_data": {
      "sandbox": true,
      "user_data": {
        "conversation_state": "browsing",
        "is_blocked": false,
        "last_intent": "product_confirmation",
        "last_product_sku": "12da",
        "pending_data": "",
        "total_orders": 0,
        "total_spent": 0
      }
    },
    "message": {
      "conversation_id": "zuber",
      "is_group": false,
      "message_id": "sandbox-msg-1784758236354183000",
      "sender": {
        "user_id": "zuber",
        "username": "",
        "display_name": "",
        "profile_url": "",
        "avatar_url": "",
        "is_verified": false,
        "is_business": false
      },
      "recipients": null,
      "text": "Do you have wick",
      "timestamp": "2026-07-22T15:10:36.354183-07:00",
      "is_read": false,
      "is_edited": false,
      "is_forwarded": false,
      "delivery_status": "delivered",
      "media_attached": null,
      "platform_data": null
    },
    "collected_at": "2026-07-22T15:10:36.354183-07:00",
    "processed": false
  },
  "notification_id": "sandbox-1784758236354183000",
  "notification_type": "message",
  "platform": "telegram",
  "product": {
    "_search_score": 3,
    "category": "Kerosene heater",
    "created_at": "2026-07-21T20:43:46Z",
    "currency": "IQD",
    "description": "a fiber glass wick for the turbo model kerosene heater ",
    "dimensions": "13x12x14",
    "id": "310a533679bb9d7a8f1380294350969b",
    "image_url": "C:\\Users\\Thukunna\\domain\\SailStream\\media\\product_images\\310a533679bb9d7a8f1380294350969b\\1_b82870e8.png",
    "is_active": true,
    "is_featured": false,
    "low_stock_threshold": 10,
    "metadata": null,
    "name": "Wick",
    "price": 2000,
    "price_per_pack": 12000,
    "quantity_per_pack": 10,
    "reserved_stock": 0,
    "sku": "12da",
    "stock": 40,
    "subcategory": "Turbo wick",
    "tags": null,
    "thumbnail_url": "C:\\Users\\Thukunna\\domain\\SailStream\\media\\product_images\\310a533679bb9d7a8f1380294350969b\\1_b82870e8.png",
    "updated_at": "2026-07-21T20:43:46Z",
    "weight_kg": 0.2
  },
  "raw_text": "Do you have wick",
  "skip_image_delete": false,
  "store_info": {
    "address": "suli",
    "business_hours": {},
    "contact": {
      "email": "fluctuations.me@gmail.com",
      "phone": "07700398258"
    },
    "name": "Kamran"
  },
  "user_data": {
    "conversation_state": "browsing",
    "is_blocked": false,
    "last_intent": "product_confirmation",
    "last_product_sku": "12da",
    "pending_data": "",
    "total_orders": 0,
    "total_spent": 0
  },
  "user_id": "zuber"
}

what was sent:
-> send_message: "I'm not sure I understood. For better help, call 07700398258 or ask about products/orders."