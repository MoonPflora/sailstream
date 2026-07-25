Looking at the config structure, here are the variables that need to be set by the user (removing system-auto-detected and runtime-only variables):

## **User-Settable Variables:**

### **1. System Config:**
```json
{
  "system": {
    "language": "en",  //static for now
    "operating system":"Windows", //autodetection
    "arcitecture":"arm64", //auto detection
    "operation_mode": "scheduled_wake",  // User selects mode
    "wake_policy": {
      "interval_minutes": 5,             // User sets wake interval
      "idle_sleep_minutes": 30           // User sets idle sleep
    }
  }
}
```

### **2. AI Config:**
```json
{
  "ai": {
    "provider": "openai",               // User selects provider
    "model": "gpt-3.5-turbo",           // User selects model
    "api_key": "",                      // User enters API key
    "base_url": "https://api.openai.com/v1",  // User can change
    "generation": {
      "max_tokens": 1000,               // User sets max tokens
      "temperature": 0.7,               // User sets temperature
      "top_p": 1.0,                     // User sets top_p
      "presence_penalty": 0.0,          // User sets penalties
      "frequency_penalty": 0.0
    },
    "image_recognition": {
      "enabled": false,                 // User enables/disables
      "model_path": "./models/image_recognition",  // User sets path
      "confidence_threshold": 0.7,      // User sets threshold
      "max_image_size_px": 1024         // User sets max size
    },
    "instructions": {
      "system_prompt": "You are...",    // User writes prompt
      "post_instructions": "Create...", // User writes instructions
      "reply_instructions": "Respond...",
      "tone": "friendly",               // User selects tone
      "max_response_length": 500,       // User sets length
      "scheduled_post_instructions": "Create..."  // User writes
    }
  }
}
```

### **3. Store Config:**
```json
{
  "store": {
    "name": "My Store",                 // User enters store name
    "description": "Welcome...",        // User writes description
    "hello_message": "Hello!...",       // User writes greeting
    "address": "123 Main Street...",    // User enters address
    "contact": {
      "email": "contact@...",           // User enters email
      "phone": "+123..."                // User enters phone
    },
    "business_hours": {                 // User sets hours per day
      "monday": "09:00-18:00",
      "tuesday": "09:00-18:00",
      // ... all days
    },
    "currency": "USD"                   // User selects currency
  }
}
```

### **4. Platform Config (Per Platform):**
```json
{
  "platforms": {
    "[platform_name]": {
      "enabled": false,                 // User enables/disables
      "platform": {
        "subtype": "account"            // User selects: account/page/bot
      },
      "auth": {                         // User enters credentials
        "method": "oauth",
        "username": "",
        "password": "",
        "oauth": {
          "access_token": "",
          "client_id": "",
          "client_secret": "",
          "redirect_uri": "http://localhost:8080/callback"
        },
        "phone": {
          "number": "",
          "country_code": "+1"
        },
        "bot": {
          "bot_token": "",
          "bot_hash": "",
          "bot_username": "",
          "webhook_url": ""
        }
      },
      "[platform]_specific": {          // Platform-specific fields
        "account": {
          "email": "",
          "password": ""
        },
        "page": {
          "page_id": "",
          "access_token": ""
        },
        "groups": []                    // User adds group URLs
      },
      "automation": {                   // User sets automation rules
        "auto_reply": { "enabled": true },
        "auto_heart": { 
          "enabled": false,
          "delay_min": 5,
          "delay_max": 15,
          "max_per_day": 50
        },
        "auto_follow": { 
          "enabled": false,
          "max_per_day": 20,
          "target_hashtags": [],        // User enters hashtags
          "target_accounts": [],        // User enters accounts
          "unfollow_after": 7
        },
        "auto_repost": { 
          "enabled": false,
          "source_tags": [],           // User enters source tags
          "credit_source": true,
          "max_per_day": 3
        },
        "answer_dm": { "enabled": true },
        "answer_comments": { "enabled": true },
        "welcome_message": {
          "enabled": true,
          "text": "Thanks for..."      // User writes welcome message
        },
        "filters": {
          "ignore_words": [],          // User adds ignore words
          "block_keywords": [],        // User adds block keywords
          "allow_keywords": [],        // User adds allow keywords
          "min_word_count": 1,
          "reply_delay": 2,
          "language": "en",
          "min_char_count": 1
        }
      },
      "posting": {
        "random": {                    // User sets random posting
          "enabled": false,
          "use_global": true,
          "interval_hours": {
            "min": 2,                  // User sets min interval
            "max": 6                   // User sets max interval
          },
          "posts_per_cycle": 1         // User sets posts per cycle
        },
        "manual": { "enabled": true },
        "schedule_times": []           // User adds schedule times
      },
      "limits": {                      // User sets limits
        "daily_messages": 100,
        "daily_posts": 10,
        "daily_hearts": 100,
        "daily_follows": 20,
        "daily_comments": 50,
        "hourly_posts": 2
      },
      "metadata": {
        "notes": ""                    // User can add notes
      },
      "settings": {
        "post_hashtags": [],           // User adds hashtags
        "mention_accounts": [],        // User adds mentions
        "allowed_post_types": ["text", "image", "video"],
        "max_post_length": 5000,
        "max_image_size_mb": 5,
        "max_video_size_mb": 50,
        "allowed_media_types": ["jpg", "jpeg", "png", "gif", "mp4"]
      }
    }
  }
}
```

### **5. Global Posting Config:**
```json
{
  "posting": {
    "fallback": {
      "random": {
        "enabled": false,              // User enables/disables
        "interval_hours": {
          "min": 3,                    // User sets min
          "max": 8                     // User sets max
        },
        "posts_per_cycle": 2           // User sets count
      }
    }
  }
}
```

### **6. Paths Config:**
```json
{
  "paths": {
    "logs": "./logs",                  // User can customize paths
    "cache": "./cache",
    "media": "./media",
    "models": "./models",
    "temp": "./temp",
    "sessions": "./sessions",
    "database": "./data",
    "backup": "./backup"
  }
}
```

### **7. Content Pool:**
```json
{
  "content": {
    "posts": [                         // User adds/edit posts
      {
        "text": "Check out...",        // User writes post text
        "category": "promotion",       // User selects category
        "hashtags": ["#newcollection"], // User adds hashtags
        "platforms": ["facebook"],     // User selects platforms
        "weight": 100                  // User sets weight
      }
    ],
    "media": [                         // User adds media
      {
        "path": "./media/image.jpg",   // User sets path
        "description": "Product image" // User writes description
      }
    ],
    "hashtags": ["#mystore"],          // User adds global hashtags
    "categories": ["promotion"],       // User adds categories
    "rotation_mode": "random"          // User selects rotation mode
  }
}
```

### **8. Message Templates:**
```json
{
  "messages": {
    "greetings": ["Hello!"],           // User adds greetings
    "farewells": ["Goodbye!"],         // User adds farewells
    "questions": ["How can I help?"],  // User adds questions
    "answers": ["We have that!"],      // User adds answers
    "keywords": [                      // User adds keyword rules
      {
        "keyword": "price",
        "response": "You can check...",
        "response_type": "reply",
        "exact_match": false,
        "priority": 50
      }
    ],
    "fallback": ["I'm not sure..."]    // User adds fallback messages
  }
}
```

## **Variables REMOVED (System/Runtime Only):**

### **Auto-Detected by System:**
```json
{
  "meta": {
    "detected_os": "",          // Auto-detected
    "detected_arch": "",        // Auto-detected  
    "detected_environment": "", // Auto-detected
    "installed_at": "",         // Auto-set on first run
    "last_updated": ""          // Auto-updated on save
  }
}
```

### **Runtime/Dynamic Data (Database Only):**
```json
{
  "platforms": {
    "[platform]": {
      "metadata": {
        "created_at": "",       // Auto-set when enabled
        "last_active": "",      // Auto-updated
        "total_posts": 0,       // Runtime counter
        "total_followers": 0,   // Runtime data
        "total_following": 0    // Runtime data
      }
    }
  },
  "posting": {
    "scheduled_posts_summary": {}  // Auto-calculated from DB
  },
  "content": {
    "posts": [
      {
        "id": "",              // Auto-generated
        "used_count": 0,       // Runtime counter
        "last_used": ""        // Runtime timestamp
      }
    ],
    "media": [
      {
        "id": "",              // Auto-generated
        "url": ""              // Runtime (after upload)
      }
    ],
    "last_used_index": 0       // Runtime counter
  },
  "scheduled_posts": []        // Runtime data (in DB)
}
```

### **Database Config:**
```json
{
  "meta": {
    "database": {
      "type": "sqlite",        // Fixed, not user-changeable
      "database": "./data/socialbot.db"  // Can be user-set
    }
  }
}
```

## **Summary of User-Settable Categories:**
1. **System Settings** - Language, operation mode, wake intervals
2. **AI Configuration** - Provider, API key, model, instructions
3. **Store Information** - Name, contact, hours, currency  
4. **Platform Setup** - Enable/disable, credentials, automation rules
5. **Posting Settings** - Random posting intervals, limits
6. **Content Pool** - Pre-made posts, media, hashtags
7. **Message Templates** - Greetings, keywords, fallback responses
8. **Paths** - Directory locations (optional customization)
9. Products Management (Database & GUI Only)
Product information is dynamic business data and is managed separately from the static configuration. It resides in your SQLite database and is viewed/edited through the GUI's "Products" tab.
10. **Global Fallback** - Default posting settings

All runtime data (user conversations, scheduled posts, posting history, analytics) goes to the database, not the config file. The config is purely for user-controlled settings.

LEVEL 3 PROCESS:
  INPUT: notification with image attachment + optional text
  OUTPUT: route (DIRECT/TICKET/AI), action, image_match_data

  1. CHECK IMAGE RECOGNITION CONFIG:
     IF config.ai.image_recognition.enabled = false:
        → ROUTE: AI (send image to Claude for description)
        → ACTION: AI_ANALYZE_IMAGE
        → REASON: "image_recognition_disabled"
        END

  2. VALIDATE IMAGE:
     IF no image attachment:
        → ROUTE: AI (regular text processing)
        → REASON: "no_image_attached"
        END
     
     IF image_size > config.ai.image_recognition.max_image_size_px:
        → RESIZE image to max size
     
     EXTRACT image to temp file
     VALIDATE: is image, not corrupted

  3. PROCESS IMAGE WITH CNN MODEL:
     CALL python/image_processor.py with image_path
     
     INPUT: image file
     OUTPUT: {
        "matches": [
          {"product_id": "PRD-123", "confidence": 0.95, "similarity_score": 0.92},
          {"product_id": "PRD-456", "confidence": 0.82, "similarity_score": 0.78},
          ...
        ],
        "top_category": "electronics",
        "color_dominant": "blue",
        "estimated_price_range": "100-200 USD",
        "processing_time_ms": 150
     }

  4. ANALYZE USER'S QUESTION:
     text = notification.text (if any)
     
     A. "Do you have this?" / "هل لديكم هذا؟" / "هەیەت ئەمە؟"
        → INTENT: check_availability
        → Compare with user's last viewed product (context)
        
     B. "Does this fit [something]?" / "هل يناسب [شيء]؟"
        → INTENT: compatibility_check
        → Need to understand both items
        
     C. "What is this?" / "ما هذا؟" / "ئەمە چیە؟"
        → INTENT: identify_product
        → Just identify the item
        
     D. "Show me something like this" / "أرني شيئاً مثل هذا"
        → INTENT: find_similar
        → Search for visually similar products

  5. HANDLE CNN RESULTS:
     
     A. HIGH CONFIDENCE MATCH (top_match.confidence >= 0.90):
        product = GET from DB WHERE id = top_match.product_id
        
        IF product exists AND product.is_active = 1:
           
           RESPONSE_TEMPLATE:
             "Did you mean {product.name}?
              Price: {price} IQD
              {if wholesale: Wholesale: {price_per_pack} IQD per pack}
              {image: attach product.image_url}"
             
           → ROUTE: DIRECT
           → ACTION: SHOW_MATCHED_PRODUCT
           → UPDATE context: last_product_viewed = product.id
           
        ELSE:
           → ROUTE: TICKET (product not in DB or inactive)
           → REASON: "high_confidence_but_no_db_match"

     B. MEDIUM CONFIDENCE (0.70 <= confidence < 0.90):
        top_3_matches = GET top 3 products from matches
        
        RESPONSE_TEMPLATE:
          "Is it one of these?
           1. {product1.name} - {confidence1}% match - {price1} IQD
           2. {product2.name} - {confidence2}% match - {price2} IQD
           3. {product3.name} - {confidence3}% match - {price3} IQD
           Please confirm which one or describe more."
        
        → ROUTE: DIRECT (but needs user confirmation)
        → ACTION: SHOW_MULTIPLE_OPTIONS
        → SAVE matches to context for follow-up

     C. LOW CONFIDENCE (confidence < 0.70):
        IF user_question = "identify_product":
           → ROUTE: AI (Claude describes image)
           → ACTION: AI_DESCRIBE_IMAGE
           → PROMPT: "Describe this image for customer"
        
        ELSE IF user_question = "find_similar":
           category = CNN_output.top_category
           similar_products = GET products WHERE category = category LIMIT 3
           
           RESPONSE_TEMPLATE:
             "We couldn't identify exactly, but here are similar {category}:
              1. {product1.name} - {price1} IQD
              2. {product2.name} - {price2} IQD
              Is any of these what you're looking for?"
           
           → ROUTE: DIRECT
           → ACTION: SHOW_SIMILAR_BY_CATEGORY
        
        ELSE:
           → ROUTE: TICKET (needs human review)
           → REASON: "low_confidence_no_good_match"

  6. SPECIAL CASES:
     
     CASE 1: IMAGE + TEXT WITH PRODUCT_ID:
        IF text contains "pid=xxx" OR "#PRD123":
           product_id = EXTRACT from text
           product = GET from DB
           
           IF CNN confidence > 0.80 for same product:
              // Confirmation of match
              RESPONSE: "Yes, that looks like {product.name}!"
           ELSE:
              // Mismatch
              RESPONSE: "The image doesn't seem to match {product.name}. 
                        Here's what we found: {CNN_match.name}"
     
     CASE 2: MULTIPLE IMAGES:
        IF notification has >1 image:
           PROCESS each image separately
           COMBINE results
           
           IF all images match same product category:
              RESPONSE: "These appear to be {category}. 
                        Did you want to see our {category} collection?"
     
     CASE 3: POOR QUALITY IMAGE:
        IF CNN returns "low_quality" OR "unrecognizable":
           RESPONSE: "The image is blurry. Could you send a clearer photo?"
           → ROUTE: DIRECT (ask for better image)
           → ACTION: REQUEST_BETTER_IMAGE

  7. TICKET CREATION (when needed):
     
     CREATE urgent_messages entry:
        id: URG-{timestamp}
        user_id: notification.user_id
        platform: notification.platform_id
        message_type: 'image_identification'
        original_text: notification.text
        image_path: saved_image_path
        cnn_results: JSON of matches
        confidence: highest_confidence
        status: 'pending'
        priority: CALCULATE from:
                  - user.total_orders (premium = high)
                  - confidence_score (low = high)
                  - time_of_day (business hours = normal)
        assigned_to: NULL
        created_at: NOW()

  8. FALLBACK TO TASKER:
     
     TASKER_INSTRUCTIONS:
        IF route = TICKET:
           SEND to ticket_queue:
             - notification data
             - CNN results
             - suggested_response: "We're checking this for you. 
                                   Someone will reply shortly."
             - auto_reply_text: "Thanks for sending the photo! 
                                Our team is checking this and will reply soon."
        
        IF route = DIRECT but needs_confirmation:
           SEND to confirmation_queue:
             - product options
             - question: "Which one did you mean?"
             - timeout: 5 minutes (if no response → TICKET)
        
        IF route = AI:
           SEND to ai_queue with image + text context