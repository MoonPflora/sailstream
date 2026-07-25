PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;

CREATE TABLE IF NOT EXISTS platform_users (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL CHECK(platform IN ('whatsapp', 'telegram', 'instagram', 'facebook', 'tiktok', 'twitter', 'viber')),
    platform_user_id TEXT NOT NULL,
    username TEXT,
    display_name TEXT,
    profile_pic_url TEXT,
    conversation_state TEXT DEFAULT 'idle' CHECK(conversation_state IN ('idle', 'browsing', 'ordering', 'support')),
    last_intent TEXT,
    last_product_sku TEXT,
    pending_data TEXT,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_messages INTEGER DEFAULT 0,
    total_orders INTEGER DEFAULT 0,
    total_spent REAL DEFAULT 0,
    is_blocked INTEGER DEFAULT 0,
    tags TEXT,
    metadata TEXT,
    UNIQUE(platform, platform_user_id)
);

CREATE TABLE IF NOT EXISTS rate_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    subtype  TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    hourly_limit INTEGER DEFAULT 0,
    daily_limit INTEGER DEFAULT 0,
    current_hour_count INTEGER DEFAULT 0,
    current_day_count INTEGER DEFAULT 0,
    last_reset_hour TIMESTAMP,
    last_reset_day TIMESTAMP,
    UNIQUE(platform, subtype, action)
);

CREATE INDEX IF NOT EXISTS idx_rate_limits_platform ON rate_limits(platform, subtype, action);

CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    sku TEXT UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,
    subcategory TEXT,
    tags TEXT,
    
    price REAL NOT NULL CHECK(price >= 0),
    price_per_pack REAL CHECK(price_per_pack >= 0 OR price_per_pack IS NULL),
    quantity_per_pack INTEGER CHECK(quantity_per_pack > 0 OR quantity_per_pack IS NULL),
    currency TEXT DEFAULT 'IQD',
    
    stock INTEGER DEFAULT 0 CHECK(stock >= 0),
    reserved_stock INTEGER DEFAULT 0 CHECK(reserved_stock >= 0),
    low_stock_threshold INTEGER DEFAULT 10 CHECK(low_stock_threshold >= 0),
    
    image_count INTEGER DEFAULT 0,
    image_url TEXT,
    thumbnail_url TEXT,
    
    weight_kg REAL CHECK(weight_kg >= 0 OR weight_kg IS NULL),
    dimensions TEXT,
    
    aliases_en TEXT,
    aliases_ar TEXT,
    aliases_ku TEXT,
    uses_en TEXT,
    uses_ar TEXT,
    uses_ku TEXT,
    
    is_active INTEGER DEFAULT 1 CHECK(is_active IN (0, 1)),
    is_featured INTEGER DEFAULT 0 CHECK(is_featured IN (0, 1)),
    
    metadata TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS global_cursors (
    platform TEXT NOT NULL,
    subtype TEXT NOT NULL,
    account_id TEXT NOT NULL,
    subtype_id TEXT NOT NULL,
    cursor_type TEXT NOT NULL CHECK(cursor_type IN ('message_id','timestamp','seq','chat_message_id')),
    cursor_value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (platform, subtype, account_id, subtype_id, cursor_type)
);

CREATE INDEX IF NOT EXISTS idx_global_cursors_lookup ON global_cursors(platform, subtype, account_id, subtype_id);

CREATE TABLE IF NOT EXISTS ai_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    platform_id TEXT,
    ticket_data TEXT,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'answered', 'failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id)
);

CREATE INDEX IF NOT EXISTS idx_ai_tickets_user ON ai_tickets(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_tickets_status ON ai_tickets(status) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS product_images (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    file_path TEXT NOT NULL,
    extension TEXT NOT NULL,
    img_order INTEGER DEFAULT 0,
    is_primary BOOLEAN DEFAULT FALSE,
    size_bytes INTEGER DEFAULT 0,
    width INTEGER,
    height INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    UNIQUE(product_id, img_order)
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    user_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('incoming', 'outgoing')),
    message_text TEXT NOT NULL,
    message_type TEXT DEFAULT 'text' CHECK(message_type IN ('text', 'image', 'video', 'document', 'audio')),
    media_url TEXT,
    intent TEXT,
    confidence REAL CHECK(confidence >= 0 AND confidence <= 1 OR confidence IS NULL),
    entities TEXT,
    processed INTEGER DEFAULT 0 CHECK(processed IN (0, 1)),
    ai_response TEXT,
    final_response TEXT,
    response_type TEXT DEFAULT 'auto' CHECK(response_type IN ('auto', 'manual', 'modified')),
    needs_review INTEGER DEFAULT 0 CHECK(needs_review IN (0, 1)),
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    responded_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ai_requests (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    prompt_hash TEXT,
    response_hash TEXT,
    model TEXT NOT NULL,
    tokens_used INTEGER,
    cost REAL,
    response_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id)
);

CREATE TABLE IF NOT EXISTS product_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    view_type TEXT CHECK(view_type IN ('price_query', 'search_result', 'image_match', 'ai_suggestion')),
    source TEXT CHECK(source IN ('level1', 'level2', 'level3', 'level4', 'direct_link')),
    search_query TEXT,
    viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS search_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    query_text TEXT NOT NULL,
    results_count INTEGER DEFAULT 0 CHECK(results_count >= 0),
    top_match_id TEXT,
    confidence REAL CHECK(confidence >= 0 AND confidence <= 1),
    level TEXT CHECK(level IN ('level2', 'level4')),
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id),
    FOREIGN KEY (top_match_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS image_processing_log (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    image_hash TEXT,
    cnn_results TEXT,
    confidence REAL CHECK(confidence >= 0 AND confidence <= 1),
    match_product_id TEXT,
    level TEXT DEFAULT 'level3' CHECK(level IN ('level3', 'level4')),
    processing_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id),
    FOREIGN KEY (match_product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS urgent_messages (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    message_type TEXT CHECK(message_type IN ('image_identification', 'complex_query', 'escalation', 'support')),
    original_text TEXT,
    image_path TEXT,
    cnn_results TEXT,
    confidence REAL CHECK(confidence >= 0 AND confidence <= 1),
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'assigned', 'in_progress', 'resolved', 'closed')),
    priority INTEGER DEFAULT 50 CHECK(priority >= 1 AND priority <= 100),
    assigned_to TEXT,
    response_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id)
);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')),
    total REAL NOT NULL CHECK(total >= 0),
    subtotal REAL CHECK(subtotal >= 0),
    tax REAL DEFAULT 0 CHECK(tax >= 0),
    shipping_cost REAL DEFAULT 0 CHECK(shipping_cost >= 0),
    discount_amount REAL DEFAULT 0 CHECK(discount_amount >= 0),
    shipping_address TEXT,
    shipping_method TEXT,
    tracking_number TEXT,
    payment_method TEXT,
    payment_status TEXT DEFAULT 'pending' CHECK(payment_status IN ('pending', 'paid', 'failed', 'refunded')),
    transaction_id TEXT,
    customer_notes TEXT,
    internal_notes TEXT,
    platform_conversation_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confirmed_at TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES platform_users(id)
);

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    unit_price REAL NOT NULL CHECK(unit_price >= 0),
    total_price REAL NOT NULL CHECK(total_price >= 0),
    product_name TEXT NOT NULL,
    product_sku TEXT,
    product_image_url TEXT,
    variant_options TEXT,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS scheduled_posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT NOT NULL,
    schedule_type TEXT NOT NULL CHECK(schedule_type IN ('once', 'recurring', 'immediate')),
    scheduled_time TIMESTAMP,
    post_time TIME,
    recurring_interval TEXT,
    recurring_days TEXT,
    timezone TEXT DEFAULT 'UTC',
    status TEXT DEFAULT 'draft' CHECK(status IN ('draft', 'scheduled', 'pending', 'posting', 'posted', 'failed', 'cancelled')),
    posted_at TIMESTAMP,
    target_platforms TEXT,
    media_paths TEXT,
    created_by TEXT DEFAULT 'system',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0 CHECK(retry_count >= 0),
    max_retries INTEGER DEFAULT 3 CHECK(max_retries >= 0),
    platform_post_ids TEXT
);

CREATE TABLE IF NOT EXISTS media_cache (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL UNIQUE,
    platform TEXT,
    platform_media_id TEXT,
    local_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size INTEGER CHECK(file_size >= 0 OR file_size IS NULL),
    mime_type TEXT,
    width INTEGER CHECK(width > 0 OR width IS NULL),
    height INTEGER CHECK(height > 0 OR height IS NULL),
    accessed_count INTEGER DEFAULT 0 CHECK(accessed_count >= 0),
    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content_pool (
    id TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    category TEXT,
    tags TEXT,
    media_ids TEXT,
    used_count INTEGER DEFAULT 0 CHECK(used_count >= 0),
    last_used TIMESTAMP,
    next_suggested_use TIMESTAMP,
    weight INTEGER DEFAULT 100 CHECK(weight >= 0),
    enabled INTEGER DEFAULT 1 CHECK(enabled IN (0, 1)),
    ai_generated INTEGER DEFAULT 0 CHECK(ai_generated IN (0, 1)),
    ai_model TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_platform ON platform_users(platform, platform_user_id);
CREATE INDEX IF NOT EXISTS idx_users_last_active ON platform_users(last_active DESC);

CREATE INDEX IF NOT EXISTS idx_messages_user_time ON messages(user_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_unprocessed ON messages(processed) WHERE processed = 0;
CREATE INDEX IF NOT EXISTS idx_messages_intent ON messages(intent) WHERE intent IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_products_active ON products(is_active) WHERE is_active = 1;
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
CREATE INDEX IF NOT EXISTS idx_products_stock ON products(stock) WHERE stock > 0;
CREATE INDEX IF NOT EXISTS idx_products_aliases_en ON products(aliases_en) WHERE aliases_en IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_products_aliases_ar ON products(aliases_ar) WHERE aliases_ar IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_products_aliases_ku ON products(aliases_ku) WHERE aliases_ku IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_product_images_product ON product_images(product_id);
CREATE INDEX IF NOT EXISTS idx_product_images_order ON product_images(product_id, img_order DESC);
CREATE INDEX IF NOT EXISTS idx_product_images_primary ON product_images(product_id, is_primary) WHERE is_primary = 1;

CREATE INDEX IF NOT EXISTS idx_product_views_user ON product_views(user_id, viewed_at DESC);
CREATE INDEX IF NOT EXISTS idx_product_views_product ON product_views(product_id, viewed_at DESC);

CREATE INDEX IF NOT EXISTS idx_search_history_user ON search_history(user_id, searched_at DESC);
CREATE INDEX IF NOT EXISTS idx_search_history_query ON search_history(query_text);

CREATE INDEX IF NOT EXISTS idx_image_log_user ON image_processing_log(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_image_log_confidence ON image_processing_log(confidence) WHERE confidence IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_urgent_messages_status ON urgent_messages(status);
CREATE INDEX IF NOT EXISTS idx_urgent_messages_priority ON urgent_messages(priority DESC);

CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_created_ay ON orders(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_scheduled_posts_time ON scheduled_posts(scheduled_time, status);
CREATE INDEX IF NOT EXISTS idx_scheduled_posts_status ON scheduled_posts(status);

CREATE INDEX IF NOT EXISTS idx_media_cache_url ON media_cache(source_url);
CREATE INDEX IF NOT EXISTS idx_media_cache_expires ON media_cache(expires_at) WHERE expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_content_pool_category ON content_pool(category);
CREATE INDEX IF NOT EXISTS idx_content_pool_enabled ON content_pool(enabled) WHERE enabled = 1;