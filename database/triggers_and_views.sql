DROP TRIGGER IF EXISTS update_user_last_active; CREATE TRIGGER update_user_last_active AFTER INSERT ON messages FOR EACH ROW BEGIN UPDATE platform_users SET last_active = CURRENT_TIMESTAMP, total_messages = total_messages + 1 WHERE id = NEW.user_id; END;

DROP TRIGGER IF EXISTS calculate_order_total; CREATE TRIGGER calculate_order_total AFTER INSERT ON order_items FOR EACH ROW BEGIN UPDATE orders SET subtotal = (SELECT COALESCE(SUM(total_price), 0) FROM order_items WHERE order_id = NEW.order_id), total = (SELECT COALESCE(SUM(total_price), 0) FROM order_items WHERE order_id = NEW.order_id) + COALESCE(tax, 0) + COALESCE(shipping_cost, 0) - COALESCE(discount_amount, 0) WHERE id = NEW.order_id; END;

DROP TRIGGER IF EXISTS update_stock_on_order_confirmed; CREATE TRIGGER update_stock_on_order_confirmed AFTER UPDATE OF status ON orders FOR EACH ROW WHEN NEW.status = 'confirmed' AND OLD.status != 'confirmed' BEGIN UPDATE products SET stock = stock - (SELECT quantity FROM order_items WHERE order_id = NEW.id AND product_id = products.id) WHERE id IN (SELECT product_id FROM order_items WHERE order_id = NEW.id); END;

DROP TRIGGER IF EXISTS restore_stock_on_order_cancelled; CREATE TRIGGER restore_stock_on_order_cancelled AFTER UPDATE OF status ON orders FOR EACH ROW WHEN NEW.status = 'cancelled' AND OLD.status NOT IN ('cancelled', 'refunded') BEGIN UPDATE products SET stock = stock + (SELECT quantity FROM order_items WHERE order_id = NEW.id AND product_id = products.id) WHERE id IN (SELECT product_id FROM order_items WHERE order_id = NEW.id); END;

DROP TRIGGER IF EXISTS update_product_timestamp; CREATE TRIGGER update_product_timestamp AFTER UPDATE ON products FOR EACH ROW BEGIN UPDATE products SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id; END;

DROP TRIGGER IF EXISTS update_product_image_count; CREATE TRIGGER update_product_image_count AFTER INSERT ON product_images FOR EACH ROW BEGIN UPDATE products SET image_count = (SELECT COUNT(*) FROM product_images WHERE product_id = NEW.product_id), updated_at = CURRENT_TIMESTAMP WHERE id = NEW.product_id; END;

DROP TRIGGER IF EXISTS decrement_product_image_count; CREATE TRIGGER decrement_product_image_count AFTER DELETE ON product_images FOR EACH ROW BEGIN UPDATE products SET image_count = (SELECT COUNT(*) FROM product_images WHERE product_id = OLD.product_id), updated_at = CURRENT_TIMESTAMP WHERE id = OLD.product_id; END;

DROP TRIGGER IF EXISTS ensure_single_primary_image; CREATE TRIGGER ensure_single_primary_image BEFORE UPDATE OF is_primary ON product_images FOR EACH ROW WHEN NEW.is_primary = 1 BEGIN UPDATE product_images SET is_primary = 0 WHERE product_id = NEW.product_id AND id != NEW.id; END;

DROP TRIGGER IF EXISTS prevent_negative_stock; CREATE TRIGGER prevent_negative_stock BEFORE UPDATE OF stock ON products FOR EACH ROW WHEN NEW.stock < 0 BEGIN SELECT RAISE(ABORT, 'Stock cannot be negative'); END;

DROP VIEW IF EXISTS active_users; CREATE VIEW active_users AS SELECT * FROM platform_users WHERE last_active >= datetime('now', '-30 days') AND is_blocked = 0;

DROP VIEW IF EXISTS low_stock_products; CREATE VIEW low_stock_products AS SELECT * FROM products WHERE stock <= low_stock_threshold AND stock > 0 AND is_active = 1;

DROP VIEW IF EXISTS order_summary; CREATE VIEW order_summary AS SELECT o.id, o.created_at, u.display_name as customer_name, o.platform, o.status, o.total, o.payment_status, COUNT(oi.id) as item_count, SUM(oi.quantity) as total_quantity FROM orders o JOIN platform_users u ON o.user_id = u.id LEFT JOIN order_items oi ON o.id = oi.order_id GROUP BY o.id ORDER BY o.created_at DESC;