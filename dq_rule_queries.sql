/*
-- SQL file for inserting data quality config manually
-- Tech Design Answer by Mario Caesar // caesarmario87@gmail.com
-- Updated: 20250625
*/

-- create table config
CREATE TABLE IF NOT EXISTS tmt_dq.dq_rule_config (
    rule_id TEXT PRIMARY KEY,
    rule_name TEXT,
    table_name TEXT,
    rule_sql TEXT,
    severity TEXT
);

-- customer_transactions
INSERT INTO tmt_dq.dq_rule_config VALUES
('R001', 'Null customer_id', 'customer_transactions',
 'SELECT COUNT(*) FROM tmt.customer_transactions WHERE customer_id IS NULL', 'high'),
('R002', 'Negative amount', 'customer_transactions',
 'SELECT COUNT(*) FROM tmt.customer_transactions WHERE amount < 0', 'medium'),
('R003', 'Missing transaction_date', 'customer_transactions',
 'SELECT COUNT(*) FROM tmt.customer_transactions WHERE transaction_date IS NULL', 'high');

-- order_reviews
INSERT INTO tmt_dq.dq_rule_config VALUES
('R004', 'Invalid review score', 'order_reviews',
 'SELECT COUNT(*) FROM tmt.order_reviews WHERE review_score NOT BETWEEN 1 AND 5', 'high'),
('R005', 'Missing review creation date', 'order_reviews',
 'SELECT COUNT(*) FROM tmt.order_reviews WHERE review_creation_date IS NULL', 'medium');

-- orders
INSERT INTO tmt_dq.dq_rule_config VALUES
('R006', 'Missing order timestamp', 'orders',
 'SELECT COUNT(*) FROM tmt.orders WHERE order_purchase_timestamp IS NULL', 'high'),
('R007', 'Delivered earlier than purchased', 'orders',
 'SELECT COUNT(*) FROM tmt.orders WHERE order_delivered_customer_date < order_purchase_timestamp', 'high');

-- products
INSERT INTO tmt_dq.dq_rule_config VALUES
('R008', 'Product with 0 weight', 'products',
 'SELECT COUNT(*) FROM tmt.products WHERE product_weight_g <= 0', 'medium'),
('R009', 'Product with missing category', 'products',
 'SELECT COUNT(*) FROM tmt.products WHERE product_category_name IS NULL', 'low');

-- geolocation
INSERT INTO tmt_dq.dq_rule_config VALUES
('R010', 'Missing geolocation coordinates', 'geolocation',
 'SELECT COUNT(*) FROM tmt.geolocation WHERE geolocation_lat IS NULL OR geolocation_lng IS NULL', 'medium');

-- sellers
INSERT INTO tmt_dq.dq_rule_config VALUES
('R011', 'Seller without city/state', 'sellers',
 'SELECT COUNT(*) FROM tmt.sellers WHERE seller_city IS NULL OR seller_state IS NULL', 'medium');

-- customers
INSERT INTO tmt_dq.dq_rule_config VALUES
('R012', 'Customer without location', 'customers',
 'SELECT COUNT(*) FROM tmt.customers WHERE customer_city IS NULL OR customer_state IS NULL', 'medium');

-- order_items
INSERT INTO tmt_dq.dq_rule_config VALUES
('R013', 'Order item with null price', 'order_items',
 'SELECT COUNT(*) FROM tmt.order_items WHERE price IS NULL OR price <= 0', 'high');

-- order_payments
INSERT INTO tmt_dq.dq_rule_config VALUES
('R014', 'Payment with null value', 'order_payments',
 'SELECT COUNT(*) FROM tmt.order_payments WHERE payment_value IS NULL OR payment_value < 0', 'high');

-- leads_qualified
INSERT INTO tmt_dq.dq_rule_config VALUES
('R015', 'Missing first contact date', 'leads_qualified',
 'SELECT COUNT(*) FROM tmt.leads_qualified WHERE first_contact_date IS NULL', 'low');

-- leads_closed
INSERT INTO tmt_dq.dq_rule_config VALUES
('R016', 'Closed lead without business type', 'leads_closed',
 'SELECT COUNT(*) FROM tmt.leads_closed WHERE business_type IS NULL', 'low'),
('R017', 'Closed lead with empty revenue', 'leads_closed',
 'SELECT COUNT(*) FROM tmt.leads_closed WHERE declared_monthly_revenue IS NULL OR declared_monthly_revenue < 0', 'medium');
