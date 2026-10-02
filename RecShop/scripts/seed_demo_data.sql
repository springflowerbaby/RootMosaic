-- Optional synthetic demo rows for a NEW dedicated RecShop database only.
-- Run after database_schema.sql and the demo product from deploy_recshop.py.
-- Not research data; never run during collection or as a readiness check.
USE shopify2;
INSERT IGNORE INTO inventory (item_id, stock, reserved)
SELECT item_id, 100, 0 FROM items WHERE item_id = 'RECSHOP_DEMO_001';
INSERT IGNORE INTO promotions (code, discount, active, expires_at)
VALUES ('RECSHOP_DEMO10', 10.00, 1, NULL);
