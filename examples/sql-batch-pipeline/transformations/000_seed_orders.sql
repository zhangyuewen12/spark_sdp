CREATE TEMPORARY VIEW orders_source AS
SELECT 1L AS orderId, 'east' AS region, '2026-04-15' AS orderDate, 120.0D AS amount
UNION ALL
SELECT 2L AS orderId, 'east' AS region, '2026-04-15' AS orderDate, 80.0D AS amount
UNION ALL
SELECT 3L AS orderId, 'west' AS region, '2026-04-16' AS orderDate, 55.0D AS amount
UNION ALL
SELECT 4L AS orderId, 'north' AS region, '2026-04-16' AS orderDate, -12.0D AS amount;
