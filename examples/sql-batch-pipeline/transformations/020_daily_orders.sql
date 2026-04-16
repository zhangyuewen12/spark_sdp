CREATE MATERIALIZED VIEW daily_orders AS
SELECT
  region,
  order_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders_clean
GROUP BY region, order_date;
