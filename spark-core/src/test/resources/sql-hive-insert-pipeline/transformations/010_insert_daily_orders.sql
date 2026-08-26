INSERT INTO TABLE daily_orders_sink
SELECT
  region,
  TO_DATE(orderDate) AS order_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders_source
WHERE amount > 0
GROUP BY region, TO_DATE(orderDate);
