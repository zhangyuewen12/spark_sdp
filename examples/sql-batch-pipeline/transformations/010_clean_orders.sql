CREATE MATERIALIZED VIEW orders_clean AS
SELECT
  orderId,
  region,
  orderDate,
  amount,
  TO_DATE(orderDate) AS order_date
FROM orders_source
WHERE amount > 0;
