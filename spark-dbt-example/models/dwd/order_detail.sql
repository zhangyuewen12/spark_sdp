-- 作业：清洗内置订单数据，关联已加工的用户画像，生成有效支付订单明细。
-- 上游作业：dwd user_profile；目标表由 tests/sql/setup.sql 创建。
-- 取消、负金额、零金额、未知用户及被用户清洗作业过滤的用户订单均被剔除。
-- 预期保留订单 1001、1002、1003、1004；重复测试前执行 tests/sql/reset.sql。
INSERT INTO TABLE spark_dbt_example_dwd.order_detail
SELECT
    CAST(raw_order.order_id AS BIGINT) AS order_id,
    profile.user_id,
    raw_order.order_date,
    profile.region,
    raw_order.amount
FROM (
    VALUES
        (1001, 1, DATE '2026-01-01', CAST(100.00 AS DECIMAL(18, 2)), 'paid'),
        (1002, 1, DATE '2026-01-01', CAST(50.50 AS DECIMAL(18, 2)), 'paid'),
        (1003, 2, DATE '2026-01-01', CAST(80.00 AS DECIMAL(18, 2)), 'paid'),
        (1004, 3, DATE '2026-01-02', CAST(120.00 AS DECIMAL(18, 2)), 'paid'),
        (1005, 1, DATE '2026-01-01', CAST(30.00 AS DECIMAL(18, 2)), 'cancelled'),
        (1006, 2, DATE '2026-01-01', CAST(-10.00 AS DECIMAL(18, 2)), 'paid'),
        (1007, 99, DATE '2026-01-01', CAST(60.00 AS DECIMAL(18, 2)), 'paid'),
        (1008, 5, DATE '2026-01-01', CAST(20.00 AS DECIMAL(18, 2)), 'paid'),
        (1009, 2, DATE '2026-01-01', CAST(0.00 AS DECIMAL(18, 2)), 'paid')
) AS raw_order(order_id, user_id, order_date, amount, order_status)
INNER JOIN spark_dbt_example_dwd.user_profile AS profile
    ON raw_order.user_id = profile.user_id
WHERE raw_order.order_status = 'paid'
  AND raw_order.amount > 0;
