-- 作业：合并用户汇总与日期地区订单汇总，形成地区经营指标。
-- 上游作业：dws user_order_summary、dws order；两个上游均成功后才可执行。
-- 两侧分别聚合到地区粒度再关联，避免多用户、多日期关联造成金额放大。
-- user_count 统计全部有效用户（含无订单用户），order_count 统计有效支付订单。
-- 预期 east/south 两行；重复测试前执行 tests/sql/reset.sql。
INSERT INTO TABLE spark_dbt_example_ads.region_value
SELECT
    users.region,
    users.user_count,
    users.high_value_user_count,
    COALESCE(orders.order_count, CAST(0 AS BIGINT)) AS order_count,
    CAST(COALESCE(orders.total_amount, 0) AS DECIMAL(18, 2)) AS total_amount,
    CASE
        WHEN orders.order_count > 0
            THEN CAST(orders.total_amount / orders.order_count AS DECIMAL(18, 2))
        ELSE CAST(0 AS DECIMAL(18, 2))
    END AS avg_order_amount,
    orders.first_order_date,
    orders.last_order_date
FROM (
    SELECT
        region,
        COUNT(*) AS user_count,
        SUM(CASE WHEN value_level = 'HIGH' THEN 1 ELSE 0 END) AS high_value_user_count
    FROM spark_dbt_example_dws.user_order_summary
    GROUP BY region
) AS users
LEFT JOIN (
    SELECT
        region,
        SUM(order_count) AS order_count,
        SUM(total_amount) AS total_amount,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM spark_dbt_example_dws.order_summary
    GROUP BY region
) AS orders
    ON users.region = orders.region;
