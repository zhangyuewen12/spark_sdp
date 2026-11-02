-- 作业：按用户汇总订单，补充最近下单日期和价值等级。
-- 上游作业：dwd user_profile、dwd order_detail。
-- LEFT JOIN 保留无订单用户；累计金额达到 150.00 标记为 HIGH，否则 STANDARD。
-- 预期 3 行，其中 Alice 为 HIGH；重复测试前执行 tests/sql/reset.sql。
INSERT INTO TABLE spark_dbt_example_dws.user_order_summary
SELECT
    profile.user_id,
    profile.user_name,
    profile.region,
    COUNT(detail.order_id) AS order_count,
    CAST(COALESCE(SUM(detail.amount), 0) AS DECIMAL(18, 2)) AS total_amount,
    MAX(detail.order_date) AS last_order_date,
    CASE
        WHEN COALESCE(SUM(detail.amount), 0) >= 150.00 THEN 'HIGH'
        ELSE 'STANDARD'
    END AS value_level
FROM spark_dbt_example_dwd.user_profile AS profile
LEFT JOIN spark_dbt_example_dwd.order_detail AS detail
    ON profile.user_id = detail.user_id
GROUP BY profile.user_id, profile.user_name, profile.region;
