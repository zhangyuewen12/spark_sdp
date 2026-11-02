-- 作业：按日期、地区汇总清洗后的订单明细。
-- 上游作业：dwd order_detail；订单过滤和用户关联已在上游完成。
-- 预期 3 行，金额分别为 150.50、80.00、120.00，保持原样例的汇总结果。
-- INSERT INTO 为追加写入，重复测试前执行 tests/sql/reset.sql。
INSERT INTO TABLE spark_dbt_example_dws.order_summary
SELECT
    order_date,
    region,
    COUNT(*) AS order_count,
    COUNT(DISTINCT user_id) AS user_count,
    CAST(SUM(amount) AS DECIMAL(18, 2)) AS total_amount
FROM spark_dbt_example_dwd.order_detail
GROUP BY order_date, region;
