-- 完整链路执行后，用 spark-sql 验证六张表的行数及每行字段。
-- 断言失败会报错；成功时每条 assert_true 返回 NULL。

SELECT assert_true(
    COUNT(*) = 3
    AND SUM(CASE WHEN user_id = 1
            AND user_name = 'Alice'
            AND region = 'east'
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN user_id = 2
            AND user_name = 'Bob'
            AND region = 'south'
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN user_id = 3
            AND user_name = 'Charlie'
            AND region = 'east'
        THEN 1 ELSE 0 END) = 1,
    'Unexpected rows: spark_dbt_example_dwd.user'
)
FROM spark_dbt_example_dwd.user;

SELECT assert_true(
    COUNT(*) = 3
    AND SUM(CASE WHEN user_id = 1
            AND user_name = 'Alice'
            AND region = 'east'
            AND region_group = 'east_cn'
            AND name_length = 5
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN user_id = 2
            AND user_name = 'Bob'
            AND region = 'south'
            AND region_group = 'south_cn'
            AND name_length = 3
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN user_id = 3
            AND user_name = 'Charlie'
            AND region = 'east'
            AND region_group = 'east_cn'
            AND name_length = 7
        THEN 1 ELSE 0 END) = 1,
    'Unexpected rows: spark_dbt_example_dwd.user_profile'
)
FROM spark_dbt_example_dwd.user_profile;

SELECT assert_true(
    COUNT(*) = 4
    AND SUM(CASE WHEN order_id = 1001
            AND user_id = 1
            AND order_date = DATE '2026-01-01'
            AND region = 'east'
            AND amount = 100.00
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN order_id = 1002
            AND user_id = 1
            AND order_date = DATE '2026-01-01'
            AND region = 'east'
            AND amount = 50.50
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN order_id = 1003
            AND user_id = 2
            AND order_date = DATE '2026-01-01'
            AND region = 'south'
            AND amount = 80.00
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN order_id = 1004
            AND user_id = 3
            AND order_date = DATE '2026-01-02'
            AND region = 'east'
            AND amount = 120.00
        THEN 1 ELSE 0 END) = 1,
    'Unexpected rows: spark_dbt_example_dwd.order_detail'
)
FROM spark_dbt_example_dwd.order_detail;

SELECT assert_true(
    COUNT(*) = 3
    AND SUM(CASE WHEN order_date = DATE '2026-01-01'
            AND region = 'east'
            AND order_count = 2
            AND user_count = 1
            AND total_amount = 150.50
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN order_date = DATE '2026-01-01'
            AND region = 'south'
            AND order_count = 1
            AND user_count = 1
            AND total_amount = 80.00
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN order_date = DATE '2026-01-02'
            AND region = 'east'
            AND order_count = 1
            AND user_count = 1
            AND total_amount = 120.00
        THEN 1 ELSE 0 END) = 1,
    'Unexpected rows: spark_dbt_example_dws.order_summary'
)
FROM spark_dbt_example_dws.order_summary;

SELECT assert_true(
    COUNT(*) = 3
    AND SUM(CASE WHEN user_id = 1
            AND user_name = 'Alice'
            AND region = 'east'
            AND order_count = 2
            AND total_amount = 150.50
            AND last_order_date = DATE '2026-01-01'
            AND value_level = 'HIGH'
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN user_id = 2
            AND user_name = 'Bob'
            AND region = 'south'
            AND order_count = 1
            AND total_amount = 80.00
            AND last_order_date = DATE '2026-01-01'
            AND value_level = 'STANDARD'
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN user_id = 3
            AND user_name = 'Charlie'
            AND region = 'east'
            AND order_count = 1
            AND total_amount = 120.00
            AND last_order_date = DATE '2026-01-02'
            AND value_level = 'STANDARD'
        THEN 1 ELSE 0 END) = 1,
    'Unexpected rows: spark_dbt_example_dws.user_order_summary'
)
FROM spark_dbt_example_dws.user_order_summary;

SELECT assert_true(
    COUNT(*) = 2
    AND SUM(CASE WHEN region = 'east'
            AND user_count = 2
            AND high_value_user_count = 1
            AND order_count = 3
            AND total_amount = 270.50
            AND avg_order_amount = 90.17
            AND first_order_date = DATE '2026-01-01'
            AND last_order_date = DATE '2026-01-02'
        THEN 1 ELSE 0 END) = 1
    AND SUM(CASE WHEN region = 'south'
            AND user_count = 1
            AND high_value_user_count = 0
            AND order_count = 1
            AND total_amount = 80.00
            AND avg_order_amount = 80.00
            AND first_order_date = DATE '2026-01-01'
            AND last_order_date = DATE '2026-01-01'
        THEN 1 ELSE 0 END) = 1,
    'Unexpected rows: spark_dbt_example_ads.region_value'
)
FROM spark_dbt_example_ads.region_value;
