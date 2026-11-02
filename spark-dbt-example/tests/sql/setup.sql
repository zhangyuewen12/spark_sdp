-- 仅初始化专用测试库和目标表，不写入数据。
-- 使用 spark-sql 执行；CREATE TABLE 不属于模型入口支持的 Pipeline SQL 声明。
CREATE DATABASE IF NOT EXISTS spark_dbt_example_dwd;
CREATE DATABASE IF NOT EXISTS spark_dbt_example_dws;
CREATE DATABASE IF NOT EXISTS spark_dbt_example_ads;

CREATE TABLE IF NOT EXISTS spark_dbt_example_dwd.user (
    user_id BIGINT,
    user_name STRING,
    region STRING
) USING PARQUET;

CREATE TABLE IF NOT EXISTS spark_dbt_example_dwd.user_profile (
    user_id BIGINT,
    user_name STRING,
    region STRING,
    region_group STRING,
    name_length INT
) USING PARQUET;

CREATE TABLE IF NOT EXISTS spark_dbt_example_dwd.order_detail (
    order_id BIGINT,
    user_id BIGINT,
    order_date DATE,
    region STRING,
    amount DECIMAL(18, 2)
) USING PARQUET;

CREATE TABLE IF NOT EXISTS spark_dbt_example_dws.order_summary (
    order_date DATE,
    region STRING,
    order_count BIGINT,
    user_count BIGINT,
    total_amount DECIMAL(18, 2)
) USING PARQUET;

CREATE TABLE IF NOT EXISTS spark_dbt_example_dws.user_order_summary (
    user_id BIGINT,
    user_name STRING,
    region STRING,
    order_count BIGINT,
    total_amount DECIMAL(18, 2),
    last_order_date DATE,
    value_level STRING
) USING PARQUET;

CREATE TABLE IF NOT EXISTS spark_dbt_example_ads.region_value (
    region STRING,
    user_count BIGINT,
    high_value_user_count BIGINT,
    order_count BIGINT,
    total_amount DECIMAL(18, 2),
    avg_order_amount DECIMAL(18, 2),
    first_order_date DATE,
    last_order_date DATE
) USING PARQUET;
