-- 清空本模块全部六张专用测试表；仅在确认可以丢弃测试数据时执行。
-- 清理顺序从下游到上游；后续需按 README 顺序重跑整条链。
TRUNCATE TABLE spark_dbt_example_ads.region_value;
TRUNCATE TABLE spark_dbt_example_dws.user_order_summary;
TRUNCATE TABLE spark_dbt_example_dws.order_summary;
TRUNCATE TABLE spark_dbt_example_dwd.order_detail;
TRUNCATE TABLE spark_dbt_example_dwd.user_profile;
TRUNCATE TABLE spark_dbt_example_dwd.user;
