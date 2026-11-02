-- 作业：在清洗后的用户数据上继续加工，补充区域分类与姓名长度。
-- 上游作业：dwd user；目标表由 tests/sql/setup.sql 创建。
-- 预期 3 行，user_id 唯一；重复测试前执行 tests/sql/reset.sql。
INSERT INTO TABLE spark_dbt_example_dwd.user_profile
SELECT
    user_id,
    user_name,
    region,
    CASE
        WHEN region = 'east' THEN 'east_cn'
        WHEN region = 'south' THEN 'south_cn'
        ELSE 'other'
    END AS region_group,
    LENGTH(user_name) AS name_length
FROM spark_dbt_example_dwd.user;
