-- 测试作业：清洗内置用户数据，保留启用且姓名非空的用户。
-- 前置条件：按 README 创建 spark_dbt_example_dwd.user 表。
-- 预期写入 3 行：Alice/east、Bob/south、Charlie/east。
-- INSERT INTO 为追加写入，重复测试前执行 tests/sql/reset.sql 清空所有测试目标表。
INSERT INTO TABLE spark_dbt_example_dwd.user
SELECT
    CAST(raw_user.user_id AS BIGINT) AS user_id,
    TRIM(raw_user.user_name) AS user_name,
    raw_user.region
FROM (
    VALUES
        (1, ' Alice ', 'east', 'active'),
        (2, ' Bob ', 'south', 'active'),
        (3, ' Charlie ', 'east', 'active'),
        (4, 'Disabled', 'north', 'inactive'),
        (5, '   ', 'west', 'active'),
        (6, CAST(NULL AS STRING), 'west', 'active')
) AS raw_user(user_id, user_name, region, user_status)
WHERE raw_user.user_status = 'active'
  AND raw_user.user_name IS NOT NULL
  AND TRIM(raw_user.user_name) <> '';
