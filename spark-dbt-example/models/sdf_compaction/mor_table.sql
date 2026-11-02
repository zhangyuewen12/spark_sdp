-- Hudi MOR 维护任务：每条语句指定一张已注册到 Hive metastore 的待处理表。
-- 对应配置：config/sdf_compaction/mor_table.yaml（job-type: hudi-compaction）。
-- 运行前确认表为 MOR、已有 compaction 计划，且没有其他任务同时维护这些表。
-- 此文件用于指定表清单；入口将查询已有计划并附加 instant 执行，不生成新计划。
-- sdf.user 和 sdf.alter 沿用本项目配置的样例表名，执行前替换为真实测试表。
-- 完整行为、运行环境要求和启动方式见 docs/HUDI-COMPACTION.md。
RUN COMPACTION ON sdf.user;
RUN COMPACTION ON sdf.alter;
