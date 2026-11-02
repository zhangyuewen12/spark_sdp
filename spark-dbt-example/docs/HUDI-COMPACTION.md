# sdf_compaction：Hudi MOR 表维护样例

`models/sdf_compaction/` 存放 Hudi MOR 表 compaction 任务，属于表维护作业集合。
`mor_table.sql` 指定本次处理的表清单，`config/sdf_compaction/mor_table.yaml`
设置 `job-type: hudi-compaction` 及 Spark 资源，`flow.yaml` 设置 DS 工作流与定时。
维护任务不参与六个数仓样例的建表、清空及结果断言流程。

## 文件与样例

```text
models/sdf_compaction/mor_table.sql
config/sdf_compaction/mor_table.yaml
config/sdf_compaction/flow.yaml
```

SQL 文件中每条语句指定一张 Hive 注册表：

```sql
RUN COMPACTION ON sdf.user;
RUN COMPACTION ON sdf.alter;
```

这两个表名沿用当前工程中的样例。运行前替换为真实的测试 MOR 表；不提供自动建表或计划创建。
这里的语句用来指定表，入口实际执行时会附加查询到的 compaction instant。
表名须是 `schema.table`，库名、表名使用字母或下划线开头、后接字母数字或下划线。
表清单不写反引号，入口会在实际执行 SQL 时将库名和表名分别加反引号。
支持空行、SQL 注释及重复表名去重，每条语句必须以分号结束。
不接受 `INSERT`、带显式 instant 的语句或 `SCHEDULE COMPACTION` 混入该文件。

配套 YAML：

```yaml
name: sdf_mor_table_compaction
job-type: hudi-compaction
configuration:
  hoodie.cleaner.commits.retained: "20"
  hoodie.keep.min.commits: "30"
  hoodie.keep.max.commits: "50"
spark-submit:
  master: yarn
  deploy-mode: cluster
  queue: default
  driver.memory: 2g
  executor.num: "2"
  executor.memory: 4g
  executor.cores: "2"
```

资源和保留参数是样例值，请按实际数据量、查询回溯需求和所用 Hudi 版本验证后调整。
本工程运行 JAR 不包含 Hudi bundle，需要由 Spark 环境提供，或在该 YAML 的 `spark-submit.jars`
中设置实际依赖文件列表。

## 当前入口的实际行为

`start_pipeline_job.sh` 启动 `SparkSubmitStarter`，提交后由 `SqlPipelineRunApplication` 的
`hudi-compaction` 分支执行。对每张表依次进行：

1. 调用 `SHOW COMPACTION ON schema.table`。
2. 按三列 `timestamp`、`action`、`size` 读取结果，选择 `action=compaction` 且时间最早的计划。
3. 没有已有计划则跳过，不创建新计划。
4. 执行 `RUN COMPACTION ON schema.table AT instant`。
5. 成功后执行 `RUN CLEAN ON schema.table`。
6. 再执行 `RUN ARCHIVELOG ON schema.table`。

单表任一步失败会停止该表剩余步骤，记录失败阶段后继续处理下一张表。
所有表处理完成后，只要有失败就以非零状态退出。日志包含表名、选中计划、阶段和耗时。

这份入口没有读取 Hudi Java Timeline、验证 MOR 类型或区分 REQUESTED/INFLIGHT 状态，
也没有通过 Timeline 确认 compaction 已完成；以 SQL 成功返回为准。
运行前由操作方确认 MOR 类型、已有计划及没有其他任务并发维护相同表。

运行环境必须实际支持上述 SQL，且 `SHOW COMPACTION` 的结果结构与入口一致。
不能仅凭存在 Hudi bundle 就认为这些维护语句都可用。
若环境仅支持 `CALL run_compaction`，需要另行适配运行逻辑，
不能直接按本样例提交并期待自动兼容。

## 单次提交

在平台已准备好的 SQL 工程根目录执行：

```bash
./bin/start_pipeline_job.sh sdf_compaction mor_table
```

Hive 配置和 Hudi 依赖写在 `properties/application.properties` 或作业 YAML 中，启动命令不增加参数。
例如按当前环境填写公共属性：

```properties
spark.yarn.submit.waitAppCompletion=true
spark.files=/etc/hive/conf/hive-site.xml
spark.jars=lib/spark-connector-1.0.jar,/opt/spark/lib/hudi-compatible-bundle.jar
```

框架和连接器 JAR 由平台组支持，Hudi 依赖需与运行环境兼容；文件路径由项目组按部署环境配置。

## 配置 DolphinScheduler

```bash
./bin/ds-sync.sh generate . sdf_compaction sit
./bin/ds-sync.sh apply . sdf_compaction sit
```

生成的工作流只有 `sdf_compaction/mor_table` 一个 `SHELL` 节点，按 SQL 中的表顺序维护。
节点进入部署工程根目录后执行 `./bin/start_pipeline_job.sh sdf_compaction mor_table`，
由统一框架读取 YAML 的 `job-type: hudi-compaction` 并生成提交命令。
DS 生成器复用现有 compaction 表清单校验逻辑，不执行维护操作。
表名记录在 `manifest.json` 的输入和外部输入清单中，不推断跨工作流依赖。

`config/sdf_compaction/flow.yaml` 示例定时为每天 03:00、`Asia/Shanghai` 时区，
实际启用由对应环境的 `ds.schedule.enabled` 决定。
默认不自动重试，避免不清楚已有计划状态时重复运行。工作流使用 `SERIAL_WAIT`，
还需避免手工执行、其他工程或其他工具同时维护相同表。

`generate` 的配置和表清单校验可以离线验证；真实 compaction、clean、archive 必须在兼容的
Hudi 测试环境中验证。本仓库的普通 Parquet 样例不能替代 Hudi 集成测试。
