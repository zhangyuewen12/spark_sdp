# spark_platform

基于 Spark 3.3.1 / Scala 2.12 的 Spark SQL 作业开发框架。每个作业由一个 SQL 文件和一份同名
YAML 配置组成，开发人员通过统一脚本提交独立 Spark 作业。

业务开发请阅读[开发者手册](/Users/ywzhang/github_project/spark_sdp/docs/SQL_SDP_DEVELOPER_GUIDE.md)。

## 项目组成

| 模块 | 用途 |
| --- | --- |
| `spark-core` | 配置加载、SQL 解析、Spark 作业提交与执行、DS 工作流生成与发布 |
| `spark-example` | 框架使用示例 |
| `spark-dbt-example` | SQL 工程模板，管理业务 SQL、作业配置、环境配置和调度配置 |

业务工程按以下结构组织：

```text
my-sql-project/
├── models/
│   ├── dwd/user.sql
│   ├── dws/order.sql
│   └── ads/region_value.sql
├── config/
│   ├── dwd/user.yaml
│   ├── dwd/flow.yaml
│   ├── dws/order.yaml
│   ├── dws/flow.yaml
│   ├── ads/region_value.yaml
│   └── ads/flow.yaml
├── properties/
│   ├── application.properties         # 当前部署环境的公共 Spark 配置
│   ├── application-sit.properties     # SIT 的 DS 同步配置
│   └── application-uat.properties     # UAT 的 DS 同步配置
├── tests/sql/                         # 测试初始化与结果校验
├── docs/                              # 业务说明
├── dbt_project.yaml                   # DS 项目信息及公共调度配置
├── lib/                               # 平台组提供和支持
│   ├── spark-sdp-1.0.jar
│   └── spark-connector-1.0.jar
└── bin/                               # 平台组提供和支持
    ├── start_pipeline_job.sh          # 作业统一启动入口
    └── ds-sync.sh                     # 工作流生成与发布
```

`bin/` 下的脚本和 `lib/` 下的 JAR 由平台组提供和支持；其他目录及工程配置由项目组自行维护。
SQL 与配置保持相同的分层目录和文件名，例如 `models/dws/order.sql` 对应 `config/dws/order.yaml`。

## 执行作业

在 SQL 工程根目录执行，命令只有两个参数：

```bash
./bin/start_pipeline_job.sh dws order
```

脚本自动加载：

| 内容 | 路径 |
| --- | --- |
| 框架运行包 | `lib/spark-sdp-1.0.jar` |
| 连接器 | `lib/spark-connector-1.0.jar` |
| 作业 SQL | `models/dws/order.sql` |
| 作业配置 | `config/dws/order.yaml` |
| 当前环境配置 | `properties/application.properties` |

启动命令不需要也不接受额外的配置路径、依赖或环境参数。脚本检查文件后实际提交 Spark 作业，
配置中的相对依赖路径按工程根目录解析。一个命令只运行一个作业，不会自动补跑上游。

仓库中的示例工程可以这样使用：

```bash
cd spark-dbt-example
spark-sql -f tests/sql/setup.sql
./bin/start_pipeline_job.sh dwd user
```

示例表需要连接相同的 Hive metastore。`INSERT INTO` 会追加写入，重复测试前按
[示例说明](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/README.md)处理测试数据。

## 编写 SQL 和作业配置

`models/dws/order.sql` 示例：

```sql
INSERT INTO TABLE warehouse_dws.order_summary
SELECT
    order_date,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM warehouse_dwd.order_detail
WHERE amount > 0
GROUP BY order_date, region;
```

`config/dws/order.yaml` 示例：

```yaml
name: dws_order
job-type: spark-sql
configuration:
  spark.sql.shuffle.partitions: "200"
spark-submit:
  master: yarn
  deploy-mode: cluster
  queue: default
  driver.memory: 1g
  executor.num: "2"
  executor.memory: 2g
  executor.cores: "2"
```

目标表应提前创建，字段顺序和类型与输出一致。上游数据应在启动前就绪。
作业资源和队列在 YAML 中维护，SQL 参数写入 `configuration`；SQL 中的 `SET` 会被忽略。
单文件模式不需要配置 `transformation`。同文件内多条语句按声明顺序执行，应先定义中间视图，再使用它。

## 维护当前环境配置

`properties/application.properties` 由项目组按当前部署环境维护，启动时自动加载：

```properties
spark.yarn.submit.waitAppCompletion=true
spark.jars=lib/spark-connector-1.0.jar
# 按环境需要配置：
# spark.files=/etc/hive/conf/hive-site.xml
```

环境属性提供公共 Spark 提交默认值，作业 YAML 的同名提交配置优先。
连接器加入启动进程类路径，并通过 `spark.jars` 分发给 Spark；在作业中单独指定依赖列表时，
应保留所需连接器。SIT、UAT 等部署使用相同文件名，启动命令不传环境参数。

`application-sit.properties`、`application-uat.properties` 则用于 DS 同步，配置 DS 地址、项目、
Worker Group、告警及部署路径等，不作为业务作业的公共 Spark 参数传入。

## 批量调度

SQL 作业之间通过持久化表传递数据。工作流内部依赖由 SQL 读写表关系生成，调度信息来自
`dbt_project.yaml`、`config/<工作流>/flow.yaml` 及分环境 DS 配置：

```bash
./bin/ds-sync.sh generate . ads sit
./bin/ds-sync.sh apply . ads sit
```

工作流同步使用四个位置参数：`generate/apply projectHome workflowHome env`，环境标识必须显式填写。`generate` 离线生成并校验文件；`apply` 重新生成并校验，
验证目标项目后新增带环境、时间戳和唯一后缀的工作流，并配置定时策略。
每次发布新建版本，不覆盖已有工作流定义。

DS 节点统一为 SHELL，直接使用同一个启动入口：

```bash
cd /opt/spark/my-sql-project
./bin/start_pipeline_job.sh dws order
```

当前只处理选定工作流内部的作业依赖，跨工作流的数据就绪关系需另行安排。
部署时保留工程结构，并确保启动脚本可执行、运行包完整及公共环境配置有效。

## Hudi MOR 维护作业

Hudi 维护沿用相同启动方式：

```bash
./bin/start_pipeline_job.sh sdf_compaction mor_table
```

对应配置使用 `job-type: hudi-compaction`，SQL 文件列出 `RUN COMPACTION ON schema.table;`。
当前入口执行已有计划，成功后继续 clean 和 archive，运行环境需支持对应维护 SQL。

## 详细文档

- [开发者手册：项目结构、SQL/YAML、测试与调度](/Users/ywzhang/github_project/spark_sdp/docs/SQL_SDP_DEVELOPER_GUIDE.md)
- [SQL 工程示例与测试数据](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/README.md)
- [框架说明](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/docs/FRAMEWORK.md)
- [DolphinScheduler 工作流生成与发布](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/docs/DOLPHINSCHEDULER.md)
- [Hudi MOR 维护样例](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/docs/HUDI-COMPACTION.md)
