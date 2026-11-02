# DolphinScheduler 工作流生成与发布

本功能对接本地 DolphinScheduler 仓库的 **3.4.3-SNAPSHOT** 原生 REST API。
每次处理一个指定工作流，支持普通 Spark SQL 和 `hudi-compaction` 维护任务。
业务工程通过平台提供的脚本生成和发布工作流。

## 使用方式

使用平台提供的 SQL 工程，启动脚本、运行包及所需运行环境由平台准备。
以下命令在 SQL 工程根目录执行。
`bin/` 脚本和 `lib/` JAR 由平台组支持；项目组维护 `dbt_project.yaml`、`properties/`、
`config/` 及其他业务目录，负责本项目的环境、工作流、定时和告警配置。

生成阶段需要 Spark 3.3.1 / Scala 2.12 的解析器类库，但不会创建 SparkContext、连接 Hive、
执行 SQL 或访问 DS。无需配置 DS Token，也不要求目标项目已经创建。

```bash
./bin/ds-sync.sh generate . ads sit
./bin/ds-sync.sh generate . ads uat
./bin/ds-sync.sh generate . dwd sit
./bin/ds-sync.sh generate . sdf_compaction sit
```

入口参数固定为：

```text
ds-sync.sh generate/apply projectHome workflowHome env
```

| 参数 | 含义 | 示例 |
| --- | --- | --- |
| operation | generate 离线生成，apply 创建并发布新工作流 | `generate` |
| projectHome | SQL 工程根目录，可使用相对或绝对路径 | `.`、`/opt/spark/my-project` |
| workflowHome | models 下的分组名，直接定位 models/<group> 和 config/<group> | `ads`、`dws/trade` |
| env | 逻辑环境标识，选择 application-<env>.properties | `sit`、`uat`、`prd` |

四个参数都必须填写，不设置默认工程或默认环境。缺少环境配置文件时直接报错，不回退其他环境。
例如 `generate /opt/spark/my-project ads sit` 读取
`/opt/spark/my-project/properties/application-sit.properties` 和 `config/ads/flow.yaml`。
可在任意工作目录通过启动脚本的绝对路径运行，projectHome 决定加载哪一个工程。

## 配置职责

| 位置 | 内容 |
| --- | --- |
| `properties/application.properties` | 当前部署环境的公共 Spark 配置，由作业启动脚本自动读取 |
| `properties/application-sit.properties` | SIT 的地址、项目编码、API Token、Worker 路径等 |
| `properties/application-uat.properties` | UAT 对应配置，与 SIT 隔离 |
| `dbt_project.yaml` | 工程名称、DS 项目配置引用、公共调度默认值 |
| `config/<工作流>/flow.yaml` | 工作流名称、描述、定时及调度覆盖值 |
| `config/<工作流>/<作业>.yaml` | Spark 运行配置，可附加 `dolphinscheduler` 节点配置 |

项目配置固定读取工程根目录的 `dbt_project.yaml`。
properties 按 UTF-8 读取，`${属性名}` 仅引用当前 env 对应的 Properties 文件中的属性，不回退操作系统环境变量。
支持嵌套引用，缺失属性、引用循环、重复 YAML 键、无效配置均报告错误。
分环境的 `application-sit.properties` 等用于 DS 同步，不会作为业务 Spark 参数传入。
业务作业固定使用 `properties/application.properties` 作为当前部署环境配置。

### 环境配置

```properties
ds.api.url=http://ds-sit:12345/dolphinscheduler
ds.project.code=123456789
ds.project.name=spark_sql_example
ds.api.token=
ds.worker.group=default
ds.tenant.code=default
ds.environment.code=-1
ds.warning.type=NONE
ds.warning.group.id=0
ds.schedule.enabled=false
spark.project.dir=/opt/spark/spark-dbt-example
```

`ds.api.url` 包含服务上下文路径，不包含 Token、查询参数或用户密码。
项目、Worker Group、租户、环境和告警组需预先在 DS 中准备好并授权给 Token 对应用户。
项目编码为已有 DS 项目的 code，不是数据库自增 id。

仓库示例配置的 `ds.project.code=0` 仅用于离线预览，`apply` 要求真实的正整数编码。
`ds.project.name` 在发布时与远端项目名称核对，避免误发到编码配置错误的项目。
默认告警类型为 `NONE`；启用 `FAILURE`、`SUCCESS` 或 `ALL` 时，必须配置有效的正数告警组 id。

默认 `ds.schedule.enabled=false`，意味着创建停用状态的新定时，并在成功发布时停用旧版本定时。
需要自动运行时，显式改为 `true`。也可以在单个 `flow.yaml` 中覆盖 `schedule.enabled`。

Token 由所选环境 Properties 的 `ds.api.token` 提供，离线生成可留空，发布前填写实际值。
Token 不写入工作流、定时、清单或发布结果文件，入口也不接受系统环境变量转发参数。

```bash
./bin/ds-sync.sh apply . ads sit
```

### 工程与工作流配置

`dbt_project.yaml` 引用环境属性：

```yaml
name: spark_dbt_example
dolphinscheduler:
  api-profile: ds34
  api-url: ${ds.api.url}
  project-code: ${ds.project.code}
  project-name: ${ds.project.name}
  deployment-dir: ${spark.project.dir}
defaults:
  worker-group: ${ds.worker.group}
  tenant-code: ${ds.tenant.code}
  environment-code: ${ds.environment.code}
  warning-type: ${ds.warning.type}
  warning-group-id: ${ds.warning.group.id}
  execution-type: SERIAL_WAIT
  priority: MEDIUM
  fail-retry-times: 0
  fail-retry-interval: 1
  timeout-minutes: 60
```

`config/ads/flow.yaml` 示例：

```yaml
name: spark_dbt_ads
description: ADS region value aggregation
schedule:
  enabled: ${ds.schedule.enabled}
  cron: "0 20 1 * * ?"
  timezone: Asia/Shanghai
  start-time: "2026-01-01 00:00:00"
  end-time: "2099-12-31 23:59:59"
```

Cron 使用 Quartz 格式，通过 Quartz 解析器验证；时区必须是有效的 Java 时区 id，
起止时间必须加引号，格式为 `yyyy-MM-dd HH:mm:ss`，且开始时间早于结束时间。
每次发布都创建定时，`enabled` 决定最终是否启用，因此停用状态也需要完整的定时配置。

节点设置按“工程 defaults → flow.yaml 顶层 → 作业 YAML 的 dolphinscheduler”覆盖。
支持 `worker-group`、`environment-code`、`priority`、重试次数、重试间隔（分钟）和超时（分钟）。
工作流执行模式默认 `SERIAL_WAIT`，防止同一版本的工作流实例并发；不同版本仍由定时切换控制。
示例为追加写入和维护任务，默认不自动重试；改动重试次数前需评估写入是否幂等。

## 当前工作流内的依赖

`generate . ads sit` 仅加载 `models/ads/` 下由 `config/ads/flow.yaml` 管理的模型，
并读取同目录结构的作业 YAML。更深目录如果有自己的 `flow.yaml`，归属于独立工作流，不包含在本次生成中。
其他工作流中无效的 SQL 或缺失配置不会影响本次生成。

生成器使用 Spark SQL 未解析语法树提取输入表，包括 CTE、子查询和表达式子查询，
忽略字符串中的伪表名以及文件内部已声明的临时视图引用。
持久化目标表映射到生产作业，形成当前工作流的有向无环图：

- 输入表由同工作流的另一个作业生产：生成前后置连线。
- 输入表在当前工作流没有生产者：记录为外部输入，不自动启动或等待其他工作流。
- 多个作业写同一持久化目标或依赖成环：拒绝生成。

对未完整限定的表名，使用作业 YAML 的 `database`、可选 `catalog` 补齐后进行匹配。
本工具沿用默认不区分大小写的表名匹配约定，建议跨文件显式使用相同的数据库限定名。
模型声明语法仍须是现有运行入口支持的范围；例如 CTE 可放在 `INSERT INTO target WITH ... SELECT ...`
中，不能依赖运行入口未支持的任意 SQL 声明或模板宏。

必要时可以补充显式的同工作流作业依赖，自动识别的依赖仍然保留：

```yaml
# config/dwd/order_detail.yaml 中可选的附加内容
dolphinscheduler:
  depends-on:
    - dwd/user_profile
```

显式依赖必须指向当前工作流里的完整作业标识。
跨工作流依赖本次不实现；DWS/ADS 的输入表需由外部调度或操作方保证数据就绪。
错开定时分钟数不提供跨工作流的成功依赖保证。

## 新建版本与定时切换

每次 `apply . ads sit` 先重新生成、校验，再创建全新的工作流和任务，不更新、删除旧定义。
名称格式为：

```text
<逻辑名称>__<环境>__yyyyMMdd_HHmmss_SSS__<8位随机码>
spark_dbt_ads__sit__20261026_140000_123__a7c93e21
```

时间戳使用工作流的时区。即使配置不变或短时间连续执行 `apply`，也会创建新的版本。
任务编码使用 DS 原生接口重新分配，不共享旧版本的节点。

发布步骤：

1. 检查目标 DS 项目编码和名称，查询本工具管理的旧版本及在线定时。
2. 通过 DS 原生接口分配全新的任务编码，写回本次 JSON。
3. 创建新工作流。
4. 发布新工作流。这版 DS 要求工作流 ONLINE 后才允许创建定时。
5. 创建新定时，DS 初始状态为 OFFLINE；旧版本定时仍然保持运行。
6. 停用当前逻辑工作流、环境和项目对应的旧版本在线定时。
7. 根据配置启用新定时，或者保持新定时 OFFLINE，保存发布结果。

通过说明字段里的所有权标记识别历史版本，标记包含工程、环境、目录和 DS 项目编码的摘要。
同名或相似前缀的人工作业没有该标记，不会被停用；其他环境和其他逻辑工作流也不会被操作。
更改工程名称或项目编码会改变所有权标识，旧标识下的定时需单独检查。

当前 DS 源码已没有文件上传式工作流导入接口。本工具将生成文件转换为原生
`workflow-definition` 创建接口的表单，使用 `schedules` 接口创建定时，未修改 DS 代码或数据库。

| API | 用途 |
| --- | --- |
| `GET /projects/{code}` | 验证目标项目 |
| `GET /projects/{code}/workflow-definition/simple-list` | 查找历史版本 |
| `GET /projects/{code}/workflow-definition/{workflowCode}` | 检查所有权 |
| `GET /projects/{code}/task-definition/gen-task-codes` | 分配新任务编码 |
| `POST /projects/{code}/workflow-definition` | 创建新工作流 |
| `POST /projects/{code}/workflow-definition/{workflowCode}/release` | 发布新工作流 |
| `GET /projects/{code}/schedules` | 分页读取旧定时 |
| `POST /projects/{code}/schedules` | 创建新定时 |
| `POST /projects/{code}/schedules/{id}/online`、`offline` | 切换定时 |

## 产物和失败恢复

```text
target/dolphinscheduler/sit/ads/<发布编号>/
  workflow.json
  schedule.json
  manifest.json
  deployment-result.json        # 仅 apply 创建
```

`workflow.json` 使用原生 DagData 的结构；`schedule.json` 是创建定时的表单字段。
离线生成的工作流编码为 `0`、任务编码是局部占位编号；`apply` 会重建产物并替换为原生分配的编码。
`manifest.json` 包含环境、工作流、内部依赖、外部输入及 SQL SHA-256，供审核追踪。
输出目录由发布编号隔离，不覆盖此前的发布文件。

发布结果逐阶段落盘，记录工作流编码、定时 id、旧定时 id、失败位置及补偿结果。
API 超时后不自动重试创建请求，避免一次未知成功的请求创建多个对象。

如果切换失败，先停用并确认新定时，再恢复本次尝试停用的旧定时。
无法确认新定时已停用时，不恢复旧定时，避免两个版本重复调度，并标记 `manualCheckRequired=true`。
创建接口响应丢失时也会标记人工检查，可按记录中的唯一工作流名称在 DS 查找实际创建结果。

DS 多次 API 调用不是整体事务。旧版本已运行的实例不会被终止，更新应避开定时触发点。
同机的相同工程、环境、工作流使用文件锁阻止并发 apply；多机/多份 checkout 发布需在 CI 中
设置同一逻辑工作流的发布互斥，当前不提供跨主机分布式发布锁。

发布只配置调度，不上传 SQL 工程或 JAR。执行前必须将工程、运行 JAR 和所需环境部署到
所有目标 Worker 的 `spark.project.dir`。旧工作流同样引用该路径，工作流历史版本不等于 SQL
文件快照；若需回退 SQL 代码，需另行使用不可变部署目录或恢复对应文件。

## Spark 执行状态

DS 节点的 `taskType` 固定为 `SHELL`，普通 SQL 和 Hudi compaction 都使用同一种调度类型。
SQL 解析负责确定作业输入、输出和当前工作流内部的依赖；业务类型由作业 YAML 的 `job-type`
决定，不转换为 DS 的 Spark 或 SQL 节点。

生成的任务脚本只设置工作目录并调用统一入口，例如：

```sh
#!/bin/sh
set -eu
cd '/opt/spark/spark-dbt-example'
./bin/start_pipeline_job.sh dws order
```

对应普通 DWD 作业使用 `./bin/start_pipeline_job.sh dwd user`，维护作业使用
`./bin/start_pipeline_job.sh sdf_compaction mor_table`。多级业务目录保持完整，例如
`./bin/start_pipeline_job.sh dws/trade order`。

手工执行和 DS 调度共用同一入口，只传分层和作业名。脚本自动加载 `lib/` 中的框架运行包和连接器、
同名 SQL/YAML 以及 `properties/application.properties`。
部署时保留工程目录结构，并确保启动脚本具备执行权限。

环境文件默认设置 `spark.yarn.submit.waitAppCompletion=true`；同步工具拒绝环境或作业 YAML 中的
异步提交设置，当前要求 YARN cluster 模式。入口返回实际执行状态，下游等待上游完成后启动。

Hudi 维护任务的 SQL 是表清单，使用现有 `hudi-compaction` 配置加载器校验并列为外部输入；
不会当成 `INSERT` 或建表语句解析。详情见 [Hudi Compaction](HUDI-COMPACTION.md)。
