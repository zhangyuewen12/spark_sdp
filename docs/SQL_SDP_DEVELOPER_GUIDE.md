# Spark SQL 作业开发者手册

本手册面向数仓和 Spark SQL 作业开发人员，说明如何在 SQL 工程中新增作业、配置运行资源、
验证结果和配置批量调度。以下以已准备好启动脚本和运行包的独立工程 `my-sql-project` 为例，
所有命令均在该工程根目录执行。`spark-dbt-example` 可以作为项目模板。

开发前从平台组获取 SQL 工程模板和配套运行包；Java、Spark、Hadoop/Hive 连接等提交环境由平台准备。
`bin/` 下的脚本和 `lib/` 下的 JAR 由平台组提供和支持，缺失或需要调整时联系平台组。
业务开发从编写 SQL 和对应 YAML 开始。

一个 SQL 文件对应一个独立 Spark 作业，一份同名 YAML 管理该作业的运行配置。
除 `bin/`、`lib/` 外，其他目录及工程配置文件由项目组自行维护。

## 1. 开发者项目的目录结构

```text
my-sql-project/
├── models/                             # 业务 SQL，一个文件一个作业
│   ├── dwd/
│   │   └── user.sql
│   ├── dws/
│   │   ├── order.sql                   # 用户订单汇总
│   │   └── region_sales.sql            # 基于 order 继续加工
│   ├── ads/
│   │   └── region_value.sql
│   └── sdf_compaction/
│       └── mor_table.sql               # Hudi MOR 维护表清单
├── config/                             # 与 models 保持相同目录和文件名
│   ├── dwd/
│   │   ├── user.yaml
│   │   └── flow.yaml
│   ├── dws/
│   │   ├── order.yaml
│   │   ├── region_sales.yaml
│   │   └── flow.yaml                   # DWS 工作流及定时配置
│   ├── ads/
│   │   ├── region_value.yaml
│   │   └── flow.yaml
│   └── sdf_compaction/
│       ├── mor_table.yaml
│       └── flow.yaml
├── properties/
│   ├── application.properties          # 当前部署环境的公共 Spark 配置，启动时自动加载
│   ├── application-sit.properties      # SIT 的 DS 地址、项目和调度环境
│   └── application-uat.properties      # UAT 对应配置
├── tests/sql/
│   ├── setup.sql                       # 测试目标表初始化
│   └── verify.sql                      # 结果校验 SQL
├── docs/                               # 业务口径、数据依赖等说明
├── dbt_project.yaml                    # DS 项目信息及公共调度默认值
├── lib/                                # 平台组支持：运行包和连接器依赖
│   ├── spark-sdp-1.0.jar               # 框架运行包
│   └── spark-connector-1.0.jar         # 连接器依赖
└── bin/                                # 平台组支持：启动脚本
    ├── start_pipeline_job.sh
    └── ds-sync.sh
```

| 位置 | 负责团队 | 维护内容 |
| --- | --- | --- |
| `bin/` | 平台组 | 提供、维护并支持作业启动和工作流同步脚本 |
| `lib/` | 平台组 | 提供、维护并支持框架运行包和连接器 JAR |
| `models/` | 项目组 | 开发业务 SQL、设计输入输出和加工逻辑 |
| `config/` | 项目组 | 维护作业资源、工作流、定时和告警配置 |
| `properties/` | 项目组 | 维护当前环境的公共 Spark 配置及各环境的 DS 地址、项目和部署路径 |
| `tests/`、`docs/` 及其他业务目录 | 项目组 | 维护测试数据、结果校验、业务说明及项目自建内容 |
| `dbt_project.yaml`、项目 README 等工程文件 | 项目组 | 维护项目信息、公共调度配置和使用说明 |

`bin/` 和 `lib/` 保持同级。运行包、连接器、SQL、作业配置和当前环境配置均由启动脚本自动定位。
项目组维护 `properties/application.properties`，其中 `spark.jars` 指定需要分发的连接器依赖，
默认包含 `lib/spark-connector-1.0.jar`。执行命令只接收分层和作业名两个参数。

DWD、DWS、ADS 目录表达数仓层次；也可以继续按业务域拆分，例如 `models/dws/trade/`。
新增层级时，`config/` 必须保持相同结构：

| 启动参数 | SQL 文件 | 作业配置 |
| --- | --- | --- |
| `dws order` | `models/dws/order.sql` | `config/dws/order.yaml` |
| `dws region_sales` | `models/dws/region_sales.sql` | `config/dws/region_sales.yaml` |
| `dws/trade order` | `models/dws/trade/order.sql` | `config/dws/trade/order.yaml` |

建议目录和文件名使用小写字母、数字及下划线。作业名不包含 `/`，多级目录放在第一个启动参数中。
`flow.yaml` 保留给工作流配置，不作为业务作业的 YAML 文件名。

## 2. 新增一个作业

新增作业的基本步骤：

1. 确定所属分层、作业名、输入表、目标表及字段口径。
2. 创建 `models/<分层>/<作业名>.sql`。
3. 创建 `config/<分层>/<作业名>.yaml`。
4. 在测试环境准备目标表和输入数据，单独运行并核对结果。
5. 配置所属工作流，生成调度文件并检查依赖，再发布到指定环境。

### 2.1 编写 SQL：models/dws/order.sql

本例读取已就绪的 `warehouse_dwd.order_detail`，按日期、用户和地区汇总支付订单。
源表需要包含 `order_time`、`user_id`、`region`、`amount`、`order_status` 字段。

```sql
INSERT INTO TABLE warehouse_dws.order_summary
SELECT
    CAST(order_time AS DATE) AS order_date,
    user_id,
    region,
    COUNT(*) AS order_count,
    CAST(SUM(amount) AS DECIMAL(18, 2)) AS total_amount
FROM warehouse_dwd.order_detail
WHERE order_status = 'PAID'
  AND amount > 0
  AND user_id IS NOT NULL
GROUP BY CAST(order_time AS DATE), user_id, region;
```

SQL 中显式使用 `数据库.表名`，确保上下游引用一致。
目标表应提前创建，字段顺序和类型应与 `SELECT` 输出一致；金额类字段建议使用 `DECIMAL`。
SQL 文件使用 UTF-8，每条语句以分号结束。

### 2.2 编写配置：config/dws/order.yaml

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

| 字段 | 开发时如何设置 |
| --- | --- |
| `name` | 填写便于识别的作业名称，例如 `dws_order`，用于 Spark/YARN 应用名称 |
| `job-type` | 普通业务 SQL 填 `spark-sql` |
| `database` | 可选，设置未带库名前缀的表使用的默认数据库 |
| `configuration` | 填写 Spark SQL / 会话参数，例如 shuffle 分区数 |
| `spark-submit.queue` | 使用当前环境已授权的 YARN 队列 |
| `spark-submit.driver.memory` | driver 内存 |
| `spark-submit.executor.num` | executor 数量 |
| `spark-submit.executor.memory` | 每个 executor 的内存 |
| `spark-submit.executor.cores` | 每个 executor 的核数 |

以上资源数值仅为示例，应按数据量调整。单作业 YAML 应包含独立完整的运行配置，
`dbt_project.yaml` 的调度默认值不会自动补充这些 Spark 资源参数。
单文件作业无需填写 `transformation`。

### 2.3 SQL 的编写范围

普通作业主要使用 `INSERT INTO [TABLE] ... SELECT ...`。
同一个文件内需要中间结果时，可以先声明 `CREATE TEMPORARY VIEW ... AS SELECT ...`，
再在后续语句中使用；文件内语句应按实际执行顺序排列。

当前还支持 `CREATE MATERIALIZED VIEW ... AS SELECT ...` 和 `TRUNCATE TABLE`。
跨作业传递数据使用持久化表，临时视图仅属于当前作业会话。

当前入口不是任意 SQL 脚本的透传工具，以下内容需要特别留意：

- 建库、建表和独立查询放在测试或初始化脚本中，通过 `spark-sql` 单独执行。
- `INSERT OVERWRITE`、`MERGE` 等语句尚未支持，不能直接作为模型提交。
- SQL 中的 `SET` 会被忽略，Spark 参数写入作业 YAML。
- 不支持 dbt 的 `ref()`、Jinja 和宏；SQL 直接使用真实表名，不会自动替换日期等模板变量。
- `INSERT INTO` 为追加写入，重跑可能产生重复数据。开发时明确批次和重跑方案，启用自动重试前确认写入可安全重做。

## 3. 开发有上下游依赖的作业

继续新增一个地区销售汇总，读取上一作业的输出表。

### 3.1 models/dws/region_sales.sql

```sql
INSERT INTO TABLE warehouse_dws.region_sales
SELECT
    order_date,
    region,
    COUNT(DISTINCT user_id) AS user_count,
    SUM(order_count) AS order_count,
    CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_amount
FROM warehouse_dws.order_summary
GROUP BY order_date, region;
```

### 3.2 config/dws/region_sales.yaml

```yaml
name: dws_region_sales
job-type: spark-sql
configuration:
  spark.sql.shuffle.partitions: "100"
spark-submit:
  master: yarn
  deploy-mode: cluster
  queue: default
  driver.memory: 1g
  executor.num: "2"
  executor.memory: 2g
  executor.cores: "2"
```

这两个作业通过表建立依赖：

```text
warehouse_dwd.order_detail（外部输入）
    → dws/order
    → warehouse_dws.order_summary
    → dws/region_sales
    → warehouse_dws.region_sales
```

`start_pipeline_job.sh` 每次只运行一个作业，不会自动运行上游。手动测试时先确认 `order` 成功，
再运行 `region_sales`。生成 DWS 工作流时，工具通过写入表与读取表识别出
`dws/order → dws/region_sales` 的前后置关系，不需要依靠文件名数字前缀排序。

依赖识别仅覆盖本次选择的工作流。此例的 DWD 输入属于外部输入，需要另外保证就绪；
仅将 DWD 定时设得更早，不能保证 DWS 一定在其成功后运行。
一个工作流内应避免多个作业写同一目标表，依赖也不能成环，否则生成会报错。

## 4. 单作业运行和测试

### 4.1 通过分层和作业名运行

执行作业只需要：

```bash
./bin/start_pipeline_job.sh dws order
```

脚本自动加载以下文件：

| 内容 | 对应文件 |
| --- | --- |
| 框架运行包 | `lib/spark-sdp-1.0.jar` |
| 连接器 | `lib/spark-connector-1.0.jar` |
| 本次执行的 SQL | `models/dws/order.sql` |
| 本次作业配置 | `config/dws/order.yaml` |
| 当前部署环境配置 | `properties/application.properties` |

命令会实际提交一个 Spark 作业，不接受额外的文件、资源或环境参数。
文件缺失、不可读或为空时直接报错。脚本按自身位置定位工程，配置中的相对依赖路径按工程根目录解析。

当前环境默认等待 YARN 作业完成并返回状态。连续测试有依赖的作业时，先确认上游成功，再执行下游：

```bash
./bin/start_pipeline_job.sh dws region_sales
```

### 4.2 通过文件配置环境、资源和依赖

在 `properties/application.properties` 中维护当前部署环境的公共 Spark 参数：

```properties
spark.yarn.submit.waitAppCompletion=true
spark.jars=lib/spark-connector-1.0.jar
# 按当前环境需要配置 Hive 文件：
# spark.files=/etc/hive/conf/hive-site.xml
```

项目部署到 SIT、UAT 等环境时，由项目组维护该环境下的同名文件，启动命令始终不变。
脚本不自动选择 SIT/UAT，也不增加环境参数。文件采用 UTF-8，属性作为 Spark 提交默认值。
连接器的 JAR 由平台组提供，`spark.jars` 应保留连接器路径；额外依赖按平台提供的实际文件配置。

每个作业的队列、driver/executor 资源和作业级 Spark 参数在 `config/<分层>/<作业名>.yaml` 中调整。
作业 YAML 的同名提交配置优先于环境默认值；运行时 `configuration` 仍按作业 YAML 应用。
如在作业的 `spark-submit.jars` 中单独指定依赖列表，应同时包含所需连接器。

`application-sit.properties`、`application-uat.properties` 用于 DS 同步，
由 `ds-sync.sh` 的环境参数选择，与本节固定加载的 `application.properties` 分工不同。

### 4.3 准备测试表并校验结果

按实际表规范在测试库创建源表和目标表，将初始化语句放入 `tests/sql/setup.sql`。
使用与作业相同的 Hive metastore 执行：

```bash
spark-sql -f tests/sql/setup.sql
```

`tests/sql/verify.sql` 可针对上述样例检查粒度和金额：

```sql
-- 目标粒度应唯一，预期返回零行。
SELECT order_date, user_id, region, COUNT(*) AS duplicate_count
FROM warehouse_dws.order_summary
GROUP BY order_date, user_id, region
HAVING COUNT(*) > 1;

-- 比较上下游订单数与金额，应保持一致。
SELECT SUM(order_count) AS order_count, SUM(total_amount) AS total_amount
FROM warehouse_dws.order_summary;

SELECT SUM(order_count) AS order_count, SUM(total_amount) AS total_amount
FROM warehouse_dws.region_sales;
```

```bash
spark-sql -f tests/sql/verify.sql
```

这些查询是基础校验，应结合业务增加过滤条件、空值、边界金额及预期明细的断言。
样例按全表追加，重复测试前需要按测试环境约定清理对应数据；生产重跑遵循作业的数据处理方案。

## 5. 配置工作流和环境

### 5.1 properties/application-sit.properties

```properties
ds.api.url=http://ds-sit:12345/dolphinscheduler
ds.project.code=123456789
ds.project.name=my_sql_project
ds.api.token=
ds.worker.group=default
ds.tenant.code=default
ds.environment.code=-1
ds.warning.type=NONE
ds.warning.group.id=0
ds.schedule.enabled=false
spark.project.dir=/opt/spark/my-sql-project
```

将地址、项目编码和名称替换为当前环境的实际值。DS 项目、Worker Group、租户等应事先准备好。
`ds.project.code` 使用 DS 的项目 code，不能使用数据库自增 id。
`spark.project.dir` 是 DS Worker 上的工程绝对路径，各执行节点都需要能访问对应工程。

`ds.api.token` 填写当前 DS 环境的 API Token；离线生成时可以留空，发布前填写实际值。
程序从所选 Properties 文件读取，不读取操作系统环境变量，Token 不写入生成产物。UAT 等环境使用同结构的 `application-uat.properties`。
这些环境配置用于工作流同步；业务 SQL 的表名和作业资源不会随 `env` 参数自动替换。

### 5.2 dbt_project.yaml

```yaml
name: my_sql_project
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

该文件管理项目及公共调度默认值。`${...}` 在 DS 配置加载时引用环境 Properties 中的属性，
不是业务 SQL 模板功能。普通业务作业仍通过各自 YAML 设置 Spark 参数。

### 5.3 config/dws/flow.yaml

```yaml
name: my_sql_dws
description: 用户订单与地区销售汇总
schedule:
  enabled: ${ds.schedule.enabled}
  cron: "0 20 1 * * ?"
  timezone: Asia/Shanghai
  start-time: "2026-01-01 00:00:00"
  end-time: "2099-12-31 23:59:59"
```

上述 Quartz cron 表示每天上海时间 01:20 触发，需结合外部输入的就绪时间安排。
起止时间加引号，格式为 `yyyy-MM-dd HH:mm:ss`，开始时间必须早于结束时间。
即使 `enabled=false`，也必须填写完整、合法的定时配置。

`models/dws/` 下的作业由 `config/dws/flow.yaml` 管理。
若 `dws/trade` 有独立的 `config/dws/trade/flow.yaml`，它属于单独工作流，不包含在父工作流生成结果中。

### 5.4 覆盖单作业的调度配置

调度节点配置按“项目 defaults → flow.yaml 顶层 → 作业 YAML 的 dolphinscheduler”覆盖。
例如在 `config/dws/region_sales.yaml` 末尾追加：

```yaml
dolphinscheduler:
  worker-group: default
  priority: HIGH
  fail-retry-times: 0
  timeout-minutes: 90
  depends-on:
    - dws/order
```

表依赖已识别时不需要重复填写 `depends-on`。需要额外执行顺序时可显式补充，
但必须填写当前工作流内的完整作业标识，不能引用其他工作流。

告警可在项目环境配置中设置 `ds.warning.type=FAILURE`，并将 `ds.warning.group.id`
改为有效的正数告警组 id。重试间隔和超时的单位为分钟；追加写入作业应先确认可重跑，再增加重试次数。

## 6. 生成与发布工作流

入口使用四个位置参数：

```text
ds-sync.sh generate/apply projectHome workflowHome env
```

| 参数 | 说明 | 示例 |
| --- | --- | --- |
| generate/apply | 离线生成，或创建并发布新工作流 | `generate` |
| projectHome | SQL 工程根目录，可为相对或绝对路径 | `.`、`/opt/spark/my-project` |
| workflowHome | models 下的分组名，直接定位 models/<group> 和 config/<group> | `dws`、`dws/trade` |
| env | 选择 application-<env>.properties 的环境标识 | `sit`、`uat`、`prd` |

先生成本次工作流：

```bash
./bin/ds-sync.sh generate . dws sit
```

`env` 是第四个位置参数，必填；必须存在 `projectHome/properties/application-<env>.properties`。
`generate` 只处理指定工作流，不连接 DS、不执行 SQL，也不验证 Hive 表和数据是否已经就绪。
它还会读取 `properties/application.properties`，拒绝将 YARN 等待完成设置为 `false`。

产物位于：

```text
target/dolphinscheduler/sit/dws/<版本目录>/
├── workflow.json     # 工作流节点与依赖
├── schedule.json     # 定时策略
└── manifest.json     # 作业、输入输出、上游依赖及外部输入清单
```

检查两个样例之间是否存在 `dws/order → dws/region_sales`，外部输入、Worker 路径、
定时、队列及资源是否符合当前环境。然后将同一份已验证的 SQL 工程部署到 Worker 的工程路径。
工作流运行时从该路径读取 SQL/YAML，生成的工作流不携带可执行的 SQL 工程快照。

发布到 DS：

```bash
./bin/ds-sync.sh apply . dws sit
```

`apply` 会重新生成并校验，验证目标 DS 项目后创建一个带环境、时间戳及唯一后缀的新工作流，
配置定时并切换本工具管理的历史版本定时。它每次新建版本，不覆盖原工作流定义。
`schedule.enabled=false` 时新定时保持停用，成功发布仍会停用对应历史版本的定时。

DS 中的节点统一为 **SHELL**，例如：

```bash
cd /opt/spark/my-sql-project
./bin/start_pipeline_job.sh dws order
```

开发者通过 SQL/YAML 配置业务作业，不需要在 DS 节点中手写 `spark-submit`。
当前只管理所选工作流内部的依赖，不会自动建立跨工作流的调度关系。

## 7. 可选：开发 Hudi MOR 维护作业

维护作业同样使用一份 SQL 和一份 YAML，SQL 作为待维护表清单。例如
`models/sdf_compaction/mor_table.sql`：

```sql
RUN COMPACTION ON warehouse_dwd.user_mor;
RUN COMPACTION ON warehouse_dwd.order_mor;
```

对应 YAML 使用 `job-type: hudi-compaction`，并配置实际 Spark 资源。
每条语句填写 `schema.table` 并以分号结束；表清单不使用反引号，不填写 instant。

```bash
./bin/start_pipeline_job.sh sdf_compaction mor_table
./bin/ds-sync.sh generate . sdf_compaction sit
```

运行前确认表为 MOR、已有待执行计划，且没有其他作业并发维护相同表。
当前维护入口执行已有 compaction 计划，成功后继续 clean 和 archive，不自动生成新计划；
Spark 环境需要支持对应维护 SQL。配置样例和运行限制见
[Hudi MOR 维护说明](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/docs/HUDI-COMPACTION.md)。

## 8. 常见问题

| 现象 | 检查方式 |
| --- | --- |
| SQL 或 YAML 找不到 | 核对 `models/` 与 `config/` 的相对目录和同名规则 |
| 运行包或提交环境不可用 | 联系平台维护人员核对已提供的运行包和提交环境 |
| 运行时提示目标表不存在 | 提前建表，确认作业连接的 metastore、库名及访问权限 |
| 下游数据为空或不完整 | 确认上游已成功完成，核对输入批次和过滤条件 |
| 重跑后结果翻倍 | 检查追加写入及清理方案，核对上游是否也被重复写入 |
| 生成时没有期望的依赖 | 核对上下游是否属于同一个工作流，且读写表的完整名称一致 |
| 生成提示重复目标或循环依赖 | 拆清各作业的目标表职责，去除循环引用 |
| 未指定环境或找不到环境文件 | 填写第四个位置参数 `sit` 等，并准备对应 Properties 文件 |
| 发布成功但没有自动运行 | 核对 `schedule.enabled`、定时有效期与时区 |
| 换了 DS 同步的 `env` 仍读原来的业务表 | `env` 只选择 DS 同步配置文件，业务表名和 Spark 资源需在工程中单独配置 |

完整的关联 SQL 与测试数据见
[示例工程](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/README.md)，
工作流配置与发布细节见
[DolphinScheduler 使用说明](/Users/ywzhang/github_project/spark_sdp/spark-dbt-example/docs/DOLPHINSCHEDULER.md)。
