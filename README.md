# spark_platform

这个仓库现在是一个 Spark 3.3.x 上的 SQL-first 作业开发平台，包含两个 Maven module：

- `spark-core`：Spark 作业定义、SQL pipeline 编译、依赖分析、执行规划和任务执行逻辑
- `spark-example`：基于 `spark-core` 的 Spark 作业样例

目标是让开发人员基于：

- `spark-pipeline.yaml`
- `transformations/*.sql`

来开发批处理 Spark 作业，而不是直接编写 Java `main()`。

## 当前能力

- 支持从 `spark-pipeline.yaml` 加载项目配置
- 支持扫描一个或多个 SQL 目录
- 支持批处理 SQL 声明：
  - `CREATE MATERIALIZED VIEW ... AS SELECT ...`
  - `CREATE TEMPORARY VIEW ... AS SELECT ...`
  - `INSERT INTO [TABLE] target SELECT ...`
- 自动从 SQL 中抽取 `FROM` / `JOIN` 依赖并生成执行顺序
- 复用现有的依赖分析、拓扑规划和本地批执行器
- 支持生产提交和 IDEA 本地执行 pipeline
- 支持通过 Hive metastore 读取和写入 Hive 表

## 项目结构

一个最小 SQL 项目形态如下：

```text
my-pipeline/
  spark-pipeline.yaml
  transformations/
    000_seed_orders.sql
    010_clean_orders.sql
    020_daily_orders.sql
```

示例 `spark-pipeline.yaml`：

```yaml
name: sql_orders_pipeline
libraries:
  - transformations
configuration:
  spark.sql.shuffle.partitions: "1"
spark-submit:
  master: yarn
  deploy-mode: cluster
  queue: default
  executor.num: "1"
  executor.memory: 1g
  executor.cores: "1"
```

示例 SQL：

```sql
CREATE MATERIALIZED VIEW orders_clean AS
SELECT
  orderId,
  region,
  orderDate,
  amount,
  TO_DATE(orderDate) AS order_date
FROM orders_source
WHERE amount > 0;
```

```sql
CREATE MATERIALIZED VIEW daily_orders AS
SELECT
  region,
  order_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders_clean
GROUP BY region, order_date;
```

如果目标表已经由外部提前创建好，也可以直接写：

```sql
INSERT INTO TABLE daily_orders_sink
SELECT
  region,
  TO_DATE(orderDate) AS order_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders_source
WHERE amount > 0
GROUP BY region, TO_DATE(orderDate);
```

## 运行方式

生产提交入口（入口类 A）读取 Job 目录中的配置，生成 `spark-submit` 命令，并以 Yarn
cluster 模式启动入口类 B：

```bash
export SPARK_HOME=/path/to/spark
java -cp spark-core/target/spark-sdp-1.0.jar \
  com.bocom.rdss.spark.sdp3x.starter.SparkSubmitStarter \
  --spec examples/sql-batch-pipeline
```

其中入口类 B 是 `com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication`。在 IDEA 中
直接运行它并传入下面的参数，会自动使用 `local[*]`：

```text
--spec examples/sql-batch-pipeline
```

入口类 A 默认补充 `--master yarn --deploy-mode cluster`，读取 `driver.memory`、
`driver.cores`、`executor.memory`、`executor.cores`、`executor.num`、`queue` 以及
`conf.*` 等配置，并自动将整个 Job 目录通过 `--archives` 分发给 cluster driver。

编译和测试：

```bash
./mvnw test
```

打包一个可直接执行的 fat jar：

```bash
./mvnw package
```

查看提交入口帮助：

```bash
bin/spark-sdp.sh --help
```

提交到 Yarn 运行：

```bash
export SPARK_HOME=/path/to/your/spark

bin/spark-sdp.sh \
  --spec examples/sql-batch-pipeline
```

提交到 Yarn `cluster` 模式并通过 Hive metastore 读写 Hive 表时，建议把 `hive-site.xml`
通过 Spark 参数一起带上：

```bash
export SPARK_HOME=/path/to/your/spark

bin/spark-sdp.sh --spec examples/sql-hive-insert-pipeline
```

执行前先打包，并把产物复制到 `bin/` 目录，与脚本同级：

```bash
./mvnw -pl spark-core package
cp spark-core/target/spark-sdp-1.0.jar bin/
```

脚本只负责定位 Jar 并转发参数。`SparkSubmitStarter` 读取 YAML、打包 Job 目录并调用
`${SPARK_HOME}/bin/spark-submit`；Spark 入口类是 `SqlPipelineRunApplication`。

`database` 配置表示这条 pipeline 的默认数据库，效果等同于在执行所有 SQL 之前先做一次
`USE <database>`。如果 SQL 里已经显式写了库名，例如 `db1.orders_source` 或
`INSERT INTO TABLE db2.daily_orders_sink`，则以 SQL 自己写的库名为准。

本地调试时可以直接在 IDE 里运行 `com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication`
或 `com.bocom.rdss.spark.sdp3x.example.SqlPipelineLocalDebugMain`：

```text
--spec examples/sql-batch-pipeline --master local[*]
```

之所以同一个 main 同时支持本地和 `spark-sdp`，是因为 `spark-sdp` 提交时会额外传入
`--submitted` 标记，此时程序继承 `spark-submit` 创建好的 Spark 环境；直接本地执行 main 时没有这个标记，程序会自己创建 `local[*]` SparkSession。

运行带样例输入数据的 demo：

```bash
${SPARK_HOME}/bin/spark-submit \
  --master local[*] \
  --class com.bocom.rdss.spark.sdp3x.example.SqlBatchSdp3xExampleJob \
  bin/spark-sdp.sh-1.0.jar
```

开发人员中文使用样例见：

- [docs/SQL_SDP_DEVELOPER_GUIDE.md](/Users/ywzhang/github_project/spark_sdp/docs/SQL_SDP_DEVELOPER_GUIDE.md)

## 仓库内置示例

- SQL 项目示例：`examples/sql-batch-pipeline`
- Hive 插入模板：`examples/sql-hive-insert-pipeline`
- SQL demo 主类：`com.bocom.rdss.spark.sdp3x.example.SqlBatchSdp3xExampleJob`
- SQL 端到端测试：`SqlPipelineProjectRunnerTest`

## 设计说明

当前仓库仍然复用原有 Java SDP 内核：

- `PipelineBuilder` / `PipelineDefinition`
- `DefaultDependencyAnalyzer`
- `TopologicalPipelinePlanner`
- `LocalPipelineExecutor`

SQL 层做的事情，是把项目目录里的 SQL 声明编译成现有的 dataset / flow 模型。

## 已知边界

- 当前仅支持 batch MVP，不支持 streaming
- 当前 SQL 依赖抽取是轻量实现，主要覆盖常见 `FROM` / `JOIN` 场景
- 当前已支持 `CREATE MATERIALIZED VIEW`、`CREATE TEMPORARY VIEW` 和 `INSERT INTO TABLE`
- 当前还不支持 `CREATE STREAMING TABLE` 和 `CREATE FLOW`
- 目标是让 Spark 3.x 团队先能“用 SQL 开发作业”，不是完整复刻 Spark 4.x 官方 SDP
