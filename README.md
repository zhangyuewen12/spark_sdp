# Spark 3.x SQL SDP MVP

这个仓库现在是一个 Spark 3.3.x 上的 SQL-first SDP MVP，目标是让开发人员基于：

- `spark-pipeline.yml`
- `transformations/*.sql`

来开发批处理 Spark 作业，而不是直接编写 Java `main()`。

## 当前能力

- 支持从 `spark-pipeline.yml` 加载项目配置
- 支持扫描一个或多个 SQL 目录
- 支持批处理 SQL 声明：
  - `CREATE MATERIALIZED VIEW ... AS SELECT ...`
  - `CREATE TEMPORARY VIEW ... AS SELECT ...`
  - `INSERT INTO [TABLE] target SELECT ...`
- 自动从 SQL 中抽取 `FROM` / `JOIN` 依赖并生成执行顺序
- 复用现有的依赖分析、拓扑规划和本地批执行器
- 支持 `run` / `dry-run` 命令生成或执行 pipeline
- 支持通过 Hive metastore 读取和写入 Hive 表

## 项目结构

一个最小 SQL 项目形态如下：

```text
my-pipeline/
  spark-pipeline.yml
  transformations/
    000_seed_orders.sql
    010_clean_orders.sql
    020_daily_orders.sql
```

示例 `spark-pipeline.yml`：

```yaml
name: sql_orders_pipeline
configuration:
  spark.sql.shuffle.partitions: "1"
libraries:
  - transformations
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

编译和测试：

```bash
./mvnw test
```

打包一个可直接执行的 fat jar：

```bash
./mvnw package
```

查看可用命令：

```bash
bin/spark-sdp help
```

对 SQL 项目做 dry-run：

```bash
bin/spark-sdp dry-run examples/sql-batch-pipeline
```

本地 dry-run：

```bash
bin/spark-sdp dry-run --spec examples/sql-batch-pipeline/spark-pipeline.yml
```

提交到 Yarn 运行：

```bash
export SPARK_HOME=/path/to/your/spark

bin/spark-sdp \
  --master yarn \
  --deploy-mode cluster \
  run \
  --spec examples/sql-batch-pipeline/spark-pipeline.yml
```

提交到 Yarn `cluster` 模式并通过 Hive metastore 读写 Hive 表时，建议把 `hive-site.xml`
通过 Spark 参数一起带上：

```bash
export SPARK_HOME=/path/to/your/spark

bin/spark-sdp \
  --master yarn \
  --deploy-mode cluster \
  --files /path/to/hive-site.xml \
  run \
  --spec examples/sql-hive-insert-pipeline/spark-pipeline.yml
```

也可以先进入项目目录，再让脚本从当前目录读取 `spark-pipeline.yml`：

```bash
cd examples/sql-batch-pipeline

../../bin/spark-sdp \
  --master yarn \
  --deploy-mode cluster \
  run
```

执行前先打包，并把产物复制到 `bin/` 目录，与脚本同级：

```bash
./mvnw package
cp target/spark-sdp-1.0.jar bin/
```

`bin/spark-sdp` 只会读取同目录下的 `spark-sdp-1.0.jar`。`run` 和 `dry-run` 都会通过 `${SPARK_HOME}/bin/spark-submit` 启动，`help` 直接走本地 jar；当使用 `--deploy-mode cluster` 时，脚本会自动把 `spark-pipeline.yml` 和 SQL 目录打包成 archive，随作业一起分发。打包产物里不会包含 `spark-core` 和 `spark-sql`，提交到 Yarn 时会使用 Spark 安装自带的依赖。

`database` 配置表示这条 pipeline 的默认数据库，效果等同于在执行所有 SQL 之前先做一次
`USE <database>`。如果 SQL 里已经显式写了库名，例如 `db1.orders_source` 或
`INSERT INTO TABLE db2.daily_orders_sink`，则以 SQL 自己写的库名为准。

本地调试时可以直接在 IDE 里运行 `com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain` 或
`com.bocom.rdss.spark.sdp3x.example.SqlPipelineLocalDebugMain`：

```text
run --spec examples/sql-batch-pipeline/spark-pipeline.yml --master local[*]
```

之所以同一个 main 同时支持本地和 `spark-sdp`，是因为 `spark-sdp` 提交时会额外传入
`--submitted` 标记，此时程序继承 `spark-submit` 创建好的 Spark 环境；直接本地执行 main 时没有这个标记，程序会自己创建 `local[*]` SparkSession。

运行带样例输入数据的 demo：

```bash
${SPARK_HOME}/bin/spark-submit \
  --master local[*] \
  --class com.bocom.rdss.spark.sdp3x.example.SqlBatchSdp3xExampleJob \
  bin/spark-sdp-1.0.jar
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
