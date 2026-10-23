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
transformation: transformations
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

生产提交入口（入口类 A）读取作业配置文件，生成 `spark-submit` 命令，并以 Yarn
cluster 模式启动入口类 B。目录模式通过 `--jobPath` 选取目录中的所有 SQL 文件：

```bash
export SPARK_HOME=/path/to/spark
java -cp spark-core/target/spark-sdp-1.0.jar \
  com.bocom.rdss.spark.sdp3x.starter.SparkSubmitStarter \
  --spec examples/sql-batch-pipeline/spark-pipeline.yaml \
  --jobPath examples/sql-batch-pipeline
```

其中入口类 B 是 `com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication`。在 IDEA 中
直接运行它并传入下面的参数，会自动使用 `local[*]`：

```text
--spec examples/sql-batch-pipeline/spark-pipeline.yaml --jobPath examples/sql-batch-pipeline
```

单文件模式使用 `--taskFile`，只分发这份 YAML 和指定文件。`--spec` 可以指向任意
`.yaml`/`.yml` 配置文件：

```bash
bin/spark-sdp.sh \
  --spec /path/to/job.yaml \
  --taskFile /path/to/one-job.sql
```

对于默认的 `spark-sql` 类型，YAML 的 `transformation` 可指定 SQL 目录，作业会执行
目录下的所有 `.sql` 文件；在目录模式中，该路径相对于 `--jobPath` 解析。传入 `--taskFile`
指向单个 `.sql` 文件时，只执行该文件。SQL 内容仍使用现有的 Pipeline SQL 语法。
`--jobPath` 与 `--taskFile` 不能同时指定。对于 `hudi-compaction` 类型，必须用
`--taskFile` 指定 `.sql` 文件，文件中每条语句为 `RUN COMPACTION ON schema.table;`，
YAML 不再配置表清单路径。两种类型都沿用 YAML 中的其他作业配置。

入口类 A 默认补充 `--master yarn --deploy-mode cluster`，读取 `driver.memory`、
`driver.cores`、`executor.memory`、`executor.cores`、`executor.num`、`queue` 以及
`conf.*` 等配置，并通过 `--archives` 将所选目录或单文件分发给 cluster driver。

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
  --spec examples/sql-batch-pipeline/spark-pipeline.yaml \
  --jobPath examples/sql-batch-pipeline
```

两种模式都可以通过 `--props` 指定 UTF-8 Properties 文件。文件中的每个配置都会转换为一个
`spark-submit --conf key=value` 参数；如果存在同名配置，`spark-pipeline.yaml` 中的值会覆盖
properties 文件中的值：

```bash
bin/spark-sdp.sh \
  --spec examples/sql-batch-pipeline/spark-pipeline.yaml \
  --jobPath examples/sql-batch-pipeline \
  --props /path/to/spark.properties
```

```properties
spark.sql.catalog=mysql
spark.sql.catalog.mysql=com.example.MysqlCatalog
spark.sql.catalog.mysql.url=jdbc:mysql://localhost:3306/example
```

两种模式都可以通过 `--jars` 把额外依赖传给 `spark-submit`。多个 jar 必须在同一个参数中使用逗号
分隔；命令行中的 `--jars` 会覆盖 `spark-pipeline.yaml` 的 `spark-submit.jars`：

```bash
bin/spark-sdp.sh \
  --spec examples/sql-batch-pipeline/spark-pipeline.yaml \
  --jobPath examples/sql-batch-pipeline \
  --props /path/to/spark.properties \
  --jars /opt/spark/jars/mysql-connector.jar,/opt/spark/jars/custom-catalog.jar
```

提交到 Yarn `cluster` 模式并通过 Hive metastore 读写 Hive 表时，建议把 `hive-site.xml`
通过 Spark 参数一起带上：

```bash
export SPARK_HOME=/path/to/your/spark
export SPARK_HIVE_CONF=/path/to/hive/conf

sh examples/run_sql_hive_pipeline_on_yarn.sh
```

`SPARK_HIVE_CONF` 可以指向 `hive-site.xml` 本身，也可以指向包含该文件的目录；脚本会自动
将其通过 `spark-submit --files` 分发给 Yarn cluster driver。

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

Spark SQL 参数统一写在作业 YAML 的 `configuration` 中，资源和队列写在
`spark-submit` 中。SQL 文件中的 `SET` 语句会被忽略，不会修改后续 SQL 的配置。

本地调试时可以直接在 IDE 里运行 `com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication`
或 `com.bocom.rdss.spark.sdp3x.example.SqlPipelineLocalDebugMain`：

```text
--spec examples/sql-batch-pipeline/spark-pipeline.yaml --jobPath examples/sql-batch-pipeline --master local[*]
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
- Hudi Compaction 项目示例：`examples/hudi-compaction-pipeline`
- Hive 插入模板：`examples/sql-hive-insert-pipeline`
- SQL demo 主类：`com.bocom.rdss.spark.sdp3x.example.SqlBatchSdp3xExampleJob`
- SQL 端到端测试：`SqlPipelineProjectRunnerTest`

## 设计说明

`spark-pipeline.yaml` 未设置 `job-type` 时沿用 Spark SQL 流程。设置
`job-type: hudi-compaction` 后，必须通过 `--taskFile` 指定一个 UTF-8 `.sql`
文件；文件中按顺序写 `RUN COMPACTION ON schema.table;` 语句，支持空行及 SQL
注释，重复表名只执行一次。提交入口将配置文件和 SQL 文件一起分发给 YARN driver。

Compaction 分支复用 SQL 作业的 SparkSession 和 Hive 配置，不在 Java 中读取 Hudi
Timeline 或判断表类型。每张表先执行 `SHOW COMPACTION ON schema.table`，有已生成的
`REQUESTED` 计划且没有 `INFLIGHT` 计划时，执行
`RUN COMPACTION ON schema.table AT instant`，最后再次通过 SQL 确认 `COMPLETED`。
任务文件中的语句用于指定待处理表，实际执行时会附加查到的已有计划 instant。
这种写法只执行已有计划，不生成新计划。每张表串行处理，单表报错后继续下一张；
全部处理后如有失败，应用返回非零状态。该入口将状态写入 driver 日志，不读取 MySQL
配置表。`spark-core` 不依赖 Hudi Java API，`SparkSubmitStarter` 不会额外处理 Hudi
依赖；运行这些 SQL 所需的 Hudi jar 由 Spark 环境提供。

```bash
bin/spark-sdp.sh --spec examples/hudi-compaction-pipeline/spark-pipeline.yaml \
  --taskFile examples/hudi-compaction-pipeline/transformations/compaction.sql \
  --files /path/to/hive-site.xml
```

也可以从任意路径指定本次使用的 SQL 文件：

```bash
bin/spark-sdp.sh --spec /path/to/compaction-job.yaml \
  --taskFile /path/to/compaction.sql \
  --files /path/to/hive-site.xml
```

生产环境请先把示例 `.sql` 中的表名替换为实际的 Hive 注册表。

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
# spark-app: Hudi compaction

### Standalone single-table job

`SingleTableHudiCompactionJob` owns the full Spark and Hudi execution for one Hive-registered MOR table; it does not call the batch job. Pass `<schema.table>` to select the earliest `requested` plan, or add `[compaction-instant]` to select one specific plan. It never schedules a plan. An existing `inflight` compaction fails the task for manual recovery; no requested plan exits with `HUDI_COMPACTION_WORKER_STATUS=SKIPPED`. After `RUN COMPACTION ON ... AT ...`, the job checks the Hudi timeline and only reports success when the instant is completed. This entry point writes its status to the driver log and uses the process exit code; it does not require the HDFS table list or MySQL.

In IDEA, run `com.bocom.rdss.spark.app.SingleTableHudiCompactionJob` with program arguments such as `sales.orders_mor`. Set `HADOOP_CONF_DIR` and Hive configuration as described below. For YARN:

```sh
./mvnw -pl spark-app -am package -DskipTests
export SPARK_HOME=/path/to/spark
export SPARK_HIVE_CONF=/path/to/hive/conf
./spark-app/run-single-table-on-yarn.sh sales.orders_mor
# Optional: ./spark-app/run-single-table-on-yarn.sh sales.orders_mor 20261019120000000
```

### Local-file multi-table job

`LocalFileMultiTableHudiCompactionJob <local-table-list-file>` reads a UTF-8 file on the Spark driver at startup. Each nonempty line is one `schema.table`; blank lines and `#` comments are ignored, duplicates are removed while retaining file order, and an invalid or empty list fails before any compaction. The driver uses one SparkSession and processes every listed table serially. The former MySQL configuration-table lookup has been removed.

```text
# One Hudi table per line
sales.orders_mor
sales.customers_mor
```

For an IDEA Application run, select `com.bocom.rdss.spark.app.LocalFileMultiTableHudiCompactionJob` and pass the absolute path of the local file as the sole program argument. Set Hadoop/Hive configuration and the JDBC result-table environment variables on the driver. For YARN cluster mode, the submit script accepts the file path on the submit host, distributes it with `--files`, and passes the localized name to the driver:

```sh
./mvnw -pl spark-app -am package -DskipTests
export SPARK_HOME=/path/to/spark
export SPARK_HIVE_CONF=/path/to/hive/conf
./spark-app/run-local-file-multi-table-on-yarn.sh /path/to/tables.txt
```

`HUDI_COMPACTION_JDBC_URL`, `HUDI_COMPACTION_JDBC_USER`, and `HUDI_COMPACTION_JDBC_PASSWORD` are required for result recording in MySQL `t_spark_job` and `t_spark_job_instance`; create those tables with [compaction-results-mysql.sql](spark-app/docs/sql/compaction-results-mysql.sql). There is no MySQL table-list/configuration table. The MySQL host must be reachable from the YARN driver; `127.0.0.1` refers to the driver container in cluster mode. The script forwards JDBC credentials as YARN driver environment properties, so protect DolphinScheduler task definitions and submit-host process listings. `SPARK_YARN_QUEUE` selects the YARN queue and defaults to `default`.

For each listed table, the job verifies MOR type and reads the Hudi timeline. It skips a table without a `requested` plan or with an `inflight` plan. For the earliest requested instant, it calls Hudi 0.15.0's `CALL run_compaction(op => 'run', table => 'schema.table', timestamp => <instant>)` (or `path => '...'` for Flink-registered tables). Supplying the timestamp selects Hudi's execute-only mode, so no new plan is scheduled. The driver waits for the SQL result and confirms the instant completed on the Hudi timeline. Incremental-write count is recorded as a metric, not an execution threshold; `-1` means the count was unavailable.

Each claimed attempt is written to the MySQL result tables before SQL execution. A failed table is recorded and the batch moves to the next table. After processing the full list, the application exits nonzero if any table failed, so DolphinScheduler can report the batch failure. A driver crash leaves the attempt `RUNNING`; a later run skips that instant until an operator checks Hudi/YARN and marks it `RETRY`. A completed instant is reconciled on a later run. Disable overlapping DolphinScheduler runs for the same table list.

The former HDFS table-list and MySQL configuration-table entry points have been removed. `HudiCompactionJob <source-schema> <max-incremental-records>` remains available for direct whole-database scans and writes a Hive result table.

The older `HudiCompactionJob --table schema.table [instant]` command remains as a compatibility alias for existing callers and forwards to the dedicated single-table class. The two-argument whole-database mode below remains available for direct/manual execution and continues to write its results to a Hive table.

`spark-app` contains standalone Spark applications. `HudiCompactionJob` scans all tables in the specified Hive database and selects Hudi Merge On Read (MOR) tables. Its two arguments are the source database and a positive incremental record threshold. The result table defaults to `<source-schema>.hudi_compaction_result`; override it with Spark configuration `spark.app.compaction.result.table=schema.table`.

The job counts the `numWrites` values in completed Hudi `deltacommit` metadata since the last completed compaction (including archived timeline metadata when needed). Inserts and repeated updates both count as writes. A MOR table runs compaction only when this total is strictly less than the threshold and a requested plan already exists. It runs each plan with `spark.sql("RUN COMPACTION ON schema.table AT instant")`; it never schedules a new plan. An inflight plan is recorded as a failure for operator review. Hudi 0.15.0 does not support `RUN CLEAN ON` or `RUN ARCHIVELOG ON` SQL, so this job does not run clean or archive.

For a Flink-registered Hudi table (`flink.connector=hudi`), the Hive `Location` may point to a local warehouse while `flink.path` contains the real HDFS dataset path. The job uses `flink.path` for those tables. Spark Hudi tables use their catalog name in `RUN COMPACTION`; Hudi tables registered with another Hive provider use their data path. Non-Hudi and COW tables are excluded. The job prints the resolved Hudi path and incremental count.

Tables must be registered in the configured external Hive metastore. If the result table already exists, the job keeps its current metadata and location and only appends this run's rows. Otherwise, it creates a Parquet Hive table at an explicit HDFS warehouse path. The result fields are `schema_name`, `table_name`, `start_time`, `end_time`, `duration_ms`, `status`, and `error_message`; `SKIPPED` means no pending plan, and `SKIPPED_THRESHOLD` means the incremental write count reached the threshold. This module targets Spark 3.3.1 / Scala 2.12 and Hudi 0.15.0, using the Spark 3.3 bundle.

Build the module with:

```sh
./mvnw -pl spark-app -am package -DskipTests
```

Spark and Hudi dependencies are compile-scope so IDE run configurations include them. The produced jar remains thin; submit it through `spark-submit` and provide Hudi with `--packages` as shown below.

### IDEA local run

Create an IDEA **Application** run configuration:

- Main class: `com.bocom.rdss.spark.app.HudiCompactionJob`
- Use classpath of module: `spark-app`
- Program arguments: `<source-schema> <max-incremental-records>`, for example `sales 100000`
- Set `HADOOP_CONF_DIR` to the Hadoop config directory. The job also loads `hive-site.xml` from `HIVE_CONF_DIR`, `HIVE_HOME/conf`, `SPARK_CONF_DIR`, the application classpath, or the driver's working directory (for YARN `--files`). It prints the metastore URI and warehouse path, then checks connectivity at startup. Missing external metastore or warehouse configuration fails before any table service or result-table write.

When no `spark.master` is supplied, the job defaults to `local[*]`, so IDEA can run it directly. When launched by `spark-submit`, it uses the master from Spark's submit configuration (for example, `yarn`).

### YARN run

First package the thin jar, then use the included submit script. `SPARK_HIVE_CONF` is optional when the cluster already distributes Hive configuration; when set, it can point to `hive-site.xml` or its directory.

```sh
./mvnw -pl spark-app -am package -DskipTests
export SPARK_HOME=/path/to/spark
export SPARK_HIVE_CONF=/path/to/hive/conf
./spark-app/run-on-yarn.sh sales 100000
```

Submit it with the Hudi bundle on the driver and executor classpaths (for example, `--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0`):

```sh
spark-submit \
  --class com.bocom.rdss.spark.app.HudiCompactionJob \
  --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.15.0 \
  --conf spark.app.compaction.result.table=ops.hudi_compaction_result \
  spark-app/target/spark-app-1.0.jar \
  sales \
  100000
```
