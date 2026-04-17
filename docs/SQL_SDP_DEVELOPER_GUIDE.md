# SQL SDP 开发使用样例

这份文档面向项目组开发人员，介绍如何基于当前仓库的 SQL SDP MVP 开发一个 Spark 批处理作业。

当前版本支持：

- `spark-pipeline.yml` 项目配置
- `transformations/*.sql` SQL 开发方式
- `CREATE MATERIALIZED VIEW ... AS SELECT ...`
- `CREATE TEMPORARY VIEW ... AS SELECT ...`
- `INSERT INTO [TABLE] target SELECT ...`
- `run` / `dry-run` 命令
- 本地 Spark 批处理运行
- 基于 `SPARK_HOME` 的 `spark-submit` 提交入口
- `cluster` 模式自动分发 pipeline 项目
- Hive metastore 表读写

## 1. 目录结构

推荐一个 pipeline 项目目录长这样：

```text
my-pipeline/
  spark-pipeline.yml
  transformations/
    000_seed_orders.sql
    010_clean_orders.sql
    020_daily_orders.sql
```

说明：

- `spark-pipeline.yml`：pipeline 顶层配置
- `transformations/`：存放 SQL 文件
- 一个 SQL 文件通常定义一个结果数据集
- 文件名前缀建议带顺序号，便于人读和排查

仓库里已经提供了一个可运行样例：

```text
examples/sql-batch-pipeline/
  spark-pipeline.yml
  transformations/
    000_seed_orders.sql
    010_clean_orders.sql
    020_daily_orders.sql
```

另外还提供了一个更贴近生产的 Hive 插入模板：

```text
examples/sql-hive-insert-pipeline/
  spark-pipeline.yml
  transformations/
    010_insert_daily_orders.sql
```

## 2. 配置文件写法

示例 `spark-pipeline.yml`：

```yaml
name: sql_orders_pipeline
configuration:
  spark.sql.shuffle.partitions: "1"
libraries:
  - transformations
```

字段说明：

- `name`：pipeline 名称
- `configuration`：传递给 SparkSession 的配置项
- `libraries`：SQL 文件目录列表，支持一个或多个目录

当前 MVP 里最常用的就是这几个字段。

如果你希望 SQL 默认在某个 Hive 库下执行，可以加：

```yaml
database: dwd_demo
```

这样这条 pipeline 在执行 SQL 前，会先把当前 session 切到这个库，效果等同于执行一次
`USE dwd_demo`。如果 SQL 里自己写了带库名的表，例如 `ods.orders_source` 或
`INSERT INTO TABLE ads.daily_orders_sink ...`，则以 SQL 中显式写出的库名为准。

## 3. SQL 写法

### 3.1 物化视图

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

说明：

- `orders_clean` 会被当作一个持久化输出数据集
- 当前实现默认以表方式落地
- 上游依赖会从 `FROM` / `JOIN` 中自动提取

### 3.2 临时视图

```sql
CREATE TEMPORARY VIEW orders_stage AS
SELECT *
FROM orders_source
WHERE amount > 0;
```

说明：

- 临时视图只在当前 Spark 应用内有效
- 适合做中间层加工

### 3.3 多层依赖示例

`010_clean_orders.sql`

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

`020_daily_orders.sql`

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

这里 `daily_orders` 会自动依赖 `orders_clean`。

### 3.4 插入到已存在的 Hive 表

如果目标表由平台侧或数仓侧提前创建好了，可以直接写：

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

说明：

- `daily_orders_sink` 必须提前存在
- 当前实现会走 Spark 的 `insertInto(...)` 语义
- 这种写法适合写入 Hive 中已经建好的明细表、汇总表或分区表
- 上游依赖仍然会从 `FROM` / `JOIN` 中自动提取

## 4. 开发步骤

### 第一步：准备输入表

当前 MVP 默认假设源表已经存在，比如：

- Hive 表
- 外部表
- 任务启动前通过 Spark 代码提前写入的表
- 当前 SparkSession 中已有的临时视图

不过仓库内置的 `examples/sql-batch-pipeline` 为了方便演示，已经在
`000_seed_orders.sql` 里先定义了一个 `orders_source` 临时视图，所以它可以直接运行。

如果你走的是 Hive 模式，通常就是：

- 上游输入表已经在 Hive 中建好
- 目标表已经在 Hive 中建好，或者由 `CREATE MATERIALIZED VIEW` 自动落表
- `spark-pipeline.yml` 里通过 `database` 指定默认库

### 第二步：编写 `spark-pipeline.yml`

至少要写：

- pipeline 名称
- SQL 目录

### 第三步：编写 SQL 文件

建议规范：

- 一个文件只定义一个主要输出
- 文件名使用数字前缀
- 中间层和结果层分文件管理

### 第四步：先做 dry-run

在真正执行前，建议先看执行计划：

```bash
./mvnw package

bin/spark-sdp dry-run --spec examples/sql-batch-pipeline/spark-pipeline.yml
```

预期输出类似：

```text
Pipeline: sql_orders_pipeline
Stage 0:
  - sql_flow_001_orders_clean -> orders_clean
Stage 1:
  - sql_flow_002_daily_orders -> daily_orders
```

这一步适合检查：

- SQL 文件是否被正确发现
- 依赖顺序是否符合预期
- pipeline 名称和目标数据集是否正确

## 5. 如何执行

### 5.1 编译和测试

```bash
./mvnw test
```

### 5.2 运行一个 SQL pipeline 项目

```bash
./mvnw package

export SPARK_HOME=/path/to/your/spark

bin/spark-sdp \
  --master yarn \
  --deploy-mode client \
  run \
  --spec examples/sql-batch-pipeline/spark-pipeline.yml
```

如果提交到 Yarn `cluster` 模式，并且你的 Hive 配置依赖 `hive-site.xml`，可以这样：

```bash
bin/spark-sdp \
  --master yarn \
  --deploy-mode cluster \
  --files /path/to/hive-site.xml \
  run \
  --spec examples/sql-hive-insert-pipeline/spark-pipeline.yml
```

如果你想先看帮助信息：

```bash
bin/spark-sdp help
```

先执行打包，并把产物复制到 `bin/` 目录：

```bash
./mvnw package
cp target/spark-sdp-1.0.jar bin/
```

`bin/spark-sdp` 只会读取同目录下的 `spark-sdp-1.0.jar`。其中：

- `run` 会通过 `${SPARK_HOME}/bin/spark-submit` 提交
- `dry-run` 也会通过 `${SPARK_HOME}/bin/spark-submit` 启动
- `help` 直接走本地 jar
- `--deploy-mode cluster` 时会自动把 `spark-pipeline.yml` 和 SQL 目录打包随作业分发
- `spark-sdp-1.0.jar` 不包含 `spark-core` 和 `spark-sql`，提交到 Yarn 时会使用 Spark 安装自带的依赖
- 运行时会启用 Hive support，所以只要 Spark 环境里 metastore 配置可用，SQL 就能直接读写 Hive 表

如果你已经 `cd` 到 pipeline 目录下，也可以省略 `--spec`：

```bash
cd examples/sql-batch-pipeline

../../bin/spark-sdp \
  --master yarn \
  --deploy-mode cluster \
  run
```

此时脚本只会在当前目录下查找 `spark-pipeline.yml` / `spark-pipeline.yaml`。

## 5.4 本地 Main 调试

当你需要在 IDE 里打断点排查逻辑时，可以直接运行下面任一 main：

- `com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain`
- `com.bocom.rdss.spark.sdp3x.example.SqlPipelineLocalDebugMain`

推荐传参：

```text
run --spec examples/sql-batch-pipeline/spark-pipeline.yml --master local[*]
```

如果你运行的是 `SqlPipelineLocalDebugMain`，不传参数时也会默认执行上面的本地样例。

为什么同一套代码既能本地 main 调试，也能通过 `spark-sdp` 提交：

- 通过 `spark-sdp` 提交时，脚本会给主类追加 `--submitted`
- Java 入口检测到这个标记后，不再强制创建本地 `local[*]` SparkSession，而是直接继承 `spark-submit` / Yarn 的 Spark 环境
- 直接在 IDE 里运行 main 时没有 `--submitted`，代码就回退到本地模式，自动创建 `local[*]` SparkSession

### 5.3 运行仓库内置完整 demo

这个 demo 会：

- 直接执行自包含的 `examples/sql-batch-pipeline`
- 最后打印 `orders_clean` 和 `daily_orders`
- 本地 demo 会自动使用嵌入式 Derby metastore，避免受你机器外部 Hive 配置影响

执行命令：

```bash
./mvnw package

${SPARK_HOME}/bin/spark-submit \
  --master local[*] \
  --class com.bocom.rdss.spark.sdp3x.example.SqlBatchSdp3xExampleJob \
  bin/spark-sdp-1.0.jar
```

运行成功后你会看到类似输出：

```text
+-------+------+----------+------+----------+
|orderId|region|orderDate |amount|order_date|
+-------+------+----------+------+----------+
|2      |east  |2026-04-15|80.0  |2026-04-15|
|1      |east  |2026-04-15|120.0 |2026-04-15|
|3      |west  |2026-04-16|55.0  |2026-04-16|
+-------+------+----------+------+----------+

+------+----------+-----------+------------+
|region|order_date|order_count|total_amount|
+------+----------+-----------+------------+
|east  |2026-04-15|2          |200.0       |
|west  |2026-04-16|1          |55.0        |
+------+----------+-----------+------------+
```

## 6. 开发建议

建议项目组先按下面约定来开发：

- 源表命名稳定，不要在 SQL 里频繁改名
- 一个 SQL 文件一个结果数据集
- 中间层优先用 `TEMPORARY VIEW`
- 最终结果层优先用 `MATERIALIZED VIEW`
- 外部已建表优先用 `INSERT INTO TABLE ... SELECT ...`
- SQL 文件命名按加工层次排序

一个比较实用的分层方式：

```text
transformations/
  010_source_clean.sql
  020_dim_enrich.sql
  030_metric_daily.sql
  040_ads_result.sql
```

## 7. 当前边界

当前 MVP 还不支持：

- `CREATE STREAMING TABLE`
- `CREATE FLOW ... AS INSERT INTO ...`
- 更复杂的 SQL 血缘分析
- 完整复刻 Spark 4.x 官方 SDP SQL 语义

所以现在最适合的使用场景是：

- Spark 3.x
- 批处理
- 团队希望主要用 SQL 写作业
- 平台侧统一托管执行顺序和运行入口

## 8. 推荐上手方式

新同学第一次使用时，建议按这个顺序：

1. 先运行仓库自带 demo
2. 再复制 `examples/sql-batch-pipeline`
3. 修改 SQL 指向自己的源表
4. 先做 `dry-run`
5. 再正式运行

这样最容易定位问题，也最符合当前 MVP 的设计边界。
