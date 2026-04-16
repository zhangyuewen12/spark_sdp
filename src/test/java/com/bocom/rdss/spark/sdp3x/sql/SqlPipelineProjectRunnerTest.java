package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SqlPipelineProjectRunnerTest {
  private SparkSession spark;
  private Path warehouseDir;

  @Before
  public void setUp() throws IOException {
    warehouseDir = Files.createTempDirectory("sql-pipeline-runner-test-warehouse");
    String metastoreUrl = "jdbc:derby:;databaseName="
      + warehouseDir.resolve("metastore_db").toAbsolutePath()
      + ";create=true";
    spark = SparkSession.builder()
      .appName("SqlPipelineProjectRunnerTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
      .config("hive.metastore.uris", "")
      .config("spark.hadoop.hive.metastore.uris", "")
      .config("javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateAll", "true")
      .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.hadoop.hive.metastore.schema.verification", "false")
      .enableHiveSupport()
      .getOrCreate();
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void shouldCompileAndRunSqlPipelineProject() throws URISyntaxException {
    Path projectRoot = Paths.get(
      SqlPipelineProjectRunnerTest.class.getResource("/sql-batch-pipeline").toURI());

    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner();
    PipelineDefinition pipelineDefinition = runner.compile(projectRoot);
    Assert.assertEquals("sql_orders_pipeline", pipelineDefinition.name());
    Assert.assertEquals(3, pipelineDefinition.datasets().size());
    Assert.assertEquals(3, pipelineDefinition.flows().size());

    ExecutionReport report = runner.run(
      projectRoot,
      spark,
      ExecutionOptions.defaults().withMaterializedViewSaveMode(SaveMode.Overwrite));
    Assert.assertEquals(3, report.results().size());

    Dataset<Row> dailyOrders = spark.table("daily_orders");
    Assert.assertEquals(2L, dailyOrders.count());

    Row eastRow = dailyOrders.where("region = 'east'").head();
    Number orderCount = eastRow.getAs("order_count");
    Number totalAmount = eastRow.getAs("total_amount");
    Assert.assertEquals(2L, orderCount.longValue());
    Assert.assertEquals(200.0D, totalAmount.doubleValue(), 0.001D);
  }

  @Test
  public void shouldReadAndWriteHiveTablesWithInsertInto() throws URISyntaxException {
    Path projectRoot = Paths.get(
      SqlPipelineProjectRunnerTest.class.getResource("/sql-hive-insert-pipeline").toURI());

    spark.sql("CREATE DATABASE IF NOT EXISTS sql_sdp_test_db");
    spark.sql("DROP TABLE IF EXISTS sql_sdp_test_db.orders_source");
    spark.sql("DROP TABLE IF EXISTS sql_sdp_test_db.daily_orders_sink");

    spark.sql("CREATE TABLE sql_sdp_test_db.orders_source (" +
      "orderId BIGINT, region STRING, orderDate STRING, amount DOUBLE) USING PARQUET");
    spark.sql("INSERT INTO sql_sdp_test_db.orders_source VALUES " +
      "(1L, 'east', '2026-04-15', 120.0D), " +
      "(2L, 'east', '2026-04-15', 80.0D), " +
      "(3L, 'west', '2026-04-16', 55.0D), " +
      "(4L, 'north', '2026-04-16', -12.0D)");
    spark.sql("CREATE TABLE sql_sdp_test_db.daily_orders_sink (" +
      "region STRING, order_date DATE, order_count BIGINT, total_amount DOUBLE) USING PARQUET");

    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner();
    PipelineDefinition pipelineDefinition = runner.compile(projectRoot);
    Assert.assertEquals(1, pipelineDefinition.datasets().size());
    Assert.assertEquals(1, pipelineDefinition.flows().size());

    ExecutionReport report = runner.run(
      projectRoot,
      spark,
      ExecutionOptions.defaults().withMaterializedViewSaveMode(SaveMode.Overwrite));
    Assert.assertEquals(1, report.results().size());

    Dataset<Row> sinkTable = spark.table("sql_sdp_test_db.daily_orders_sink");
    Assert.assertEquals(2L, sinkTable.count());

    Row eastRow = sinkTable.where("region = 'east'").head();
    Number orderCount = eastRow.getAs("order_count");
    Number totalAmount = eastRow.getAs("total_amount");
    Assert.assertEquals(2L, orderCount.longValue());
    Assert.assertEquals(200.0D, totalAmount.doubleValue(), 0.001D);
  }

  @Test
  public void shouldParseRunCliCommand() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"run", "examples/sql-batch-pipeline", "--master", "local[2]"});

    Assert.assertEquals(SqlPipelineCliOptions.Command.RUN, cliOptions.command());
    Assert.assertEquals(Paths.get("examples/sql-batch-pipeline"), cliOptions.projectPath());
    Assert.assertEquals("local[2]", cliOptions.master());
  }

  @Test
  public void shouldParseDryRunCliCommand() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"dry-run", "examples/sql-batch-pipeline"});

    Assert.assertEquals(SqlPipelineCliOptions.Command.DRY_RUN, cliOptions.command());
    Assert.assertEquals(Paths.get("examples/sql-batch-pipeline"), cliOptions.projectPath());
    Assert.assertEquals(null, cliOptions.master());
  }

  @Test
  public void shouldKeepLegacyDryRunCompatibility() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"examples/sql-batch-pipeline", "--dry-run"});

    Assert.assertEquals(SqlPipelineCliOptions.Command.DRY_RUN, cliOptions.command());
    Assert.assertEquals(Paths.get("examples/sql-batch-pipeline"), cliOptions.projectPath());
  }

  @Test
  public void shouldParseSpecPathAndSparkSubmitMarker() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"run", "--spec", "examples/sql-batch-pipeline/spark-pipeline.yml", "--submitted"});

    Assert.assertEquals(SqlPipelineCliOptions.Command.RUN, cliOptions.command());
    Assert.assertEquals(
      Paths.get("examples/sql-batch-pipeline/spark-pipeline.yml"),
      cliOptions.projectPath());
    Assert.assertTrue(cliOptions.submittedViaSparkSubmit());
  }

  @Test
  public void shouldParseClusterSubmissionStyleArguments() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"--submitted", "run", "--spec", "spark-sdp-project/spark-pipeline.yml"});

    Assert.assertEquals(SqlPipelineCliOptions.Command.RUN, cliOptions.command());
    Assert.assertEquals(
      Paths.get("spark-sdp-project/spark-pipeline.yml"),
      cliOptions.projectPath());
    Assert.assertTrue(cliOptions.submittedViaSparkSubmit());
  }

  @Test
  public void shouldFindSpecFromNestedDirectory() throws URISyntaxException {
    Path projectRoot = Paths.get(
      SqlPipelineProjectRunnerTest.class.getResource("/sql-batch-pipeline").toURI());

    SqlPipelineProjectSpec spec = new SqlPipelineProjectSpecLoader()
      .load(projectRoot.resolve("transformations"));

    Assert.assertEquals(projectRoot.toAbsolutePath().normalize(), spec.rootDirectory());
  }
}
