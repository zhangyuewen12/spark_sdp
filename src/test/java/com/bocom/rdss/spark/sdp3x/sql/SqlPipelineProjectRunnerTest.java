package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import com.bocom.rdss.spark.sdp3x.testsupport.SparkTestSupport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class SqlPipelineProjectRunnerTest {
  private SparkSession spark;
  private Path warehouseDir;

  @BeforeEach
  void setUp() throws IOException {
    warehouseDir = Files.createTempDirectory("sql-pipeline-runner-test-warehouse");
    spark = SparkTestSupport.newHiveSparkSession("SqlPipelineProjectRunnerTest", warehouseDir);
  }

  @AfterEach
  void tearDown() {
    SparkTestSupport.stop(spark);
    spark = null;
  }

  @Test
  void shouldCompileAndRunSqlPipelineProject() throws URISyntaxException {
    Path projectRoot = Paths.get(
      SqlPipelineProjectRunnerTest.class.getResource("/sql-batch-pipeline").toURI());

    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner();
    PipelineDefinition pipelineDefinition = runner.compile(projectRoot);
    assertEquals("sql_orders_pipeline", pipelineDefinition.name());
    assertEquals(3, pipelineDefinition.datasets().size());
    assertEquals(3, pipelineDefinition.flows().size());

    ExecutionReport report = runner.run(
      projectRoot,
      spark,
      ExecutionOptions.defaults().withMaterializedViewSaveMode(SaveMode.Overwrite));
    assertEquals(3, report.results().size());

    Dataset<Row> dailyOrders = spark.table("daily_orders");
    assertEquals(2L, dailyOrders.count());

    Row eastRow = dailyOrders.where("region = 'east'").head();
    Number orderCount = eastRow.getAs("order_count");
    Number totalAmount = eastRow.getAs("total_amount");
    assertEquals(2L, orderCount.longValue());
    assertEquals(200.0D, totalAmount.doubleValue(), 0.001D);
  }

  @Test
  void shouldReadAndWriteHiveTablesWithInsertInto() throws URISyntaxException {
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
    assertEquals(1, pipelineDefinition.datasets().size());
    assertEquals(1, pipelineDefinition.flows().size());

    ExecutionReport report = runner.run(
      projectRoot,
      spark,
      ExecutionOptions.defaults().withMaterializedViewSaveMode(SaveMode.Overwrite));
    assertEquals(1, report.results().size());

    Dataset<Row> sinkTable = spark.table("sql_sdp_test_db.daily_orders_sink");
    assertEquals(2L, sinkTable.count());

    Row eastRow = sinkTable.where("region = 'east'").head();
    Number orderCount = eastRow.getAs("order_count");
    Number totalAmount = eastRow.getAs("total_amount");
    assertEquals(2L, orderCount.longValue());
    assertEquals(200.0D, totalAmount.doubleValue(), 0.001D);
  }

  @Test
  void shouldParseRunCliCommand() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"run", "examples/sql-batch-pipeline", "--master", "local[2]"});

    assertEquals(SqlPipelineCliOptions.Command.RUN, cliOptions.command());
    assertEquals(Paths.get("examples/sql-batch-pipeline"), cliOptions.projectPath());
    assertEquals("local[2]", cliOptions.master());
  }

  @Test
  void shouldParseDryRunCliCommand() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"dry-run", "examples/sql-batch-pipeline"});

    assertEquals(SqlPipelineCliOptions.Command.DRY_RUN, cliOptions.command());
    assertEquals(Paths.get("examples/sql-batch-pipeline"), cliOptions.projectPath());
    assertNull(cliOptions.master());
  }

  @Test
  void shouldKeepLegacyDryRunCompatibility() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"examples/sql-batch-pipeline", "--dry-run"});

    assertEquals(SqlPipelineCliOptions.Command.DRY_RUN, cliOptions.command());
    assertEquals(Paths.get("examples/sql-batch-pipeline"), cliOptions.projectPath());
  }

  @Test
  void shouldParseSpecPathAndSparkSubmitMarker() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"run", "--spec", "examples/sql-batch-pipeline/spark-pipeline.yml", "--submitted"});

    assertEquals(SqlPipelineCliOptions.Command.RUN, cliOptions.command());
    assertEquals(
      Paths.get("examples/sql-batch-pipeline/spark-pipeline.yml"),
      cliOptions.projectPath());
    assertTrue(cliOptions.submittedViaSparkSubmit());
  }

  @Test
  void shouldParseClusterSubmissionStyleArguments() {
    SqlPipelineCliOptions cliOptions = SqlPipelineCliOptions.parse(
      new String[] {"--submitted", "run", "--spec", "spark-sdp-project/spark-pipeline.yml"});

    assertEquals(SqlPipelineCliOptions.Command.RUN, cliOptions.command());
    assertEquals(
      Paths.get("spark-sdp-project/spark-pipeline.yml"),
      cliOptions.projectPath());
    assertTrue(cliOptions.submittedViaSparkSubmit());
  }

  @Test
  void shouldFailWhenSpecIsMissingInSpecifiedDirectory() throws IOException {
    Path tempDir = Files.createTempDirectory("sql-pipeline-project-spec-loader");

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> new SqlPipelineProjectSpecLoader().load(tempDir));

    assertTrue(exception.getMessage().contains("Missing SQL pipeline spec file under"));
  }

  @Test
  void shouldLoadSpecFromSpecifiedDirectory() throws URISyntaxException {
    Path projectRoot = Paths.get(
      SqlPipelineProjectRunnerTest.class.getResource("/sql-batch-pipeline").toURI());

    SqlPipelineProjectSpec spec = new SqlPipelineProjectSpecLoader()
      .load(projectRoot);

    assertEquals(projectRoot.toAbsolutePath().normalize(), spec.rootDirectory());
  }
}
