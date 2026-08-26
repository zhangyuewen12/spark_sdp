package com.bocom.rdss.spark.sdp3x.example;

import com.bocom.rdss.spark.sdp3x.PipelineOrchestrator;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.testsupport.SparkTestSupport;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExampleJobsTest {
  @TempDir
  Path tempDir;

  @Test
  void batchExampleJobShouldBuildPipelineAndSeedSourceData() throws Exception {
    Method buildPipeline = BatchSdp3xExampleJob.class.getDeclaredMethod("buildPipeline");
    buildPipeline.setAccessible(true);
    PipelineDefinition pipeline = (PipelineDefinition) buildPipeline.invoke(null);

    assertEquals("batch_orders_pipeline", pipeline.name());
    assertEquals(2, pipeline.datasets().size());
    assertEquals(2, pipeline.flows().size());

    SparkSession spark = SparkTestSupport.newLocalSparkSession("BatchSdp3xExampleJobTest", tempDir.resolve("warehouse"));
    try {
      Method prepareSourceData = BatchSdp3xExampleJob.class.getDeclaredMethod("prepareSourceData", SparkSession.class);
      prepareSourceData.setAccessible(true);
      prepareSourceData.invoke(null, spark);

      assertEquals(3L, spark.table("sdp3x_orders_source").count());

      new PipelineOrchestrator().run(
        pipeline,
        spark,
        ExecutionOptions.defaults().withMaterializedViewSaveMode(SaveMode.Overwrite));

      assertEquals(3L, spark.table("sdp3x_orders_clean").count());
      assertEquals(2L, spark.table("sdp3x_daily_orders").count());
      assertEquals(
        200.0D,
        spark.sql("SELECT total_amount FROM sdp3x_daily_orders WHERE region = 'east'")
          .head()
          .getDouble(0));
    } finally {
      SparkTestSupport.stop(spark);
    }
  }

  @Test
  void sqlPipelineLocalDebugMainShouldHandleProvidedArgs() throws URISyntaxException {
    Path projectRoot = Paths.get(ExampleJobsTest.class.getResource("/sql-batch-pipeline").toURI());

    SqlPipelineLocalDebugMain.main(new String[] {"dry-run", projectRoot.toString()});
  }

  @Test
  void batchExampleJobMainShouldRunEndToEnd() throws Exception {
    SparkTestSupport.deleteRecursively(Paths.get("target", "sdp3x-warehouse"));

    String output = captureStdout(() -> BatchSdp3xExampleJob.main(new String[0]));

    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    assertTrue(output.contains("east"));
    assertTrue(output.contains("2026-04-15"));
  }

  @Test
  void sqlBatchExampleJobMainShouldRunEndToEnd() throws Exception {
    Path projectRoot = copyResourceDirectory("sql-batch-pipeline", tempDir.resolve("sql-batch-pipeline"));
    Path warehouse = Paths.get("target", "sql-example-warehouse");
    SparkTestSupport.deleteRecursively(Paths.get("target", "sql-example-warehouse"));

    captureStdout(() -> SqlBatchSdp3xExampleJob.main(new String[] {projectRoot.toString()}));

    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    assertTrue(Files.exists(warehouse.resolve("orders_clean")));
    assertTrue(Files.exists(warehouse.resolve("daily_orders")));

    SparkSession verificationSpark = SparkTestSupport.newLocalSparkSession(
      "SqlBatchSdp3xExampleJobVerification",
      tempDir.resolve("verification-warehouse"));
    try {
      assertEquals(3L, verificationSpark.read().parquet(warehouse.resolve("orders_clean").toString()).count());
      assertEquals(2L, verificationSpark.read().parquet(warehouse.resolve("daily_orders").toString()).count());
    } finally {
      SparkTestSupport.stop(verificationSpark);
    }
  }

  @Test
  void sqlPipelineLocalDebugMainShouldHandleDefaultArgs() throws Exception {
    SparkTestSupport.deleteRecursively(Paths.get("examples", "sql-batch-pipeline", "target", "spark-warehouse"));

    String output = captureStdout(() -> SqlPipelineLocalDebugMain.main(new String[0]));

    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    assertTrue(output.contains("Executed"));
    assertTrue(output.contains("sql_orders_pipeline"));
  }

  @Test
  void orderRecordBeanShouldExposeMutators() {
    BatchSdp3xExampleJob.OrderRecord record = new BatchSdp3xExampleJob.OrderRecord();
    record.setOrderId(1L);
    record.setRegion("east");
    record.setOrderDate("2026-04-15");
    record.setAmount(12.5D);

    assertEquals(1L, record.getOrderId());
    assertEquals("east", record.getRegion());
    assertEquals("2026-04-15", record.getOrderDate());
    assertEquals(12.5D, record.getAmount());

    BatchSdp3xExampleJob.OrderRecord second = new BatchSdp3xExampleJob.OrderRecord(
      2L, "west", "2026-04-16", 7.0D);
    assertEquals(2L, second.getOrderId());
    assertEquals("west", second.getRegion());
    assertEquals("2026-04-16", second.getOrderDate());
    assertEquals(7.0D, second.getAmount());
  }

  private Path copyResourceDirectory(String resourceName, Path targetDirectory) throws Exception {
    Path sourceDirectory = Paths.get(ExampleJobsTest.class.getResource("/" + resourceName).toURI());
    Files.walk(sourceDirectory).forEach(source -> {
      try {
        Path relativePath = sourceDirectory.relativize(source);
        Path target = targetDirectory.resolve(relativePath.toString());
        if (Files.isDirectory(source)) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
      } catch (IOException e) {
        throw new IllegalStateException("Failed to copy resource directory " + resourceName, e);
      }
    });
    return targetDirectory;
  }

  private String captureStdout(ThrowingRunnable action) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8.name()));
      action.run();
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    } finally {
      System.setOut(originalOut);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
