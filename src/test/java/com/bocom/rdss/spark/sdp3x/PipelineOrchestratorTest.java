package com.bocom.rdss.spark.sdp3x;

import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class PipelineOrchestratorTest {
  private SparkSession spark;
  private Path warehouseDir;

  @Before
  public void setUp() throws IOException {
    warehouseDir = Files.createTempDirectory("pipeline-orchestrator-test-warehouse");
    spark = SparkSession.builder()
      .appName("PipelineOrchestratorTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
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
  public void shouldExecutePipelineEndToEnd() {
    String suffix = String.valueOf(System.nanoTime());
    String sourceTable = "test_orders_source_" + suffix;
    String cleanView = "test_orders_clean_" + suffix;
    String dailyView = "test_daily_orders_" + suffix;

    spark.sql("SELECT 1L AS orderId, 'east' AS region, '2026-04-15' AS orderDate, 120.0D AS amount "
      + "UNION ALL SELECT 2L, 'east', '2026-04-15', 80.0D "
      + "UNION ALL SELECT 3L, 'west', '2026-04-16', 55.0D")
      .createOrReplaceTempView(sourceTable);

    PipelineDefinition pipeline = new PipelineBuilder("test_orders_pipeline")
      .addDataset(ImmutableDatasetDefinition.temporaryView(cleanView))
      .addDataset(ImmutableDatasetDefinition.temporaryView(dailyView))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "clean_orders",
        cleanView,
        Collections.singleton(sourceTable),
        runtime -> runtime.read(sourceTable)
          .filter(functions.col("amount").gt(0))
          .withColumn("order_date", functions.to_date(functions.col("orderDate")))))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "daily_orders",
        dailyView,
        Collections.singleton(cleanView),
        runtime -> runtime.read(cleanView)
          .groupBy(functions.col("region"), functions.col("order_date"))
          .agg(
            functions.count(functions.lit(1)).alias("order_count"),
            functions.sum(functions.col("amount")).alias("total_amount"))))
      .build();

    ExecutionReport report = new PipelineOrchestrator().run(
      pipeline,
      spark,
      ExecutionOptions.defaults().withMaterializedViewSaveMode(SaveMode.Overwrite));

    Assert.assertEquals(2, report.results().size());

    Dataset<Row> dailyOrders = spark.table(dailyView);
    Assert.assertEquals(2L, dailyOrders.count());

    Row eastRow = dailyOrders.where("region = 'east'").head();
    Number orderCount = eastRow.getAs("order_count");
    Number totalAmount = eastRow.getAs("total_amount");
    Assert.assertEquals(2L, orderCount.longValue());
    Assert.assertEquals(200.0D, totalAmount.doubleValue(), 0.001D);
  }
}
