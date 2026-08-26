package com.bocom.rdss.spark.sdp3x.execution;

import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineRuntime;
import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineDatasetProperties;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchFlowRunnerTest {
  @Mock
  private PipelineRuntime runtime;
  @Mock
  private SparkSession sparkSession;
  @Mock
  private RuntimeConfig runtimeConfig;
  @SuppressWarnings("unchecked")
  @Mock
  private Dataset<Row> dataset;
  @SuppressWarnings("unchecked")
  @Mock
  private DataFrameWriter<Row> writer;

  @Test
  void shouldSupportBatchAndOnceFlows() {
    BatchFlowRunner runner = new BatchFlowRunner();

    assertTrue(runner.supports(
      ImmutableFlowDefinition.batchFlow("flow_a", "dataset_a", Collections.<String>emptySet(), rt -> null),
      ImmutableDatasetDefinition.table("dataset_a")));
    assertTrue(runner.supports(
      ImmutableFlowDefinition.onceFlow("flow_b", "dataset_b", Collections.<String>emptySet(), rt -> null),
      ImmutableDatasetDefinition.table("dataset_b")));
  }

  @Test
  void shouldPopulateTemporaryViews() throws Exception {
    BatchFlowRunner runner = new BatchFlowRunner();
    ImmutableFlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "clean_orders",
      "orders_clean",
      Collections.singleton("orders_source"),
      ignored -> dataset).withSparkConf("spark.sql.shuffle.partitions", "1");
    when(runtime.spark()).thenReturn(sparkSession);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getOption("spark.sql.shuffle.partitions")).thenReturn(scala.Option.empty());
    when(dataset.isStreaming()).thenReturn(false);

    FlowExecutionResult result = runner.run(
      flow,
      ImmutableDatasetDefinition.temporaryView("orders_clean"),
      runtime,
      ExecutionOptions.defaults());

    verify(dataset).createOrReplaceTempView("orders_clean");
    verify(runtimeConfig).set("spark.sql.shuffle.partitions", "1");
    verify(runtimeConfig).unset("spark.sql.shuffle.partitions");
    assertEquals("clean_orders", result.flowName());
    assertEquals("orders_clean", result.targetDataset());
  }

  @Test
  void shouldInsertIntoConfiguredTables() throws Exception {
    BatchFlowRunner runner = new BatchFlowRunner();
    ImmutableFlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "daily_orders",
      "daily_orders",
      Collections.singleton("orders_clean"),
      ignored -> dataset);
    when(runtime.spark()).thenReturn(sparkSession);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(dataset.isStreaming()).thenReturn(false);
    when(dataset.write()).thenReturn(writer);
    when(writer.mode("append")).thenReturn(writer);

    runner.run(
      flow,
      ImmutableDatasetDefinition.table("daily_orders")
        .withProperty(SqlPipelineDatasetProperties.WRITE_MODE, SqlPipelineDatasetProperties.WRITE_MODE_INSERT_INTO),
      runtime,
      ExecutionOptions.defaults());

    verify(writer).insertInto("daily_orders");
  }

  @Test
  void shouldSaveAsTableWithConfiguredFormat() throws Exception {
    BatchFlowRunner runner = new BatchFlowRunner();
    ImmutableFlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "daily_orders",
      "daily_orders",
      Collections.singleton("orders_clean"),
      ignored -> dataset);
    when(runtime.spark()).thenReturn(sparkSession);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(dataset.isStreaming()).thenReturn(false);
    when(dataset.write()).thenReturn(writer);
    when(writer.mode(SaveMode.Overwrite)).thenReturn(writer);
    when(writer.format("parquet")).thenReturn(writer);

    runner.run(
      flow,
      ImmutableDatasetDefinition.materializedView("daily_orders").withFormat("parquet"),
      runtime,
      ExecutionOptions.defaults());

    verify(writer).format("parquet");
    verify(writer).saveAsTable("daily_orders");
  }

  @Test
  void shouldRejectNullOrStreamingQueryResults() {
    BatchFlowRunner runner = new BatchFlowRunner();
    when(runtime.spark()).thenReturn(sparkSession);
    when(sparkSession.conf()).thenReturn(runtimeConfig);

    NullPointerException nullResult = assertThrows(
      NullPointerException.class,
      () -> runner.run(
        ImmutableFlowDefinition.batchFlow("flow_a", "dataset_a", Collections.<String>emptySet(), ignored -> null),
        ImmutableDatasetDefinition.table("dataset_a"),
        runtime,
        ExecutionOptions.defaults()));
    assertTrue(nullResult.getMessage().contains("Flow query returned null"));

    when(dataset.isStreaming()).thenReturn(true);
    PipelineExecutionException streaming = assertThrows(
      PipelineExecutionException.class,
      () -> runner.run(
        ImmutableFlowDefinition.batchFlow("flow_b", "dataset_b", Collections.<String>emptySet(), ignored -> dataset),
        ImmutableDatasetDefinition.table("dataset_b"),
        runtime,
        ExecutionOptions.defaults()));
    assertTrue(streaming.getMessage().contains("does not support streaming datasets"));
  }
}
