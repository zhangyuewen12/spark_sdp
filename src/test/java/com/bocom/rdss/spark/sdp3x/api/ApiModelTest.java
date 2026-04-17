package com.bocom.rdss.spark.sdp3x.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class ApiModelTest {

  @Test
  void immutableDatasetDefinitionShouldExposeOptionalFieldsAndRemainImmutable() {
    StructType schema = new StructType().add("id", DataTypes.LongType, false);

    ImmutableDatasetDefinition dataset = ImmutableDatasetDefinition.table("orders")
      .withComment("orders table")
      .withFormat("parquet")
      .withSchema(schema)
      .withProperty("quality", "gold");

    assertEquals("orders", dataset.name());
    assertEquals(DatasetKind.TABLE, dataset.kind());
    assertEquals(Optional.of("orders table"), dataset.comment());
    assertEquals(Optional.of("parquet"), dataset.format());
    assertEquals(Optional.of(schema), dataset.schema());
    assertEquals("gold", dataset.properties().get("quality"));
    assertTrue(dataset.materialized());
    assertThrows(UnsupportedOperationException.class, () -> dataset.properties().put("x", "y"));
  }

  @Test
  void immutableDatasetDefinitionShouldRecognizeTemporaryViewsAsNonMaterialized() {
    DatasetDefinition dataset = ImmutableDatasetDefinition.temporaryView("tmp_orders");

    assertFalse(dataset.materialized());
  }

  @Test
  void immutableFlowDefinitionShouldExposeSettingsAndDefaultBatchFlag() {
    FlowQuery query = runtime -> mock(Dataset.class);

    ImmutableFlowDefinition batchFlow = ImmutableFlowDefinition.batchFlow(
      "clean_orders",
      "orders_clean",
      Collections.singleton("orders_source"),
      query).withSparkConf("spark.sql.shuffle.partitions", "1");

    ImmutableFlowDefinition onceFlow = ImmutableFlowDefinition.onceFlow(
      "seed_orders",
      "orders_seed",
      Collections.<String>emptySet(),
      query);

    assertEquals("clean_orders", batchFlow.name());
    assertEquals("orders_clean", batchFlow.targetDataset());
    assertEquals(FlowMode.BATCH, batchFlow.mode());
    assertTrue(batchFlow.isBatch());
    assertEquals(Collections.singleton("orders_source"), batchFlow.inputDatasets());
    assertEquals("1", batchFlow.sparkConf().get("spark.sql.shuffle.partitions"));
    assertSame(query, batchFlow.query());

    assertEquals(FlowMode.ONCE, onceFlow.mode());
    assertFalse(onceFlow.isBatch());
    assertThrows(UnsupportedOperationException.class, () -> batchFlow.sparkConf().put("x", "y"));
  }

  @Test
  void pipelineBuilderShouldBuildDefinitionInDeclarationOrder() {
    DatasetDefinition source = ImmutableDatasetDefinition.table("orders_source");
    DatasetDefinition sink = ImmutableDatasetDefinition.materializedView("orders_daily");
    FlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "daily_orders",
      "orders_daily",
      Collections.singleton("orders_source"),
      runtime -> mock(Dataset.class));

    PipelineDefinition pipeline = new PipelineBuilder("orders_pipeline")
      .catalog("spark_catalog")
      .database("analytics")
      .configuration("spark.sql.shuffle.partitions", "1")
      .addDataset(source)
      .addDataset(sink)
      .addFlow(flow)
      .build();

    assertEquals("orders_pipeline", pipeline.name());
    assertEquals(Optional.of("spark_catalog"), pipeline.catalog());
    assertEquals(Optional.of("analytics"), pipeline.database());
    assertEquals("1", pipeline.configuration().get("spark.sql.shuffle.partitions"));
    assertEquals(Arrays.asList(source, sink), Arrays.asList(pipeline.datasets().toArray(new DatasetDefinition[0])));
    assertEquals(Collections.singletonList(flow), Arrays.asList(pipeline.flows().toArray(new FlowDefinition[0])));
    assertEquals(Optional.of(sink), pipeline.dataset("orders_daily"));
    assertEquals(Optional.of(flow), pipeline.flow("daily_orders"));
  }

  @Test
  void pipelineBuilderShouldRejectNullsAndDuplicates() {
    PipelineBuilder builder = new PipelineBuilder("orders_pipeline");
    DatasetDefinition dataset = ImmutableDatasetDefinition.table("orders");
    FlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "flow_orders",
      "orders",
      Collections.<String>emptySet(),
      runtime -> mock(Dataset.class));

    assertThrows(NullPointerException.class, () -> new PipelineBuilder(null));
    assertThrows(NullPointerException.class, () -> builder.addDataset(null));
    assertThrows(NullPointerException.class, () -> builder.addFlow(null));

    builder.addDataset(dataset);
    assertThrows(IllegalArgumentException.class, () -> builder.addDataset(dataset));

    builder.addFlow(flow);
    assertThrows(IllegalArgumentException.class, () -> builder.addFlow(flow));
  }

  @Test
  void immutablePipelineDefinitionShouldCopyCollectionsDefensively() {
    LinkedHashMap<String, String> configuration = new LinkedHashMap<>();
    configuration.put("spark.sql.adaptive.enabled", "true");
    LinkedHashMap<String, DatasetDefinition> datasets = new LinkedHashMap<>();
    datasets.put("orders", ImmutableDatasetDefinition.table("orders"));
    LinkedHashMap<String, FlowDefinition> flows = new LinkedHashMap<>();
    flows.put("flow_orders", ImmutableFlowDefinition.batchFlow(
      "flow_orders",
      "orders",
      Collections.<String>emptySet(),
      runtime -> mock(Dataset.class)));

    ImmutablePipelineDefinition pipeline = new ImmutablePipelineDefinition(
      "pipeline",
      null,
      null,
      configuration,
      datasets,
      flows);

    configuration.put("mutated", "no");
    datasets.clear();
    flows.clear();

    assertEquals("true", pipeline.configuration().get("spark.sql.adaptive.enabled"));
    assertEquals(1, pipeline.datasets().size());
    assertEquals(1, pipeline.flows().size());
    assertThrows(UnsupportedOperationException.class, () -> pipeline.configuration().put("x", "y"));
  }
}
