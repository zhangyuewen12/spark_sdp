package com.bocom.rdss.spark.sdp3x.graph;

import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DefaultDependencyAnalyzerTest {
  private final DefaultDependencyAnalyzer analyzer = new DefaultDependencyAnalyzer();

  @Test
  void shouldAnalyzeDependenciesAndExposeStableRelationships() {
    PipelineDefinition pipeline = new PipelineBuilder("orders_pipeline")
      .addDataset(ImmutableDatasetDefinition.table("orders_source"))
      .addDataset(ImmutableDatasetDefinition.temporaryView("orders_clean"))
      .addDataset(ImmutableDatasetDefinition.materializedView("daily_orders"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "seed_orders",
        "orders_source",
        Collections.<String>emptySet(),
        runtime -> mock(Dataset.class)))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "clean_orders",
        "orders_clean",
        Collections.singleton("orders_source"),
        runtime -> mock(Dataset.class)))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "daily_orders",
        "daily_orders",
        Collections.singleton("orders_clean"),
        runtime -> mock(Dataset.class)))
      .build();

    DependencyGraph graph = analyzer.analyze(pipeline);

    assertEquals(3, graph.flows().size());
    assertEquals(Collections.singletonList("seed_orders"), graph.producers("orders_source"));
    assertEquals(Collections.singleton("clean_orders"), graph.upstreamFlows("daily_orders"));
    assertEquals(Collections.singleton("daily_orders"), graph.downstreamFlows("clean_orders"));
    assertTrue(graph.flow("seed_orders").isPresent());
    assertFalse(graph.flow("missing").isPresent());
    assertTrue(graph.upstreamFlows("missing").isEmpty());
    assertTrue(graph.downstreamFlows("missing").isEmpty());
    assertTrue(graph.producers("missing").isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> graph.producers("orders_source").add("x"));
  }

  @Test
  void shouldRejectFlowTargetingUndefinedDataset() {
    PipelineDefinition pipeline = new PipelineBuilder("invalid")
      .addDataset(ImmutableDatasetDefinition.table("orders_source"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "flow_missing",
        "orders_missing",
        Collections.<String>emptySet(),
        runtime -> mock(Dataset.class)))
      .build();

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> analyzer.analyze(pipeline));

    assertTrue(exception.getMessage().contains("undefined dataset"));
  }

  @Test
  void shouldRejectDatasetWithoutIncomingFlow() {
    PipelineDefinition pipeline = new PipelineBuilder("invalid")
      .addDataset(ImmutableDatasetDefinition.table("orders_source"))
      .build();

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> analyzer.analyze(pipeline));

    assertTrue(exception.getMessage().contains("does not have an incoming flow"));
  }

  @Test
  void shouldRejectMultipleWritersForMaterializedView() {
    PipelineDefinition pipeline = new PipelineBuilder("invalid")
      .addDataset(ImmutableDatasetDefinition.table("orders_source"))
      .addDataset(ImmutableDatasetDefinition.materializedView("daily_orders"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "seed_orders",
        "orders_source",
        Collections.<String>emptySet(),
        runtime -> mock(Dataset.class)))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "daily_orders_a",
        "daily_orders",
        Collections.singleton("orders_source"),
        runtime -> mock(Dataset.class)))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "daily_orders_b",
        "daily_orders",
        Collections.singleton("orders_source"),
        runtime -> mock(Dataset.class)))
      .build();

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> analyzer.analyze(pipeline));

    assertTrue(exception.getMessage().contains("exactly one flow"));
  }

  @Test
  void shouldRejectNonBatchWriterForTemporaryView() {
    PipelineDefinition pipeline = new PipelineBuilder("invalid")
      .addDataset(ImmutableDatasetDefinition.table("orders_source"))
      .addDataset(ImmutableDatasetDefinition.temporaryView("orders_clean"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "seed_orders",
        "orders_source",
        Collections.<String>emptySet(),
        runtime -> mock(Dataset.class)))
      .addFlow(ImmutableFlowDefinition.onceFlow(
        "clean_orders",
        "orders_clean",
        Collections.singleton("orders_source"),
        runtime -> mock(Dataset.class)))
      .build();

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> analyzer.analyze(pipeline));

    assertTrue(exception.getMessage().contains("Temporary view only accepts normal batch flow"));
  }

  @Test
  void shouldRejectCyclicDependencies() {
    PipelineDefinition pipeline = new PipelineBuilder("invalid")
      .addDataset(ImmutableDatasetDefinition.table("dataset_a"))
      .addDataset(ImmutableDatasetDefinition.table("dataset_b"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "flow_a",
        "dataset_a",
        Collections.singleton("dataset_b"),
        runtime -> mock(Dataset.class)))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "flow_b",
        "dataset_b",
        Collections.singleton("dataset_a"),
        runtime -> mock(Dataset.class)))
      .build();

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> analyzer.analyze(pipeline));

    assertTrue(exception.getMessage().contains("cyclic flow dependency"));
  }

  @Test
  void dependencyGraphShouldDefensivelyCopyConstructorInputs() {
    PipelineDefinition pipeline = new PipelineBuilder("pipeline").build();
    LinkedHashMap<String, com.bocom.rdss.spark.sdp3x.api.FlowDefinition> flows = new LinkedHashMap<>();
    LinkedHashMap<String, java.util.Set<String>> upstream = new LinkedHashMap<>();
    LinkedHashMap<String, java.util.Set<String>> downstream = new LinkedHashMap<>();
    LinkedHashMap<String, java.util.List<String>> producers = new LinkedHashMap<>();

    DependencyGraph graph = new DependencyGraph(pipeline, flows, upstream, downstream, producers);

    flows.put("flow", ImmutableFlowDefinition.batchFlow(
      "flow",
      "dataset",
      new LinkedHashSet<String>(),
      runtime -> mock(Dataset.class)));
    upstream.put("flow", new LinkedHashSet<>(Arrays.asList("a", "b")));
    downstream.put("flow", new LinkedHashSet<>(Collections.singletonList("x")));
    producers.put("dataset", Collections.singletonList("flow"));

    assertTrue(graph.flows().isEmpty());
    assertTrue(graph.producers("dataset").isEmpty());
  }

  @Test
  void pipelineValidationExceptionShouldPreserveMessage() {
    PipelineValidationException exception = new PipelineValidationException("bad graph");

    assertEquals("bad graph", exception.getMessage());
  }
}
