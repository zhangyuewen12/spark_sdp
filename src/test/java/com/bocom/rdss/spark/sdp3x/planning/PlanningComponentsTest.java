package com.bocom.rdss.spark.sdp3x.planning;

import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.graph.DefaultDependencyAnalyzer;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.graph.PipelineValidationException;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class PlanningComponentsTest {

  @Test
  void plannerShouldCreateStableStages() {
    PipelineDefinition pipeline = new PipelineBuilder("pipeline")
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

    DependencyGraph graph = new DefaultDependencyAnalyzer().analyze(pipeline);
    ExecutionPlan plan = new TopologicalPipelinePlanner().plan(graph);

    assertEquals(3, plan.stages().size());
    assertEquals("seed_orders", plan.stages().get(0).flows().get(0).name());
    assertEquals("clean_orders", plan.stages().get(1).flows().get(0).name());
    assertEquals("daily_orders", plan.stages().get(2).flows().get(0).name());
  }

  @Test
  void plannerShouldRejectIncompletePlans() {
    FlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "flow_a",
      "dataset_a",
      Collections.<String>emptySet(),
      runtime -> mock(Dataset.class));
    PipelineDefinition pipeline = new PipelineBuilder("pipeline")
      .addDataset(ImmutableDatasetDefinition.table("dataset_a"))
      .addFlow(flow)
      .build();

    LinkedHashMap<String, FlowDefinition> flows = new LinkedHashMap<>();
    flows.put("flow_a", flow);
    LinkedHashMap<String, java.util.Set<String>> upstream = new LinkedHashMap<>();
    upstream.put("flow_a", new LinkedHashSet<>(Collections.singleton("ghost_flow")));
    LinkedHashMap<String, java.util.Set<String>> downstream = new LinkedHashMap<>();
    downstream.put("flow_a", new LinkedHashSet<String>());
    DependencyGraph graph = new DependencyGraph(
      pipeline,
      flows,
      upstream,
      downstream,
      new LinkedHashMap<String, java.util.List<String>>());

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> new TopologicalPipelinePlanner().plan(graph));

    assertTrue(exception.getMessage().contains("Unable to build a complete execution plan"));
  }

  @Test
  void executionPlanAndStageShouldCopyListsDefensively() {
    FlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "flow_a",
      "dataset_a",
      Collections.<String>emptySet(),
      runtime -> mock(Dataset.class));
    ArrayList<FlowDefinition> flows = new ArrayList<>();
    flows.add(flow);
    ExecutionStage stage = new ExecutionStage(0, flows);
    flows.clear();

    ArrayList<ExecutionStage> stages = new ArrayList<>();
    stages.add(stage);
    ExecutionPlan plan = new ExecutionPlan(null, null, stages);
    stages.clear();

    assertEquals(0, stage.index());
    assertEquals(1, stage.flows().size());
    assertEquals(1, plan.stages().size());
    assertThrows(UnsupportedOperationException.class, () -> stage.flows().add(flow));
    assertThrows(UnsupportedOperationException.class, () -> plan.stages().add(stage));
  }
}
