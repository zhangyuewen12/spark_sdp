package com.bocom.rdss.spark.sdp3x.execution;

import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.graph.PipelineValidationException;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionStage;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LocalPipelineExecutorTest {
  @Mock
  private DatasetMaterializer datasetMaterializer;
  @Mock
  private FlowRunner flowRunner;
  @Mock
  private SparkSession sparkSession;
  @Mock
  private RuntimeConfig runtimeConfig;
  @Mock
  private Catalog catalog;

  @AfterEach
  void clearInterruptFlag() {
    Thread.interrupted();
  }

  @Test
  void shouldMaterializeEachDatasetOnlyOnceAndCollectResults() throws Exception {
    PipelineDefinition pipeline = new PipelineBuilder("pipeline")
      .addDataset(ImmutableDatasetDefinition.table("sink"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "flow_a",
        "sink",
        Collections.<String>emptySet(),
        runtime -> null))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "flow_b",
        "sink",
        Collections.<String>emptySet(),
        runtime -> null))
      .build();
    ExecutionPlan plan = planFor(pipeline, pipeline.flows().toArray(new FlowDefinition[0]));
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkSession.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn(null);
    when(flowRunner.supports(any(FlowDefinition.class), any(DatasetDefinition.class))).thenReturn(true);
    when(flowRunner.run(any(FlowDefinition.class), any(DatasetDefinition.class), any(), any()))
      .thenAnswer(invocation -> {
        FlowDefinition flow = invocation.getArgument(0);
        return FlowExecutionResult.completed(flow.name(), flow.targetDataset());
      });

    LocalPipelineExecutor executor = new LocalPipelineExecutor(
      datasetMaterializer,
      Collections.singletonList(flowRunner));

    ExecutionReport report = executor.execute(plan, sparkSession, ExecutionOptions.defaults().withMaxConcurrentFlows(2));

    assertEquals(2, report.results().size());
    verify(datasetMaterializer, times(1)).materialize(any(DatasetDefinition.class), any(), any());
    verify(flowRunner, times(2)).run(any(FlowDefinition.class), any(DatasetDefinition.class), any(), any());
  }

  @Test
  void shouldWrapRunnerFailures() throws Exception {
    PipelineDefinition pipeline = singleFlowPipeline("flow_a", "sink");
    ExecutionPlan plan = planFor(pipeline, pipeline.flow("flow_a").get());
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkSession.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn(null);
    when(flowRunner.supports(any(FlowDefinition.class), any(DatasetDefinition.class))).thenReturn(true);
    when(flowRunner.run(any(FlowDefinition.class), any(DatasetDefinition.class), any(), any()))
      .thenThrow(new IllegalStateException("boom"));

    LocalPipelineExecutor executor = new LocalPipelineExecutor(
      datasetMaterializer,
      Collections.singletonList(flowRunner));

    PipelineExecutionException exception = assertThrows(
      PipelineExecutionException.class,
      () -> executor.execute(plan, sparkSession, ExecutionOptions.defaults()));

    assertEquals("Pipeline execution failed", exception.getMessage());
    assertEquals("boom", exception.getCause().getMessage());
  }

  @Test
  void shouldWrapMissingRunnersAsPipelineFailures() {
    PipelineDefinition pipeline = singleFlowPipeline("flow_a", "sink");
    ExecutionPlan plan = planFor(pipeline, pipeline.flow("flow_a").get());
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkSession.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn(null);
    when(flowRunner.supports(any(FlowDefinition.class), any(DatasetDefinition.class))).thenReturn(false);

    LocalPipelineExecutor executor = new LocalPipelineExecutor(
      datasetMaterializer,
      Collections.singletonList(flowRunner));

    PipelineExecutionException exception = assertThrows(
      PipelineExecutionException.class,
      () -> executor.execute(plan, sparkSession, ExecutionOptions.defaults()));

    assertEquals("Pipeline execution failed", exception.getMessage());
    assertTrue(exception.getCause().getMessage().contains("No runner available for flow"));
  }

  @Test
  void shouldFailFastWhenTargetDatasetIsUnknown() {
    FlowDefinition flow = ImmutableFlowDefinition.batchFlow(
      "flow_missing",
      "missing_dataset",
      Collections.<String>emptySet(),
      runtime -> null);
    PipelineDefinition pipeline = new PipelineBuilder("pipeline").addFlow(flow).build();
    ExecutionPlan plan = planFor(pipeline, flow);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkSession.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn(null);

    LocalPipelineExecutor executor = new LocalPipelineExecutor(
      datasetMaterializer,
      Collections.singletonList(flowRunner));

    PipelineValidationException exception = assertThrows(
      PipelineValidationException.class,
      () -> executor.execute(plan, sparkSession, ExecutionOptions.defaults()));

    assertTrue(exception.getMessage().contains("Unknown target dataset"));
  }

  @Test
  void shouldPreserveInterruptedStatusWhenExecutionIsInterrupted() {
    PipelineDefinition pipeline = singleFlowPipeline("flow_a", "sink");
    ExecutionPlan plan = planFor(pipeline, pipeline.flow("flow_a").get());
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkSession.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn(null);

    LocalPipelineExecutor executor = new LocalPipelineExecutor(
      datasetMaterializer,
      Collections.singletonList(flowRunner));

    Thread.currentThread().interrupt();
    PipelineExecutionException exception = assertThrows(
      PipelineExecutionException.class,
      () -> executor.execute(plan, sparkSession, ExecutionOptions.defaults()));

    assertEquals("Pipeline execution was interrupted", exception.getMessage());
    assertTrue(Thread.currentThread().isInterrupted());
  }

  private PipelineDefinition singleFlowPipeline(String flowName, String targetDataset) {
    return new PipelineBuilder("pipeline")
      .addDataset(ImmutableDatasetDefinition.table(targetDataset))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        flowName,
        targetDataset,
        Collections.<String>emptySet(),
        runtime -> null))
      .build();
  }

  private ExecutionPlan planFor(PipelineDefinition pipeline, FlowDefinition... flows) {
    LinkedHashMap<String, FlowDefinition> flowsByName = new LinkedHashMap<>();
    LinkedHashMap<String, java.util.Set<String>> upstream = new LinkedHashMap<>();
    LinkedHashMap<String, java.util.Set<String>> downstream = new LinkedHashMap<>();
    for (FlowDefinition flow : flows) {
      flowsByName.put(flow.name(), flow);
      upstream.put(flow.name(), Collections.<String>emptySet());
      downstream.put(flow.name(), Collections.<String>emptySet());
    }
    DependencyGraph graph = new DependencyGraph(
      pipeline,
      flowsByName,
      upstream,
      downstream,
      new LinkedHashMap<String, java.util.List<String>>());
    return new ExecutionPlan(
      pipeline,
      graph,
      Collections.singletonList(new ExecutionStage(0, Arrays.asList(flows))));
  }
}
