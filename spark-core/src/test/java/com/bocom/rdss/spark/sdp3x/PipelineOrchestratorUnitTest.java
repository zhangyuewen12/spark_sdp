package com.bocom.rdss.spark.sdp3x;

import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import com.bocom.rdss.spark.sdp3x.execution.FlowExecutionResult;
import com.bocom.rdss.spark.sdp3x.execution.PipelineExecutor;
import com.bocom.rdss.spark.sdp3x.graph.DependencyAnalyzer;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.PipelinePlanner;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PipelineOrchestratorUnitTest {
  @Mock
  private DependencyAnalyzer dependencyAnalyzer;
  @Mock
  private PipelinePlanner pipelinePlanner;
  @Mock
  private PipelineExecutor pipelineExecutor;
  @Mock
  private PipelineDefinition pipelineDefinition;
  @Mock
  private DependencyGraph dependencyGraph;
  @Mock
  private SparkSession sparkSession;

  @Test
  void shouldPlanWithInjectedCollaborators() {
    ExecutionPlan executionPlan = mock(ExecutionPlan.class);
    when(dependencyAnalyzer.analyze(pipelineDefinition)).thenReturn(dependencyGraph);
    when(pipelinePlanner.plan(dependencyGraph)).thenReturn(executionPlan);

    PipelineOrchestrator orchestrator = new PipelineOrchestrator(
      dependencyAnalyzer,
      pipelinePlanner,
      pipelineExecutor);

    assertSame(executionPlan, orchestrator.plan(pipelineDefinition));
    verify(dependencyAnalyzer).analyze(pipelineDefinition);
    verify(pipelinePlanner).plan(dependencyGraph);
    verifyNoInteractions(pipelineExecutor);
  }

  @Test
  void shouldRunWithInjectedCollaborators() {
    ExecutionPlan executionPlan = mock(ExecutionPlan.class);
    ExecutionReport report = new ExecutionReport(
      Collections.singletonList(FlowExecutionResult.completed("flow_a", "dataset_a")));
    ExecutionOptions executionOptions = ExecutionOptions.defaults();
    when(dependencyAnalyzer.analyze(pipelineDefinition)).thenReturn(dependencyGraph);
    when(pipelinePlanner.plan(dependencyGraph)).thenReturn(executionPlan);
    when(pipelineExecutor.execute(executionPlan, sparkSession, executionOptions)).thenReturn(report);

    PipelineOrchestrator orchestrator = new PipelineOrchestrator(
      dependencyAnalyzer,
      pipelinePlanner,
      pipelineExecutor);

    assertSame(report, orchestrator.run(pipelineDefinition, sparkSession, executionOptions));
    verify(pipelineExecutor).execute(executionPlan, sparkSession, executionOptions);
  }
}
