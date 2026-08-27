package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.graph.DefaultDependencyAnalyzer;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionStage;

import java.nio.file.Path;

/** Ordinary utility for compiling and displaying a pipeline execution plan. */
public final class SqlPipelinePlanSupport {
  private SqlPipelinePlanSupport() {
  }

  public static ExecutionPlan plan(Path projectPath) {
    PipelineDefinition pipeline = new SqlPipelineProjectCompiler().compile(projectPath);
    DependencyGraph graph = new DefaultDependencyAnalyzer().analyze(pipeline);
    return new com.bocom.rdss.spark.sdp3x.planning.TopologicalPipelinePlanner().plan(graph);
  }

  public static void print(ExecutionPlan plan) {
    System.out.println("Pipeline: " + plan.pipeline().name());
    for (ExecutionStage stage : plan.stages()) {
      System.out.println("Stage " + stage.index() + ":");
      stage.flows().forEach(flow ->
        System.out.println("  - " + flow.name() + " -> " + flow.targetDataset()));
    }
  }
}
