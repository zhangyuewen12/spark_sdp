package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.graph.DefaultDependencyAnalyzer;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionStage;
import com.bocom.rdss.spark.sdp3x.planning.TopologicalPipelinePlanner;

import java.lang.reflect.InvocationTargetException;

/**
 * Command-line entry point that keeps help and dry-run available without Spark runtime jars.
 */
public final class SqlPipelineCliMain {
  private SqlPipelineCliMain() {
  }

  public static void main(String[] args) {
    SqlPipelineCliOptions cliOptions;
    try {
      cliOptions = SqlPipelineCliOptions.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.err.println();
      printUsage();
      return;
    }

    if (cliOptions.command() == SqlPipelineCliOptions.Command.HELP) {
      printUsage();
      return;
    }

    if (cliOptions.command() == SqlPipelineCliOptions.Command.DRY_RUN) {
      printPlan(dryRun(cliOptions.projectPath()));
      return;
    }

    launchRunApplication(cliOptions);
  }

  private static ExecutionPlan dryRun(java.nio.file.Path projectPath) {
    PipelineDefinition pipelineDefinition = new SqlPipelineProjectCompiler().compile(projectPath);
    DependencyGraph dependencyGraph = new DefaultDependencyAnalyzer().analyze(pipelineDefinition);
    return new TopologicalPipelinePlanner().plan(dependencyGraph);
  }

  private static void launchRunApplication(SqlPipelineCliOptions cliOptions) {
    try {
      Class<?> appClass = Class.forName("com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication");
      appClass.getMethod("run", SqlPipelineCliOptions.class).invoke(null, cliOptions);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Missing Spark run application class.", e);
    } catch (NoClassDefFoundError e) {
      throw new IllegalStateException(
        "Spark runtime classes are not available. Use spark-submit, the spark-sdp script, or run the main method from an IDE with Spark dependencies.",
        e);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Missing Spark run application entrypoint.", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Cannot access Spark run application entrypoint.", e);
    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      if (target instanceof RuntimeException) {
        throw (RuntimeException) target;
      }
      throw new IllegalStateException("Failed to run SQL pipeline.", target);
    }
  }

  static void printPlan(ExecutionPlan executionPlan) {
    System.out.println("Pipeline: " + executionPlan.pipeline().name());
    for (ExecutionStage stage : executionPlan.stages()) {
      System.out.println("Stage " + stage.index() + ":");
      stage.flows().forEach(flow ->
        System.out.println("  - " + flow.name() + " -> " + flow.targetDataset()));
    }
  }

  static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  bin/spark-sdp [spark-submit options] run [--spec <specPath> | <projectRoot>]");
    System.out.println("  bin/spark-sdp dry-run [--spec <specPath> | <projectRoot>]");
    System.out.println("  bin/spark-sdp help");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  bin/spark-sdp dry-run --spec examples/sql-batch-pipeline/spark-pipeline.yml");
    System.out.println("  bin/spark-sdp --master yarn --deploy-mode client run --spec examples/sql-batch-pipeline/spark-pipeline.yml");
    System.out.println("  bin/spark-sdp run examples/sql-batch-pipeline");
    System.out.println();
    System.out.println("Direct jar compatibility:");
    System.out.println("  java -jar spark-sdp-1.0.jar help");
    System.out.println();
    System.out.println("Packaged dry-run:");
    System.out.println("  ${SPARK_HOME}/bin/spark-submit --class com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain spark-sdp-1.0.jar dry-run --spec examples/sql-batch-pipeline/spark-pipeline.yml");
    System.out.println();
    System.out.println("Local debug:");
    System.out.println("  Run com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain or");
    System.out.println("  com.bocom.rdss.spark.sdp3x.example.SqlPipelineLocalDebugMain from your IDE.");
  }
}
