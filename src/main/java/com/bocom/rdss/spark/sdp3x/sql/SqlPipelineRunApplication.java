package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.PipelineOrchestrator;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import org.apache.spark.sql.SparkSession;

/**
 * Spark-aware entrypoint for executing SQL pipeline projects.
 */
public final class SqlPipelineRunApplication {
  private SqlPipelineRunApplication() {
  }

  public static void run(SqlPipelineCliOptions cliOptions) {
    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner();
    PipelineDefinition pipelineDefinition = runner.compile(cliOptions.projectPath());
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName(pipelineDefinition.name())
      .enableHiveSupport();
    if (cliOptions.master() != null) {
      sparkBuilder.master(cliOptions.master());
    } else if (!cliOptions.submittedViaSparkSubmit()) {
      sparkBuilder.master("local[*]");
      sparkBuilder.config(
        "spark.sql.warehouse.dir",
        cliOptions.projectPath().toAbsolutePath().normalize().resolve("target/spark-warehouse").toString());
    }
    SparkSession sparkSession = sparkBuilder.getOrCreate();

    try {
      ExecutionReport report = new PipelineOrchestrator().run(
        pipelineDefinition,
        sparkSession,
        ExecutionOptions.defaults());
      System.out.println(
        "Executed " + report.results().size() + " flow(s) for pipeline '" + pipelineDefinition.name() + "'.");
    } finally {
      sparkSession.stop();
    }
  }
}
