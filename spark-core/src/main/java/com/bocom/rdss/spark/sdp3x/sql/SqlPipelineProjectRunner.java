package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.PipelineOrchestrator;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

/**
 * Loads and runs SQL pipeline projects made of a pipeline spec and {@code .sql} files.
 */
public final class SqlPipelineProjectRunner {
  private final SqlPipelineProjectCompiler compiler;

  public SqlPipelineProjectRunner() {
    this(new SqlPipelineProjectCompiler());
  }

  public SqlPipelineProjectRunner(SqlPipelineProjectCompiler compiler) {
    this.compiler = compiler;
  }

  public PipelineDefinition compile(Path projectRoot) {
    return compiler.compile(projectRoot);
  }

  public PipelineDefinition compile(SqlPipelineProjectSpec spec) {
    return compiler.compile(spec);
  }

  public ExecutionReport run(Path projectRoot, SparkSession sparkSession,
      ExecutionOptions executionOptions) {
    return new PipelineOrchestrator().runInDeclarationOrder(
      compile(projectRoot), sparkSession, executionOptions);
  }
}
