package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.enums.ExecutionMode;
import com.bocom.rdss.spark.sdp3x.enums.JobType;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compiles one SQL pipeline project into the existing Java pipeline model.
 */
public final class SqlPipelineProjectCompiler {
  private final SqlPipelineProjectSpecLoader specLoader;
  private final SqlPipelineParser sqlPipelineParser;

  public SqlPipelineProjectCompiler() {
    this(new SqlPipelineProjectSpecLoader(), new SqlPipelineParser());
  }

  public SqlPipelineProjectCompiler(
      SqlPipelineProjectSpecLoader specLoader,
      SqlPipelineParser sqlPipelineParser) {
    this.specLoader = specLoader;
    this.sqlPipelineParser = sqlPipelineParser;
  }

  public PipelineDefinition compile(Path projectRoot) {
    return compile(specLoader.load(projectRoot));
  }

  public PipelineDefinition compile(SqlPipelineProjectSpec spec) {
    if (spec.jobType() != JobType.SPARK_SQL) {
      throw new SqlPipelineProjectException("SQL compiler cannot compile job-type " + spec.jobType());
    }
    PipelineBuilder pipelineBuilder = new PipelineBuilder(spec.name());
    if (spec.catalog() != null) {
      pipelineBuilder.catalog(spec.catalog());
    }
    if (spec.database() != null) {
      pipelineBuilder.database(spec.database());
    }
    spec.configuration().forEach(pipelineBuilder::configuration);

    List<Path> sqlFiles = spec.sqlFiles();
    if (sqlFiles.isEmpty()) {
      throw new SqlPipelineProjectException(
        "No SQL files were found in project: " + spec.rootDirectory());
    }

    int statementIndex = 1;
    Set<String> declaredDatasets = new HashSet<>();
    for (Path sqlFile : sqlFiles) {
      List<SqlPipelineDefinition> definitions = sqlPipelineParser.parse(sqlFile, statementIndex);
      statementIndex += definitions.size();
      for (SqlPipelineDefinition definition : definitions) {
        if (declaredDatasets.add(definition.datasetName())) {
          pipelineBuilder.addDataset(createDatasetDefinition(spec, definition));
        }
        ImmutableFlowDefinition flowDefinition = ImmutableFlowDefinition.batchFlow(
          flowName(definition),
          definition.datasetName(),
          definition.inputDatasets(),
          runtime -> runtime.spark().sql(definition.querySql()));
        pipelineBuilder.addFlow(flowDefinition);
      }
    }
    return pipelineBuilder.build();
  }

  private DatasetDefinition createDatasetDefinition(
      SqlPipelineProjectSpec spec,
      SqlPipelineDefinition definition) {
    ImmutableDatasetDefinition datasetDefinition;
    if (definition.datasetKind() == DatasetKind.MATERIALIZED_VIEW) {
      datasetDefinition = ImmutableDatasetDefinition.materializedView(definition.datasetName())
        .withFormat("parquet");
    } else if (definition.datasetKind() == DatasetKind.TEMPORARY_VIEW) {
      datasetDefinition = ImmutableDatasetDefinition.temporaryView(definition.datasetName());
    } else if (definition.datasetKind() == DatasetKind.TABLE) {
      datasetDefinition = ImmutableDatasetDefinition.table(definition.datasetName());
    } else {
      throw new SqlPipelineProjectException(
        "Unsupported SQL dataset kind for MVP: " + definition.datasetKind());
    }

    Path relativePath = spec.rootDirectory().relativize(definition.sourceFile());
    return datasetDefinition
      .withProperty(SqlPipelineDatasetProperties.SOURCE_FILE, relativePath.toString())
      .withProperty(SqlPipelineDatasetProperties.STATEMENT_INDEX, String.valueOf(definition.statementIndex()))
      .withProperty(
        SqlPipelineDatasetProperties.EXECUTION_MODE,
        definition.executionMode() == ExecutionMode.SQL_STATEMENT
          ? SqlPipelineDatasetProperties.EXECUTION_MODE_SQL_STATEMENT
          : SqlPipelineDatasetProperties.EXECUTION_MODE_QUERY_RESULT);
  }

  private String flowName(SqlPipelineDefinition definition) {
    return "sql_flow_" + String.format("%03d", definition.statementIndex())
      + "_" + sanitize(definition.datasetName());
  }

  private String sanitize(String text) {
    return text.replaceAll("[^A-Za-z0-9_.-]", "_");
  }

}
