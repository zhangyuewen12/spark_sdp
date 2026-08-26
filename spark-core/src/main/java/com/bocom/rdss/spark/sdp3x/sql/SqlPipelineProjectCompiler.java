package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    SqlPipelineProjectSpec spec = specLoader.load(projectRoot);
    PipelineBuilder pipelineBuilder = new PipelineBuilder(spec.name());
    if (spec.catalog() != null) {
      pipelineBuilder.catalog(spec.catalog());
    }
    if (spec.database() != null) {
      pipelineBuilder.database(spec.database());
    }
    spec.configuration().forEach(pipelineBuilder::configuration);

    List<Path> sqlFiles = discoverSqlFiles(spec.sqlDirectories());
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
        for (java.util.Map.Entry<String, String> sparkConfEntry : definition.sparkConf().entrySet()) {
          flowDefinition = flowDefinition.withSparkConf(
            sparkConfEntry.getKey(),
            sparkConfEntry.getValue());
        }
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
        SqlPipelineDatasetProperties.WRITE_MODE,
        definition.writeMode() == SqlPipelineDefinition.WriteMode.INSERT_INTO
          ? SqlPipelineDatasetProperties.WRITE_MODE_INSERT_INTO
          : SqlPipelineDatasetProperties.WRITE_MODE_SAVE_AS_TABLE);
  }

  private String flowName(SqlPipelineDefinition definition) {
    return "sql_flow_" + String.format("%03d", definition.statementIndex())
      + "_" + sanitize(definition.datasetName());
  }

  private String sanitize(String text) {
    return text.replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  private List<Path> discoverSqlFiles(List<Path> sqlDirectories) {
    Set<Path> uniqueFiles = new LinkedHashSet<>();
    for (Path sqlDirectory : sqlDirectories) {
      try (Stream<Path> stream = Files.walk(sqlDirectory)) {
        List<Path> discovered = stream
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".sql"))
          .sorted(Comparator.comparing(Path::toString))
          .collect(Collectors.toList());
        uniqueFiles.addAll(discovered);
      } catch (IOException e) {
        throw new SqlPipelineProjectException(
          "Failed to scan SQL library directory: " + sqlDirectory, e);
      }
    }
    return new ArrayList<>(uniqueFiles);
  }
}
