package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.DatasetKind;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds one parsed SQL dataset definition before it is translated to the Java pipeline model.
 */
public final class SqlPipelineDefinition {
  public enum ExecutionMode {
    QUERY_RESULT,
    SQL_STATEMENT
  }

  private final String datasetName;
  private final DatasetKind datasetKind;
  private final String querySql;
  private final Set<String> inputDatasets;
  private final Path sourceFile;
  private final int statementIndex;
  private final ExecutionMode executionMode;
  private final Map<String, String> sparkConf;

  public SqlPipelineDefinition(
      String datasetName,
      DatasetKind datasetKind,
      String querySql,
      Set<String> inputDatasets,
      Path sourceFile,
      int statementIndex,
      ExecutionMode executionMode,
      Map<String, String> sparkConf) {
    this.datasetName = datasetName;
    this.datasetKind = datasetKind;
    this.querySql = querySql;
    this.inputDatasets = Collections.unmodifiableSet(new LinkedHashSet<>(inputDatasets));
    this.sourceFile = sourceFile;
    this.statementIndex = statementIndex;
    this.executionMode = executionMode;
    this.sparkConf = Collections.unmodifiableMap(new LinkedHashMap<>(sparkConf));
  }

  public String datasetName() {
    return datasetName;
  }

  public DatasetKind datasetKind() {
    return datasetKind;
  }

  public String querySql() {
    return querySql;
  }

  public Set<String> inputDatasets() {
    return inputDatasets;
  }

  public Path sourceFile() {
    return sourceFile;
  }

  public int statementIndex() {
    return statementIndex;
  }

  public ExecutionMode executionMode() {
    return executionMode;
  }

  public Map<String, String> sparkConf() {
    return sparkConf;
  }
}
