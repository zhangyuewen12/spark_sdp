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
  public enum WriteMode {
    SAVE_AS_TABLE,
    INSERT_INTO
  }

  private final String datasetName;
  private final DatasetKind datasetKind;
  private final String querySql;
  private final Set<String> inputDatasets;
  private final Path sourceFile;
  private final int statementIndex;
  private final WriteMode writeMode;
  private final Map<String, String> sparkConf;

  public SqlPipelineDefinition(
      String datasetName,
      DatasetKind datasetKind,
      String querySql,
      Set<String> inputDatasets,
      Path sourceFile,
      int statementIndex,
      WriteMode writeMode,
      Map<String, String> sparkConf) {
    this.datasetName = datasetName;
    this.datasetKind = datasetKind;
    this.querySql = querySql;
    this.inputDatasets = Collections.unmodifiableSet(new LinkedHashSet<>(inputDatasets));
    this.sourceFile = sourceFile;
    this.statementIndex = statementIndex;
    this.writeMode = writeMode;
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

  public WriteMode writeMode() {
    return writeMode;
  }

  public Map<String, String> sparkConf() {
    return sparkConf;
  }
}
