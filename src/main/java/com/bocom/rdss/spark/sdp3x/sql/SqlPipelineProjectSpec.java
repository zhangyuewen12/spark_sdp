package com.bocom.rdss.spark.sdp3x.sql;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes one SQL pipeline project rooted at a directory with a {@code spark-pipeline.yml} file.
 */
public final class SqlPipelineProjectSpec {
  private final Path rootDirectory;
  private final String name;
  private final String catalog;
  private final String database;
  private final Map<String, String> configuration;
  private final List<Path> sqlDirectories;

  public SqlPipelineProjectSpec(
      Path rootDirectory,
      String name,
      String catalog,
      String database,
      Map<String, String> configuration,
      List<Path> sqlDirectories) {
    this.rootDirectory = rootDirectory;
    this.name = name;
    this.catalog = catalog;
    this.database = database;
    this.configuration = Collections.unmodifiableMap(new LinkedHashMap<>(configuration));
    this.sqlDirectories = Collections.unmodifiableList(new ArrayList<>(sqlDirectories));
  }

  public Path rootDirectory() {
    return rootDirectory;
  }

  public String name() {
    return name;
  }

  public String catalog() {
    return catalog;
  }

  public String database() {
    return database;
  }

  public Map<String, String> configuration() {
    return configuration;
  }

  public List<Path> sqlDirectories() {
    return sqlDirectories;
  }
}
