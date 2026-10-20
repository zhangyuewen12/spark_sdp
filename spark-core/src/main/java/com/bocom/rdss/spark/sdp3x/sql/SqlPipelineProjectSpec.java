package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.enums.JobType;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a pipeline job and its optional selected task file.
 */
public final class SqlPipelineProjectSpec {
  private final Path rootDirectory;
  private final String name;
  private final String catalog;
  private final String database;
  private final Map<String, String> configuration;
  private final SqlFileSource sqlFileSource;
  private final Path taskFile;
  private final JobType jobType;
  private final List<String> compactionTables;

  public SqlPipelineProjectSpec(
      Path rootDirectory,
      String name,
      String catalog,
      String database,
      Map<String, String> configuration,
      List<Path> sqlDirectories) {
    this(rootDirectory, name, catalog, database, configuration, sqlDirectories,
      JobType.SPARK_SQL, Collections.emptyList(), null);
  }

  public SqlPipelineProjectSpec(
      Path rootDirectory,
      String name,
      String catalog,
      String database,
      Map<String, String> configuration,
      List<Path> sqlDirectories,
      JobType jobType,
      List<String> compactionTables) {
    this(rootDirectory, name, catalog, database, configuration, sqlDirectories,
      jobType, compactionTables, null);
  }

  public SqlPipelineProjectSpec(
      Path rootDirectory,
      String name,
      String catalog,
      String database,
      Map<String, String> configuration,
      List<Path> sqlDirectories,
      JobType jobType,
      List<String> compactionTables,
      Path taskFile) {
    this(rootDirectory, name, catalog, database, configuration,
      jobType == JobType.SPARK_SQL && taskFile != null
        ? SqlFileSource.fromSqlFile(taskFile)
        : SqlFileSource.fromSqlDirectories(sqlDirectories),
      jobType, compactionTables, taskFile);
  }

  SqlPipelineProjectSpec(
      Path rootDirectory,
      String name,
      String catalog,
      String database,
      Map<String, String> configuration,
      SqlFileSource sqlFileSource,
      JobType jobType,
      List<String> compactionTables,
      Path taskFile) {
    this.rootDirectory = rootDirectory;
    this.name = name;
    this.catalog = catalog;
    this.database = database;
    this.configuration = Collections.unmodifiableMap(new LinkedHashMap<>(configuration));
    this.sqlFileSource = sqlFileSource;
    this.taskFile = taskFile;
    this.jobType = jobType;
    this.compactionTables = Collections.unmodifiableList(new ArrayList<>(compactionTables));
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
    return sqlFileSource.sqlDirectories();
  }

  List<Path> sqlFiles() {
    return sqlFileSource.resolveSqlFiles();
  }

  public Path taskFile() {
    return taskFile;
  }

  public JobType jobType() {
    return jobType;
  }

  public List<String> compactionTables() {
    return compactionTables;
  }
}
