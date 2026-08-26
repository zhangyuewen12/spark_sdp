package com.bocom.rdss.spark.sdp3x.sql;

/**
 * Shared dataset property keys used by the SQL-first compiler and batch executor.
 */
public final class SqlPipelineDatasetProperties {
  public static final String SOURCE_FILE = "sql.source.file";
  public static final String STATEMENT_INDEX = "sql.statement.index";
  public static final String WRITE_MODE = "sql.write.mode";
  public static final String WRITE_MODE_SAVE_AS_TABLE = "save_as_table";
  public static final String WRITE_MODE_INSERT_INTO = "insert_into";

  private SqlPipelineDatasetProperties() {
  }
}
