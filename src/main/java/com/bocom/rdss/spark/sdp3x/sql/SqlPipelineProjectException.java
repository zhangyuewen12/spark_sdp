package com.bocom.rdss.spark.sdp3x.sql;

/**
 * Raised when a SQL pipeline project cannot be loaded or compiled.
 */
public final class SqlPipelineProjectException extends RuntimeException {
  public SqlPipelineProjectException(String message) {
    super(message);
  }

  public SqlPipelineProjectException(String message, Throwable cause) {
    super(message, cause);
  }
}
