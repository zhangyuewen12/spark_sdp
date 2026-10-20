package com.bocom.rdss.spark.sdp3x.sql;

import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

/** Spark session and optional local warehouse created for one pipeline run. */
final class SparkSessionContext {
  private final SparkSession sparkSession;
  private final Path localWarehouseDirectory;

  SparkSessionContext(SparkSession sparkSession, Path localWarehouseDirectory) {
    this.sparkSession = sparkSession;
    this.localWarehouseDirectory = localWarehouseDirectory;
  }

  SparkSession sparkSession() {
    return sparkSession;
  }

  Path localWarehouseDirectory() {
    return localWarehouseDirectory;
  }
}
