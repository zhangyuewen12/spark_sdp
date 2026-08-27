package com.bocom.rdss.spark.sdp3x.example;

import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Convenience main entry for local IDE debugging.
 */
public final class SqlPipelineLocalDebugMain {
  private SqlPipelineLocalDebugMain() {
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      SqlPipelineRunApplication.main(new String[] {
        "--spec",
        resolveDefaultSpec().toString(),
        "--master",
        "local[*]"
      });
      return;
    }
    SqlPipelineRunApplication.main(args);
  }

  private static Path resolveDefaultSpec() {
    Path currentDirectory = Paths.get("").toAbsolutePath().normalize();
    Path fromCurrent = currentDirectory.resolve("examples/sql-batch-pipeline/spark-pipeline.yaml");
    if (Files.exists(fromCurrent)) {
      return fromCurrent;
    }
    Path fromParent = currentDirectory.getParent() == null
      ? fromCurrent
      : currentDirectory.getParent().resolve("examples/sql-batch-pipeline/spark-pipeline.yaml");
    return fromParent;
  }
}
