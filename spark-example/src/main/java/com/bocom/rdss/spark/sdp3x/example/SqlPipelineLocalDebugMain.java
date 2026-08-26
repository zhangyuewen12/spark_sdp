package com.bocom.rdss.spark.sdp3x.example;

import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain;

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
      SqlPipelineCliMain.main(new String[] {
        "run",
        "--spec",
        resolveDefaultSpec().toString(),
        "--master",
        "local[*]"
      });
      return;
    }
    SqlPipelineCliMain.main(args);
  }

  private static Path resolveDefaultSpec() {
    Path currentDirectory = Paths.get("").toAbsolutePath().normalize();
    Path fromCurrent = currentDirectory.resolve("examples/sql-batch-pipeline/spark-pipeline.properties");
    if (Files.exists(fromCurrent)) {
      return fromCurrent;
    }
    Path fromParent = currentDirectory.getParent() == null
      ? fromCurrent
      : currentDirectory.getParent().resolve("examples/sql-batch-pipeline/spark-pipeline.properties");
    return fromParent;
  }
}
