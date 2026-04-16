package com.bocom.rdss.spark.sdp3x.example;

import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineCliMain;

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
        "examples/sql-batch-pipeline/spark-pipeline.yml",
        "--master",
        "local[*]"
      });
      return;
    }
    SqlPipelineCliMain.main(args);
  }
}
