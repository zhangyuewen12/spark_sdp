package com.bocom.rdss.spark.sdp3x.sql;

import java.nio.file.Path;
import java.nio.file.Paths;

final class SqlPipelineRunOptions {
  private final Path projectPath;
  private final String master;
  private final boolean submittedViaSparkSubmit;

  private SqlPipelineRunOptions(
      Path projectPath,
      String master,
      boolean submittedViaSparkSubmit) {
    this.projectPath = projectPath;
    this.master = master;
    this.submittedViaSparkSubmit = submittedViaSparkSubmit;
  }

  Path projectPath() {
    return projectPath;
  }

  String master() {
    return master;
  }

  boolean submittedViaSparkSubmit() {
    return submittedViaSparkSubmit;
  }

  static SqlPipelineRunOptions parse(String[] args) {
    int index = 0;
    Path specPath = null;
    String master = null;
    boolean submittedViaSparkSubmit = false;

    while (index < args.length) {
      String arg = args[index];
      if ("--spec".equals(arg)) {
        if (index + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for --spec.");
        }
        specPath = Paths.get(args[index + 1]);
        index += 2;
      } else if ("--master".equals(arg)) {
        if (index + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for --master.");
        }
        master = args[index + 1];
        index += 2;
      } else if ("--submitted".equals(arg)) {
        submittedViaSparkSubmit = true;
        index++;
      } else {
        throw new IllegalArgumentException("Unexpected argument: " + arg);
      }
    }

    if (specPath == null) {
      throw new IllegalArgumentException("Missing required --spec <job-directory-or-spec-file>.");
    }

    return new SqlPipelineRunOptions(specPath, master, submittedViaSparkSubmit);
  }
}
