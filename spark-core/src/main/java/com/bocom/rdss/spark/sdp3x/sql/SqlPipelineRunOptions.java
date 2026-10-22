package com.bocom.rdss.spark.sdp3x.sql;

import java.nio.file.Path;
import java.nio.file.Paths;

final class SqlPipelineRunOptions {
  private final Path projectPath;
  private final Path jobPath;
  private final Path taskFilePath;
  private final String master;
  private final boolean submittedViaSparkSubmit;

  private SqlPipelineRunOptions(
      Path projectPath,
      Path jobPath,
      Path taskFilePath,
      String master,
      boolean submittedViaSparkSubmit) {
    this.projectPath = projectPath;
    this.jobPath = jobPath;
    this.taskFilePath = taskFilePath;
    this.master = master;
    this.submittedViaSparkSubmit = submittedViaSparkSubmit;
  }

  Path projectPath() {
    return projectPath;
  }

  Path taskFilePath() {
    return taskFilePath;
  }

  Path jobPath() {
    return jobPath;
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
    Path jobPath = null;
    Path taskFilePath = null;
    String master = null;
    boolean submittedViaSparkSubmit = false;

    while (index < args.length) {
      String arg = args[index];
      if ("--spec".equals(arg)) {
        if (index + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for --spec.");
        }
        if (specPath != null) {
          throw new IllegalArgumentException("Option --spec may only be specified once.");
        }
        specPath = Paths.get(args[index + 1]);
        index += 2;
      } else if ("--jobPath".equals(arg)) {
        if (index + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for --jobPath.");
        }
        if (jobPath != null) {
          throw new IllegalArgumentException("Option --jobPath may only be specified once.");
        }
        jobPath = Paths.get(args[index + 1]);
        index += 2;
      } else if ("--taskFile".equals(arg) || "--task-file".equals(arg)) {
        if (index + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for --taskFile.");
        }
        if (taskFilePath != null) {
          throw new IllegalArgumentException("Option --taskFile may only be specified once.");
        }
        taskFilePath = Paths.get(args[index + 1]);
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
      throw new IllegalArgumentException("Missing required --spec <job.yaml>.");
    }
    if (jobPath != null && taskFilePath != null) {
      throw new IllegalArgumentException("--jobPath and --taskFile cannot be used together.");
    }

    return new SqlPipelineRunOptions(specPath, jobPath, taskFilePath, master, submittedViaSparkSubmit);
  }
}
