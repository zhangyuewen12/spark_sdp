package com.bocom.rdss.spark.sdp3x.sql;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

final class SqlPipelineCliOptions {
  enum Command {
    RUN,
    DRY_RUN,
    HELP
  }

  private final Command command;
  private final Path projectPath;
  private final String master;
  private final boolean submittedViaSparkSubmit;

  private SqlPipelineCliOptions(
      Command command,
      Path projectPath,
      String master,
      boolean submittedViaSparkSubmit) {
    this.command = command;
    this.projectPath = projectPath;
    this.master = master;
    this.submittedViaSparkSubmit = submittedViaSparkSubmit;
  }

  Command command() {
    return command;
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

  static SqlPipelineCliOptions parse(String[] args) {
    if (args.length == 0) {
      return new SqlPipelineCliOptions(Command.HELP, null, null, false);
    }

    String firstArg = args[0].toLowerCase(Locale.ROOT);
    if ("help".equals(firstArg) || "--help".equals(firstArg) || "-h".equals(firstArg)) {
      return new SqlPipelineCliOptions(Command.HELP, null, null, false);
    }

    Command command = Command.RUN;
    // Some invocation paths prepend transport flags before the logical command
    // (for example "--submitted run --spec ..."). Once we have recognized run or
    // dry-run, do not reinterpret later tokens with the same text as another command.
    boolean commandExplicitlySet = false;
    int index = 0;
    if ("run".equals(firstArg)) {
      command = Command.RUN;
      commandExplicitlySet = true;
      index = 1;
    } else if ("dry-run".equals(firstArg)) {
      command = Command.DRY_RUN;
      commandExplicitlySet = true;
      index = 1;
    }

    Path projectRoot = null;
    Path specPath = null;
    String master = null;
    boolean legacyDryRun = false;
    boolean submittedViaSparkSubmit = false;

    while (index < args.length) {
      String arg = args[index];
      if ("--dry-run".equals(arg)) {
        legacyDryRun = true;
        index++;
      } else if ("--spec".equals(arg)) {
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
      } else if (!commandExplicitlySet && "run".equals(arg)) {
        command = Command.RUN;
        commandExplicitlySet = true;
        index++;
      } else if (!commandExplicitlySet && "dry-run".equals(arg)) {
        command = Command.DRY_RUN;
        commandExplicitlySet = true;
        index++;
      } else if (projectRoot == null) {
        projectRoot = Paths.get(arg);
        index++;
      } else {
        throw new IllegalArgumentException("Unexpected argument: " + arg);
      }
    }

    if (specPath != null && projectRoot != null) {
      throw new IllegalArgumentException("Specify either --spec or <projectRoot>, not both.");
    }

    if (legacyDryRun) {
      command = Command.DRY_RUN;
    }

    Path projectPath = specPath != null ? specPath : projectRoot;
    if (projectPath == null && command != Command.HELP) {
      projectPath = Paths.get(".");
    }

    return new SqlPipelineCliOptions(command, projectPath, master, submittedViaSparkSubmit);
  }
}
