package com.bocom.rdss.spark.sdp3x.starter;

import java.nio.file.Path;
import java.nio.file.Paths;

/** Parsed command-line options for the Spark submit entrypoint. */
final class StarterOptions {
  final Path specPath;
  final Path taskFilePath;
  final Path confPath;
  final String files;
  final String jars;
  final boolean help;

  private StarterOptions(Path specPath, Path taskFilePath, Path confPath,
      String files, String jars, boolean help) {
    this.specPath = specPath;
    this.taskFilePath = taskFilePath;
    this.confPath = confPath;
    this.files = files;
    this.jars = jars;
    this.help = help;
  }

  static StarterOptions parse(String[] args) {
    if (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
      return new StarterOptions(null, null, null, null, null, true);
    }
    Path specPath = null;
    Path taskFilePath = null;
    Path confPath = null;
    String files = null;
    String jars = null;
    for (int index = 0; index < args.length; index += 2) {
      if (index + 1 >= args.length) {
        throw new IllegalArgumentException("Missing value for option: " + args[index]);
      }
      if ("--spec".equals(args[index])) {
        if (specPath != null) {
          throw new IllegalArgumentException("Option --spec may only be specified once.");
        }
        specPath = Paths.get(args[index + 1]);
      } else if ("--task-file".equals(args[index])) {
        if (taskFilePath != null) {
          throw new IllegalArgumentException("Option --task-file may only be specified once.");
        }
        taskFilePath = Paths.get(args[index + 1]);
      } else if ("--conf".equals(args[index])) {
        if (confPath != null) {
          throw new IllegalArgumentException("Option --conf may only be specified once.");
        }
        confPath = Paths.get(args[index + 1]);
      } else if ("--files".equals(args[index])) {
        if (files != null) {
          throw new IllegalArgumentException("Option --files may only be specified once.");
        }
        files = args[index + 1].trim();
        if (files.isEmpty()) {
          throw new IllegalArgumentException("Option --files must not be empty.");
        }
      } else if ("--jars".equals(args[index])) {
        if (jars != null) {
          throw new IllegalArgumentException("Option --jars may only be specified once.");
        }
        jars = args[index + 1].trim();
        if (jars.isEmpty()) {
          throw new IllegalArgumentException("Option --jars must not be empty.");
        }
      } else {
        throw new IllegalArgumentException("Unknown option: " + args[index]);
      }
    }
    if (specPath == null) {
      throw new IllegalArgumentException("Required option is missing: --spec");
    }
    return new StarterOptions(specPath, taskFilePath, confPath, files, jars, false);
  }
}
