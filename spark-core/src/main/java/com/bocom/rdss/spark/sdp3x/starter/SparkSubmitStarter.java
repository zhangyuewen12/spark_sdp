package com.bocom.rdss.spark.sdp3x.starter;

import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineProjectException;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Submission entrypoint (entrypoint A). It packages a job directory or a spec plus one task file,
 * then starts entrypoint B through {@code spark-submit} in Yarn cluster mode.
 */
public final class SparkSubmitStarter {
  static final String APPLICATION_CLASS =
    "com.bocom.rdss.spark.sdp3x.sql.SqlPipelineRunApplication";
  static final String DISTRIBUTED_JOB_DIRECTORY = "spark-sdp-job";

  private SparkSubmitStarter() {
  }

  public static void main(String[] args) {
    int exitCode = run(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(String[] args) {
    StarterOptions options;
    try {
      options = StarterOptions.parse(args);
      if (options.help) {
        printUsage();
        return 0;
      }
      List<String> command = buildCommand(options, locateApplicationJar());
      System.out.println("Executing: " + printable(command));
      Process process = new ProcessBuilder(command).inheritIO().start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        System.err.println("spark-submit exited with code " + exitCode);
      }
      return exitCode;
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      printUsage();
      return 2;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start spark-submit.", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for spark-submit.", e);
    }
  }

  static List<String> buildCommand(StarterOptions options, Path applicationJar) {
    Path taskFile = options.taskFilePath == null ? null : resolveTaskFile(options.taskFilePath);
    Path specPath = resolveSpec(options.specPath);
    Path jobDirectory = specPath.getParent();
    Map<String, String> configuration = loadConfiguration(specPath);
    if ("hudi-compaction".equals(configuration.get("job-type")) && taskFile == null) {
      throw new IllegalArgumentException("Hudi compaction job requires --task-file <tables.list>");
    }
    Map<String, String> commonSparkConfiguration =
      loadCommonSparkConfiguration(options.confPath);
    Path jobArchive = createJobArchive(jobDirectory, specPath, taskFile);

    List<String> command = new ArrayList<>();
    command.add(resolveSparkSubmit());
    appendSubmitOptions(
      command, configuration, commonSparkConfiguration, options.files, options.jars);
    command.add("--archives");
    command.add(jobArchive + "#" + DISTRIBUTED_JOB_DIRECTORY);
    command.add("--class");
    command.add(APPLICATION_CLASS);
    command.add(applicationJar.toAbsolutePath().normalize().toString());
    command.add("--submitted");
    command.add("--spec");
    command.add(DISTRIBUTED_JOB_DIRECTORY + "/" + specPath.getFileName());
    if (taskFile != null) {
      command.add("--task-file");
      command.add(DISTRIBUTED_JOB_DIRECTORY + "/task/" + taskFile.getFileName());
    }
    return Collections.unmodifiableList(command);
  }

  private static void appendSubmitOptions(
      List<String> command,
      Map<String, String> config,
      Map<String, String> externalSparkConfiguration,
      String files,
      String jars) {
    append(command, "--master", value(config, "master", null, "yarn"));
    append(command, "--deploy-mode", value(config, "deploy-mode", "deployMode", "cluster"));
    // Keep the Yarn application name identical to the pipeline name declared in the job YAML.
    appendKnown(command, config, "name", "--name");
    appendKnown(command, config, "queue", "--queue");
    appendKnown(command, config, "driver.memory", "--driver-memory");
    appendKnown(command, config, "driver.cores", "--driver-cores");
    appendKnown(command, config, "executor.memory", "--executor-memory");
    appendKnown(command, config, "executor.cores", "--executor-cores");
    String executorNumber = value(config, "executor.num", "executor.number", null);
    if (executorNumber != null) {
      append(command, "--num-executors", executorNumber);
    }
    if (files == null) {
      appendKnown(command, config, "files", "--files");
    } else {
      append(command, "--files", files);
    }
    if (jars == null) {
      appendKnown(command, config, "jars", "--jars");
    } else {
      append(command, "--jars", jars);
    }
    appendKnown(command, config, "principal", "--principal");
    appendKnown(command, config, "keytab", "--keytab");
    // The external properties file supplies defaults; job YAML has the final say.
    LinkedHashMap<String, String> sparkConfiguration =
      new LinkedHashMap<>(externalSparkConfiguration);
    for (Map.Entry<String, String> entry : config.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("conf.")) {
        sparkConfiguration.put(key.substring("conf.".length()), entry.getValue());
      } else if (key.startsWith("spark.")) {
        sparkConfiguration.put(key, entry.getValue());
      }
    }
    sparkConfiguration.forEach((key, configured) ->
      append(command, "--conf", key + "=" + configured));
  }

  private static void appendKnown(
      List<String> command, Map<String, String> config, String key, String option) {
    String configured = config.get(key);
    if (configured != null && !configured.trim().isEmpty()) {
      append(command, option, configured.trim());
    }
  }

  private static void append(List<String> command, String option, String configuredValue) {
    command.add(option);
    command.add(configuredValue);
  }

  private static String value(
      Map<String, String> config, String primary, String alternative, String defaultValue) {
    String configured = config.get(primary);
    if (configured == null && alternative != null) {
      configured = config.get(alternative);
    }
    return configured == null || configured.trim().isEmpty() ? defaultValue : configured.trim();
  }

  private static Path resolveSpec(Path requestedPath) {
    Path path = requestedPath.toAbsolutePath().normalize();
    if (Files.isRegularFile(path)) {
      String fileName = path.getFileName().toString();
      if (!fileName.endsWith(".yaml") && !fileName.endsWith(".yml")) {
        throw new IllegalArgumentException("Job spec must be a YAML file: " + path);
      }
      return path;
    }
    if (!Files.isDirectory(path)) {
      throw new IllegalArgumentException("Job path does not exist: " + path);
    }
    Path specPath = path.resolve("spark-pipeline.yaml");
    if (!Files.isRegularFile(specPath)) {
      throw new IllegalArgumentException("Job directory must contain spark-pipeline.yaml: " + path);
    }
    return specPath;
  }

  private static Path resolveTaskFile(Path requestedPath) {
    Path path = requestedPath.toAbsolutePath().normalize();
    if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
      throw new IllegalArgumentException("--task-file must be a readable file: " + path);
    }
    return path;
  }

  private static Map<String, String> loadConfiguration(Path specPath) {
    try (InputStream input = Files.newInputStream(specPath)) {
      Object loaded = new Yaml().load(input);
      if (!(loaded instanceof Map)) {
        return Collections.emptyMap();
      }
      LinkedHashMap<String, String> result = new LinkedHashMap<>();
      @SuppressWarnings("unchecked")
      Map<Object, Object> yaml = (Map<Object, Object>) loaded;
      yaml.forEach((key, val) -> {
        if (!(val instanceof Map) && !(val instanceof List)) {
          result.put(String.valueOf(key), String.valueOf(val));
        }
      });
      Object submit = yaml.get("spark-submit");
      if (submit instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<Object, Object> submitMap = (Map<Object, Object>) submit;
        submitMap.forEach((key, val) -> result.put(String.valueOf(key), String.valueOf(val)));
      }
      return result;
    } catch (IOException e) {
      throw new SqlPipelineProjectException("Failed to read job spec: " + specPath, e);
    }
  }

  private static Map<String, String> loadCommonSparkConfiguration(Path requestedPath) {
    if (requestedPath == null) {
      return Collections.emptyMap();
    }
    Path path = requestedPath.toAbsolutePath().normalize();
    if (!Files.isRegularFile(path)) {
      throw new IllegalArgumentException("Spark properties file does not exist: " + path);
    }

    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(path, java.nio.charset.StandardCharsets.UTF_8)) {
      properties.load(reader);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to read Spark properties file: " + path, e);
    }

    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    properties.stringPropertyNames().stream().sorted().forEach(key -> {
      String normalizedKey = key.trim();
      if (normalizedKey.isEmpty()) {
        throw new IllegalArgumentException("Spark properties file contains an empty key: " + path);
      }
      result.put(normalizedKey, properties.getProperty(key));
    });
    return result;
  }

  private static Path createJobArchive(Path jobDirectory, Path specPath, Path taskFile) {
    try {
      Path archive = Files.createTempFile("spark-sdp-job-", ".zip");
      Files.deleteIfExists(archive);
      Map<String, String> environment = Collections.singletonMap("create", "true");
      try (FileSystem zip = FileSystems.newFileSystem(
          java.net.URI.create("jar:" + archive.toUri()), environment)) {
        if (taskFile != null) {
          copyToArchive(specPath, zip.getPath("/" + specPath.getFileName()));
          copyToArchive(taskFile, zip.getPath("/task/" + taskFile.getFileName()));
        } else {
          try (java.util.stream.Stream<Path> paths = Files.walk(jobDirectory)) {
            paths.filter(Files::isRegularFile)
              .filter(source -> shouldArchive(jobDirectory.relativize(source)))
              .forEach(source -> copyToArchive(
                source, zip.getPath("/" + jobDirectory.relativize(source).toString())));
          }
        }
      }
      archive.toFile().deleteOnExit();
      return archive;
    } catch (IOException | UncheckedIOException e) {
      Throwable cause = e instanceof UncheckedIOException ? e.getCause() : e;
      throw new IllegalStateException("Failed to archive job directory: " + jobDirectory, cause);
    }
  }

  private static void copyToArchive(Path source, Path target) {
    try {
      if (target.getParent() != null) {
        Files.createDirectories(target.getParent());
      }
      Files.copy(source, target);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static boolean shouldArchive(Path relativePath) {
    if (relativePath.getNameCount() == 0) {
      return false;
    }
    String first = relativePath.getName(0).toString();
    return !"target".equals(first) && !".git".equals(first) && !".idea".equals(first);
  }

  private static Path locateApplicationJar() {
    String configured = System.getProperty("spark.sdp.jar");
    if (configured != null && !configured.trim().isEmpty()) {
      return Paths.get(configured);
    }
    try {
      Path location = Paths.get(
        SparkSubmitStarter.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      if (!Files.isRegularFile(location)) {
        throw new IllegalStateException(
          "Entrypoint A must run from a jar. Set -Dspark.sdp.jar=/path/to/spark-sdp.jar when developing.");
      }
      return location;
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot locate the Spark SDP jar.", e);
    }
  }

  private static String resolveSparkSubmit() {
    String sparkHome = System.getenv("SPARK_HOME");
    if (sparkHome != null && !sparkHome.trim().isEmpty()) {
      Path executable = Paths.get(sparkHome, "bin", "spark-submit");
      if (!Files.isExecutable(executable)) {
        throw new IllegalArgumentException("spark-submit is not executable: " + executable);
      }
      return executable.toString();
    }
    return "spark-submit";
  }

  private static String printable(List<String> command) {
    StringBuilder text = new StringBuilder();
    for (String argument : command) {
      if (text.length() > 0) {
        text.append(' ');
      }
      text.append(argument.indexOf(' ') >= 0 ? "'" + argument.replace("'", "'\\''") + "'" : argument);
    }
    return text.toString();
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  java -cp spark-sdp-1.0.jar " + SparkSubmitStarter.class.getName()
      + " --spec <job-directory-or-spec-file> [--conf <spark-properties-file>]"
      + " [--task-file <job-input-file>]"
      + " [--files <file1,file2,...>]"
      + " [--jars <jar1,jar2,...>]");
    System.out.println();
    System.out.println("SPARK_HOME must point to the Spark installation. Defaults: master=yarn, deploy-mode=cluster.");
  }

}
