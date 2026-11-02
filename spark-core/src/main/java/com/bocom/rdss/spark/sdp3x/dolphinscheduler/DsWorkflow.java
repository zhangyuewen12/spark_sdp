package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineDefinition;
import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineParser;
import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineProjectSpecLoader;
import org.quartz.CronExpression;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;

/**
 * 加载单个工作流及其作业，合并工程/工作流/作业调度配置，并从 SQL 输入输出表建立内部依赖。
 * DS 配置只取自选定环境文件；跨工作流输入仅记录为外部表，不自动建立跨工作流调度。
 */
final class DsWorkflow {
  final Path root;
  final String environment;
  final String group;
  final String projectName;
  final String apiToken;
  final Map<String, Object> settings;
  final Map<String, Object> options;
  final Map<String, Object> schedule;
  final Map<String, Job> jobs = new LinkedHashMap<>();
  final List<String> order;

  static final class Job {
    final String id;
    final String type;
    final Set<String> inputs = new LinkedHashSet<>();
    final Set<String> outputs = new LinkedHashSet<>();
    final Set<String> upstream = new LinkedHashSet<>();
    final Set<String> external = new LinkedHashSet<>();
    final Map<String, Object> options;

    Job(String id, String type, Map<String, Object> options) {
      this.id = id;
      this.type = type;
      this.options = options;
    }
  }

  /**
   * @param root SQL 工程根目录，包含 models、config、properties 及 dbt_project.yaml
   * @param environment sit/uat/prd 等环境标识，用于选择 application-&lt;environment&gt;.properties
   * @param group 工作流相对目录，例如 ads、dws/trade，对应同层的 models 和 config 目录
   */
  DsWorkflow(Path root, String environment, String group) throws IOException {
    require(environment.matches("[A-Za-z0-9_-]+"), "Invalid environment name");
    require(group.matches("[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*"), "Invalid workflow directory");
    this.root = root.toRealPath();
    this.environment = environment;
    this.group = group;
    DsConfig config = new DsConfig(this.root.resolve("properties/application-" + environment + ".properties"));
    apiToken = config.optionalProperty("ds.api.token");
    Path projectFile = this.root.resolve("dbt_project.yaml");
    Map<String, Object> project = config.read(projectFile);
    projectName = text(project, "name", null);
    settings = child(project, "dolphinscheduler");
    require("ds34".equals(text(settings, "api-profile", "ds34")), "Only api-profile ds34 is supported");
    require(!settings.containsKey("token") && !settings.containsKey("token-env"),
      "Configure ds.api.token in properties/application-<env>.properties");
    require(Paths.get(text(settings, "deployment-dir", null)).isAbsolute(), "deployment-dir must be an absolute worker path");
    require(number(settings, "project-code", 0) >= 0, "project-code must not be negative");
    DsApiClient.validateUrl(text(settings, "api-url", null));
    Map<String, Object> flow = config.read(this.root.resolve("config/" + group + "/flow.yaml"));
    options = new LinkedHashMap<>(child(project, "defaults"));
    options.putAll(flow);
    String name = text(options, "name", null);
    require(name.chars().noneMatch(ch -> ch < 32 || ch == 127), "Workflow name must not contain control characters");
    validateOptions(options);
    schedule = child(flow, "schedule");
    validateSchedule(schedule);
    validateRuntimeEnvironment(this.root.resolve("properties/application.properties"));
    Path models = this.root.resolve("models");
    Path selected = models.resolve(group).toRealPath();
    require(selected.startsWith(models.toRealPath()), "Model directory escapes models/");
    List<Path> files;
    try (java.util.stream.Stream<Path> paths = Files.walk(selected)) {
      files = paths.filter(Files::isRegularFile).filter(path -> path.toString().endsWith(".sql"))
        .sorted().collect(Collectors.toList());
    }
    Map<String, String> producers = new LinkedHashMap<>();
    Map<String, String> maintenance = new LinkedHashMap<>();
    SqlPipelineParser parser = new SqlPipelineParser();
    SqlLineage lineage = new SqlLineage();
    for (Path sql : files) {
      Path relative = models.relativize(sql);
      if (belongsToNestedWorkflow(relative.getParent())) {
        continue;
      }
      require(sql.toRealPath().startsWith(models.toRealPath()), "SQL file escapes models/: " + relative);
      String id = relative.toString().replace('\\', '/').replaceFirst("\\.sql$", "");
      Map<String, Object> jobYaml = config.read(this.root.resolve("config/" + id + ".yaml"));
      String type = text(jobYaml, "job-type", "spark-sql");
      require(type.equals("spark-sql") || type.equals("hudi-compaction"), "Unsupported job-type: " + type);
      validateSubmission(jobYaml, id);
      Map<String, Object> taskOptions = new LinkedHashMap<>(options);
      taskOptions.putAll(child(jobYaml, "dolphinscheduler"));
      validateOptions(taskOptions);
      Job job = new Job(id, type, taskOptions);
      if (type.equals("hudi-compaction")) {
        for (String target : new SqlPipelineProjectSpecLoader()
            .load(this.root.resolve("config/" + id + ".yaml"), sql).compactionTables()) {
          String table = table(target.replace("`", ""), jobYaml);
          String previous = maintenance.putIfAbsent(table, id);
          require(previous == null, "Multiple compaction jobs maintain " + table + ": " + previous + ", " + id);
          job.inputs.add(table);
        }
        jobs.put(id, job);
        continue;
      }
      List<SqlPipelineDefinition> definitions = parser.parse(sql, 1);
      require(!definitions.isEmpty(), "No executable SQL in " + id);
      Set<String> localViews = new LinkedHashSet<>();
      for (SqlPipelineDefinition definition : definitions) {
        if (!definition.querySql().trim().toUpperCase(Locale.ROOT).startsWith("TRUNCATE TABLE")) {
          for (String input : lineage.inputs(definition.querySql())) {
            String table = table(input, jobYaml);
            if (!localViews.contains(table)) {
              job.inputs.add(table);
            }
          }
        }
        String output = table(definition.datasetName(), jobYaml);
        if (definition.datasetKind() == DatasetKind.TEMPORARY_VIEW) {
          localViews.add(output);
        } else {
          job.outputs.add(output);
          String previous = producers.putIfAbsent(output, id);
          require(previous == null || previous.equals(id), "Multiple jobs write " + output + ": " + previous + ", " + id);
        }
      }
      jobs.put(id, job);
    }
    require(!jobs.isEmpty(), "No SQL jobs in workflow " + group);
    for (Job job : jobs.values()) {
      for (String input : job.inputs) {
        String producer = producers.get(input);
        if (producer == null) {
          job.external.add(input);
        } else if (!producer.equals(job.id)) {
          job.upstream.add(producer);
        }
      }
      for (Object dependency : list(job.options.getOrDefault("depends-on", Collections.emptyList()), "depends-on")) {
        String id = dependency.toString();
        require(jobs.containsKey(id), "Explicit dependency must be inside selected workflow: " + id);
        job.upstream.add(id);
      }
    }
    order = new ArrayList<>();
    Set<String> remaining = new LinkedHashSet<>(jobs.keySet());
    while (!remaining.isEmpty()) {
      List<String> ready = remaining.stream().filter(id -> order.containsAll(jobs.get(id).upstream))
        .collect(Collectors.toList());
      require(!ready.isEmpty(), "Cyclic job dependencies: " + remaining);
      order.addAll(ready);
      remaining.removeAll(ready);
    }
  }

  private boolean belongsToNestedWorkflow(Path directory) {
    Path selected = Paths.get(group);
    while (directory != null && !directory.equals(selected)) {
      if (Files.isRegularFile(root.resolve("config").resolve(directory).resolve("flow.yaml"))) {
        return true;
      }
      directory = directory.getParent();
    }
    return false;
  }

  private static void validateRuntimeEnvironment(Path file) throws IOException {
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
    require("true".equalsIgnoreCase(properties.getProperty("spark.yarn.submit.waitAppCompletion", "true").trim()),
      "Asynchronous YARN submission is forbidden in properties/application.properties");
  }

  private static void validateSubmission(Map<String, Object> job, String id) {
    for (Map<String, Object> values : Arrays.asList(job, child(job, "spark-submit"))) {
      for (String key : Arrays.asList("spark.yarn.submit.waitAppCompletion", "conf.spark.yarn.submit.waitAppCompletion")) {
        if (values.containsKey(key)) {
          require(bool(values, key, true), "Asynchronous YARN submission is forbidden for scheduled job " + id);
        }
      }
      require("yarn".equals(text(values, "master", "yarn")), "Scheduled jobs currently require master=yarn: " + id);
      require("cluster".equals(text(values, "deploy-mode", "cluster")), "Scheduled jobs require deploy-mode=cluster: " + id);
    }
  }

  private static String table(String name, Map<String, Object> job) {
    String result = name.toLowerCase(Locale.ROOT);
    if (!result.contains(".") && job.containsKey("database")) {
      result = text(job, "database", null).toLowerCase(Locale.ROOT) + "." + result;
    }
    if (result.split("\\.").length == 2 && job.containsKey("catalog")) {
      result = text(job, "catalog", null).toLowerCase(Locale.ROOT) + "." + result;
    }
    return result;
  }

  private static void validateOptions(Map<String, Object> options) {
    oneOf(text(options, "execution-type", "SERIAL_WAIT"), "PARALLEL", "SERIAL_WAIT", "SERIAL_DISCARD", "SERIAL_PRIORITY");
    oneOf(text(options, "warning-type", "NONE"), "NONE", "SUCCESS", "FAILURE", "ALL");
    oneOf(text(options, "failure-strategy", "END"), "END", "CONTINUE");
    oneOf(text(options, "priority", "MEDIUM"), "HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST");
    require(number(options, "warning-group-id", 0) >= 0, "warning-group-id must not be negative");
    require(text(options, "warning-type", "NONE").equals("NONE") || number(options, "warning-group-id", 0) > 0,
      "Alerts require a positive warning-group-id");
    require(number(options, "fail-retry-times", 0) >= 0 && number(options, "fail-retry-interval", 1) >= 0,
      "Retry settings must not be negative");
    require(number(options, "timeout-minutes", 60) > 0, "timeout-minutes must be positive");
    require(number(options, "environment-code", -1) >= -1, "Invalid environment-code");
    text(options, "worker-group", "default");
    text(options, "tenant-code", "default");
  }

  private static void validateSchedule(Map<String, Object> schedule) {
    bool(schedule, "enabled", false);
    String cron = text(schedule, "cron", null);
    require(CronExpression.isValidExpression(cron), "Invalid Quartz cron: " + cron);
    ZoneId.of(text(schedule, "timezone", "Asia/Shanghai"));
    DateTimeFormatter format = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);
    LocalDateTime start = LocalDateTime.parse(text(schedule, "start-time", null), format);
    LocalDateTime end = LocalDateTime.parse(text(schedule, "end-time", null), format);
    require(start.isBefore(end), "Schedule start-time must precede end-time");
  }

  private static void oneOf(String value, String... choices) {
    require(Arrays.asList(choices).contains(value), "Unsupported configuration value: " + value);
  }
}
