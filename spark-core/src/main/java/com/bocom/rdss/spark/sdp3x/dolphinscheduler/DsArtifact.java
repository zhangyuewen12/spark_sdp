package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;

/**
 * 将已校验的工作流转换为 DS 原生节点、依赖、定时和清单文件。
 * 每次生成独立版本目录，名称包含环境、配置时区下的时间及随机后缀；不将 API Token 写入产物。
 */
final class DsArtifact {
  final DsWorkflow source;
  final String releaseId;
  final String name;
  final String marker;
  final Path directory;
  Map<String, Object> workflow;
  Map<String, Object> schedule;
  final Map<String, Object> manifest;

  /**
   * @param source 已完成配置和依赖校验的单个工作流
   * @param clock 版本时间来源，用于工作流名称和产物目录，不决定 cron 触发时间
   */
  DsArtifact(DsWorkflow source, Clock clock) throws IOException {
    this.source = source;
    releaseId = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
      .withZone(ZoneId.of(text(source.schedule, "timezone", "Asia/Shanghai"))).format(clock.instant())
      + "__" + UUID.randomUUID().toString().substring(0, 8);
    name = text(source.options, "name", null) + "__" + source.environment + "__" + releaseId;
    require(name.length() <= 200, "Versioned workflow name exceeds 200 characters");
    marker = "[spark-dbt:v1:" + digest(source.projectName + "|" + source.environment + "|"
      + source.group + "|" + number(source.settings, "project-code", 0)) + "]";
    Path parent = source.root.resolve("target/dolphinscheduler").resolve(source.environment).resolve(source.group);
    Files.createDirectories(parent);
    directory = Files.createDirectory(parent.resolve(releaseId));
    List<Object> jobs = new ArrayList<>();
    for (String id : source.order) {
      DsWorkflow.Job job = source.jobs.get(id);
      Path sql = source.root.resolve("models/" + id + ".sql");
      jobs.add(map("id", id, "jobType", job.type, "inputs", job.inputs, "outputs", job.outputs,
        "upstreamJobs", job.upstream, "externalInputs", job.external,
        "sqlSha256", digest(new String(Files.readAllBytes(sql), StandardCharsets.UTF_8))));
    }
    manifest = map("format", "dolphinscheduler-3.4-native-rest", "project", source.projectName,
      "projectCode", number(source.settings, "project-code", 0), "apiUrl", text(source.settings, "api-url", null),
      "environment", source.environment, "workflowDirectory", source.group, "workflowName", name,
      "releaseId", releaseId, "ownershipMarker", marker, "crossWorkflowDependenciesManaged", false, "jobs", jobs);
    manifest.put("scheduleEnabled", bool(source.schedule, "enabled", false));
    manifest.put("taskCodeSource", "preview-local");
    List<Long> previewCodes = new ArrayList<>();
    for (int index = 0; index < source.order.size(); index++) {
      previewCodes.add((long) index + 1);
    }
    render(previewCodes);
  }

  void render(List<Long> codes) throws IOException {
    require(codes.size() == source.order.size(), "Wrong number of task codes");
    require(new java.util.HashSet<>(codes).size() == codes.size() && codes.stream().allMatch(code -> code > 0),
      "Task codes must be positive and unique");
    Map<String, Long> byId = new LinkedHashMap<>();
    for (int index = 0; index < codes.size(); index++) {
      byId.put(source.order.get(index), codes.get(index));
    }
    List<Object> tasks = new ArrayList<>();
    List<Object> relations = new ArrayList<>();
    List<Object> locations = new ArrayList<>();
    Map<String, Integer> levels = new LinkedHashMap<>();
    for (String id : source.order) {
      DsWorkflow.Job job = source.jobs.get(id);
      long code = byId.get(id);
      int level = job.upstream.stream().mapToInt(upstream -> levels.get(upstream) + 1).max().orElse(0);
      levels.put(id, level);
      locations.add(map("taskCode", code, "x", 120 + level * 250, "y", 100 + tasks.size() * 100));
      tasks.add(map("code", code, "version", 1, "name", id,
        "description", "SQL model: " + id, "taskType", "SHELL", "flag", "YES", "taskExecuteType", "BATCH",
        "taskPriority", text(job.options, "priority", "MEDIUM"),
        "workerGroup", text(job.options, "worker-group", "default"),
        "environmentCode", number(job.options, "environment-code", -1),
        "failRetryTimes", number(job.options, "fail-retry-times", 0),
        "failRetryInterval", number(job.options, "fail-retry-interval", 1),
        "timeoutFlag", "OPEN", "timeoutNotifyStrategy", "FAILED",
        "timeout", number(job.options, "timeout-minutes", 60), "delayTime", 0,
        "taskGroupId", 0, "taskGroupPriority", 0, "cpuQuota", -1, "memoryMax", -1,
        "taskParams", map("rawScript", script(id), "localParams", Collections.emptyList(),
          "resourceList", Collections.emptyList())));
      if (job.upstream.isEmpty()) {
        relations.add(relation(0, code));
      } else {
        for (String upstream : job.upstream) {
          relations.add(relation(byId.get(upstream), code));
        }
      }
    }
    String description = marker + " " + text(source.options, "description", "Managed Spark SQL workflow");
    require(description.length() <= 255, "Workflow description and ownership marker exceed 255 characters");
    workflow = map("workflowDefinition", map("code", 0, "name", name, "version", 1,
      "projectCode", number(source.settings, "project-code", 0), "releaseState", "OFFLINE", "flag", "YES",
      "description", description, "globalParams", "[]", "locations", json(locations), "timeout", 0,
      "executionType", text(source.options, "execution-type", "SERIAL_WAIT")),
      "taskDefinitionList", tasks, "workflowTaskRelationList", relations);
    schedule = map("workflowDefinitionCode", 0,
      "schedule", json(map("startTime", text(source.schedule, "start-time", null),
        "endTime", text(source.schedule, "end-time", null), "timezoneId", text(source.schedule, "timezone", "Asia/Shanghai"),
        "crontab", text(source.schedule, "cron", null))),
      "warningType", text(source.options, "warning-type", "NONE"),
      "warningGroupId", number(source.options, "warning-group-id", 0),
      "failureStrategy", text(source.options, "failure-strategy", "END"),
      "workerGroup", text(source.options, "worker-group", "default"),
      "tenantCode", text(source.options, "tenant-code", "default"),
      "environmentCode", number(source.options, "environment-code", -1),
      "workflowInstancePriority", text(source.options, "priority", "MEDIUM"));
    write(directory.resolve("workflow.json"), workflow);
    write(directory.resolve("schedule.json"), schedule);
    write(directory.resolve("manifest.json"), manifest);
  }

  Map<String, Object> createForm() {
    Map<String, Object> definition = child(workflow, "workflowDefinition");
    return map("name", name, "description", definition.get("description"), "globalParams", "[]",
      "locations", definition.get("locations"), "timeout", 0, "executionType", definition.get("executionType"),
      "taskDefinitionJson", json(workflow.get("taskDefinitionList")),
      "taskRelationJson", json(workflow.get("workflowTaskRelationList")));
  }

  private Map<String, Object> relation(long from, long to) {
    return map("name", "", "preTaskCode", from, "preTaskVersion", from == 0 ? 0 : 1,
      "postTaskCode", to, "postTaskVersion", 1, "conditionType", "NONE", "conditionParams", "{}");
  }

  private String script(String id) {
    int slash = id.lastIndexOf('/');
    String project = text(source.settings, "deployment-dir", null);
    return "#!/bin/sh\nset -eu\ncd " + quote(project) + "\n"
      + "./bin/start_pipeline_job.sh " + argument(id.substring(0, slash)) + " " + argument(id.substring(slash + 1)) + "\n";
  }

  static String digest(String value) {
    try {
      byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder result = new StringBuilder();
      for (byte item : bytes) {
        result.append(String.format("%02x", item & 255));
      }
      return result.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String quote(String value) {
    return "'" + value.replace("'", "'\\''") + "'";
  }

  private static String argument(String value) {
    return value.matches("[A-Za-z0-9_./-]+") ? value : quote(value);
  }
}
