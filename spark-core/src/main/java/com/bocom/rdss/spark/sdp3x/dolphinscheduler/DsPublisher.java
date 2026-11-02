package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;

/**
 * 通过 DS 原生 API 新建工作流、发布并配置定时，只切换带有本工具归属标记的历史定时。
 * 记录各阶段发布结果，切换失败时尝试停用新定时并恢复旧定时。
 */
final class DsPublisher {
  private final DsApiClient api;

  /** @param api 已按目标环境配置 DS 地址和 Token 的原生 API 客户端 */
  DsPublisher(DsApiClient api) {
    this.api = api;
  }

  /** @param artifact 本次新版本的工作流、定时及归属信息；发布记录写入其版本目录 */
  void apply(DsArtifact artifact) throws IOException {
    long projectCode = number(artifact.source.settings, "project-code", 0);
    require(projectCode > 0, "Set a positive ds.project.code before apply");
    String project = "/projects/" + projectCode;
    String definitions = project + "/workflow-definition";
    String schedules = project + "/schedules";
    Path resultFile = artifact.directory.resolve("deployment-result.json");
    List<Long> previous = new ArrayList<>();
    List<Long> attemptedOffline = new ArrayList<>();
    List<Object> events = new ArrayList<>();
    Map<String, Object> state = map("status", "IN_PROGRESS", "workflowName", artifact.name,
      "releaseId", artifact.releaseId, "previousOnlineScheduleIds", previous, "events", events);
    long newCode = 0;
    long newSchedule = 0;
    boolean enableAttempted = false;
    try {
      checkpoint(state, resultFile, "VERIFY_PROJECT");
      Map<String, Object> remoteProject = object(api.get(project, Collections.emptyMap()), "DS project");
      require(number(remoteProject, "code", 0) == projectCode, "DS returned a different project code");
      if (artifact.source.settings.containsKey("project-name")) {
        require(text(artifact.source.settings, "project-name", null).equals(text(remoteProject, "name", null)),
          "DS project name does not match configured project-name");
      }
      checkpoint(state, resultFile, "FIND_PREVIOUS_VERSIONS");
      List<?> existing = list(api.get(definitions + "/simple-list", Collections.emptyMap()), "workflow list");
      String environmentSegment = "__" + artifact.source.environment + "__";
      Set<Long> seen = new LinkedHashSet<>();
      for (Object item : existing) {
        Map<String, Object> summary = object(item, "workflow");
        if (!text(summary, "name", "unmanaged").contains(environmentSegment)) {
          continue;
        }
        long code = number(summary, "code", 0);
        require(code > 0 && seen.add(code), "Invalid or duplicate workflow code returned by DS");
        Map<String, Object> details = object(api.get(definitions + "/" + code, Collections.emptyMap()), "workflow details");
        Map<String, Object> definition = child(details, "workflowDefinition");
        if (!text(definition, "description", "unmanaged").startsWith(artifact.marker + " ")) {
          continue;
        }
        for (Map<String, Object> schedule : schedules(schedules, code)) {
          if ("ONLINE".equals(text(schedule, "releaseState", "OFFLINE"))) {
            long id = number(schedule, "id", 0);
            require(id > 0 && !previous.contains(id), "Invalid or duplicate schedule id");
            previous.add(id);
          }
        }
      }
      checkpoint(state, resultFile, "ALLOCATE_FRESH_TASK_CODES");
      List<Long> codes = new ArrayList<>();
      for (Object code : list(api.get(project + "/task-definition/gen-task-codes",
          map("genNum", artifact.source.jobs.size())), "generated task codes")) {
        codes.add(Long.parseLong(code.toString()));
      }
      artifact.manifest.put("taskCodeSource", "dolphinscheduler");
      artifact.render(codes);
      checkpoint(state, resultFile, "CREATE_NEW_WORKFLOW");
      Map<String, Object> created = object(api.post(definitions, artifact.createForm()), "new workflow");
      newCode = number(created, "code", 0);
      require(newCode > 0, "DS did not return the new workflow code");
      state.put("workflowCode", newCode);
      child(artifact.workflow, "workflowDefinition").put("code", newCode);
      artifact.schedule.put("workflowDefinitionCode", newCode);
      write(artifact.directory.resolve("workflow.json"), artifact.workflow);
      write(artifact.directory.resolve("schedule.json"), artifact.schedule);
      // DS 3.4 insertSchedule calls checkWorkflowDefinitionValid, requiring ONLINE first.
      checkpoint(state, resultFile, "PUBLISH_NEW_WORKFLOW");
      action(definitions + "/" + newCode + "/release", map("releaseState", "ONLINE"));
      checkpoint(state, resultFile, "CREATE_OFFLINE_SCHEDULE");
      Map<String, Object> createdSchedule = object(api.post(schedules, artifact.schedule), "new schedule");
      newSchedule = number(createdSchedule, "id", 0);
      require(newSchedule > 0, "DS did not return the new schedule id");
      state.put("scheduleId", newSchedule);
      checkpoint(state, resultFile, "DISABLE_PREVIOUS_SCHEDULES");
      for (Long id : previous) {
        // Include ambiguous timeout outcomes in compensation, even if DS applied the request.
        attemptedOffline.add(id);
        action(schedules + "/" + id + "/offline", Collections.emptyMap());
        events.add("Previous schedule offline: " + id);
        write(resultFile, state);
      }
      if (bool(artifact.source.schedule, "enabled", false)) {
        checkpoint(state, resultFile, "ENABLE_NEW_SCHEDULE");
        enableAttempted = true;
        action(schedules + "/" + newSchedule + "/online", Collections.emptyMap());
      }
      state.put("scheduleEnabled", bool(artifact.source.schedule, "enabled", false));
      state.put("status", "SUCCESS");
      checkpoint(state, resultFile, "COMPLETE");
    } catch (Exception failure) {
      String failedStage = String.valueOf(state.get("stage"));
      state.put("status", "FAILED");
      state.put("failedStage", failedStage);
      state.put("error", failure.getMessage());
      boolean newConfirmedOffline = !enableAttempted;
      List<Object> recovery = new ArrayList<>();
      state.put("recovery", recovery);
      if (newSchedule > 0) {
        try {
          action(schedules + "/" + newSchedule + "/offline", Collections.emptyMap());
          newConfirmedOffline = true;
          recovery.add("New schedule confirmed offline: " + newSchedule);
        } catch (Exception cleanup) {
          recovery.add("Could not confirm new schedule offline: " + newSchedule + ": " + cleanup.getMessage());
        }
      }
      boolean restored = true;
      if (newConfirmedOffline) {
        for (Long id : attemptedOffline) {
          try {
            action(schedules + "/" + id + "/online", Collections.emptyMap());
            recovery.add("Previous schedule restored: " + id);
          } catch (Exception cleanup) {
            restored = false;
            recovery.add("Could not restore previous schedule: " + id + ": " + cleanup.getMessage());
          }
        }
      } else {
        restored = false;
        recovery.add("Previous schedules were not re-enabled because the new schedule state is unknown");
      }
      // An unacknowledged create can leave an offline object: never retry a mutating create automatically.
      state.put("manualCheckRequired", !restored || failedStage.startsWith("CREATE_"));
      try {
        write(resultFile, state);
      } catch (IOException recording) {
        failure.addSuppressed(recording);
      }
      throw new IOException("DS apply failed at " + failedStage + "; inspect " + resultFile
        + ": " + failure.getMessage(), failure);
    }
  }

  private List<Map<String, Object>> schedules(String endpoint, long code) throws IOException {
    List<Map<String, Object>> result = new ArrayList<>();
    for (int page = 1; page <= 10000; page++) {
      Map<String, Object> data = object(api.get(endpoint,
        map("workflowDefinitionCode", code, "pageNo", page, "pageSize", 100)), "schedule page");
      List<?> items = list(data.get("totalList"), "schedule page totalList");
      for (Object item : items) {
        Map<String, Object> schedule = object(item, "schedule");
        require(number(schedule, "workflowDefinitionCode", 0) == code, "DS returned a schedule for a different workflow");
        result.add(schedule);
      }
      long total = number(data, "total", -1);
      require(total >= 0, "Schedule page has no valid total");
      if (result.size() >= total) {
        return result;
      }
      require(!items.isEmpty(), "DS returned an incomplete schedule page");
    }
    throw new IOException("Too many schedule pages");
  }

  private void action(String path, Map<String, Object> form) throws IOException {
    if (!Boolean.TRUE.equals(api.post(path, form))) {
      throw new IOException("DS did not acknowledge action: " + path);
    }
  }

  private static void checkpoint(Map<String, Object> state, Path file, String stage) throws IOException {
    state.put("stage", stage);
    write(file, state);
  }
}
