package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;
import static org.junit.jupiter.api.Assertions.*;

class DsSyncTest {
  @TempDir Path root;
  private final Clock clock = Clock.fixed(Instant.parse("2026-10-26T06:00:00Z"), ZoneOffset.UTC);

  private void write(String path, String value) throws IOException {
    Path file = root.resolve(path);
    Files.createDirectories(file.getParent());
    Files.write(file, value.getBytes(StandardCharsets.UTF_8));
  }

  private void fixture() throws IOException {
    write("properties/application.properties", "spark.yarn.submit.waitAppCompletion=true\n"
      + "spark.jars=lib/spark-connector-1.0.jar\n");
    write("properties/application-sit.properties", "ds.api.url=http://localhost:12345/dolphinscheduler\n"
      + "ds.project.code=42\nds.api.token=\nworker.group=sit-workers\n");
    write("properties/application-uat.properties", "ds.api.url=http://uat:12345/dolphinscheduler\n"
      + "ds.project.code=84\nds.api.token=uat-test-token\nworker.group=uat-workers\n");
    write("dbt_project.yaml", "name: sample\ndolphinscheduler:\n  api-url: ${ds.api.url}\n"
      + "  project-code: ${ds.project.code}\n"
      + "  deployment-dir: /opt/project with spaces\ndefaults:\n  worker-group: ${worker.group}\n");
    write("config/ads/flow.yaml", "name: example\ndescription: example workflow\nschedule:\n  enabled: true\n"
      + "  cron: '0 0 1 * * ?'\n  timezone: Asia/Shanghai\n"
      + "  start-time: '2026-01-01 00:00:00'\n  end-time: '2099-12-31 23:59:59'\n");
    job("ads/z_seed", "INSERT INTO demo.seed SELECT 1 AS id;");
    job("ads/a_result", "INSERT INTO demo.result SELECT * FROM demo.seed;");
  }

  private void job(String id, String sql) throws IOException {
    write("models/" + id + ".sql", sql);
    write("config/" + id + ".yaml", "name: test\njob-type: spark-sql\n");
  }

  private DsWorkflow workflow() throws IOException {
    return new DsWorkflow(root, "sit", "ads");
  }

  private Map<String, Object> json(Path path) throws IOException {
    return object(yaml().load(new String(Files.readAllBytes(path), StandardCharsets.UTF_8)), "test JSON");
  }

  @Test void buildsOnlySelectedWorkflowWithAstDependenciesAndIsolatedEnvironments() throws Exception {
    fixture();
    job("ads/a_result", "INSERT INTO demo.result WITH chosen AS (SELECT * FROM demo.seed), "
      + "second AS (SELECT * FROM chosen) SELECT 'from ghost' AS label, * FROM second "
      + "WHERE EXISTS (SELECT 1 FROM ext.allowed);");
    write("models/dwd/bad.sql", "THIS IS NOT SQL");
    write("config/ads/nested/flow.yaml", "malformed ignored flow");
    write("models/ads/nested/invalid.sql", "ignored nested workflow");
    DsWorkflow flow = workflow();
    assertEquals(Arrays.asList("ads/z_seed", "ads/a_result"), flow.order);
    assertEquals(Collections.singleton("ads/z_seed"), flow.jobs.get("ads/a_result").upstream);
    assertEquals(Collections.singleton("ext.allowed"), flow.jobs.get("ads/a_result").external);
    DsArtifact artifact = new DsArtifact(flow, clock);
    assertTrue(artifact.name.startsWith("example__sit__20261026_140000_000__"));
    assertEquals(3, list(artifact.workflow.get("workflowTaskRelationList"), "relations").size() + 1);
    for (Object item : list(artifact.workflow.get("taskDefinitionList"), "tasks")) {
      Map<String, Object> task = object(item, "task");
      assertEquals("SHELL", task.get("taskType"));
      String command = text(child(task, "taskParams"), "rawScript", null);
      assertTrue(command.startsWith("#!/bin/sh\nset -eu\ncd '/opt/project with spaces'\n"));
      assertEquals("./bin/start_pipeline_job.sh " + task.get("name").toString().replace('/', ' '),
        command.trim().substring(command.trim().lastIndexOf('\n') + 1));
      assertFalse(command.contains("spark-submit"));
      assertFalse(command.contains("--props"));
    }
    assertFalse(Files.readAllLines(artifact.directory.resolve("manifest.json")).toString().contains("uat-test-token"));
    DsWorkflow uat = new DsWorkflow(root, "uat", "ads");
    assertEquals(84, number(uat.settings, "project-code", 0));
    assertEquals("uat-workers", uat.options.get("worker-group"));
    assertNotEquals(artifact.marker, new DsArtifact(uat, clock).marker);
    assertEquals(false, json(artifact.directory.resolve("manifest.json")).get("crossWorkflowDependenciesManaged"));
  }

  @Test void resolvesTemporaryViewsAndDefaultDatabaseWithoutFalseProducerEdges() throws Exception {
    fixture();
    write("config/ads/a_result.yaml", "database: demo\n");
    job("ads/z_seed", "INSERT INTO demo.seed SELECT 1;");
    write("models/ads/a_result.sql", "CREATE TEMP VIEW local_source AS SELECT * FROM seed;\n"
      + "INSERT INTO result SELECT * FROM local_source;");
    DsWorkflow flow = workflow();
    assertEquals(Collections.singleton("demo.seed"), flow.jobs.get("ads/a_result").inputs);
    assertEquals(Collections.singleton("demo.result"), flow.jobs.get("ads/a_result").outputs);
    assertEquals(Collections.singleton("ads/z_seed"), flow.jobs.get("ads/a_result").upstream);
  }

  @Test void rejectsCyclesDuplicateWritersMissingPropertiesAndInvalidSchedule() throws Exception {
    fixture();
    job("ads/z_seed", "INSERT INTO demo.seed SELECT * FROM demo.result;");
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("Cyclic"));
    job("ads/z_seed", "INSERT INTO demo.result SELECT 1;");
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("Multiple jobs write"));
    job("ads/z_seed", "INSERT INTO demo.seed SELECT 1;");
    write("properties/application-sit.properties", "ds.api.url=http://localhost\n");
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("Unresolved"));
    fixture();
    String schedule = new String(Files.readAllBytes(root.resolve("config/ads/flow.yaml")), StandardCharsets.UTF_8);
    write("config/ads/flow.yaml", schedule.replace("0 0 1 * * ?", "nonsense"));
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("Quartz"));
  }

  @Test void acceptsExplicitInternalDependenciesButRejectsCrossWorkflowOnesAndAsyncSubmission() throws Exception {
    fixture();
    write("config/ads/a_result.yaml", "dolphinscheduler:\n  depends-on: [dwd/user]\n");
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("inside selected"));
    write("config/ads/a_result.yaml", "dolphinscheduler:\n  depends-on: [ads/z_seed]\n");
    assertTrue(workflow().jobs.get("ads/a_result").upstream.contains("ads/z_seed"));
    write("config/ads/a_result.yaml", "spark-submit:\n  conf.spark.yarn.submit.waitAppCompletion: false\n");
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("Asynchronous"));
    write("config/ads/a_result.yaml", "name: result\n");
    write("properties/application.properties", "spark.yarn.submit.waitAppCompletion=false\n");
    assertTrue(assertThrows(IllegalArgumentException.class, this::workflow).getMessage().contains("application.properties"));
    Files.delete(root.resolve("properties/application.properties"));
    assertThrows(IOException.class, this::workflow);
  }

  @Test void parsesHudiMaintenanceAsExternalInputsAndValidatesItsTaskFile() throws Exception {
    fixture();
    write("config/sdf_compaction/flow.yaml", new String(Files.readAllBytes(root.resolve("config/ads/flow.yaml")), StandardCharsets.UTF_8));
    write("config/sdf_compaction/mor_table.yaml", "name: compact\njob-type: hudi-compaction\n");
    write("models/sdf_compaction/mor_table.sql", "RUN COMPACTION ON sdf.user;\nRUN COMPACTION ON sdf.alter;\n");
    DsWorkflow flow = new DsWorkflow(root, "sit", "sdf_compaction");
    DsWorkflow.Job job = flow.jobs.get("sdf_compaction/mor_table");
    assertEquals("hudi-compaction", job.type);
    assertEquals(new java.util.LinkedHashSet<>(Arrays.asList("sdf.user", "sdf.alter")), job.external);
    assertTrue(job.outputs.isEmpty());
    Map<String, Object> task = object(list(new DsArtifact(flow, clock).workflow.get("taskDefinitionList"), "tasks").get(0), "task");
    assertEquals("SHELL", task.get("taskType"));
    assertTrue(text(child(task, "taskParams"), "rawScript", null).endsWith("./bin/start_pipeline_job.sh sdf_compaction mor_table"));
    write("models/sdf_compaction/mor_table.sql", "RUN COMPACTION ON sdf.user");
    assertThrows(RuntimeException.class, () -> new DsWorkflow(root, "sit", "sdf_compaction"));
  }

  @Test void eachApplyCreatesFreshDefinitionCodesAndSwitchesOnlyOwnedSchedules() throws Exception {
    fixture();
    DsArtifact first = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(first);
    new DsPublisher(api).apply(first);
    assertEquals("OFFLINE", api.timers.get(11L).get("releaseState"));
    assertEquals("ONLINE", api.timers.get(12L).get("releaseState")); // same-prefix human workflow
    assertEquals("ONLINE", api.timers.get(301L).get("releaseState"));
    assertEquals("SUCCESS", json(first.directory.resolve("deployment-result.json")).get("status"));
    DsArtifact second = new DsArtifact(workflow(), clock);
    new DsPublisher(api).apply(second);
    assertNotEquals(first.name, second.name);
    assertNotEquals(first.directory, second.directory);
    assertEquals(2, api.created.size());
    assertNotEquals(api.created.get(0).get("taskDefinitionJson"), api.created.get(1).get("taskDefinitionJson"));
    assertEquals("OFFLINE", api.timers.get(301L).get("releaseState"));
    assertEquals("ONLINE", api.timers.get(302L).get("releaseState"));
    int disable = api.calls.indexOf("POST /projects/42/schedules/11/offline");
    assertTrue(disable > api.calls.indexOf("POST /projects/42/workflow-definition/201/release"));
    assertTrue(disable < api.calls.indexOf("POST /projects/42/schedules/301/online"));
    assertEquals(201, number(child(json(first.directory.resolve("workflow.json")), "workflowDefinition"), "code", 0));
  }

  @Test void disabledScheduleDisablesPriorManagedTimersWithoutEnablingNewOne() throws Exception {
    fixture();
    Path flow = root.resolve("config/ads/flow.yaml");
    write("config/ads/flow.yaml", new String(Files.readAllBytes(flow), StandardCharsets.UTF_8).replace("enabled: true", "enabled: false"));
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    new DsPublisher(api).apply(artifact);
    assertEquals("OFFLINE", api.timers.get(11L).get("releaseState"));
    assertEquals("OFFLINE", api.timers.get(301L).get("releaseState"));
    assertFalse(api.calls.contains("POST /projects/42/schedules/301/online"));
  }

  @Test void failedEnableConfirmsNewOfflineBeforeRestoringPriorSchedule() throws Exception {
    fixture();
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    api.failEnable = true;
    assertThrows(IOException.class, () -> new DsPublisher(api).apply(artifact));
    assertEquals("OFFLINE", api.timers.get(301L).get("releaseState"));
    assertEquals("ONLINE", api.timers.get(11L).get("releaseState"));
    assertTrue(api.calls.indexOf("POST /projects/42/schedules/301/offline")
      < api.calls.lastIndexOf("POST /projects/42/schedules/11/online"));
    Map<String, Object> result = json(artifact.directory.resolve("deployment-result.json"));
    assertEquals("FAILED", result.get("status"));
    assertEquals("ENABLE_NEW_SCHEDULE", result.get("failedStage"));
    assertEquals(false, result.get("manualCheckRequired"));
  }

  @Test void ambiguousNewScheduleStateDoesNotReenableOldTimers() throws Exception {
    fixture();
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    api.failEnable = true;
    api.failCleanup = true;
    assertThrows(IOException.class, () -> new DsPublisher(api).apply(artifact));
    assertFalse(api.calls.contains("POST /projects/42/schedules/11/online"));
    assertEquals(true, json(artifact.directory.resolve("deployment-result.json")).get("manualCheckRequired"));
  }

  @Test void failureBeforeCutoverLeavesPreviousScheduleUntouchedAndRecordsUnknownCreate() throws Exception {
    fixture();
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    api.failCreate = true;
    assertThrows(IOException.class, () -> new DsPublisher(api).apply(artifact));
    assertEquals("ONLINE", api.timers.get(11L).get("releaseState"));
    assertFalse(api.calls.contains("POST /projects/42/schedules/11/offline"));
    assertEquals(true, json(artifact.directory.resolve("deployment-result.json")).get("manualCheckRequired"));
  }

  @Test void cliUsesFourPositionalArgumentsAndResolvesGroupsUnderProjectRoot() throws Exception {
    fixture();
    write("config/dws/trade/flow.yaml",
      new String(Files.readAllBytes(root.resolve("config/ads/flow.yaml")), StandardCharsets.UTF_8));
    job("dws/trade/order", "INSERT INTO demo.orders SELECT * FROM demo.seed;");
    for (String group : new String[] {"ads", "dws/trade"}) {
      assertEquals(0, DsSyncApplication.run(new String[] {"generate", root.toString(), group, "sit"}, clock));
      assertTrue(Files.isDirectory(root.resolve("target/dolphinscheduler/sit").resolve(group)));
    }
    assertEquals(1, DsSyncApplication.run(new String[] {"apply", root.toString(), "ads", "sit"}, clock));
    for (String[] args : new String[][] {
      {"generate", root.toString(), "../ads", "sit"},
      {"generate", root.toString(), root.toString(), "sit"},
      {"generate", root.toString(), "config/ads", "sit"},
      {"generate", root.toString(), "models/ads", "sit"},
      {"generate", root.toString(), "ads"},
      {"generate", root.toString(), "ads", "sit", "extra"},
      {"unknown", root.toString(), "ads", "sit"},
      {"generate", "", "ads", "sit"},
      {"generate", root.toString(), "", "sit"}
    }) {
      assertEquals(1, DsSyncApplication.run(args, clock));
    }
    assertEquals(0, DsSyncApplication.run(new String[] {"--help"}, clock));
  }

  @Test void readsDsCredentialsOnlyFromTheSelectedPropertiesAndNeverWritesThemToArtifacts() throws Exception {
    fixture();
    Path sitFile = root.resolve("properties/application-sit.properties");
    write("properties/application-sit.properties", new String(Files.readAllBytes(sitFile), StandardCharsets.UTF_8)
      .replace("ds.api.token=\n", "ds.api.token=sit-test-secret\n"));
    DsWorkflow sit = workflow();
    DsWorkflow uat = new DsWorkflow(root, "uat", "ads");
    assertEquals("sit-test-secret", sit.apiToken);
    assertEquals("uat-test-token", uat.apiToken);
    DsArtifact artifact = new DsArtifact(sit, clock);
    for (String name : new String[] {"workflow.json", "schedule.json", "manifest.json"}) {
      assertFalse(new String(Files.readAllBytes(artifact.directory.resolve(name)), StandardCharsets.UTF_8)
        .contains("sit-test-secret"));
    }
    assertEquals(1, DsSyncApplication.run(new String[] {"generate", root.toString(), "ads", "prd"}, clock));
  }

  @Test void partialOldScheduleFailureRestoresAllAttemptedDisables() throws Exception {
    fixture();
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    api.timers.put(13L, map("id", 13, "workflowDefinitionCode", 100, "releaseState", "ONLINE"));
    api.failOldDisable = true;
    assertThrows(IOException.class, () -> new DsPublisher(api).apply(artifact));
    assertEquals("ONLINE", api.timers.get(11L).get("releaseState"));
    assertEquals("ONLINE", api.timers.get(13L).get("releaseState"));
    assertEquals("OFFLINE", api.timers.get(301L).get("releaseState"));
    assertEquals("DISABLE_PREVIOUS_SCHEDULES", json(artifact.directory.resolve("deployment-result.json")).get("failedStage"));
  }

  @Test void failureToRestoreOldScheduleRequiresManualCheck() throws Exception {
    fixture();
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    api.failEnable = true;
    api.failRestore = true;
    assertThrows(IOException.class, () -> new DsPublisher(api).apply(artifact));
    assertEquals(true, json(artifact.directory.resolve("deployment-result.json")).get("manualCheckRequired"));
  }

  @Test void readsEverySchedulePageAndStopsOldVersionsAfterLogicalNameChanges() throws Exception {
    fixture();
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    FakeApi api = new FakeApi(artifact);
    api.definitions.get(100L).put("name", "renamed_old_base__sit__older");
    api.timers.put(13L, map("id", 13, "workflowDefinitionCode", 100, "releaseState", "ONLINE"));
    api.paged = true;
    new DsPublisher(api).apply(artifact);
    assertEquals("OFFLINE", api.timers.get(11L).get("releaseState"));
    assertEquals("OFFLINE", api.timers.get(13L).get("releaseState"));
  }

  @Test void loadsStandardProjectFileAndRejectsMissingConfiguration() throws Exception {
    fixture();
    assertEquals(2, workflow().jobs.size());
    assertThrows(IllegalArgumentException.class, () -> new DsWorkflow(root, "../uat", "ads"));
    assertThrows(IllegalArgumentException.class, () -> new DsWorkflow(root, "sit", "../ads"));
    Files.delete(root.resolve("dbt_project.yaml"));
    assertTrue(assertThrows(IOException.class, this::workflow).getMessage().contains("dbt_project.yaml"));
    fixture();
    Files.delete(root.resolve("properties/application-sit.properties"));
    assertThrows(IOException.class, this::workflow);
  }

  @Test void appliesThroughRealHttpTransportUsingOnlyNativeCreateAndScheduleEndpoints() throws Exception {
    fixture();
    FakeApi nativeApi = new FakeApi(new DsArtifact(workflow(), clock));
    com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
      new java.net.InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/dolphinscheduler", exchange -> {
      String path = exchange.getRequestURI().getPath().substring("/dolphinscheduler".length());
      java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int length;
      while ((length = exchange.getRequestBody().read(buffer)) != -1) {
        bytes.write(buffer, 0, length);
      }
      boolean get = exchange.getRequestMethod().equals("GET");
      String encoded = get ? exchange.getRequestURI().getRawQuery() : new String(bytes.toByteArray(), StandardCharsets.UTF_8);
      Map<String, Object> form = new LinkedHashMap<>();
      if (encoded != null && !encoded.isEmpty()) {
        for (String entry : encoded.split("&")) {
          String[] pair = entry.split("=", 2);
          form.put(java.net.URLDecoder.decode(pair[0], "UTF-8"), java.net.URLDecoder.decode(pair[1], "UTF-8"));
        }
      }
      assertEquals("secret", exchange.getRequestHeaders().getFirst("token"));
      Object data = get ? nativeApi.get(path, form) : nativeApi.post(path, form);
      byte[] response = DsConfig.json(map("code", 0, "data", data)).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, response.length);
      exchange.getResponseBody().write(response);
      exchange.close();
    });
    server.start();
    try {
      write("properties/application-sit.properties", "ds.api.url=http://127.0.0.1:" + server.getAddress().getPort()
        + "/dolphinscheduler\nds.project.code=42\nds.api.token=secret\nworker.group=default\n");
      assertEquals(0, DsSyncApplication.run(new String[] {"apply", root.toString(), "ads", "sit"}, clock));
      assertEquals(1, nativeApi.created.size());
      assertEquals("ONLINE", nativeApi.timers.get(301L).get("releaseState"));
      assertEquals("OFFLINE", nativeApi.timers.get(11L).get("releaseState"));
      assertTrue(nativeApi.calls.contains("POST /projects/42/workflow-definition"));
    } finally {
      server.stop(0);
    }
    DsSyncApplication.main(new String[] {"--help"});
  }

  @Test void requiresExplicitNonemptyEnvironmentBeforeReadingConfigurationOrCreatingArtifacts() throws Exception {
    java.io.ByteArrayOutputStream errors = new java.io.ByteArrayOutputStream();
    java.io.PrintStream original = System.err;
    try (java.io.PrintStream captured = new java.io.PrintStream(errors, true, "UTF-8")) {
      System.setErr(captured);
      for (String operation : new String[] {"generate", "apply"}) {
        for (String[] args : new String[][] {
          {operation, root.toString(), "ads"},
          {operation, root.toString(), "ads", ""},
          {operation, root.toString(), "ads", " "}
        }) {
          errors.reset();
          assertEquals(1, DsSyncApplication.run(args, clock));
          assertTrue(errors.toString("UTF-8").contains("env"));
          assertFalse(Files.exists(root.resolve("target")));
        }
      }
    } finally {
      System.setErr(original);
    }
  }

  @Test void generatedShellUsesUnifiedFrameworkForPathsArgumentsCompletionAndExitStatus() throws Exception {
    fixture();
    Path worker = Files.createDirectories(root.resolve("worker's project"));
    Path bin = Files.createDirectories(worker.resolve("bin"));
    Path scripts = java.nio.file.Paths.get("../spark-dbt-example/bin");
    if (!Files.isDirectory(scripts)) {
      scripts = java.nio.file.Paths.get("spark-dbt-example/bin");
    }
    Files.copy(scripts.resolve("start_pipeline_job.sh"), bin.resolve("start_pipeline_job.sh"));
    assertTrue(bin.resolve("start_pipeline_job.sh").toFile().setExecutable(true));
    Path libraries = Files.createDirectories(worker.resolve("lib"));
    Files.write(libraries.resolve("spark-sdp-1.0.jar"), new byte[] {1});
    Files.write(libraries.resolve("spark-connector-1.0.jar"), new byte[] {1});
    Path environment = Files.createDirectories(worker.resolve("properties")).resolve("application.properties");
    Files.write(environment, ("spark.yarn.submit.waitAppCompletion=true\n"
      + "spark.jars=lib/spark-connector-1.0.jar\n").getBytes(StandardCharsets.UTF_8));
    Files.createDirectories(worker.resolve("models/ads"));
    Files.createDirectories(worker.resolve("config/ads"));
    Files.copy(root.resolve("models/ads/z_seed.sql"), worker.resolve("models/ads/z_seed.sql"));
    Files.copy(root.resolve("config/ads/z_seed.yaml"), worker.resolve("config/ads/z_seed.yaml"));
    Path project = root.resolve("dbt_project.yaml");
    write("dbt_project.yaml", new String(Files.readAllBytes(project), StandardCharsets.UTF_8)
      .replace("/opt/project with spaces", worker.toString()));
    Path javaExecutable = Files.createDirectories(root.resolve("fake-jdk/bin")).resolve("java");
    Files.write(javaExecutable, ("#!/bin/sh\n"
      + "printf '%s\\n' \"$@\" > \"$DS_CAPTURE\"\n"
      + "previous=\nfor argument in \"$@\"; do\n"
      + "  if [ \"$previous\" = '--props' ]; then\n"
      + "    printf '%s\\n' \"$argument\" > \"$DS_PROPS_PATH\"\n"
      + "    cat \"$argument\" > \"$DS_PROPS_CONTENT\"\n  fi\n"
      + "  previous=$argument\ndone\nexit \"$DS_MOCK_EXIT\"\n").getBytes(StandardCharsets.UTF_8));
    assertTrue(javaExecutable.toFile().setExecutable(true));
    Path capture = root.resolve("java-args");
    Path propsPath = root.resolve("props-path");
    Path propsContent = root.resolve("props-content");
    DsArtifact artifact = new DsArtifact(workflow(), clock);
    Map<String, Object> task = object(list(artifact.workflow.get("taskDefinitionList"), "tasks").get(0), "task");
    String rawScript = text(child(task, "taskParams"), "rawScript", null);
    assertTrue(rawScript.endsWith("./bin/start_pipeline_job.sh ads z_seed"));
    for (int code : new int[] {17, 0}) {
      ProcessBuilder process = new ProcessBuilder("sh", "-c", rawScript).directory(root.toFile());
      process.environment().put("JAVA_HOME", javaExecutable.getParent().getParent().toString());
      process.environment().put("DS_CAPTURE", capture.toString());
      process.environment().put("DS_PROPS_PATH", propsPath.toString());
      process.environment().put("DS_PROPS_CONTENT", propsContent.toString());
      process.environment().put("DS_MOCK_EXIT", Integer.toString(code));
      assertEquals(code, process.start().waitFor());
      List<String> args = Files.readAllLines(capture, StandardCharsets.UTF_8);
      assertTrue(args.contains("com.bocom.rdss.spark.sdp3x.starter.SparkSubmitStarter"));
      assertEquals(libraries.resolve("spark-sdp-1.0.jar") + java.io.File.pathSeparator
        + libraries.resolve("spark-connector-1.0.jar"), args.get(args.indexOf("-cp") + 1));
      assertEquals(worker.resolve("models/ads/z_seed.sql").toString(), args.get(args.indexOf("--task-file") + 1));
      assertEquals(worker.resolve("config/ads/z_seed.yaml").toString(), args.get(args.indexOf("--spec") + 1));
      assertEquals(environment.toString(), args.get(args.indexOf("--props") + 1));
      assertEquals(new String(Files.readAllBytes(environment), StandardCharsets.UTF_8),
        new String(Files.readAllBytes(propsContent), StandardCharsets.UTF_8));
      assertTrue(Files.exists(environment));
    }
    assertEquals(0, new ProcessBuilder("bash", bin.resolve("start_pipeline_job.sh").toString(), "--help").start().waitFor());
    assertEquals(2, new ProcessBuilder("bash", bin.resolve("start_pipeline_job.sh").toString(), "ads").start().waitFor());
    assertEquals(2, new ProcessBuilder("bash", bin.resolve("start_pipeline_job.sh").toString(),
      "ads", "z_seed", "--props", environment.toString()).start().waitFor());
    assertEquals(2, new ProcessBuilder("bash", bin.resolve("start_pipeline_job.sh").toString(),
      "../ads", "z_seed").start().waitFor());
    Files.delete(environment);
    assertEquals(2, new ProcessBuilder("bash", bin.resolve("start_pipeline_job.sh").toString(),
      "ads", "z_seed").start().waitFor());
  }

  static final class FakeApi extends DsApiClient {
    final Map<Long, Map<String, Object>> definitions = new LinkedHashMap<>();
    final Map<Long, Map<String, Object>> timers = new LinkedHashMap<>();
    final List<Map<String, Object>> created = new ArrayList<>();
    final List<String> calls = new ArrayList<>();
    final java.util.Set<Long> released = new java.util.HashSet<>();
    long sequence = 0;
    boolean failEnable;
    boolean failCleanup;
    boolean failCreate;
    boolean failOldDisable;
    boolean failRestore;
    boolean paged;

    FakeApi(DsArtifact artifact) {
      super("http://localhost", "test-token");
      definitions.put(100L, map("code", 100, "name", "example__sit__old", "description", artifact.marker + " previous"));
      definitions.put(101L, map("code", 101, "name", "example__sit__human", "description", "Human workflow"));
      timers.put(11L, map("id", 11, "workflowDefinitionCode", 100, "releaseState", "ONLINE"));
      timers.put(12L, map("id", 12, "workflowDefinitionCode", 101, "releaseState", "ONLINE"));
    }

    @Override Object get(String path, Map<String, Object> query) throws IOException {
      calls.add("GET " + path);
      if (path.equals("/projects/42")) {
        return map("code", 42, "name", "sample");
      }
      if (path.endsWith("/simple-list")) {
        return new ArrayList<>(definitions.values());
      }
      if (path.contains("/workflow-definition/")) {
        long id = Long.parseLong(path.substring(path.lastIndexOf('/') + 1));
        return map("workflowDefinition", definitions.get(id));
      }
      if (path.endsWith("/gen-task-codes")) {
        List<Long> result = new ArrayList<>();
        for (int i = 0; i < number(query, "genNum", 0); i++) {
          result.add(5000 + ++sequence);
        }
        return result;
      }
      if (path.endsWith("/schedules")) {
        List<Object> result = new ArrayList<>();
        for (Map<String, Object> timer : timers.values()) {
          if (number(timer, "workflowDefinitionCode", 0) == number(query, "workflowDefinitionCode", 0)) {
            result.add(timer);
          }
        }
        int total = result.size();
        if (paged && total > 1) {
          int index = (int) number(query, "pageNo", 1) - 1;
          result = Collections.singletonList(result.get(index));
        }
        return map("totalList", result, "total", total);
      }
      throw new IOException("Unexpected GET " + path);
    }

    @Override Object post(String path, Map<String, Object> form) throws IOException {
      calls.add("POST " + path);
      if (path.endsWith("/workflow-definition")) {
        if (failCreate) {
          throw new IOException("Connection lost after create; outcome unknown");
        }
        created.add(new LinkedHashMap<>(form));
        long code = 200 + created.size();
        definitions.put(code, map("code", code, "name", form.get("name"), "description", form.get("description")));
        return map("code", code);
      }
      if (path.endsWith("/schedules")) {
        assertTrue(released.contains(number(form, "workflowDefinitionCode", 0)),
          "Native DS requires workflow ONLINE before creating a schedule");
        long id = 300 + created.size();
        timers.put(id, map("id", id, "workflowDefinitionCode", form.get("workflowDefinitionCode"), "releaseState", "OFFLINE"));
        return map("id", id);
      }
      if (path.endsWith("/release")) {
        assertEquals("ONLINE", form.get("releaseState"));
        String[] parts = path.split("/");
        released.add(Long.parseLong(parts[parts.length - 2]));
        return true;
      }
      String[] segments = path.split("/");
      long id = Long.parseLong(segments[segments.length - 2]);
      if (path.endsWith("/online")) {
        if (id == 11 && failRestore) {
          throw new IOException("Restore failed");
        }
        timers.get(id).put("releaseState", "ONLINE"); // Simulate server-side success even when acknowledgement is lost.
        if (id > 300 && failEnable) {
          throw new IOException("Enable acknowledgement lost");
        }
      } else if (path.endsWith("/offline")) {
        if (id > 300 && failCleanup) {
          throw new IOException("Cannot confirm cleanup");
        }
        timers.get(id).put("releaseState", "OFFLINE");
        if (id == 13 && failOldDisable) {
          throw new IOException("Old schedule disable acknowledgement lost");
        }
      } else {
        throw new IOException("Unexpected POST " + path);
      }
      return true;
    }
  }
}
