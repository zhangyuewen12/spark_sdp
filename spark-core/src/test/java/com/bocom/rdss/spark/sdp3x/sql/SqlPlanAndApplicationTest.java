package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.enums.ExecutionMode;
import com.bocom.rdss.spark.sdp3x.enums.JobType;

import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionStage;
import com.bocom.rdss.spark.sdp3x.testsupport.SparkTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqlPlanAndApplicationTest {
  @TempDir
  Path tempDir;

  @Test
  void shouldPrintPlan() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;

    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8.name()));
      SqlPipelinePlanSupport.print(new ExecutionPlan(
        new PipelineBuilder("pipeline")
          .addDataset(ImmutableDatasetDefinition.table("dataset_a"))
          .addFlow(ImmutableFlowDefinition.batchFlow(
            "flow_a",
            "dataset_a",
            Collections.<String>emptySet(),
            runtime -> null))
          .build(),
        Collections.singletonList(new ExecutionStage(0, Collections.singletonList(
          ImmutableFlowDefinition.batchFlow("flow_a", "dataset_a", Collections.<String>emptySet(), runtime -> null))))));
    } catch (Exception e) {
      fail(e);
    } finally {
      System.setOut(originalOut);
    }

    String output = new String(out.toByteArray(), StandardCharsets.UTF_8);
    assertTrue(output.contains("Pipeline: pipeline"));
    assertTrue(output.contains("flow_a -> dataset_a"));
  }

  @Test
  void shouldRequireSpecForRunApplication() {
    assertThrows(IllegalArgumentException.class,
      () -> SqlPipelineRunApplication.main(new String[0]));
    assertThrows(IllegalArgumentException.class,
      () -> SqlPipelineRunApplication.main(new String[] {"--spec"}));
  }

  @Test
  void shouldCompileAndPrintPlanThroughUtility() throws Exception {
    Path projectRoot = exampleProject("sql-batch-pipeline");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;

    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8.name()));
      SqlPipelinePlanSupport.print(SqlPipelinePlanSupport.plan(projectRoot));
    } finally {
      System.setOut(originalOut);
    }

    String stdout = new String(out.toByteArray(), StandardCharsets.UTF_8);
    assertTrue(stdout.contains("Pipeline: sql_orders_pipeline"));
    assertTrue(stdout.contains("Stage 0"));
  }

  @Test
  void shouldRethrowRuntimeFailuresFromRunApplication() {
    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> SqlPipelineRunApplication.main(new String[] {"--spec", tempMissingPath()}));

    assertTrue(exception.getMessage().contains("does not exist"));
  }

  @Test
  void shouldRunApplicationWithExplicitAndDefaultMaster() throws Exception {
    Path explicitProject = createTemporaryViewProject("explicit-project");
    Path defaultProject = createTemporaryViewProject("default-project");
    String originalProperty = System.getProperty(SqlPipelineRunApplication.LOCAL_DEV_HIVE_ENABLED_PROPERTY);

    try {
      System.setProperty(SqlPipelineRunApplication.LOCAL_DEV_HIVE_ENABLED_PROPERTY, "false");
      SqlPipelineRunApplication.run(SqlPipelineRunOptions.parse(new String[] {
        "--spec", explicitProject.resolve("spark-pipeline.yaml").toString(), "--master", "local[1]"
      }));
      SqlPipelineRunApplication.run(SqlPipelineRunOptions.parse(new String[] {
        "--spec", defaultProject.resolve("spark-pipeline.yaml").toString()
      }));
    } finally {
      restoreProperty(SqlPipelineRunApplication.LOCAL_DEV_HIVE_ENABLED_PROPERTY, originalProperty);
    }
  }

  @Test
  void shouldLoadDevHiveSitePropertiesFromClasspath() {
    Map<String, String> properties = SqlPipelineRunApplication
      .loadPreferredLocalHiveSiteProperties(SqlPlanAndApplicationTest.class.getClassLoader())
      .orElseThrow(() -> new AssertionError("Expected dev hive-site.xml on the test classpath."));

    assertEquals("thrift://localhost:9083", properties.get("hive.metastore.uris"));
    assertEquals("jdbc:mysql://localhost:3306/metastore?useSSL=false&allowPublicKeyRetrieval=true",
      properties.get("javax.jdo.option.ConnectionURL"));
  }

  @Test
  void shouldAllowDisablingDevHivePreferenceWithSystemProperty() {
    String originalProperty = System.getProperty(SqlPipelineRunApplication.LOCAL_DEV_HIVE_ENABLED_PROPERTY);
    try {
      System.setProperty(SqlPipelineRunApplication.LOCAL_DEV_HIVE_ENABLED_PROPERTY, "false");
      assertFalse(SqlPipelineRunApplication.isLocalDevHiveEnabled());
      assertFalse(SqlPipelineRunApplication
        .loadPreferredLocalHiveSiteProperties(SqlPlanAndApplicationTest.class.getClassLoader())
        .isPresent());
    } finally {
      restoreProperty(SqlPipelineRunApplication.LOCAL_DEV_HIVE_ENABLED_PROPERTY, originalProperty);
    }
  }

  @Test
  void shouldCoverSimpleProjectRunnerDelegation() {
    SqlPipelineProjectCompiler compiler = mock(SqlPipelineProjectCompiler.class);
    when(compiler.compile(Paths.get("custom-project"))).thenReturn(
      new PipelineBuilder("custom-project").build());

    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner(compiler);
    Path project = Paths.get("custom-project");

    assertEquals("custom-project", runner.compile(project).name());
  }

  @Test
  void sqlPipelineProjectSpecAndDefinitionShouldExposeFields() {
    SqlPipelineProjectSpec spec = new SqlPipelineProjectSpec(
      Paths.get("/tmp/project"),
      "pipeline",
      "catalog",
      "database",
      Collections.singletonMap("k", "v"),
      Collections.singletonList(Paths.get("/tmp/project/sql")));
    SqlPipelineDefinition definition = new SqlPipelineDefinition(
      "dataset",
      com.bocom.rdss.spark.sdp3x.api.DatasetKind.TABLE,
      "SELECT 1",
      Collections.singleton("source"),
      Paths.get("/tmp/project/sql/001.sql"),
      9,
      ExecutionMode.SQL_STATEMENT);
    SqlPipelineProjectException exception = new SqlPipelineProjectException("bad", new RuntimeException("cause"));

    assertEquals(Paths.get("/tmp/project"), spec.rootDirectory());
    assertEquals("pipeline", spec.name());
    assertEquals("catalog", spec.catalog());
    assertEquals("database", spec.database());
    assertEquals("v", spec.configuration().get("k"));
    assertEquals(Paths.get("/tmp/project/sql"), spec.sqlDirectories().get(0));
    assertEquals("dataset", definition.datasetName());
    assertEquals("SELECT 1", definition.querySql());
    assertEquals(9, definition.statementIndex());
    assertEquals(ExecutionMode.SQL_STATEMENT, definition.executionMode());
    assertEquals("bad", exception.getMessage());
    assertEquals("cause", exception.getCause().getMessage());
    assertEquals("sql.source.file", SqlPipelineDatasetProperties.SOURCE_FILE);
    assertEquals("sql_statement", SqlPipelineDatasetProperties.EXECUTION_MODE_SQL_STATEMENT);
  }

  @Test
  void projectSpecShouldSelectTaskFileAndProtectCompactionTableList() throws Exception {
    Path sql = Files.write(tempDir.resolve("one.sql"), "SELECT 1;".getBytes(StandardCharsets.UTF_8));
    SqlPipelineProjectSpec selected = new SqlPipelineProjectSpec(
      tempDir, "one", null, null, Collections.emptyMap(), Collections.singletonList(tempDir),
      JobType.SPARK_SQL, Collections.emptyList(), sql);
    assertEquals(Collections.singletonList(sql), selected.sqlFiles());
    assertTrue(selected.sqlDirectories().isEmpty());

    SqlPipelineProjectSpec compaction = new SqlPipelineProjectSpec(
      tempDir, "compact", null, null, Collections.emptyMap(), Collections.emptyList(),
      JobType.HUDI_COMPACTION, Collections.singletonList("db.table"));
    assertEquals(JobType.HUDI_COMPACTION, compaction.jobType());
    assertEquals(Collections.singletonList("db.table"), compaction.compactionTables());
    assertThrows(UnsupportedOperationException.class,
      () -> compaction.compactionTables().add("other.table"));
  }

  private Path exampleProject(String name) throws URISyntaxException {
    return Paths.get(SqlPlanAndApplicationTest.class.getResource("/" + name).toURI());
  }

  private Path createTemporaryViewProject(String name) throws IOException {
    Path projectRoot = Files.createDirectories(tempDir.resolve(name));
    Path transformations = Files.createDirectories(projectRoot.resolve("transformations"));
    Files.write(projectRoot.resolve("spark-pipeline.yaml"), "name: temp_view_pipeline\n".getBytes(StandardCharsets.UTF_8));
    Files.write(transformations.resolve("001_temp_view.sql"), (
      "CREATE TEMP VIEW temp_orders AS\n"
        + "SELECT 1 AS order_id, 'east' AS region;\n").getBytes(StandardCharsets.UTF_8));
    return projectRoot;
  }

  private String tempMissingPath() {
    return Paths.get("target", "missing-sql-project-" + System.nanoTime()).toString();
  }

  private void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }
}
