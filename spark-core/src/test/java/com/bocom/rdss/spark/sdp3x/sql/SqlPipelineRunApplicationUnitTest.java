package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.enums.JobType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.InOrder;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqlPipelineRunApplicationUnitTest {
  @TempDir Path tempDir;

  @SuppressWarnings("unchecked")
  @Test
  void executesOldestCompactionPlanAndSkipsOtherActionsOrAbsentPlans() {
    SparkSession spark = mock(SparkSession.class);
    Dataset<Row> otherAction = mock(Dataset.class);
    Dataset<Row> absent = mock(Dataset.class);
    Dataset<Row> plans = mock(Dataset.class);
    Dataset<Row> run = mock(Dataset.class);
    Dataset<Row> clean = mock(Dataset.class);
    Dataset<Row> archive = mock(Dataset.class);
    when(spark.sql("SHOW COMPACTION ON `db`.`busy`")).thenReturn(otherAction);
    when(otherAction.collectAsList()).thenReturn(Collections.singletonList(row("20261020000000", "commit")));
    when(spark.sql("SHOW COMPACTION ON `db`.`empty`")).thenReturn(absent);
    when(absent.collectAsList()).thenReturn(Collections.emptyList());
    when(spark.sql("SHOW COMPACTION ON `db`.`ready`")).thenReturn(plans);
    when(plans.collectAsList()).thenReturn(
      Arrays.asList(row("20261020000000", "commit"),
        row("20261020000002", "compaction"), row("20261020000001", "compaction")));
    when(spark.sql("RUN COMPACTION ON `db`.`ready` AT 20261020000001")).thenReturn(run);
    when(run.collectAsList()).thenReturn(Collections.emptyList());
    when(spark.sql("RUN CLEAN ON `db`.`ready`")).thenReturn(clean);
    when(clean.collectAsList()).thenReturn(Collections.emptyList());
    when(spark.sql("RUN ARCHIVELOG ON `db`.`ready`")).thenReturn(archive);
    when(archive.collectAsList()).thenReturn(Collections.emptyList());

    SqlPipelineRunApplication.executeCompactionTables(spark,
      Arrays.asList("db.busy", "db.empty", "db.ready"));

    InOrder ordered = inOrder(spark, plans, run, clean, archive);
    ordered.verify(spark).sql("SHOW COMPACTION ON `db`.`ready`");
    ordered.verify(plans).collectAsList();
    ordered.verify(spark).sql("RUN COMPACTION ON `db`.`ready` AT 20261020000001");
    ordered.verify(run).collectAsList();
    ordered.verify(spark).sql("RUN CLEAN ON `db`.`ready`");
    ordered.verify(clean).collectAsList();
    ordered.verify(spark).sql("RUN ARCHIVELOG ON `db`.`ready`");
    ordered.verify(archive).collectAsList();
    verify(spark, never())
      .sql("RUN COMPACTION ON `db`.`busy` AT 20261020000000");
  }

  @SuppressWarnings("unchecked")
  @Test
  void aggregatesCompactionFailuresAndKeepsProcessingNextTable() {
    SparkSession spark = mock(SparkSession.class);
    Dataset<Row> badInstant = mock(Dataset.class);
    Dataset<Row> incomplete = mock(Dataset.class);
    Dataset<Row> completed = mock(Dataset.class);
    Dataset<Row> run = mock(Dataset.class);
    when(spark.sql("SHOW COMPACTION ON `db`.`bad_instant`")).thenReturn(badInstant);
    when(badInstant.collectAsList()).thenReturn(Collections.singletonList(row("invalid", "compaction")));
    when(spark.sql("SHOW COMPACTION ON `db`.`incomplete`")).thenReturn(incomplete);
    when(incomplete.collectAsList()).thenReturn(
      Collections.singletonList(row("20261020000003", "compaction")));
    when(spark.sql("RUN COMPACTION ON `db`.`incomplete` AT 20261020000003")).thenReturn(run);
    when(run.collectAsList()).thenThrow(new IllegalStateException("compaction failed"));
    when(spark.sql("SHOW COMPACTION ON `db`.`last`")).thenReturn(completed);
    when(completed.collectAsList()).thenReturn(Collections.emptyList());

    IllegalStateException error = assertThrows(IllegalStateException.class,
      () -> SqlPipelineRunApplication.executeCompactionTables(spark,
        Arrays.asList("db.bad_instant", "db.incomplete", "bad", "db.last")));
    assertTrue(error.getMessage().contains("3 table(s)"));
    verify(spark).sql("SHOW COMPACTION ON `db`.`last`");
    verify(spark, never()).sql("RUN CLEAN ON `db`.`incomplete`");
    verify(spark, never()).sql("RUN ARCHIVELOG ON `db`.`incomplete`");
  }

  @SuppressWarnings("unchecked")
  @Test
  void doesNotArchiveWhenCleanFails() {
    SparkSession spark = mock(SparkSession.class);
    Dataset<Row> plans = mock(Dataset.class);
    Dataset<Row> run = mock(Dataset.class);
    Dataset<Row> clean = mock(Dataset.class);
    when(spark.sql("SHOW COMPACTION ON `db`.`ready`")).thenReturn(plans);
    when(plans.collectAsList()).thenReturn(
      Collections.singletonList(row("20261020000001", "compaction")));
    when(spark.sql("RUN COMPACTION ON `db`.`ready` AT 20261020000001")).thenReturn(run);
    when(run.collectAsList()).thenReturn(Collections.emptyList());
    when(spark.sql("RUN CLEAN ON `db`.`ready`")).thenReturn(clean);
    when(clean.collectAsList()).thenThrow(new IllegalStateException("clean failed"));

    IllegalStateException error = assertThrows(IllegalStateException.class,
      () -> SqlPipelineRunApplication.executeCompactionTables(spark,
        Collections.singletonList("db.ready")));
    assertTrue(error.getMessage().contains("db.ready"));
    verify(spark, never()).sql("RUN ARCHIVELOG ON `db`.`ready`");
  }

  @Test
  void loadsParsesAndRejectsLocalHiveConfiguration() throws Exception {
    ClassLoader missing = new ClassLoader() {
      @Override public URL getResource(String name) { return null; }
    };
    assertFalse(SqlPipelineRunApplication.loadPreferredLocalHiveSiteProperties(missing).isPresent());
    Path xml = Files.write(tempDir.resolve("hive-site.xml"), (
      "<configuration><property><name>hive.metastore.uris</name><value> thrift://host:9083 </value></property>"
        + "<property><name>missing-value</name></property><property><value>missing-name</value></property>"
        + "</configuration>").getBytes(StandardCharsets.UTF_8));
    ClassLoader present = resourceLoader(xml.toUri().toURL());
    Map<String, String> settings = SqlPipelineRunApplication
      .loadPreferredLocalHiveSiteProperties(present).get();
    assertEquals("thrift://host:9083", settings.get("hive.metastore.uris"));
    assertEquals(1, settings.size());

    Path invalid = Files.write(tempDir.resolve("invalid.xml"), "<broken".getBytes(StandardCharsets.UTF_8));
    assertThrows(IllegalStateException.class,
      () -> SqlPipelineRunApplication.loadPreferredLocalHiveSiteProperties(resourceLoader(invalid.toUri().toURL())));
  }

  @Test
  void appliesHiveSettingsAndWarehouseFallback() {
    SparkSession.Builder builder = mock(SparkSession.Builder.class);
    SqlPipelineRunApplication.applyHiveSiteProperties(builder,
      Collections.singletonMap("hive.metastore.warehouse.dir", "/warehouse"));
    verify(builder).config("hive.metastore.warehouse.dir", "/warehouse");
    verify(builder).config("spark.hadoop.hive.metastore.warehouse.dir", "/warehouse");
    verify(builder).config("spark.sql.warehouse.dir", "/warehouse");
    SqlPipelineRunApplication.applyHiveSiteProperties(builder,
      Collections.singletonMap("spark.sql.warehouse.dir", "/direct"));
    verify(builder).config("spark.sql.warehouse.dir", "/direct");
  }

  @Test
  void cleansManagedWarehouseDirectoriesIncludingNestedPaths() throws Exception {
    SparkSession spark = mock(SparkSession.class);
    Path warehouse = Files.createDirectory(tempDir.resolve("warehouse"));
    Path defaultTable = Files.createDirectories(warehouse.resolve("plain"));
    Files.write(defaultTable.resolve("part"), new byte[] {1});
    Path qualifiedTable = Files.createDirectories(warehouse.resolve("db.db").resolve("qualified"));
    Files.write(qualifiedTable.resolve("part"), new byte[] {2});
    SqlPipelineRunApplication.resetLocalManagedDatasets(spark,
      new PipelineBuilder("cleanup")
        .addDataset(ImmutableDatasetDefinition.table("plain"))
        .addDataset(ImmutableDatasetDefinition.table("db.qualified"))
        .addDataset(ImmutableDatasetDefinition.temporaryView("view_only"))
        .build(), warehouse);
    verify(spark).sql("DROP TABLE IF EXISTS plain");
    verify(spark).sql("DROP TABLE IF EXISTS db.qualified");
    assertFalse(Files.exists(defaultTable));
    assertFalse(Files.exists(qualifiedTable));
    SqlPipelineRunApplication.deleteIfExists(warehouse.resolve("absent"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void createsRemoteAndPreferredLocalSessions() {
    SparkSession.Builder builder = builderMock();
    SparkSession spark = mock(SparkSession.class);
    when(builder.getOrCreate()).thenReturn(spark);
    Catalog catalog = mock(Catalog.class);
    Dataset databases = mock(Dataset.class);
    when(spark.catalog()).thenReturn(catalog);
    when(catalog.listDatabases()).thenReturn(databases);
    when(databases.collectAsList()).thenReturn(Collections.emptyList());
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);
      SqlPipelineRunOptions remote = SqlPipelineRunOptions.parse(new String[] {
        "--spec", tempDir.resolve("spark-pipeline.yaml").toString(), "--submitted"});
      SparkSessionContext remoteContext = SqlPipelineRunApplication.createSparkSession(
        remote, "remote-job", JobType.HUDI_COMPACTION, Collections.emptyMap());
      assertEquals(spark, remoteContext.sparkSession());
      assertEquals(null, remoteContext.localWarehouseDirectory());
      verify(builder).config("spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");

      SqlPipelineRunOptions local = SqlPipelineRunOptions.parse(new String[] {
        "--spec", tempDir.resolve("spark-pipeline.yaml").toString(), "--master", "local[1]"});
      SparkSessionContext preferred = SqlPipelineRunApplication.createSparkSession(
        local, "local-job", JobType.SPARK_SQL, Collections.emptyMap());
      assertEquals(spark, preferred.sparkSession());
      assertEquals(null, preferred.localWarehouseDirectory());
      verify(builder).master("local[1]");
      verify(catalog).listDatabases();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void fallsBackToDerbyAfterPreferredMetastoreFailure() {
    SparkSession.Builder builder = builderMock();
    SparkSession first = mock(SparkSession.class);
    SparkSession second = mock(SparkSession.class);
    Catalog catalog = mock(Catalog.class);
    when(first.catalog()).thenReturn(catalog);
    when(catalog.listDatabases()).thenThrow(new IllegalStateException("metastore unavailable"));
    when(builder.getOrCreate()).thenReturn(first, second);
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);
      SqlPipelineRunOptions local = SqlPipelineRunOptions.parse(new String[] {
        "--spec", tempDir.resolve("spark-pipeline.yaml").toString(), "--master", "local[1]"});
      SparkSessionContext fallback = SqlPipelineRunApplication.createSparkSession(
        local, "fallback-job", JobType.SPARK_SQL, Collections.emptyMap());
      assertEquals(second, fallback.sparkSession());
      assertTrue(fallback.localWarehouseDirectory().endsWith("spark-warehouse"));
      verify(first).stop();
    }
  }

  @Test
  void fallsBackToDerbyWhenPreferredSessionCannotStart() {
    SparkSession.Builder builder = builderMock();
    SparkSession fallbackSession = mock(SparkSession.class);
    when(builder.getOrCreate())
      .thenThrow(new IllegalStateException("preferred metastore unavailable"))
      .thenReturn(fallbackSession);
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);
      SqlPipelineRunOptions local = SqlPipelineRunOptions.parse(new String[] {
        "--spec", tempDir.resolve("spark-pipeline.yaml").toString(), "--master", "local[1]"});

      SparkSessionContext fallback = SqlPipelineRunApplication.createSparkSession(
        local, "fallback-job", JobType.SPARK_SQL, Collections.emptyMap());

      assertEquals(fallbackSession, fallback.sparkSession());
      assertTrue(fallback.localWarehouseDirectory().endsWith("spark-warehouse"));
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void appliesYamlConfigurationForCompactionJob() throws Exception {
    Path spec = Files.write(tempDir.resolve("spark-pipeline.yaml"), (
      "name: compact\njob-type: hudi-compaction\nconfiguration:\n"
        + "  spark.sql.shuffle.partitions: '8'\n"
        + "  hoodie.cleaner.commits.retained: '20'\n"
        + "  hoodie.keep.min.commits: '30'\n"
        + "  hoodie.keep.max.commits: '50'\n").getBytes(StandardCharsets.UTF_8));
    Path sqlFile = Files.write(tempDir.resolve("compaction.sql"),
      "RUN COMPACTION ON db.table;\n".getBytes(StandardCharsets.UTF_8));
    SparkSession.Builder builder = builderMock();
    SparkSession spark = mock(SparkSession.class);
    Dataset<Row> plans = mock(Dataset.class);
    when(builder.getOrCreate()).thenReturn(spark);
    when(spark.sql("SHOW COMPACTION ON `db`.`table`")).thenReturn(plans);
    when(plans.collectAsList()).thenReturn(Collections.emptyList());
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);
      SqlPipelineRunApplication.run(SqlPipelineRunOptions.parse(new String[] {
        "--spec", spec.toString(), "--task-file", sqlFile.toString(), "--submitted"}));
    }
    InOrder ordered = inOrder(builder);
    ordered.verify(builder).config("spark.sql.shuffle.partitions", "8");
    ordered.verify(builder).config("hoodie.cleaner.commits.retained", "20");
    ordered.verify(builder).config("hoodie.keep.min.commits", "30");
    ordered.verify(builder).config("hoodie.keep.max.commits", "50");
    ordered.verify(builder).getOrCreate();
    verify(spark).stop();
  }

  @Test
  void appliesSqlJobConfigurationBeforeCreatingSession() {
    SparkSession.Builder builder = builderMock();
    SparkSession spark = mock(SparkSession.class);
    when(builder.getOrCreate()).thenReturn(spark);
    SqlPipelineRunOptions options = SqlPipelineRunOptions.parse(new String[] {
      "--spec", tempDir.resolve("spark-pipeline.yaml").toString(), "--submitted"});
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);
      SqlPipelineRunApplication.createSparkSession(options, "sql-job", JobType.SPARK_SQL,
        Collections.singletonMap("spark.sql.shuffle.partitions", "8"));
    }
    InOrder ordered = inOrder(builder);
    ordered.verify(builder).config("spark.sql.shuffle.partitions", "8");
    ordered.verify(builder).getOrCreate();
  }

  @SuppressWarnings("unchecked")
  @Test
  void mainRunsSelectedCompactionTaskFile() throws Exception {
    Path spec = Files.write(tempDir.resolve("spark-pipeline.yaml"), (
      "name: compact\njob-type: hudi-compaction\n").getBytes(StandardCharsets.UTF_8));
    Path sqlFile = Files.write(tempDir.resolve("compaction.sql"),
      "RUN COMPACTION ON db.table;\n".getBytes(StandardCharsets.UTF_8));
    SparkSession.Builder builder = builderMock();
    SparkSession spark = mock(SparkSession.class);
    Dataset<Row> plans = mock(Dataset.class);
    when(builder.getOrCreate()).thenReturn(spark);
    when(spark.sql("SHOW COMPACTION ON `db`.`table`")).thenReturn(plans);
    when(plans.collectAsList()).thenReturn(Collections.emptyList());
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);

      SqlPipelineRunApplication.main(new String[] {
        "--spec", spec.toString(), "--task-file", sqlFile.toString(), "--submitted"});
    }

    verify(builder).appName("compact");
    verify(spark).sql("SHOW COMPACTION ON `db`.`table`");
    verify(spark).stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  void stopsSparkSessionWhenCompactionFails() throws Exception {
    Path spec = Files.write(tempDir.resolve("spark-pipeline.yaml"), (
      "name: compact\njob-type: hudi-compaction\n").getBytes(StandardCharsets.UTF_8));
    Path sqlFile = Files.write(tempDir.resolve("compaction.sql"),
      "RUN COMPACTION ON db.table;\n".getBytes(StandardCharsets.UTF_8));
    SparkSession.Builder builder = builderMock();
    SparkSession spark = mock(SparkSession.class);
    when(builder.getOrCreate()).thenReturn(spark);
    when(spark.sql("SHOW COMPACTION ON `db`.`table`"))
      .thenThrow(new IllegalStateException("metastore unavailable"));
    try (MockedStatic<SparkSession> ignored = mockStatic(SparkSession.class)) {
      ignored.when(SparkSession::builder).thenReturn(builder);

      IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> SqlPipelineRunApplication.run(SqlPipelineRunOptions.parse(new String[] {
          "--spec", spec.toString(), "--task-file", sqlFile.toString(), "--submitted"})));

      assertTrue(error.getMessage().contains("db.table"));
    }
    verify(spark).stop();
  }

  private SparkSession.Builder builderMock() {
    SparkSession.Builder builder = mock(SparkSession.Builder.class);
    when(builder.appName(anyString())).thenReturn(builder);
    when(builder.enableHiveSupport()).thenReturn(builder);
    when(builder.master(anyString())).thenReturn(builder);
    when(builder.config(anyString(), anyString())).thenReturn(builder);
    return builder;
  }

  private ClassLoader resourceLoader(URL url) {
    return new ClassLoader() {
      @Override public URL getResource(String name) {
        return SqlPipelineRunApplication.DEV_HIVE_SITE_RESOURCE.equals(name) ? url : null;
      }
    };
  }

  private Row row(String instant, String action) {
    return RowFactory.create(instant, action, 6);
  }
}
