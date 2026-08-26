package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqlPipelineProjectCompilerTest {
  @Mock
  private SqlPipelineProjectSpecLoader specLoader;
  @Mock
  private SqlPipelineParser parser;

  @TempDir
  Path tempDir;

  @Test
  void shouldCompileSqlDefinitionsIntoPipelineModel() throws Exception {
    Path sqlDir = Files.createDirectories(tempDir.resolve("transformations"));
    Path first = Files.write(sqlDir.resolve("001_seed.sql"), "ignored".getBytes());
    Path second = Files.write(sqlDir.resolve("002_daily.sql"), "ignored".getBytes());
    SqlPipelineProjectSpec spec = new SqlPipelineProjectSpec(
      tempDir,
      "sql_pipeline",
      "spark_catalog",
      "analytics",
      Collections.singletonMap("spark.sql.shuffle.partitions", "1"),
      Collections.singletonList(sqlDir));
    when(specLoader.load(tempDir)).thenReturn(spec);
    when(parser.parse(first, 1)).thenReturn(Collections.singletonList(new SqlPipelineDefinition(
      "ods.order$detail",
      com.bocom.rdss.spark.sdp3x.api.DatasetKind.TEMPORARY_VIEW,
      "SELECT * FROM raw.orders",
      Collections.singleton("raw.orders"),
      first,
      1,
      SqlPipelineDefinition.WriteMode.SAVE_AS_TABLE,
      Collections.singletonMap("spark.sql.shuffle.partitions", "4"))));
    when(parser.parse(second, 2)).thenReturn(Arrays.asList(
      new SqlPipelineDefinition(
        "ods.order$detail",
        com.bocom.rdss.spark.sdp3x.api.DatasetKind.TEMPORARY_VIEW,
        "SELECT * FROM raw.orders",
        Collections.singleton("raw.orders"),
        second,
        2,
        SqlPipelineDefinition.WriteMode.SAVE_AS_TABLE,
        Collections.<String, String>emptyMap()),
      new SqlPipelineDefinition(
        "ads.daily_orders",
        com.bocom.rdss.spark.sdp3x.api.DatasetKind.TABLE,
        "SELECT * FROM ods.order$detail",
        Collections.singleton("ods.order$detail"),
        second,
        3,
        SqlPipelineDefinition.WriteMode.INSERT_INTO,
        new LinkedHashMap<String, String>() {{
          put("spark.sql.shuffle.partitions", "2");
          put("spark.sql.autoBroadcastJoinThreshold", "-1");
        }})));

    PipelineDefinition pipeline = new SqlPipelineProjectCompiler(specLoader, parser).compile(tempDir);

    assertEquals("sql_pipeline", pipeline.name());
    assertEquals(2, pipeline.datasets().size());
    assertEquals(3, pipeline.flows().size());
    assertEquals("1", pipeline.configuration().get("spark.sql.shuffle.partitions"));
    DatasetDefinition tempDataset = pipeline.dataset("ods.order$detail").orElseThrow(AssertionError::new);
    assertEquals(SqlPipelineDatasetProperties.WRITE_MODE_SAVE_AS_TABLE,
      tempDataset.properties().get(SqlPipelineDatasetProperties.WRITE_MODE));
    assertEquals("transformations/001_seed.sql",
      tempDataset.properties().get(SqlPipelineDatasetProperties.SOURCE_FILE));

    DatasetDefinition tableDataset = pipeline.dataset("ads.daily_orders").orElseThrow(AssertionError::new);
    assertEquals(SqlPipelineDatasetProperties.WRITE_MODE_INSERT_INTO,
      tableDataset.properties().get(SqlPipelineDatasetProperties.WRITE_MODE));
    assertEquals("3", tableDataset.properties().get(SqlPipelineDatasetProperties.STATEMENT_INDEX));
    assertTrue(pipeline.flow("sql_flow_001_ods.order_detail").isPresent());
    assertTrue(pipeline.flow("sql_flow_003_ads.daily_orders").isPresent());
    assertEquals("4", pipeline.flow("sql_flow_001_ods.order_detail").orElseThrow(AssertionError::new)
      .sparkConf().get("spark.sql.shuffle.partitions"));
    assertEquals("2", pipeline.flow("sql_flow_003_ads.daily_orders").orElseThrow(AssertionError::new)
      .sparkConf().get("spark.sql.shuffle.partitions"));
    assertEquals("-1", pipeline.flow("sql_flow_003_ads.daily_orders").orElseThrow(AssertionError::new)
      .sparkConf().get("spark.sql.autoBroadcastJoinThreshold"));
  }

  @Test
  void shouldRejectProjectsWithoutSqlFiles() throws Exception {
    Path sqlDir = Files.createDirectories(tempDir.resolve("empty"));
    when(specLoader.load(tempDir)).thenReturn(new SqlPipelineProjectSpec(
      tempDir,
      "sql_pipeline",
      null,
      null,
      Collections.<String, String>emptyMap(),
      Collections.singletonList(sqlDir)));

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> new SqlPipelineProjectCompiler(specLoader, parser).compile(tempDir));

    assertTrue(exception.getMessage().contains("No SQL files were found"));
  }

  @Test
  void shouldWrapSqlDirectoryScanFailures() {
    Path missingDir = tempDir.resolve("missing");
    when(specLoader.load(tempDir)).thenReturn(new SqlPipelineProjectSpec(
      tempDir,
      "sql_pipeline",
      null,
      null,
      Collections.<String, String>emptyMap(),
      Collections.singletonList(missingDir)));

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> new SqlPipelineProjectCompiler(specLoader, parser).compile(tempDir));

    assertTrue(exception.getMessage().contains("Failed to scan SQL library directory"));
  }
}
