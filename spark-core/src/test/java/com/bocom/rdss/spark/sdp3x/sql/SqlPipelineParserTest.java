package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlPipelineParserTest {
  private final SqlPipelineParser parser = new SqlPipelineParser();

  @TempDir
  Path tempDir;

  @Test
  void shouldParseCreateViewAndInsertIntoStatementsWithCommentsAndQuotedIdentifiers() throws IOException {
    Path sqlFile = tempDir.resolve("pipeline.sql");
    Files.write(sqlFile, (
      "-- ignore statement separators in comments ; ; ;\n"
        + "CREATE OR REPLACE TEMP VIEW `ods`.`orders_clean` AS\n"
        + "SELECT src.id, src.region, 'from fake_table' AS note\n"
        + "FROM `raw`.`orders_source` src\n"
        + "JOIN dim.region_info dim ON src.region = dim.region;\n"
        + "/* block comment ; should not split */\n"
        + "INSERT INTO TABLE analytics.daily_orders\n"
        + "SELECT region, COUNT(1) AS order_count\n"
        + "FROM `ods`.`orders_clean`\n"
        + "WHERE note <> 'semi;colon' AND region <> 'it''s fake'\n"
        + "GROUP BY region;\n").getBytes(StandardCharsets.UTF_8));

    List<SqlPipelineDefinition> definitions = parser.parse(sqlFile, 7);

    assertEquals(2, definitions.size());

    SqlPipelineDefinition tempView = definitions.get(0);
    assertEquals("ods.orders_clean", tempView.datasetName());
    assertEquals(DatasetKind.TEMPORARY_VIEW, tempView.datasetKind());
    assertEquals(7, tempView.statementIndex());
    assertEquals(SqlPipelineDefinition.WriteMode.SAVE_AS_TABLE, tempView.writeMode());
    assertEquals("raw.orders_source", tempView.inputDatasets().iterator().next());
    assertTrue(tempView.inputDatasets().contains("dim.region_info"));

    SqlPipelineDefinition insertInto = definitions.get(1);
    assertEquals("analytics.daily_orders", insertInto.datasetName());
    assertEquals(DatasetKind.TABLE, insertInto.datasetKind());
    assertEquals(8, insertInto.statementIndex());
    assertEquals(SqlPipelineDefinition.WriteMode.INSERT_INTO, insertInto.writeMode());
    assertEquals("ods.orders_clean", insertInto.inputDatasets().iterator().next());
  }

  @Test
  void shouldApplySetStatementsToTheNextFlowOnly() throws IOException {
    Path sqlFile = tempDir.resolve("flow-conf.sql");
    Files.write(sqlFile, (
      "SET spark.sql.shuffle.partitions=8;\n"
        + "SET spark.sql.autoBroadcastJoinThreshold = -1;\n"
        + "CREATE TEMP VIEW stage_orders AS\n"
        + "SELECT * FROM raw.orders_source;\n"
        + "CREATE MATERIALIZED VIEW daily_orders AS\n"
        + "SELECT * FROM stage_orders;\n").getBytes(StandardCharsets.UTF_8));

    List<SqlPipelineDefinition> definitions = parser.parse(sqlFile, 1);

    assertEquals(2, definitions.size());
    assertEquals("8", definitions.get(0).sparkConf().get("spark.sql.shuffle.partitions"));
    assertEquals("-1", definitions.get(0).sparkConf().get("spark.sql.autoBroadcastJoinThreshold"));
    assertEquals(Collections.emptyMap(), definitions.get(1).sparkConf());
  }

  @Test
  void shouldRejectDanglingSetStatementsWithoutFollowingFlow() throws IOException {
    Path sqlFile = tempDir.resolve("dangling-set.sql");
    Files.write(sqlFile, "SET spark.sql.shuffle.partitions=8;".getBytes(StandardCharsets.UTF_8));

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> parser.parse(sqlFile, 1));

    assertTrue(exception.getMessage().contains("must be followed by a supported SQL flow statement"));
  }

  @Test
  void shouldParseMaterializedViewsAndIgnoreEmptyStatements() throws IOException {
    Path sqlFile = tempDir.resolve("mv.sql");
    Files.write(sqlFile, (
      ";\n"
        + "CREATE MATERIALIZED VIEW sales.daily_orders AS\n"
        + "SELECT * FROM sales.orders_source;\n"
        + ";\n").getBytes(StandardCharsets.UTF_8));

    List<SqlPipelineDefinition> definitions = parser.parse(sqlFile, 1);

    assertEquals(1, definitions.size());
    assertEquals(DatasetKind.MATERIALIZED_VIEW, definitions.get(0).datasetKind());
    assertEquals("sales.daily_orders", definitions.get(0).datasetName());
  }

  @Test
  void shouldRejectUnsupportedStatements() throws IOException {
    Path sqlFile = tempDir.resolve("unsupported.sql");
    Files.write(sqlFile, "DELETE FROM orders_source".getBytes(StandardCharsets.UTF_8));

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> parser.parse(sqlFile, 1));

    assertTrue(exception.getMessage().contains("Unsupported SQL statement"));
  }

  @Test
  void shouldWrapReadFailures() {
    Path missingFile = tempDir.resolve("missing.sql");

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> parser.parse(missingFile, 1));

    assertTrue(exception.getMessage().contains("Failed to read SQL file"));
  }
}
