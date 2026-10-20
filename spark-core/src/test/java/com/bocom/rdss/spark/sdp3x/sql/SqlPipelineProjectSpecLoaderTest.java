package com.bocom.rdss.spark.sdp3x.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SqlPipelineProjectSpecLoaderTest {
  private final SqlPipelineProjectSpecLoader loader = new SqlPipelineProjectSpecLoader();

  @TempDir
  Path tempDir;

  @Test
  void shouldLoadSpecFromDirectoryAndExplicitFile() throws IOException {
    Path transformations = Files.createDirectories(tempDir.resolve("sql"));
    Path specFile = tempDir.resolve("spark-pipeline.yaml");
    Files.write(specFile, (
      "name:  custom_pipeline  \n"
        + "catalog: spark_catalog\n"
        + "database: analytics\n"
        + "configuration:\n"
        + "  spark.sql.shuffle.partitions: 1\n"
        + "transformation: sql\n").getBytes(StandardCharsets.UTF_8));

    SqlPipelineProjectSpec fromDirectory = loader.load(tempDir);
    SqlPipelineProjectSpec fromFile = loader.load(specFile);

    assertEquals(tempDir.toAbsolutePath().normalize(), fromDirectory.rootDirectory());
    assertEquals("custom_pipeline", fromDirectory.name());
    assertEquals("spark_catalog", fromDirectory.catalog());
    assertEquals("analytics", fromDirectory.database());
    assertEquals("1", fromDirectory.configuration().get("spark.sql.shuffle.partitions"));
    assertEquals(1, fromDirectory.sqlDirectories().size());
    assertEquals(transformations.toAbsolutePath().normalize(), fromDirectory.sqlDirectories().get(0));
    assertEquals(fromDirectory.rootDirectory(), fromFile.rootDirectory());
  }

  @Test
  void shouldAcceptYamlFileNamesAndRejectNonYamlSpecs() throws IOException {
    Path legacyProperties = tempDir.resolve("spark-pipeline.properties");
    Path yamlSpec = tempDir.resolve("spark-pipeline.yml");
    Files.createDirectories(tempDir.resolve("transformations"));
    Files.write(legacyProperties, "name=legacy\n".getBytes(StandardCharsets.UTF_8));
    Files.write(yamlSpec, "name: yaml-job\n".getBytes(StandardCharsets.UTF_8));

    assertThrows(SqlPipelineProjectException.class, () -> loader.load(tempDir));
    assertThrows(SqlPipelineProjectException.class, () -> loader.load(legacyProperties));
    assertEquals("yaml-job", loader.load(yamlSpec).name());
  }

  @Test
  void shouldDefaultNameAndTransformationWhenOmitted() throws IOException {
    Path transformations = Files.createDirectories(tempDir.resolve("transformations"));
    Files.write(
      tempDir.resolve("spark-pipeline.yaml"),
      "name: '   '\ndatabase: '   '\n".getBytes(StandardCharsets.UTF_8));

    SqlPipelineProjectSpec spec = loader.load(tempDir);

    assertEquals(tempDir.getFileName().toString(), spec.name());
    assertNull(spec.catalog());
    assertNull(spec.database());
    assertEquals(1, spec.sqlDirectories().size());
    assertEquals(transformations.toAbsolutePath().normalize(), spec.sqlDirectories().get(0));
  }

  @Test
  void shouldRejectMissingPathOrSpec() {
    SqlPipelineProjectException missingPath = assertThrows(
      SqlPipelineProjectException.class,
      () -> loader.load(tempDir.resolve("missing")));
    assertTrue(missingPath.getMessage().contains("path does not exist"));

    SqlPipelineProjectException missingSpec = assertThrows(
      SqlPipelineProjectException.class,
      () -> loader.load(tempDir));
    assertTrue(missingSpec.getMessage().contains("Missing spark-pipeline.yaml"));
  }

  @Test
  void shouldRejectInvalidYamlShapes() throws IOException {
    Path invalidRoot = Files.createDirectory(tempDir.resolve("invalid-root"));
    Files.write(invalidRoot.resolve("spark-pipeline.yaml"), "- item\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(invalidRoot))
      .getMessage().contains("must be a YAML object"));

    Path invalidConfig = Files.createDirectory(tempDir.resolve("invalid-config"));
    Files.createDirectories(invalidConfig.resolve("transformations"));
    Files.write(invalidConfig.resolve("spark-pipeline.yaml"), (
      "configuration:\n"
        + "  - bad\n").getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(invalidConfig))
      .getMessage().contains("configuration"));

    Path invalidTransformation = Files.createDirectory(tempDir.resolve("invalid-transformation"));
    Files.write(invalidTransformation.resolve("spark-pipeline.yaml"),
      "transformation: []\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(invalidTransformation))
      .getMessage().contains("transformation"));
  }

  @Test
  void shouldRejectLegacyLibrariesAndMissingTransformationDirectory() throws IOException {
    Path legacyLibraries = Files.createDirectory(tempDir.resolve("legacy-libraries"));
    Files.write(legacyLibraries.resolve("spark-pipeline.yaml"),
      "libraries:\n  - transformations\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(legacyLibraries))
      .getMessage().contains("'libraries' is no longer supported"));

    Path missingDirectory = Files.createDirectory(tempDir.resolve("missing-directory"));
    Files.write(missingDirectory.resolve("spark-pipeline.yaml"),
      "transformation: absent\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(missingDirectory))
      .getMessage().contains("does not exist"));
  }

  @Test
  void shouldLoadOneSqlTaskFileWithoutTransformationDirectory() throws IOException {
    Path spec = write("spark-pipeline.yaml", "job-type: spark-sql\nname: selected\n");
    Path sql = write("selected.sql", "SELECT 1;\n");
    SqlPipelineProjectSpec loaded = loader.load(spec, sql);
    assertEquals(Collections.singletonList(sql.toAbsolutePath().normalize()), loaded.sqlFiles());
    assertTrue(loaded.sqlDirectories().isEmpty());
    assertThrows(SqlPipelineProjectException.class,
      () -> loader.load(spec, write("wrong.list", "SELECT 1")));
    assertThrows(SqlPipelineProjectException.class,
      () -> loader.load(spec, tempDir.resolve("missing.sql")));
  }

  @Test
  void shouldReadDistinctCompactionTablesAndValidateList() throws IOException {
    Path spec = write("spark-pipeline.yaml", "job-type: hudi-compaction\nname: compact\n");
    Path list = write("tables.list", "\uFEFF# comment\n db_a.table_a \n\ndb_a.table_a\ndb_b.table_b\n");
    SqlPipelineProjectSpec loaded = loader.load(spec, list);
    assertEquals(Arrays.asList("db_a.table_a", "db_b.table_b"), loaded.compactionTables());
    assertTrue(loaded.sqlFiles().isEmpty());
    assertThrows(SqlPipelineProjectException.class, () -> loader.load(spec));
    assertThrows(SqlPipelineProjectException.class,
      () -> loader.load(spec, write("tables.txt", "db_a.table_a\n")));
    assertThrows(SqlPipelineProjectException.class,
      () -> loader.load(spec, write("invalid.list", "db_a.table_a\nbad-name\n")));
    assertThrows(SqlPipelineProjectException.class,
      () -> loader.load(spec, write("empty.list", "# only comment\n")));
  }

  @Test
  void shouldRejectUnsupportedJobTypeAndAcceptAbsoluteTransformationPath() throws IOException {
    Path spec = write("spark-pipeline.yaml", "job-type: unknown\n");
    assertThrows(SqlPipelineProjectException.class, () -> loader.load(spec));
    Files.write(spec, "\n".getBytes(StandardCharsets.UTF_8));
    Files.createDirectories(tempDir.resolve("transformations"));
    assertEquals(1, loader.load(spec).sqlDirectories().size());
    Files.write(spec, ("transformation: " + tempDir.resolve("transformations") + "\n")
      .getBytes(StandardCharsets.UTF_8));
    assertEquals(tempDir.resolve("transformations"), loader.load(spec).sqlDirectories().get(0));
    Files.write(spec, "transformation: '  '\n".getBytes(StandardCharsets.UTF_8));
    assertThrows(SqlPipelineProjectException.class, () -> loader.load(spec));
  }

  private Path write(String name, String content) throws IOException {
    Path path = tempDir.resolve(name);
    Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    return path;
  }
}
