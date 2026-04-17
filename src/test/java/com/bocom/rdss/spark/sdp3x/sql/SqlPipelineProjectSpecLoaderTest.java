package com.bocom.rdss.spark.sdp3x.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SqlPipelineProjectSpecLoaderTest {
  private final SqlPipelineProjectSpecLoader loader = new SqlPipelineProjectSpecLoader();

  @TempDir
  Path tempDir;

  @Test
  void shouldLoadSpecFromDirectoryAndExplicitFile() throws IOException {
    Path transformations = Files.createDirectories(tempDir.resolve("sql"));
    Path extra = Files.createDirectories(tempDir.resolve("more-sql"));
    Path specFile = tempDir.resolve("spark-pipeline.yml");
    Files.write(specFile, (
      "name:  custom_pipeline  \n"
        + "catalog: spark_catalog\n"
        + "database: analytics\n"
        + "configuration:\n"
        + "  spark.sql.shuffle.partitions: 1\n"
        + "libraries:\n"
        + "  - path: sql\n"
        + "  - more-sql\n").getBytes(StandardCharsets.UTF_8));

    SqlPipelineProjectSpec fromDirectory = loader.load(tempDir);
    SqlPipelineProjectSpec fromFile = loader.load(specFile);

    assertEquals(tempDir.toAbsolutePath().normalize(), fromDirectory.rootDirectory());
    assertEquals("custom_pipeline", fromDirectory.name());
    assertEquals("spark_catalog", fromDirectory.catalog());
    assertEquals("analytics", fromDirectory.database());
    assertEquals("1", fromDirectory.configuration().get("spark.sql.shuffle.partitions"));
    assertEquals(2, fromDirectory.sqlDirectories().size());
    assertTrue(fromDirectory.sqlDirectories().contains(transformations.toAbsolutePath().normalize()));
    assertTrue(fromDirectory.sqlDirectories().contains(extra.toAbsolutePath().normalize()));
    assertEquals(fromDirectory.rootDirectory(), fromFile.rootDirectory());
  }

  @Test
  void shouldDefaultNameAndLibrariesWhenOmitted() throws IOException {
    Path transformations = Files.createDirectories(tempDir.resolve("transformations"));
    Files.write(
      tempDir.resolve("spark-pipeline.yml"),
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
    assertTrue(missingSpec.getMessage().contains("Missing SQL pipeline spec file under"));
  }

  @Test
  void shouldRejectMultipleSpecFiles() throws IOException {
    Files.write(tempDir.resolve("spark-pipeline.yml"), new byte[0]);
    Files.write(tempDir.resolve("spark-pipeline.yaml"), new byte[0]);

    SqlPipelineProjectException exception = assertThrows(
      SqlPipelineProjectException.class,
      () -> loader.load(tempDir));

    assertTrue(exception.getMessage().contains("Multiple SQL pipeline spec files found"));
  }

  @Test
  void shouldRejectInvalidYamlShapes() throws IOException {
    Path invalidRoot = Files.createDirectory(tempDir.resolve("invalid-root"));
    Files.write(invalidRoot.resolve("spark-pipeline.yml"), "- item\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(invalidRoot))
      .getMessage().contains("must be a YAML object"));

    Path invalidConfig = Files.createDirectory(tempDir.resolve("invalid-config"));
    Files.createDirectories(invalidConfig.resolve("transformations"));
    Files.write(invalidConfig.resolve("spark-pipeline.yml"), (
      "configuration:\n"
        + "  - bad\n").getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(invalidConfig))
      .getMessage().contains("configuration"));

    Path invalidLibraries = Files.createDirectory(tempDir.resolve("invalid-libraries"));
    Files.write(invalidLibraries.resolve("spark-pipeline.yml"), "libraries: bad\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(invalidLibraries))
      .getMessage().contains("libraries"));
  }

  @Test
  void shouldRejectInvalidLibraryEntries() throws IOException {
    Path missingPathEntry = Files.createDirectory(tempDir.resolve("missing-path-entry"));
    Files.write(missingPathEntry.resolve("spark-pipeline.yml"), (
      "libraries:\n"
        + "  - {}\n").getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(missingPathEntry))
      .getMessage().contains("'path' or 'file'"));

    Path unsupportedEntry = Files.createDirectory(tempDir.resolve("unsupported-entry"));
    Files.write(unsupportedEntry.resolve("spark-pipeline.yml"), (
      "libraries:\n"
        + "  - 123\n").getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(unsupportedEntry))
      .getMessage().contains("Unsupported SQL library entry"));

    Path emptyLibraries = Files.createDirectory(tempDir.resolve("empty-libraries"));
    Files.write(emptyLibraries.resolve("spark-pipeline.yml"), "libraries: []\n".getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(emptyLibraries))
      .getMessage().contains("At least one SQL library directory is required"));

    Path missingDirectory = Files.createDirectory(tempDir.resolve("missing-directory"));
    Files.write(missingDirectory.resolve("spark-pipeline.yml"), (
      "libraries:\n"
        + "  - path: absent\n").getBytes(StandardCharsets.UTF_8));
    assertTrue(assertThrows(SqlPipelineProjectException.class, () -> loader.load(missingDirectory))
      .getMessage().contains("does not exist"));
  }
}
