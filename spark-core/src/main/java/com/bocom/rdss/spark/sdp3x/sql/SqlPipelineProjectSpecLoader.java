package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.enums.JobType;
import com.bocom.rdss.spark.sdp3x.util.SqlIdentifierUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads a SQL pipeline project spec from {@code spark-pipeline.yaml}.
 */
public final class SqlPipelineProjectSpecLoader {
  private static final String SPEC_FILE = "spark-pipeline.yaml";

  public SqlPipelineProjectSpec load(Path projectRootOrSpecPath) {
    return load(projectRootOrSpecPath, null);
  }

  public SqlPipelineProjectSpec load(Path projectRootOrSpecPath, Path taskFile) {
    Path specPath = resolveSpecPath(projectRootOrSpecPath);
    Path normalizedRoot = specPath.getParent().toAbsolutePath().normalize();

    Map<String, Object> rawSpec = readYaml(specPath);
    if (rawSpec.containsKey("libraries")) {
      throw new SqlPipelineProjectException("'libraries' is no longer supported; use 'transformation'");
    }
    String name = stringValue(rawSpec.get("name"), normalizedRoot.getFileName().toString());
    JobType jobType = readJobType(rawSpec);
    Path normalizedTaskFile = taskFile == null ? null : validateTaskFile(taskFile);
    SqlFileSource sqlFileSource = SqlFileSource.none();
    if (jobType == JobType.SPARK_SQL) {
      if (normalizedTaskFile != null) {
        if (!normalizedTaskFile.getFileName().toString()
            .toLowerCase(java.util.Locale.ROOT).endsWith(".sql")) {
          throw new SqlPipelineProjectException("Spark SQL task file must end with .sql: " + normalizedTaskFile);
        }
        sqlFileSource = SqlFileSource.fromSqlFile(normalizedTaskFile);
      } else {
        sqlFileSource = SqlFileSource.fromSqlDirectories(
          readSqlDirectories(normalizedRoot, rawSpec.get("transformation")));
      }
    }
    List<String> compactionTables = jobType == JobType.HUDI_COMPACTION
      ? readCompactionTables(normalizedTaskFile)
      : Collections.emptyList();
    return new SqlPipelineProjectSpec(
      normalizedRoot,
      name,
      nullableStringValue(rawSpec.get("catalog")),
      nullableStringValue(rawSpec.get("database")),
      readConfiguration(rawSpec.get("configuration")),
      sqlFileSource,
      jobType,
      compactionTables,
      normalizedTaskFile);
  }

  private Path resolveSpecPath(Path projectRootOrSpecPath) {
    Path normalizedPath = projectRootOrSpecPath.toAbsolutePath().normalize();
    if (Files.isRegularFile(normalizedPath)) {
      String fileName = normalizedPath.getFileName().toString();
      if (!fileName.endsWith(".yaml") && !fileName.endsWith(".yml")) {
        throw new SqlPipelineProjectException("SQL pipeline spec must be a YAML file: " + normalizedPath);
      }
      return normalizedPath;
    }

    if (!Files.isDirectory(normalizedPath)) {
      throw new SqlPipelineProjectException("SQL pipeline path does not exist: " + normalizedPath);
    }

    // Only inspect the directory explicitly provided by the caller. We intentionally
    // do not walk up parent directories.
    return findSpecInDirectory(normalizedPath);
  }

  private Path validateTaskFile(Path taskFile) {
    Path normalized = taskFile.toAbsolutePath().normalize();
    if (!Files.isRegularFile(normalized) || !Files.isReadable(normalized)) {
      throw new SqlPipelineProjectException("--task-file must be a readable file: " + normalized);
    }
    return normalized;
  }

  private Path findSpecInDirectory(Path directory) {
    Path specPath = directory.resolve(SPEC_FILE);
    if (Files.isRegularFile(specPath)) {
      return specPath;
    }
    throw new SqlPipelineProjectException(
      "Missing " + SPEC_FILE + " under: " + directory);
  }

  private JobType readJobType(Map<String, Object> rawSpec) {
    String type = rawSpec.containsKey("job-type")
      ? String.valueOf(rawSpec.get("job-type")).trim() : "spark-sql";
    if ("spark-sql".equals(type)) {
      return JobType.SPARK_SQL;
    }
    if ("hudi-compaction".equals(type)) {
      return JobType.HUDI_COMPACTION;
    }
    throw new SqlPipelineProjectException("Unsupported job-type: " + type);
  }

  private List<String> readCompactionTables(Path listFile) {
    if (listFile == null) {
      throw new SqlPipelineProjectException("Hudi compaction job requires --task-file <tables.list>");
    }
    if (!listFile.getFileName().toString().endsWith(".list")) {
      throw new SqlPipelineProjectException("Hudi compaction task file must end with .list: " + listFile);
    }
    Set<String> tables = new LinkedHashSet<>();
    try {
      List<String> lines = Files.readAllLines(listFile, StandardCharsets.UTF_8);
      for (int index = 0; index < lines.size(); index++) {
        String line = index == 0 ? lines.get(index).replace("\uFEFF", "").trim()
          : lines.get(index).trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        if (!SqlIdentifierUtils.isQualifiedTableName(line)) {
          throw new SqlPipelineProjectException("Invalid schema.table in " + listFile
            + " at line " + (index + 1) + ": " + line);
        }
        tables.add(line);
      }
    } catch (IOException e) {
      throw new SqlPipelineProjectException("Failed to read table list: " + listFile, e);
    }
    if (tables.isEmpty()) {
      throw new SqlPipelineProjectException("No tables in compaction task file: " + listFile);
    }
    return new ArrayList<>(tables);
  }

  private Map<String, Object> readYaml(Path specPath) {
    try (InputStream inputStream = Files.newInputStream(specPath)) {
      Object loaded = new Yaml().load(inputStream);
      if (loaded == null) {
        return Collections.emptyMap();
      }
      if (!(loaded instanceof Map)) {
        throw new SqlPipelineProjectException(
          "SQL pipeline spec must be a YAML object: " + specPath);
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> spec = (Map<String, Object>) loaded;
      return spec;
    } catch (IOException e) {
      throw new SqlPipelineProjectException(
        "Failed to read SQL pipeline spec: " + specPath, e);
    }
  }

  private Map<String, String> readConfiguration(Object rawConfiguration) {
    LinkedHashMap<String, String> configuration = new LinkedHashMap<>();
    if (rawConfiguration == null) {
      return configuration;
    }
    if (!(rawConfiguration instanceof Map)) {
      throw new SqlPipelineProjectException("The 'configuration' section must be a YAML map");
    }
    @SuppressWarnings("unchecked")
    Map<Object, Object> rawMap = (Map<Object, Object>) rawConfiguration;
    rawMap.forEach((key, value) -> configuration.put(String.valueOf(key), String.valueOf(value)));
    return configuration;
  }

  private List<Path> readSqlDirectories(Path projectRoot, Object rawTransformation) {
    Path directory = projectRoot.resolve("transformations");
    if (rawTransformation != null) {
      if (!(rawTransformation instanceof String) || ((String) rawTransformation).trim().isEmpty()) {
        throw new SqlPipelineProjectException("The 'transformation' section must be a SQL directory path");
      }
      directory = resolveAgainstRoot(projectRoot, ((String) rawTransformation).trim());
    }
    Path normalized = directory.toAbsolutePath().normalize();
    if (!Files.isDirectory(normalized)) {
      throw new SqlPipelineProjectException("SQL transformation directory does not exist: " + normalized);
    }
    return Collections.singletonList(normalized);
  }

  private Path resolveAgainstRoot(Path projectRoot, String pathText) {
    Path path = Paths.get(pathText);
    if (path.isAbsolute()) {
      return path;
    }
    return projectRoot.resolve(path);
  }

  private String stringValue(Object value, String defaultValue) {
    String normalized = nullableStringValue(value);
    return normalized != null ? normalized : defaultValue;
  }

  private String nullableStringValue(Object value) {
    if (value == null) {
      return null;
    }
    String text = String.valueOf(value).trim();
    return text.isEmpty() ? null : text;
  }

}
