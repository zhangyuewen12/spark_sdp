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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads a SQL pipeline project spec from {@code spark-pipeline.yaml}.
 */
public final class SqlPipelineProjectSpecLoader {
  private static final Pattern COMPACTION_STATEMENT = Pattern.compile(
    "(?i)RUN\\s+COMPACTION\\s+ON\\s+(.+)");
  private static final Pattern SQL_COMMENTS = Pattern.compile(
    "(?s)/\\*.*?\\*/|(?m)--[^\\r\\n]*|(?m)^\\s*#.*$");

  public SqlPipelineProjectSpec load(Path specFile) {
    return load(specFile, null);
  }

  public SqlPipelineProjectSpec load(Path specFile, Path taskFile) {
    return load(specFile, null, taskFile);
  }

  public SqlPipelineProjectSpec load(Path specFile, Path jobPath, Path taskFile) {
    if (jobPath != null && taskFile != null) {
      throw new SqlPipelineProjectException("--jobPath and --taskFile cannot be used together");
    }
    Path specPath = resolveSpecPath(specFile);
    Path projectRoot = jobPath == null
      ? specPath.getParent()
      : validateJobPath(jobPath);

    Map<String, Object> rawSpec = readYaml(specPath);
    String name = stringValue(rawSpec.get("name"), projectRoot.getFileName().toString());
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
          readSqlDirectories(projectRoot, rawSpec.get("transformation")));
      }
    }
    List<String> compactionTables = jobType == JobType.HUDI_COMPACTION
      ? readCompactionTables(normalizedTaskFile)
      : Collections.emptyList();
    return new SqlPipelineProjectSpec(
      projectRoot,
      name,
      nullableStringValue(rawSpec.get("catalog")),
      nullableStringValue(rawSpec.get("database")),
      readConfiguration(rawSpec.get("configuration")),
      sqlFileSource,
      jobType,
      compactionTables);
  }

  private Path resolveSpecPath(Path requestedPath) {
    Path path = requestedPath.toAbsolutePath().normalize();
    if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
      throw new SqlPipelineProjectException("SQL pipeline spec must be a readable YAML file: " + path);
    }
    String fileName = path.getFileName().toString();
    if (!fileName.endsWith(".yaml") && !fileName.endsWith(".yml")) {
      throw new SqlPipelineProjectException("SQL pipeline spec must be a YAML file: " + path);
    }
    return path;
  }

  private Path validateTaskFile(Path taskFile) {
    Path normalized = taskFile.toAbsolutePath().normalize();
    if (!Files.isRegularFile(normalized) || !Files.isReadable(normalized)) {
      throw new SqlPipelineProjectException("--taskFile must be a readable file: " + normalized);
    }
    return normalized;
  }

  private Path validateJobPath(Path jobPath) {
    Path normalized = jobPath.toAbsolutePath().normalize();
    if (!Files.isDirectory(normalized) || !Files.isReadable(normalized)) {
      throw new SqlPipelineProjectException("--jobPath must be a readable directory: " + normalized);
    }
    return normalized;
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

  private List<String> readCompactionTables(Path sqlFile) {
    if (sqlFile == null) {
      throw new SqlPipelineProjectException("Hudi compaction job requires --taskFile <compaction.sql>");
    }
    if (!sqlFile.getFileName().toString().toLowerCase(java.util.Locale.ROOT).endsWith(".sql")) {
      throw new SqlPipelineProjectException("Hudi compaction task file must end with .sql: " + sqlFile);
    }
    Set<String> tables = new LinkedHashSet<>();
    try {
      String source = new String(Files.readAllBytes(sqlFile), StandardCharsets.UTF_8);
      if (source.startsWith("\uFEFF")) {
        source = source.substring(1);
      }
      String sql = SQL_COMMENTS.matcher(source).replaceAll(" ");
      if (sql.trim().isEmpty()) {
        throw new SqlPipelineProjectException("No compaction statements in task file: " + sqlFile);
      }
      if (!sql.trim().endsWith(";")) {
        throw new SqlPipelineProjectException(
          "Compaction SQL statement must end with ';': " + sqlFile);
      }
      String[] statements = sql.split(";", -1);
      for (int index = 0; index < statements.length; index++) {
        String statement = statements[index].trim();
        if (statement.isEmpty()) {
          continue;
        }
        Matcher matcher = COMPACTION_STATEMENT.matcher(statement);
        if (!matcher.matches() || !SqlIdentifierUtils.isQualifiedTableName(matcher.group(1).trim())) {
          throw new SqlPipelineProjectException("Invalid RUN COMPACTION ON schema.table statement in "
            + sqlFile + " at statement " + (index + 1) + ": " + statement);
        }
        tables.add(matcher.group(1).trim());
      }
    } catch (IOException e) {
      throw new SqlPipelineProjectException("Failed to read compaction SQL file: " + sqlFile, e);
    }
    if (tables.isEmpty()) {
      throw new SqlPipelineProjectException("No compaction statements in task file: " + sqlFile);
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
