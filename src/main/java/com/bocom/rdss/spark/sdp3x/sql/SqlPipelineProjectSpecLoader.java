package com.bocom.rdss.spark.sdp3x.sql;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads a minimal SQL pipeline project spec from {@code spark-pipeline.yml}.
 */
public final class SqlPipelineProjectSpecLoader {
  private static final String[] SPEC_FILES = {
    "spark-pipeline.yml",
    "spark-pipeline.yaml"
  };

  public SqlPipelineProjectSpec load(Path projectRootOrSpecPath) {
    Path specPath = resolveSpecPath(projectRootOrSpecPath);
    Path normalizedRoot = specPath.getParent().toAbsolutePath().normalize();

    Map<String, Object> rawSpec = readYaml(specPath);
    String name = stringValue(rawSpec.get("name"), normalizedRoot.getFileName().toString());
    String catalog = nullableStringValue(rawSpec.get("catalog"));
    String database = nullableStringValue(rawSpec.get("database"));
    Map<String, String> configuration = readConfiguration(rawSpec.get("configuration"));
    List<Path> sqlDirectories = readSqlDirectories(normalizedRoot, rawSpec.get("libraries"));
    return new SqlPipelineProjectSpec(
      normalizedRoot,
      name,
      catalog,
      database,
      configuration,
      sqlDirectories);
  }

  private Path resolveSpecPath(Path projectRootOrSpecPath) {
    Path normalizedPath = projectRootOrSpecPath.toAbsolutePath().normalize();
    if (Files.isRegularFile(normalizedPath)) {
      return normalizedPath;
    }

    if (!Files.isDirectory(normalizedPath)) {
      throw new SqlPipelineProjectException("SQL pipeline path does not exist: " + normalizedPath);
    }

    return findSpecInAncestors(normalizedPath);
  }

  private Path findSpecInAncestors(Path currentDirectory) {
    Path directory = currentDirectory;
    while (directory != null) {
      List<Path> foundFiles = new ArrayList<>();
      for (String specFile : SPEC_FILES) {
        Path candidate = directory.resolve(specFile);
        if (Files.isRegularFile(candidate)) {
          foundFiles.add(candidate);
        }
      }

      if (foundFiles.size() == 1) {
        return foundFiles.get(0);
      }
      if (foundFiles.size() > 1) {
        throw new SqlPipelineProjectException(
          "Multiple SQL pipeline spec files found under: " + directory
            + ". Expected only one of " + String.join(", ", SPEC_FILES));
      }

      directory = directory.getParent();
    }

    throw new SqlPipelineProjectException(
      "Missing SQL pipeline spec file under: " + currentDirectory
        + " or its ancestors. Expected one of " + String.join(", ", SPEC_FILES));
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

  private List<Path> readSqlDirectories(Path projectRoot, Object rawLibraries) {
    List<Path> sqlDirectories = new ArrayList<>();
    if (rawLibraries == null) {
      sqlDirectories.add(projectRoot.resolve("transformations"));
    } else {
      if (!(rawLibraries instanceof List)) {
        throw new SqlPipelineProjectException("The 'libraries' section must be a YAML list");
      }
      @SuppressWarnings("unchecked")
      List<Object> libraries = (List<Object>) rawLibraries;
      for (Object library : libraries) {
        sqlDirectories.add(resolveLibraryPath(projectRoot, library));
      }
    }

    if (sqlDirectories.isEmpty()) {
      throw new SqlPipelineProjectException("At least one SQL library directory is required");
    }

    List<Path> normalizedDirectories = new ArrayList<>();
    for (Path sqlDirectory : sqlDirectories) {
      Path normalized = sqlDirectory.toAbsolutePath().normalize();
      if (!Files.exists(normalized) || !Files.isDirectory(normalized)) {
        throw new SqlPipelineProjectException(
          "SQL library directory does not exist: " + normalized);
      }
      normalizedDirectories.add(normalized);
    }
    return normalizedDirectories;
  }

  private Path resolveLibraryPath(Path projectRoot, Object rawLibrary) {
    if (rawLibrary instanceof String) {
      return resolveAgainstRoot(projectRoot, (String) rawLibrary);
    }
    if (rawLibrary instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> rawMap = (Map<Object, Object>) rawLibrary;
      Object path = rawMap.get("path");
      if (path == null) {
        path = rawMap.get("file");
      }
      if (path == null) {
        throw new SqlPipelineProjectException(
          "Each SQL library entry must contain a 'path' or 'file' field");
      }
      return resolveAgainstRoot(projectRoot, String.valueOf(path));
    }
    throw new SqlPipelineProjectException("Unsupported SQL library entry: " + rawLibrary);
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
