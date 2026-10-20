package com.bocom.rdss.spark.sdp3x.sql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Resolves SQL files from one selected file or from SQL directories. */
final class SqlFileSource {
  private final Path selectedSqlFile;
  private final List<Path> sqlDirectories;

  private SqlFileSource(Path selectedSqlFile, List<Path> sqlDirectories) {
    this.selectedSqlFile = selectedSqlFile;
    this.sqlDirectories = Collections.unmodifiableList(new ArrayList<>(sqlDirectories));
  }

  static SqlFileSource fromSqlFile(Path sqlFile) {
    return new SqlFileSource(sqlFile, Collections.emptyList());
  }

  static SqlFileSource fromSqlDirectories(List<Path> sqlDirectories) {
    return new SqlFileSource(null, sqlDirectories);
  }

  static SqlFileSource none() {
    return fromSqlDirectories(Collections.emptyList());
  }

  List<Path> sqlDirectories() {
    return sqlDirectories;
  }

  List<Path> resolveSqlFiles() {
    if (selectedSqlFile != null) {
      return Collections.singletonList(selectedSqlFile);
    }
    Set<Path> sqlFiles = new LinkedHashSet<>();
    for (Path sqlDirectory : sqlDirectories) {
      try (Stream<Path> paths = Files.walk(sqlDirectory)) {
        sqlFiles.addAll(paths.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().toLowerCase(java.util.Locale.ROOT).endsWith(".sql"))
          .sorted(Comparator.comparing(Path::toString))
          .collect(Collectors.toList()));
      } catch (IOException e) {
        throw new SqlPipelineProjectException("Failed to scan SQL transformation directory: " + sqlDirectory, e);
      }
    }
    return new ArrayList<>(sqlFiles);
  }
}
