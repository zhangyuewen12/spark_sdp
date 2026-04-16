package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.api.DatasetKind;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a minimal subset of SDP-style SQL declarations for batch views.
 */
public final class SqlPipelineParser {
  private static final Pattern CREATE_VIEW_PATTERN = Pattern.compile(
    "(?is)^CREATE\\s+(?:OR\\s+REPLACE\\s+)?"
      + "(MATERIALIZED\\s+VIEW|TEMP(?:ORARY)?\\s+VIEW)\\s+"
      + "((?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)(?:\\s*\\.\\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*))*)"
      + "\\s+AS\\s+(.*)$");
  private static final Pattern INSERT_INTO_PATTERN = Pattern.compile(
    "(?is)^INSERT\\s+INTO\\s+(?:TABLE\\s+)?"
      + "((?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)(?:\\s*\\.\\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*))*)"
      + "\\s+(SELECT\\b.*)$");
  private static final Pattern INPUT_DATASET_PATTERN = Pattern.compile(
    "(?is)\\b(?:FROM|JOIN)\\s+"
      + "((?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)(?:\\s*\\.\\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*))*)");

  public List<SqlPipelineDefinition> parse(Path sqlFile, int firstStatementIndex) {
    List<String> statements = splitStatements(readSql(sqlFile));
    List<SqlPipelineDefinition> definitions = new ArrayList<>();
    int statementIndex = firstStatementIndex;
    for (String statement : statements) {
      String trimmed = statement.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      definitions.add(parseStatement(trimmed, sqlFile, statementIndex++));
    }
    return definitions;
  }

  private SqlPipelineDefinition parseStatement(
      String statement,
      Path sqlFile,
      int statementIndex) {
    Matcher matcher = CREATE_VIEW_PATTERN.matcher(statement);
    if (matcher.matches()) {
      String viewType = matcher.group(1).trim().toUpperCase();
      String datasetName = normalizeIdentifier(matcher.group(2));
      String querySql = matcher.group(3).trim();
      DatasetKind datasetKind = viewType.startsWith("MATERIALIZED")
        ? DatasetKind.MATERIALIZED_VIEW
        : DatasetKind.TEMPORARY_VIEW;

      return new SqlPipelineDefinition(
        datasetName,
        datasetKind,
        querySql,
        extractInputDatasets(querySql),
        sqlFile,
        statementIndex,
        SqlPipelineDefinition.WriteMode.SAVE_AS_TABLE);
    }

    matcher = INSERT_INTO_PATTERN.matcher(statement);
    if (matcher.matches()) {
      String datasetName = normalizeIdentifier(matcher.group(1));
      String querySql = matcher.group(2).trim();
      return new SqlPipelineDefinition(
        datasetName,
        DatasetKind.TABLE,
        querySql,
        extractInputDatasets(querySql),
        sqlFile,
        statementIndex,
        SqlPipelineDefinition.WriteMode.INSERT_INTO);
    }

    throw new SqlPipelineProjectException(
      "Unsupported SQL statement in " + sqlFile + ": " + statement);
  }

  private Set<String> extractInputDatasets(String querySql) {
    String masked = maskStringLiterals(querySql);
    Matcher matcher = INPUT_DATASET_PATTERN.matcher(masked);
    LinkedHashSet<String> inputDatasets = new LinkedHashSet<>();
    while (matcher.find()) {
      inputDatasets.add(normalizeIdentifier(matcher.group(1)));
    }
    return inputDatasets;
  }

  private String readSql(Path sqlFile) {
    try {
      byte[] bytes = Files.readAllBytes(sqlFile);
      return new String(bytes, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new SqlPipelineProjectException("Failed to read SQL file: " + sqlFile, e);
    }
  }

  private List<String> splitStatements(String sqlText) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean inBacktick = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;

    for (int index = 0; index < sqlText.length(); index++) {
      char currentChar = sqlText.charAt(index);
      char nextChar = index + 1 < sqlText.length() ? sqlText.charAt(index + 1) : '\0';

      if (inLineComment) {
        if (currentChar == '\n') {
          inLineComment = false;
          current.append('\n');
        }
        continue;
      }
      if (inBlockComment) {
        if (currentChar == '*' && nextChar == '/') {
          inBlockComment = false;
          index++;
        }
        continue;
      }

      if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
        if (currentChar == '-' && nextChar == '-') {
          inLineComment = true;
          index++;
          continue;
        }
        if (currentChar == '/' && nextChar == '*') {
          inBlockComment = true;
          index++;
          continue;
        }
      }

      if (currentChar == '\'' && !inDoubleQuote && !inBacktick) {
        if (inSingleQuote && nextChar == '\'') {
          current.append(currentChar).append(nextChar);
          index++;
          continue;
        }
        inSingleQuote = !inSingleQuote;
      } else if (currentChar == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote;
      } else if (currentChar == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick;
      }

      if (currentChar == ';' && !inSingleQuote && !inDoubleQuote && !inBacktick) {
        if (current.length() > 0) {
          statements.add(current.toString());
          current.setLength(0);
        }
      } else {
        current.append(currentChar);
      }
    }

    if (current.length() > 0) {
      statements.add(current.toString());
    }
    return statements;
  }

  private String maskStringLiterals(String sqlText) {
    StringBuilder masked = new StringBuilder(sqlText.length());
    boolean inSingleQuote = false;
    for (int index = 0; index < sqlText.length(); index++) {
      char currentChar = sqlText.charAt(index);
      char nextChar = index + 1 < sqlText.length() ? sqlText.charAt(index + 1) : '\0';
      if (currentChar == '\'') {
        if (inSingleQuote && nextChar == '\'') {
          masked.append(' ').append(' ');
          index++;
          continue;
        }
        inSingleQuote = !inSingleQuote;
        masked.append(' ');
      } else if (inSingleQuote) {
        masked.append(' ');
      } else {
        masked.append(currentChar);
      }
    }
    return masked.toString();
  }

  private String normalizeIdentifier(String identifier) {
    String trimmed = identifier.trim();
    String[] parts = trimmed.split("\\.");
    List<String> normalizedParts = new ArrayList<>();
    for (String part : parts) {
      String normalized = part.trim();
      if (normalized.startsWith("`") && normalized.endsWith("`") && normalized.length() >= 2) {
        normalized = normalized.substring(1, normalized.length() - 1);
      }
      normalizedParts.add(normalized);
    }
    return String.join(".", normalizedParts);
  }
}
