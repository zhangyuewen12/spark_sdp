package com.bocom.rdss.spark.sdp3x.util;

import java.util.regex.Pattern;

/** Utilities for validated SQL table identifiers. */
public final class SqlIdentifierUtils {
  private static final Pattern QUALIFIED_TABLE_NAME = Pattern.compile(
    "[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*");

  private SqlIdentifierUtils() {
  }

  public static boolean isQualifiedTableName(String table) {
    return table != null && QUALIFIED_TABLE_NAME.matcher(table).matches();
  }

  public static String quoteQualifiedTable(String table) {
    if (!isQualifiedTableName(table)) {
      throw new IllegalArgumentException("Invalid table name; expected schema.table: " + table);
    }
    int separator = table.indexOf('.');
    return "`" + table.substring(0, separator) + "`.`" + table.substring(separator + 1) + "`";
  }
}
