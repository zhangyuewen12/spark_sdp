package com.bocom.rdss.spark.sdp3x.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlIdentifierUtilsTest {
  @Test
  void acceptsOnlyQualifiedIdentifiersAndQuotesBothParts() {
    assertTrue(SqlIdentifierUtils.isQualifiedTableName("schema_1.table_2"));
    assertEquals("`schema_1`.`table_2`",
      SqlIdentifierUtils.quoteQualifiedTable("schema_1.table_2"));
    for (String invalid : new String[] {null, "table", "a.b.c", "1a.b", "a.b-c", "a.`b`"}) {
      assertFalse(SqlIdentifierUtils.isQualifiedTableName(invalid));
      assertThrows(IllegalArgumentException.class,
        () -> SqlIdentifierUtils.quoteQualifiedTable(invalid));
    }
  }
}
