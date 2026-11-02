package com.bocom.rdss.spark.sdp3x.starter;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StarterOptionsTest {
  @Test
  void parsesAllOptionsAndBothHelpForms() {
    assertTrue(StarterOptions.parse(new String[] {"--help"}).help);
    assertTrue(StarterOptions.parse(new String[] {"-h"}).help);
    StarterOptions options = StarterOptions.parse(new String[] {
      "--spec", "job", "--task-file", "table.list", "--conf", "spark.properties",
      "--files", " a,b ", "--jars", " x.jar "
    });
    assertEquals(Paths.get("job"), options.specPath);
    assertEquals(Paths.get("table.list"), options.taskFilePath);
    assertEquals(Paths.get("spark.properties"), options.propsPath);
    assertEquals("a,b", options.files);
    assertEquals("x.jar", options.jars);
  }

  @Test
  void rejectsEveryDuplicateAndMissingOption() {
    for (String flag : new String[] {"--spec", "--task-file", "--conf", "--files", "--jars"}) {
      assertThrows(IllegalArgumentException.class,
        () -> StarterOptions.parse(new String[] {"--spec", "job", flag, "one", flag, "two"}));
    }
    assertThrows(IllegalArgumentException.class,
      () -> StarterOptions.parse(new String[] {"--spec", "job", "--task-file"}));
    assertThrows(IllegalArgumentException.class,
      () -> StarterOptions.parse(new String[] {"--task-file", "table.list"}));
    assertThrows(IllegalArgumentException.class,
      () -> StarterOptions.parse(new String[] {"--spec", "job", "--files", " "}));
    assertThrows(IllegalArgumentException.class,
      () -> StarterOptions.parse(new String[] {"--spec", "job", "--jars", " "}));
  }
}
