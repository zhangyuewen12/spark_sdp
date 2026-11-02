package com.bocom.rdss.spark.sdp3x.sql;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlPipelineRunOptionsTest {
  @Test
  void parsesAllSupportedOptions() {
    SqlPipelineRunOptions options = SqlPipelineRunOptions.parse(new String[] {
      "--task-file", "compaction.sql", "--master", "local[2]", "--submitted", "--spec", "job.yaml"
    });
    assertEquals(Paths.get("job.yaml"), options.projectPath());
    assertEquals(Paths.get("compaction.sql"), options.taskFilePath());
    assertEquals("local[2]", options.master());
    assertTrue(options.submittedViaSparkSubmit());
    assertFalse(SqlPipelineRunOptions.parse(new String[] {"--spec", "job"}).submittedViaSparkSubmit());
  }

  @Test
  void rejectsMissingValuesDuplicatesAndUnknownArguments() {
    String[][] invalid = {
      {}, {"--spec"}, {"--task-file"}, {"--master"}, {"--unknown"},
      {"--spec", "a", "--spec", "b"},
      {"--spec", "a", "--task-file", "a", "--task-file", "b"}
    };
    for (String[] args : invalid) {
      assertThrows(IllegalArgumentException.class, () -> SqlPipelineRunOptions.parse(args));
    }
  }
}
