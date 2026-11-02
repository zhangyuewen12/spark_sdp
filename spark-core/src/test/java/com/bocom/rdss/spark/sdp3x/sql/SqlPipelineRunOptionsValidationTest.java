package com.bocom.rdss.spark.sdp3x.sql;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/** Prevents ambiguous directory/single-file selection in submitted driver commands. */
class SqlPipelineRunOptionsValidationTest {
  @Test void parsesDirectoryModeForwardedBySubmitStarter() {
    SqlPipelineRunOptions options = SqlPipelineRunOptions.parse(new String[] {
      "--submitted", "--spec", "spark-sdp-job/job.yaml", "--jobPath", "spark-sdp-job"
    });
    assertEquals(Paths.get("spark-sdp-job"), options.jobPath());
    assertNull(options.taskFilePath());
    assertTrue(options.submittedViaSparkSubmit());
  }

  @Test void rejectsMissingOrRepeatedDirectorySelection() {
    assertThrows(IllegalArgumentException.class,
      () -> SqlPipelineRunOptions.parse(new String[] {"--spec", "job.yaml", "--jobPath"}));
    assertThrows(IllegalArgumentException.class,
      () -> SqlPipelineRunOptions.parse(new String[] {"--spec", "job.yaml", "--jobPath", "a", "--jobPath", "b"}));
  }

  @Test void rejectsDirectoryCombinedWithEitherSingleFileSpelling() {
    for (String option : new String[] {"--taskFile", "--task-file"}) {
      IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> SqlPipelineRunOptions.parse(new String[] {"--spec", "job.yaml", "--jobPath", "jobs", option, "task.sql"}));
      assertTrue(error.getMessage().contains("cannot be used together"));
    }
  }
}
