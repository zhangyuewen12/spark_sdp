package com.bocom.rdss.spark.sdp3x.execution;

import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class ExecutionModelTest {

  @Test
  void executionOptionsShouldApplyDefaultsAndClampConcurrency() {
    ExecutionOptions defaults = ExecutionOptions.defaults();
    ExecutionOptions custom = defaults
      .withMaxConcurrentFlows(0)
      .withMaterializedViewSaveMode(SaveMode.Append);

    assertEquals(1, defaults.maxConcurrentFlows());
    assertEquals(SaveMode.Overwrite, defaults.materializedViewSaveMode());
    assertEquals(1, custom.maxConcurrentFlows());
    assertEquals(SaveMode.Append, custom.materializedViewSaveMode());
    assertThrows(NullPointerException.class, () -> defaults.withMaterializedViewSaveMode(null));
  }

  @Test
  void executionReportShouldDefensivelyCopyResults() {
    ArrayList<FlowExecutionResult> source = new ArrayList<>();
    source.add(FlowExecutionResult.completed("flow_a", "dataset_a"));

    ExecutionReport report = new ExecutionReport(source);
    source.clear();

    assertEquals(1, report.results().size());
    assertThrows(UnsupportedOperationException.class, () -> report.results().add(
      FlowExecutionResult.completed("flow_b", "dataset_b")));
  }

  @Test
  void flowExecutionResultShouldRequireNonNullFields() {
    FlowExecutionResult result = FlowExecutionResult.completed("flow_a", "dataset_a");

    assertEquals("flow_a", result.flowName());
    assertEquals("dataset_a", result.targetDataset());
    assertThrows(NullPointerException.class, () -> FlowExecutionResult.completed(null, "dataset_a"));
    assertThrows(NullPointerException.class, () -> FlowExecutionResult.completed("flow_a", null));
  }

  @Test
  void pipelineExecutionExceptionShouldPreserveMessagesAndCauses() {
    RuntimeException cause = new RuntimeException("boom");

    PipelineExecutionException withMessageOnly = new PipelineExecutionException("failed");
    PipelineExecutionException withCause = new PipelineExecutionException("failed", cause);

    assertEquals("failed", withMessageOnly.getMessage());
    assertNull(withMessageOnly.getCause());
    assertEquals("failed", withCause.getMessage());
    assertSame(cause, withCause.getCause());
  }
}
