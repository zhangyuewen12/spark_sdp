package com.bocom.rdss.spark.sdp3x.runtime;

import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class DefaultPipelineRuntimeTest {

  @Test
  void shouldDelegateDatasetLookupAndReads() {
    SparkSession sparkSession = mock(SparkSession.class);
    PipelineDefinition pipelineDefinition = mock(PipelineDefinition.class);
    DatasetDefinition datasetDefinition = mock(DatasetDefinition.class);
    @SuppressWarnings("unchecked")
    Dataset<Row> dataset = mock(Dataset.class);

    when(pipelineDefinition.dataset("orders")).thenReturn(Optional.of(datasetDefinition));
    when(sparkSession.table("orders")).thenReturn(dataset);

    DefaultPipelineRuntime runtime = new DefaultPipelineRuntime(sparkSession, pipelineDefinition);

    assertSame(sparkSession, runtime.spark());
    assertSame(pipelineDefinition, runtime.pipeline());
    assertEquals(Optional.of(datasetDefinition), runtime.dataset("orders"));
    assertSame(dataset, runtime.read("orders"));
  }
}
