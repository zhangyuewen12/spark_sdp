package com.bocom.rdss.spark.sdp3x.execution;

import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineRuntime;
import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineDatasetProperties;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Option;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ScopeAndMaterializerTest {
  @Mock
  private SparkSession sparkSession;
  @Mock
  private RuntimeConfig runtimeConfig;
  @Mock
  private Catalog catalog;
  @Mock
  private PipelineRuntime runtime;
  @SuppressWarnings("unchecked")
  @Mock
  private Dataset<Row> dataset;
  @SuppressWarnings("unchecked")
  @Mock
  private DataFrameWriter<Row> writer;

  @Test
  void sparkConfScopeShouldApplyOverridesAndRestorePreviousValues() {
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(runtimeConfig.getOption("spark.sql.adaptive.enabled")).thenReturn(Option.apply("true"));
    when(runtimeConfig.getOption("spark.sql.shuffle.partitions")).thenReturn(Option.empty());

    try (SparkConfScope ignored = new SparkConfScope(sparkSession, new LinkedHashMap<String, String>() {{
      put("spark.sql.adaptive.enabled", "false");
      put("spark.sql.shuffle.partitions", "1");
    }})) {
      verify(runtimeConfig).set("spark.sql.adaptive.enabled", "false");
      verify(runtimeConfig).set("spark.sql.shuffle.partitions", "1");
    }

    verify(runtimeConfig).set("spark.sql.adaptive.enabled", "true");
    verify(runtimeConfig).unset("spark.sql.shuffle.partitions");
  }

  @Test
  void pipelineSessionScopeShouldApplySessionStateAndRestoreIt() {
    PipelineDefinition pipeline = mock(PipelineDefinition.class);
    when(sparkSession.conf()).thenReturn(runtimeConfig);
    when(sparkSession.catalog()).thenReturn(catalog);
    when(catalog.currentDatabase()).thenReturn("default");
    when(pipeline.configuration()).thenReturn(new LinkedHashMap<String, String>() {{
      put("spark.sql.adaptive.enabled", "false");
      put("spark.sql.shuffle.partitions", "1");
    }});
    when(pipeline.catalog()).thenReturn(Optional.of("spark_catalog"));
    when(pipeline.database()).thenReturn(Optional.of("analytics"));
    when(runtimeConfig.getOption("spark.sql.adaptive.enabled")).thenReturn(Option.apply("true"));
    when(runtimeConfig.getOption("spark.sql.shuffle.partitions")).thenReturn(Option.empty());

    try (PipelineSessionScope ignored = new PipelineSessionScope(sparkSession, pipeline)) {
      verify(runtimeConfig).set("spark.sql.adaptive.enabled", "false");
      verify(runtimeConfig).set("spark.sql.shuffle.partitions", "1");
      verify(sparkSession).sql("USE CATALOG spark_catalog");
      verify(catalog).setCurrentDatabase("analytics");
    }

    verify(runtimeConfig).set("spark.sql.adaptive.enabled", "true");
    verify(runtimeConfig).unset("spark.sql.shuffle.partitions");
    verify(catalog).setCurrentDatabase("default");
  }

  @Test
  void defaultDatasetMaterializerShouldSkipTemporaryInsertIntoAndSchemalessTargets() {
    DefaultDatasetMaterializer materializer = new DefaultDatasetMaterializer();

    materializer.materialize(
      ImmutableDatasetDefinition.temporaryView("temp_orders"),
      runtime,
      ExecutionOptions.defaults());
    materializer.materialize(
      ImmutableDatasetDefinition.table("daily_orders")
        .withProperty(SqlPipelineDatasetProperties.WRITE_MODE, SqlPipelineDatasetProperties.WRITE_MODE_INSERT_INTO),
      runtime,
      ExecutionOptions.defaults());
    materializer.materialize(
      ImmutableDatasetDefinition.table("raw_orders"),
      runtime,
      ExecutionOptions.defaults());

    verifyNoInteractions(runtime);
  }

  @Test
  void defaultDatasetMaterializerShouldCreateMissingTableWhenSchemaExists() {
    StructType schema = new StructType().add("id", DataTypes.LongType, false);
    DefaultDatasetMaterializer materializer = new DefaultDatasetMaterializer();
    when(runtime.spark()).thenReturn(sparkSession);
    when(sparkSession.createDataFrame(anyList(), eq(schema))).thenReturn(dataset);
    when(dataset.write()).thenReturn(writer);
    when(writer.mode(org.apache.spark.sql.SaveMode.Ignore)).thenReturn(writer);
    when(writer.format("parquet")).thenReturn(writer);

    materializer.materialize(
      ImmutableDatasetDefinition.table("daily_orders")
        .withSchema(schema)
        .withFormat("parquet"),
      runtime,
      ExecutionOptions.defaults());

    verify(writer).format("parquet");
    verify(writer).saveAsTable("daily_orders");
  }
}
