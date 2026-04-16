/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bocom.rdss.spark.sdp3x.execution;

import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineDatasetProperties;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.FlowMode;
import com.bocom.rdss.spark.sdp3x.api.PipelineRuntime;

import java.util.Objects;

/**
 * Executes normal batch flows and one-time batch flows.
 */
public final class BatchFlowRunner implements FlowRunner {

  /** {@inheritDoc} */
  @Override
  public boolean supports(FlowDefinition flowDefinition, DatasetDefinition datasetDefinition) {
    return flowDefinition.mode() == FlowMode.BATCH || flowDefinition.mode() == FlowMode.ONCE;
  }

  /** {@inheritDoc} */
  @Override
  public FlowExecutionResult run(
      FlowDefinition flowDefinition,
      DatasetDefinition datasetDefinition,
      PipelineRuntime runtime,
      ExecutionOptions executionOptions) throws Exception {
    try (SparkConfScope ignored = new SparkConfScope(runtime.spark(), flowDefinition.sparkConf())) {
      Dataset<Row> data = Objects.requireNonNull(
        flowDefinition.query().create(runtime), "Flow query returned null");
      if (data.isStreaming()) {
        throw new PipelineExecutionException(
          "Batch-only SDP module does not support streaming datasets: " + flowDefinition.name());
      }

      // Temporary views stay inside the current Spark application; persisted datasets go to table
      // storage through standard Spark writes.
      if (datasetDefinition.kind() == DatasetKind.TEMPORARY_VIEW) {
        data.createOrReplaceTempView(datasetDefinition.name());
      } else {
        String writeMode = datasetDefinition.properties().get(SqlPipelineDatasetProperties.WRITE_MODE);
        if (SqlPipelineDatasetProperties.WRITE_MODE_INSERT_INTO.equals(writeMode)) {
          data.write().mode("append").insertInto(datasetDefinition.name());
        } else {
          DataFrameWriter<Row> writer = data.write().mode(executionOptions.materializedViewSaveMode());
          datasetDefinition.format().ifPresent(format -> writer.format(format));
          writer.saveAsTable(datasetDefinition.name());
        }
      }
      return FlowExecutionResult.completed(flowDefinition.name(), datasetDefinition.name());
    }
  }
}
