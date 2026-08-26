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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import com.bocom.rdss.spark.sdp3x.api.PipelineRuntime;

import java.util.ArrayList;

/**
 * Creates empty batch datasets up front when a declared schema is available.
 */
public final class DefaultDatasetMaterializer implements DatasetMaterializer {

  /** {@inheritDoc} */
  @Override
  public void materialize(
      DatasetDefinition datasetDefinition,
      PipelineRuntime runtime,
      ExecutionOptions executionOptions) {
    if (datasetDefinition.kind() == DatasetKind.TEMPORARY_VIEW
        || SqlPipelineDatasetProperties.WRITE_MODE_INSERT_INTO.equals(
          datasetDefinition.properties().get(SqlPipelineDatasetProperties.WRITE_MODE))
        || !datasetDefinition.schema().isPresent()) {
      return;
    }

    DataFrameWriter<Row> writer = runtime.spark()
      .createDataFrame(new ArrayList<Row>(), datasetDefinition.schema().get())
      .write()
      .mode(SaveMode.Ignore);
    datasetDefinition.format().ifPresent(format -> writer.format(format));
    writer.saveAsTable(datasetDefinition.name());
  }
}
