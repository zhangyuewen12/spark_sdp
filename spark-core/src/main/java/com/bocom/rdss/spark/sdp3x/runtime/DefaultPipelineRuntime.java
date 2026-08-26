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

package com.bocom.rdss.spark.sdp3x.runtime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineRuntime;

import java.util.Optional;

/**
 * Default runtime implementation shared by queries, materializers, and executors.
 */
public final class DefaultPipelineRuntime implements PipelineRuntime {
  private final SparkSession spark;
  private final PipelineDefinition pipeline;

  /** Creates a runtime facade for one Spark session and one pipeline definition. */
  public DefaultPipelineRuntime(SparkSession spark, PipelineDefinition pipeline) {
    this.spark = spark;
    this.pipeline = pipeline;
  }

  /** {@inheritDoc} */
  @Override
  public SparkSession spark() {
    return spark;
  }

  /** {@inheritDoc} */
  @Override
  public PipelineDefinition pipeline() {
    return pipeline;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<DatasetDefinition> dataset(String datasetName) {
    return pipeline.dataset(datasetName);
  }

  /** {@inheritDoc} */
  @Override
  public Dataset<Row> read(String datasetName) {
    return spark.table(datasetName);
  }
}
