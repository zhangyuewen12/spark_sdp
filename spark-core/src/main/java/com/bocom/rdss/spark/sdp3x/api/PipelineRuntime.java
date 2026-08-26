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

package com.bocom.rdss.spark.sdp3x.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

/**
 * Provides the runtime services that a flow query can depend on.
 *
 * <p>Only Spark base abstractions such as {@link SparkSession} and {@link Dataset} are exposed so
 * this module stays independent from helper utilities in other Spark modules.
 */
public interface PipelineRuntime {

  /** Returns the shared {@link SparkSession} used by the current pipeline run. */
  SparkSession spark();

  /** Returns the immutable pipeline definition associated with the current run. */
  PipelineDefinition pipeline();

  /** Looks up dataset metadata by dataset name. */
  Optional<DatasetDefinition> dataset(String datasetName);

  /** Reads a dataset that was already materialized during the current or a prior run. */
  Dataset<Row> read(String datasetName);

}
