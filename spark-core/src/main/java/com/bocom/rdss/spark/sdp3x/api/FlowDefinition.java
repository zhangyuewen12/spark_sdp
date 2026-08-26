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

import java.util.Map;
import java.util.Set;

/**
 * Captures the immutable declaration of a pipeline flow.
 *
 * <p>A flow reads zero or more upstream datasets, applies a batch query, and writes the result to
 * one target dataset.
 */
public interface FlowDefinition {

  /** Returns the unique flow name used in plans, reports, and diagnostics. */
  String name();

  /** Returns the dataset name that receives the flow output. */
  String targetDataset();

  /** Returns the batch execution mode used by the flow. */
  FlowMode mode();

  /** Returns the explicit upstream dataset names that this flow depends on. */
  Set<String> inputDatasets();

  /** Returns the per-flow Spark SQL configuration overrides. */
  Map<String, String> sparkConf();

  /** Returns the query callback that creates the batch output for the flow. */
  FlowQuery query();

  /** Returns whether the flow should be treated as a normal recurring batch refresh. */
  default boolean isBatch() {
    return mode() == FlowMode.BATCH;
  }
}
