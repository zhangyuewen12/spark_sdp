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

/**
 * Enumerates the dataset kinds supported by the batch-oriented Java SDP sketch.
 *
 * <p>The current module intentionally avoids implementing streaming behavior so the architecture
 * stays focused on a Spark 3.x batch pipeline baseline.
 */
public enum DatasetKind {
  /** A persisted table refreshed by a batch flow. */
  TABLE,

  /** A persisted derived dataset refreshed by a single batch flow. */
  MATERIALIZED_VIEW,

  /** A temporary view that only exists inside the current Spark application. */
  TEMPORARY_VIEW
}
