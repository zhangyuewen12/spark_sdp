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

import org.apache.spark.sql.SaveMode;

import java.util.Objects;

/**
 * Captures executor-level batch runtime options.
 */
public final class ExecutionOptions {
  private final int maxConcurrentFlows;
  private final SaveMode materializedViewSaveMode;

  /** Creates one immutable batch execution option set. */
  private ExecutionOptions(
      int maxConcurrentFlows,
      SaveMode materializedViewSaveMode) {
    this.maxConcurrentFlows = maxConcurrentFlows;
    this.materializedViewSaveMode = Objects.requireNonNull(
      materializedViewSaveMode, "materializedViewSaveMode");
  }

  /** Returns the default batch execution options. */
  public static ExecutionOptions defaults() {
    return new ExecutionOptions(1, SaveMode.Overwrite);
  }

  /** Returns a copy with a different flow concurrency limit. */
  public ExecutionOptions withMaxConcurrentFlows(int value) {
    return new ExecutionOptions(Math.max(1, value), materializedViewSaveMode);
  }

  /** Returns a copy with a different save mode for materialized views and tables. */
  public ExecutionOptions withMaterializedViewSaveMode(SaveMode saveMode) {
    return new ExecutionOptions(maxConcurrentFlows, saveMode);
  }

  /** Returns the maximum number of flows that one stage may execute concurrently. */
  public int maxConcurrentFlows() {
    return maxConcurrentFlows;
  }

  /** Returns the save mode used when persisting non-temporary datasets. */
  public SaveMode materializedViewSaveMode() {
    return materializedViewSaveMode;
  }
}
