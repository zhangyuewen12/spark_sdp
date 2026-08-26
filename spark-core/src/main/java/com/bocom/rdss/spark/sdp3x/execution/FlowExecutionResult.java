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

import java.util.Objects;

/**
 * Describes the successful completion of one flow execution.
 */
public final class FlowExecutionResult {
  private final String flowName;
  private final String targetDataset;

  /** Creates one immutable flow execution result. */
  private FlowExecutionResult(String flowName, String targetDataset) {
    this.flowName = Objects.requireNonNull(flowName, "flowName");
    this.targetDataset = Objects.requireNonNull(targetDataset, "targetDataset");
  }

  /** Creates a successful batch flow execution result. */
  public static FlowExecutionResult completed(String flowName, String targetDataset) {
    return new FlowExecutionResult(flowName, targetDataset);
  }

  /** Returns the completed flow name. */
  public String flowName() {
    return flowName;
  }

  /** Returns the dataset written by the completed flow. */
  public String targetDataset() {
    return targetDataset;
  }
}
