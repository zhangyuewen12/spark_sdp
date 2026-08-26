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

package com.bocom.rdss.spark.sdp3x.planning;

import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holds the execution-ready representation of a pipeline after planning completes.
 */
public final class ExecutionPlan {
  private final PipelineDefinition pipeline;
  private final DependencyGraph dependencyGraph;
  private final List<ExecutionStage> stages;

  /** Creates a planned pipeline snapshot. */
  public ExecutionPlan(
      PipelineDefinition pipeline,
      DependencyGraph dependencyGraph,
      List<ExecutionStage> stages) {
    this.pipeline = pipeline;
    this.dependencyGraph = dependencyGraph;
    this.stages = Collections.unmodifiableList(new ArrayList<>(stages));
  }

  /** Returns the pipeline definition that produced this plan. */
  public PipelineDefinition pipeline() {
    return pipeline;
  }

  /** Returns the validated dependency graph used by the planner. */
  public DependencyGraph dependencyGraph() {
    return dependencyGraph;
  }

  /** Returns the staged execution order chosen by the planner. */
  public List<ExecutionStage> stages() {
    return stages;
  }
}
