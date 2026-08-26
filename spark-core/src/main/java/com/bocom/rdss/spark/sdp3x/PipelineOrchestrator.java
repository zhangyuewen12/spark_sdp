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

package com.bocom.rdss.spark.sdp3x;

import org.apache.spark.sql.SparkSession;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import com.bocom.rdss.spark.sdp3x.execution.LocalPipelineExecutor;
import com.bocom.rdss.spark.sdp3x.execution.PipelineExecutor;
import com.bocom.rdss.spark.sdp3x.graph.DefaultDependencyAnalyzer;
import com.bocom.rdss.spark.sdp3x.graph.DependencyAnalyzer;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.PipelinePlanner;
import com.bocom.rdss.spark.sdp3x.planning.TopologicalPipelinePlanner;

/**
 * Coordinates dependency analysis, planning, and execution for one pipeline.
 */
public final class PipelineOrchestrator {
  private final DependencyAnalyzer dependencyAnalyzer;
  private final PipelinePlanner pipelinePlanner;
  private final PipelineExecutor pipelineExecutor;

  /** Creates an orchestrator with the default batch-only collaborators. */
  public PipelineOrchestrator() {
    this(new DefaultDependencyAnalyzer(), new TopologicalPipelinePlanner(),
      new LocalPipelineExecutor());
  }

  /** Creates an orchestrator with explicitly supplied collaborators. */
  public PipelineOrchestrator(
      DependencyAnalyzer dependencyAnalyzer,
      PipelinePlanner pipelinePlanner,
      PipelineExecutor pipelineExecutor) {
    this.dependencyAnalyzer = dependencyAnalyzer;
    this.pipelinePlanner = pipelinePlanner;
    this.pipelineExecutor = pipelineExecutor;
  }

  /** Analyzes and plans the supplied pipeline without executing it. */
  public ExecutionPlan plan(PipelineDefinition pipelineDefinition) {
    DependencyGraph dependencyGraph = dependencyAnalyzer.analyze(pipelineDefinition);
    return pipelinePlanner.plan(dependencyGraph);
  }

  /** Analyzes, plans, and executes the supplied pipeline in one call. */
  public ExecutionReport run(
      PipelineDefinition pipelineDefinition,
      SparkSession sparkSession,
      ExecutionOptions executionOptions) {
    return pipelineExecutor.execute(plan(pipelineDefinition), sparkSession, executionOptions);
  }
}
