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

import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.graph.DependencyGraph;
import com.bocom.rdss.spark.sdp3x.graph.PipelineValidationException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds execution stages with a stable topological layering algorithm.
 */
public final class TopologicalPipelinePlanner implements PipelinePlanner {

  /** {@inheritDoc} */
  @Override
  public ExecutionPlan plan(DependencyGraph dependencyGraph) {
    LinkedHashMap<String, Integer> indegree = new LinkedHashMap<>();
    dependencyGraph.flows().forEach(flow ->
      indegree.put(flow.name(), dependencyGraph.upstreamFlows(flow.name()).size()));

    ArrayDeque<String> ready = new ArrayDeque<>();
    indegree.forEach((flowName, degree) -> {
      if (degree == 0) {
        ready.addLast(flowName);
      }
    });

    List<ExecutionStage> stages = new ArrayList<>();
    int visited = 0;
    int stageIndex = 0;
    while (!ready.isEmpty()) {
      Set<String> currentStageNames = new LinkedHashSet<>(ready);
      ready.clear();

      List<FlowDefinition> currentStage = new ArrayList<>();
      for (String flowName : currentStageNames) {
        visited++;
        currentStage.add(dependencyGraph.flow(flowName).orElseThrow(() ->
          new PipelineValidationException("Unknown flow in execution plan: " + flowName)));

        for (String downstream : dependencyGraph.downstreamFlows(flowName)) {
          int newDegree = indegree.get(downstream) - 1;
          indegree.put(downstream, newDegree);
          if (newDegree == 0) {
            ready.addLast(downstream);
          }
        }
      }

      stages.add(new ExecutionStage(stageIndex++, currentStage));
    }

    if (visited != indegree.size()) {
      throw new PipelineValidationException("Unable to build a complete execution plan");
    }
    return new ExecutionPlan(dependencyGraph.pipeline(), dependencyGraph, stages);
  }
}
