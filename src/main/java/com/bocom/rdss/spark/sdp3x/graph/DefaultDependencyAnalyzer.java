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

package com.bocom.rdss.spark.sdp3x.graph;

import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.FlowMode;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds a validated dependency graph from the explicit dataset and flow declarations.
 *
 * <p>This analyzer does not inspect Catalyst plans. Instead, it uses the declared
 * {@code inputDatasets()} edges so the module can stay small and independent.
 */
public final class DefaultDependencyAnalyzer implements DependencyAnalyzer {

  /** {@inheritDoc} */
  @Override
  public DependencyGraph analyze(PipelineDefinition pipelineDefinition) {
    LinkedHashMap<String, DatasetDefinition> datasetsByName = new LinkedHashMap<>();
    pipelineDefinition.datasets().forEach(dataset -> datasetsByName.put(dataset.name(), dataset));

    LinkedHashMap<String, FlowDefinition> flowsByName = new LinkedHashMap<>();
    pipelineDefinition.flows().forEach(flow -> flowsByName.put(flow.name(), flow));

    LinkedHashMap<String, List<String>> producersByDataset = new LinkedHashMap<>();
    flowsByName.values().forEach(flow -> {
      if (!datasetsByName.containsKey(flow.targetDataset())) {
        throw new PipelineValidationException(
          "Flow targets an undefined dataset: " + flow.targetDataset());
      }
      producersByDataset
        .computeIfAbsent(flow.targetDataset(), ignored -> new ArrayList<>())
        .add(flow.name());
    });

    validateDatasetWriters(datasetsByName, producersByDataset, flowsByName);

    LinkedHashMap<String, Set<String>> upstreamFlowsByFlow = new LinkedHashMap<>();
    LinkedHashMap<String, Set<String>> downstreamFlowsByFlow = new LinkedHashMap<>();
    flowsByName.keySet().forEach(flowName -> {
      upstreamFlowsByFlow.put(flowName, new LinkedHashSet<>());
      downstreamFlowsByFlow.put(flowName, new LinkedHashSet<>());
    });

    flowsByName.values().forEach(flow -> {
      flow.inputDatasets().forEach(inputDataset -> {
        for (String upstreamFlow :
            producersByDataset.getOrDefault(inputDataset, Collections.<String>emptyList())) {
          upstreamFlowsByFlow.get(flow.name()).add(upstreamFlow);
          downstreamFlowsByFlow.get(upstreamFlow).add(flow.name());
        }
      });
    });

    validateAcyclic(flowsByName.keySet(), upstreamFlowsByFlow, downstreamFlowsByFlow);
    return new DependencyGraph(
      pipelineDefinition,
      flowsByName,
      upstreamFlowsByFlow,
      downstreamFlowsByFlow,
      producersByDataset);
  }

  /**
   * Verifies that every dataset has valid incoming flows and compatible batch semantics.
   */
  private void validateDatasetWriters(
      Map<String, DatasetDefinition> datasetsByName,
      Map<String, List<String>> producersByDataset,
      Map<String, FlowDefinition> flowsByName) {
    datasetsByName.values().forEach(dataset -> {
      List<String> producers =
        producersByDataset.getOrDefault(dataset.name(), Collections.<String>emptyList());
      if (producers.isEmpty()) {
        throw new PipelineValidationException(
          "Dataset does not have an incoming flow: " + dataset.name());
      }

      if (dataset.kind() == DatasetKind.MATERIALIZED_VIEW && producers.size() != 1) {
        throw new PipelineValidationException(
          "Materialized view must have exactly one flow: " + dataset.name());
      }
      if (dataset.kind() == DatasetKind.TEMPORARY_VIEW && producers.size() != 1) {
        throw new PipelineValidationException(
          "Temporary view must have exactly one flow: " + dataset.name());
      }

      producers.forEach(flowName -> validateFlowModeForDataset(
        dataset,
        flowsByName.get(flowName)));
    });
  }

  /**
   * Verifies that one flow mode can legally write to the given dataset kind.
   */
  private void validateFlowModeForDataset(DatasetDefinition dataset, FlowDefinition flow) {
    if (dataset.kind() == DatasetKind.TABLE) {
      return;
    }
    if (dataset.kind() == DatasetKind.MATERIALIZED_VIEW && flow.mode() != FlowMode.BATCH) {
      throw new PipelineValidationException(
        "Materialized view only accepts normal batch flow '" + flow.name() + "'");
    }
    if (dataset.kind() == DatasetKind.TEMPORARY_VIEW && flow.mode() != FlowMode.BATCH) {
      throw new PipelineValidationException(
        "Temporary view only accepts normal batch flow '" + flow.name() + "'");
    }
  }

  /**
   * Detects circular dependencies with a standard Kahn topological traversal.
   */
  private void validateAcyclic(
      Set<String> allFlows,
      Map<String, Set<String>> upstreamFlowsByFlow,
      Map<String, Set<String>> downstreamFlowsByFlow) {
    LinkedHashMap<String, Integer> indegree = new LinkedHashMap<>();
    allFlows.forEach(flow -> indegree.put(flow, upstreamFlowsByFlow.get(flow).size()));

    ArrayDeque<String> ready = new ArrayDeque<>();
    indegree.forEach((flowName, degree) -> {
      if (degree == 0) {
        ready.add(flowName);
      }
    });

    int visited = 0;
    while (!ready.isEmpty()) {
      String flowName = ready.removeFirst();
      visited++;
      for (String downstream : downstreamFlowsByFlow.get(flowName)) {
        int newDegree = indegree.get(downstream) - 1;
        indegree.put(downstream, newDegree);
        if (newDegree == 0) {
          ready.addLast(downstream);
        }
      }
    }

    if (visited != allFlows.size()) {
      throw new PipelineValidationException("Pipeline contains a cyclic flow dependency");
    }
  }
}
