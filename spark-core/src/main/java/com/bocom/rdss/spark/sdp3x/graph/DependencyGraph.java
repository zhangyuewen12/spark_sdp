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

import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Stores the validated dependency relationships between flows and datasets.
 */
public final class DependencyGraph {
  private final PipelineDefinition pipeline;
  private final Map<String, FlowDefinition> flowsByName;
  private final Map<String, Set<String>> upstreamFlowsByFlow;
  private final Map<String, Set<String>> downstreamFlowsByFlow;
  private final Map<String, List<String>> producersByDataset;

  /**
   * Creates an immutable dependency graph snapshot.
   */
  public DependencyGraph(
      PipelineDefinition pipeline,
      Map<String, FlowDefinition> flowsByName,
      Map<String, Set<String>> upstreamFlowsByFlow,
      Map<String, Set<String>> downstreamFlowsByFlow,
      Map<String, List<String>> producersByDataset) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.flowsByName = Collections.unmodifiableMap(new LinkedHashMap<>(flowsByName));
    LinkedHashMap<String, Set<String>> copiedUpstream = new LinkedHashMap<>();
    upstreamFlowsByFlow.forEach((key, value) ->
      copiedUpstream.put(key, Collections.unmodifiableSet(new LinkedHashSet<>(value))));
    this.upstreamFlowsByFlow = Collections.unmodifiableMap(copiedUpstream);
    LinkedHashMap<String, Set<String>> copiedDownstream = new LinkedHashMap<>();
    downstreamFlowsByFlow.forEach((key, value) ->
      copiedDownstream.put(key, Collections.unmodifiableSet(new LinkedHashSet<>(value))));
    this.downstreamFlowsByFlow = Collections.unmodifiableMap(copiedDownstream);
    LinkedHashMap<String, List<String>> copiedProducers = new LinkedHashMap<>();
    producersByDataset.forEach((key, value) ->
      copiedProducers.put(key, Collections.unmodifiableList(new ArrayList<>(value))));
    this.producersByDataset = Collections.unmodifiableMap(copiedProducers);
  }

  /** Returns the pipeline definition from which this graph was derived. */
  public PipelineDefinition pipeline() {
    return pipeline;
  }

  /** Returns all flows in stable declaration order. */
  public List<FlowDefinition> flows() {
    return new ArrayList<>(flowsByName.values());
  }

  /** Looks up one flow by name. */
  public Optional<FlowDefinition> flow(String flowName) {
    return Optional.ofNullable(flowsByName.get(flowName));
  }

  /** Returns the upstream flow names that must complete before the target flow can run. */
  public Set<String> upstreamFlows(String flowName) {
    return upstreamFlowsByFlow.getOrDefault(flowName, Collections.<String>emptySet());
  }

  /** Returns the downstream flow names that depend on the target flow. */
  public Set<String> downstreamFlows(String flowName) {
    return downstreamFlowsByFlow.getOrDefault(flowName, Collections.<String>emptySet());
  }

  /** Returns the flow names that produce the given dataset. */
  public List<String> producers(String datasetName) {
    return producersByDataset.getOrDefault(datasetName, Collections.<String>emptyList());
  }
}
