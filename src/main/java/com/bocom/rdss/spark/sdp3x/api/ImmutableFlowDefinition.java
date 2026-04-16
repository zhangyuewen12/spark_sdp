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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Implements {@link com.bocom.rdss.spark.sdp3x.api.FlowDefinition} as an immutable value object for Java-first pipeline code.
 */
public final class ImmutableFlowDefinition implements com.bocom.rdss.spark.sdp3x.api.FlowDefinition {
  private final String name;
  private final String targetDataset;
  private final com.bocom.rdss.spark.sdp3x.api.FlowMode mode;
  private final Set<String> inputDatasets;
  private final Map<String, String> sparkConf;
  private final com.bocom.rdss.spark.sdp3x.api.FlowQuery query;

  /**
   * Creates one immutable flow definition instance.
   */
  private ImmutableFlowDefinition(
      String name,
      String targetDataset,
      com.bocom.rdss.spark.sdp3x.api.FlowMode mode,
      Set<String> inputDatasets,
      Map<String, String> sparkConf,
      com.bocom.rdss.spark.sdp3x.api.FlowQuery query) {
    this.name = Objects.requireNonNull(name, "name");
    this.targetDataset = Objects.requireNonNull(targetDataset, "targetDataset");
    this.mode = Objects.requireNonNull(mode, "mode");
    this.inputDatasets = Collections.unmodifiableSet(new LinkedHashSet<>(inputDatasets));
    this.sparkConf = Collections.unmodifiableMap(new LinkedHashMap<>(sparkConf));
    this.query = Objects.requireNonNull(query, "query");
  }

  /** Creates a normal batch flow definition. */
  public static ImmutableFlowDefinition batchFlow(
      String name,
      String targetDataset,
      Set<String> inputDatasets,
      com.bocom.rdss.spark.sdp3x.api.FlowQuery query) {
    return new ImmutableFlowDefinition(name, targetDataset, com.bocom.rdss.spark.sdp3x.api.FlowMode.BATCH, inputDatasets,
      Collections.<String, String>emptyMap(), query);
  }

  /** Creates a one-time batch flow definition. */
  public static ImmutableFlowDefinition onceFlow(
      String name,
      String targetDataset,
      Set<String> inputDatasets,
      com.bocom.rdss.spark.sdp3x.api.FlowQuery query) {
    return new ImmutableFlowDefinition(name, targetDataset, com.bocom.rdss.spark.sdp3x.api.FlowMode.ONCE, inputDatasets,
      Collections.<String, String>emptyMap(), query);
  }

  /** Returns a copy with one additional per-flow Spark configuration entry. */
  public ImmutableFlowDefinition withSparkConf(String key, String value) {
    LinkedHashMap<String, String> updated = new LinkedHashMap<>(sparkConf);
    updated.put(key, value);
    return new ImmutableFlowDefinition(name, targetDataset, mode, inputDatasets, updated, query);
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public String targetDataset() {
    return targetDataset;
  }

  /** {@inheritDoc} */
  @Override
  public com.bocom.rdss.spark.sdp3x.api.FlowMode mode() {
    return mode;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> inputDatasets() {
    return inputDatasets;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> sparkConf() {
    return sparkConf;
  }

  /** {@inheritDoc} */
  @Override
  public com.bocom.rdss.spark.sdp3x.api.FlowQuery query() {
    return query;
  }
}
