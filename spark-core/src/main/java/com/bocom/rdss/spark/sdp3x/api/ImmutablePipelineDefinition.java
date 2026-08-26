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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Implements {@link PipelineDefinition} as an immutable container that preserves declaration order.
 */
public final class ImmutablePipelineDefinition implements PipelineDefinition {
  private final String name;
  private final String catalog;
  private final String database;
  private final Map<String, String> configuration;
  private final Map<String, DatasetDefinition> datasetsByName;
  private final Map<String, FlowDefinition> flowsByName;

  /**
   * Creates one immutable pipeline definition instance.
   */
  public ImmutablePipelineDefinition(
      String name,
      String catalog,
      String database,
      Map<String, String> configuration,
      Map<String, DatasetDefinition> datasetsByName,
      Map<String, FlowDefinition> flowsByName) {
    this.name = Objects.requireNonNull(name, "name");
    this.catalog = catalog;
    this.database = database;
    this.configuration = Collections.unmodifiableMap(new LinkedHashMap<>(configuration));
    this.datasetsByName = Collections.unmodifiableMap(new LinkedHashMap<>(datasetsByName));
    this.flowsByName = Collections.unmodifiableMap(new LinkedHashMap<>(flowsByName));
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> catalog() {
    return Optional.ofNullable(catalog);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> database() {
    return Optional.ofNullable(database);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> configuration() {
    return configuration;
  }

  /** {@inheritDoc} */
  @Override
  public Collection<DatasetDefinition> datasets() {
    return datasetsByName.values();
  }

  /** {@inheritDoc} */
  @Override
  public Collection<FlowDefinition> flows() {
    return flowsByName.values();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<DatasetDefinition> dataset(String name) {
    return Optional.ofNullable(datasetsByName.get(name));
  }

  /** {@inheritDoc} */
  @Override
  public Optional<FlowDefinition> flow(String name) {
    return Optional.ofNullable(flowsByName.get(name));
  }
}
