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

import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * Builds an immutable {@link PipelineDefinition} using a Java-friendly fluent API.
 */
public final class PipelineBuilder {
  private final String name;
  private String catalog;
  private String database;
  private final LinkedHashMap<String, String> configuration = new LinkedHashMap<>();
  private final LinkedHashMap<String, DatasetDefinition> datasets = new LinkedHashMap<>();
  private final LinkedHashMap<String, FlowDefinition> flows = new LinkedHashMap<>();

  /**
   * Creates a builder for one pipeline.
   *
   * @param name pipeline name used in diagnostics
   */
  public PipelineBuilder(String name) {
    this.name = Objects.requireNonNull(name, "name");
  }

  /** Sets the default catalog that should be used by the pipeline. */
  public PipelineBuilder catalog(String catalogName) {
    this.catalog = catalogName;
    return this;
  }

  /** Sets the default database that should be used by the pipeline. */
  public PipelineBuilder database(String databaseName) {
    this.database = databaseName;
    return this;
  }

  /** Adds one pipeline-wide Spark configuration entry. */
  public PipelineBuilder configuration(String key, String value) {
    configuration.put(key, value);
    return this;
  }

  /** Registers one dataset definition and preserves the declaration order. */
  public PipelineBuilder addDataset(DatasetDefinition datasetDefinition) {
    Objects.requireNonNull(datasetDefinition, "datasetDefinition");
    if (datasets.putIfAbsent(datasetDefinition.name(), datasetDefinition) != null) {
      throw new IllegalArgumentException(
        "Duplicate dataset definition: " + datasetDefinition.name());
    }
    return this;
  }

  /** Registers one flow definition and preserves the declaration order. */
  public PipelineBuilder addFlow(FlowDefinition flowDefinition) {
    Objects.requireNonNull(flowDefinition, "flowDefinition");
    if (flows.putIfAbsent(flowDefinition.name(), flowDefinition) != null) {
      throw new IllegalArgumentException("Duplicate flow definition: " + flowDefinition.name());
    }
    return this;
  }

  /** Materializes the current builder state into an immutable pipeline definition. */
  public PipelineDefinition build() {
    return new ImmutablePipelineDefinition(
      name,
      catalog,
      database,
      new LinkedHashMap<>(configuration),
      new LinkedHashMap<>(datasets),
      new LinkedHashMap<>(flows));
  }
}
