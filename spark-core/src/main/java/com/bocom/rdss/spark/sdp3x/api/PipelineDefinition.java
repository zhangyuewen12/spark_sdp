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
import java.util.Map;
import java.util.Optional;

/**
 * Exposes the immutable top-level description of a Java SDP pipeline.
 */
public interface PipelineDefinition {

  /** Returns the user-visible pipeline name. */
  String name();

  /** Returns the optional default catalog used by the pipeline. */
  Optional<String> catalog();

  /** Returns the optional default database used by the pipeline. */
  Optional<String> database();

  /** Returns pipeline-wide Spark configuration overrides. */
  Map<String, String> configuration();

  /** Returns all declared datasets in definition order. */
  Collection<DatasetDefinition> datasets();

  /** Returns all declared flows in definition order. */
  Collection<FlowDefinition> flows();

  /** Looks up one dataset by name. */
  Optional<DatasetDefinition> dataset(String name);

  /** Looks up one flow by name. */
  Optional<FlowDefinition> flow(String name);
}
