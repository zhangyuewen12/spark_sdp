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

import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Optional;

/**
 * Describes the immutable metadata for one dataset in a pipeline definition.
 */
public interface DatasetDefinition {

  /** Returns the unique dataset name used by flows inside the pipeline. */
  String name();

  /** Returns the logical kind that tells the executor how to publish the dataset. */
  DatasetKind kind();

  /** Returns an optional user-facing description of the dataset. */
  Optional<String> comment();

  /** Returns an optional storage format hint used when persisting the dataset. */
  Optional<String> format();

  /** Returns an optional user-defined schema used during bootstrap materialization. */
  Optional<StructType> schema();

  /** Returns dataset properties that can be used by validation or execution policies. */
  Map<String, String> properties();

  /** Returns whether the dataset should be persisted beyond the current Spark session. */
  default boolean materialized() {
    return kind() != DatasetKind.TEMPORARY_VIEW;
  }
}
