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

package com.bocom.rdss.spark.sdp3x.execution;

import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import scala.Option;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Applies pipeline-wide Spark session configuration and restores the previous state on close.
 */
final class PipelineSessionScope implements AutoCloseable {
  private final SparkSession sparkSession;
  private final RuntimeConfig runtimeConfig;
  private final String previousDatabase;
  private final Map<String, Option<String>> previousValues = new LinkedHashMap<>();

  PipelineSessionScope(SparkSession sparkSession, PipelineDefinition pipelineDefinition) {
    this.sparkSession = sparkSession;
    this.runtimeConfig = sparkSession.conf();
    this.previousDatabase = sparkSession.catalog().currentDatabase();

    pipelineDefinition.configuration().forEach((key, value) -> {
      previousValues.put(key, runtimeConfig.getOption(key));
      runtimeConfig.set(key, value);
    });

    if (pipelineDefinition.catalog().isPresent()) {
      sparkSession.sql("USE CATALOG " + pipelineDefinition.catalog().get());
    }
    // Treat the configured database as the session default, similar to running USE <database>
    // before executing the pipeline SQL. Fully qualified table names in SQL still take precedence.
    if (pipelineDefinition.database().isPresent()) {
      sparkSession.catalog().setCurrentDatabase(pipelineDefinition.database().get());
    }
  }

  @Override
  public void close() {
    previousValues.forEach((key, previousValue) -> {
      if (previousValue.isDefined()) {
        runtimeConfig.set(key, previousValue.get());
      } else {
        runtimeConfig.unset(key);
      }
    });
    if (previousDatabase != null) {
      sparkSession.catalog().setCurrentDatabase(previousDatabase);
    }
  }
}
