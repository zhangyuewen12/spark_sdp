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
import scala.Option;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Applies per-flow Spark SQL configuration overrides and restores the old values on close.
 */
final class SparkConfScope implements AutoCloseable {
  private final RuntimeConfig runtimeConfig;
  private final Map<String, Option<String>> previousValues = new LinkedHashMap<>();

  /** Captures current values and applies the supplied overrides. */
  SparkConfScope(SparkSession sparkSession, Map<String, String> overrides) {
    this.runtimeConfig = sparkSession.conf();
    overrides.forEach((key, value) -> {
      previousValues.put(key, runtimeConfig.getOption(key));
      runtimeConfig.set(key, value);
    });
  }

  /** Restores the prior Spark SQL configuration values. */
  @Override
  public void close() {
    previousValues.forEach((key, previousValue) -> {
      if (previousValue.isDefined()) {
        runtimeConfig.set(key, previousValue.get());
      } else {
        runtimeConfig.unset(key);
      }
    });
  }
}
