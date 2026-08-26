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

import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineRuntime;

/**
 * Executes a flow for a specific dataset kind and batch mode.
 */
public interface FlowRunner {

  /** Returns whether the runner can execute the supplied flow for the supplied dataset. */
  boolean supports(FlowDefinition flowDefinition, DatasetDefinition datasetDefinition);

  /** Executes the supplied flow and returns a result object on success. */
  FlowExecutionResult run(FlowDefinition flowDefinition, DatasetDefinition datasetDefinition,
                          PipelineRuntime runtime, ExecutionOptions executionOptions) throws Exception;
}
