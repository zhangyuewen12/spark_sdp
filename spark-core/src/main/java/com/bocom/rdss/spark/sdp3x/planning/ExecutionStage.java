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

package com.bocom.rdss.spark.sdp3x.planning;

import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Groups the flows that can run together after all prior stages have finished.
 */
public final class ExecutionStage {
  private final int index;
  private final List<FlowDefinition> flows;

  /** Creates one stage with its index and ordered flows. */
  public ExecutionStage(int index, List<FlowDefinition> flows) {
    this.index = index;
    this.flows = Collections.unmodifiableList(new ArrayList<>(flows));
  }

  /** Returns the zero-based stage index. */
  public int index() {
    return index;
  }

  /** Returns the flows assigned to this stage. */
  public List<FlowDefinition> flows() {
    return flows;
  }
}
