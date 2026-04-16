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

import org.apache.spark.sql.SparkSession;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.FlowDefinition;
import com.bocom.rdss.spark.sdp3x.graph.PipelineValidationException;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionPlan;
import com.bocom.rdss.spark.sdp3x.planning.ExecutionStage;
import com.bocom.rdss.spark.sdp3x.runtime.DefaultPipelineRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Executes a planned pipeline stage by stage in a bounded local thread pool.
 */
public final class LocalPipelineExecutor implements PipelineExecutor {
  private final DatasetMaterializer datasetMaterializer;
  private final List<FlowRunner> flowRunners;

  /** Creates an executor with the default batch dataset materializer and batch flow runner. */
  public LocalPipelineExecutor() {
    this(new DefaultDatasetMaterializer(),
      Collections.<FlowRunner>singletonList(new BatchFlowRunner()));
  }

  /** Creates an executor with explicitly supplied collaborators. */
  public LocalPipelineExecutor(
      DatasetMaterializer datasetMaterializer,
      List<FlowRunner> flowRunners) {
    this.datasetMaterializer = datasetMaterializer;
    this.flowRunners = Collections.unmodifiableList(new ArrayList<>(flowRunners));
  }

  /** {@inheritDoc} */
  @Override
  public ExecutionReport execute(
      ExecutionPlan executionPlan,
      SparkSession sparkSession,
      ExecutionOptions executionOptions) {
    DefaultPipelineRuntime runtime = new DefaultPipelineRuntime(sparkSession, executionPlan.pipeline());
    Set<String> materializedDatasets = new HashSet<>();
    List<FlowExecutionResult> results = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(executionOptions.maxConcurrentFlows());
    try (PipelineSessionScope ignored = new PipelineSessionScope(sparkSession, executionPlan.pipeline())) {
      for (ExecutionStage stage : executionPlan.stages()) {
        stage.flows().forEach(flow -> materializeTarget(runtime, flow, materializedDatasets, executionOptions));
        List<Callable<FlowExecutionResult>> tasks = new ArrayList<>();
        for (FlowDefinition flow : stage.flows()) {
          tasks.add(() -> selectRunner(runtime, flow)
            .run(flow, runtime.dataset(flow.targetDataset()).orElseThrow(() ->
              new PipelineValidationException("Unknown target dataset: " + flow.targetDataset())),
              runtime, executionOptions));
        }
        for (Future<FlowExecutionResult> future : executorService.invokeAll(tasks)) {
          results.add(future.get());
        }
      }
      return new ExecutionReport(results);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PipelineExecutionException("Pipeline execution was interrupted", e);
    } catch (ExecutionException e) {
      throw new PipelineExecutionException("Pipeline execution failed", e.getCause());
    } finally {
      executorService.shutdownNow();
    }
  }

  /** Materializes a target dataset once before the first flow that writes into it runs. */
  private void materializeTarget(
      DefaultPipelineRuntime runtime,
      FlowDefinition flowDefinition,
      Set<String> materializedDatasets,
      ExecutionOptions executionOptions) {
    if (materializedDatasets.contains(flowDefinition.targetDataset())) {
      return;
    }
    DatasetDefinition datasetDefinition = runtime.dataset(flowDefinition.targetDataset()).orElseThrow(() ->
      new PipelineValidationException("Unknown target dataset: " + flowDefinition.targetDataset()));
    datasetMaterializer.materialize(datasetDefinition, runtime, executionOptions);
    materializedDatasets.add(flowDefinition.targetDataset());
  }

  /** Chooses the first runner that can execute the supplied flow and dataset combination. */
  private FlowRunner selectRunner(DefaultPipelineRuntime runtime, FlowDefinition flowDefinition) {
    DatasetDefinition datasetDefinition = runtime.dataset(flowDefinition.targetDataset()).orElseThrow(() ->
      new PipelineValidationException("Unknown target dataset: " + flowDefinition.targetDataset()));
    return flowRunners.stream()
      .filter(runner -> runner.supports(flowDefinition, datasetDefinition))
      .findFirst()
      .orElseThrow(() -> new PipelineExecutionException(
        "No runner available for flow '" + flowDefinition.name() + "'"));
  }
}
