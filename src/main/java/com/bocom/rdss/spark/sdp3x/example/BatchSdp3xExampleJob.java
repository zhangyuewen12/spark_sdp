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

package com.bocom.rdss.spark.sdp3x.example;

import org.apache.spark.sql.*;
import com.bocom.rdss.spark.sdp3x.PipelineOrchestrator;
import com.bocom.rdss.spark.sdp3x.api.ImmutableDatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.ImmutableFlowDefinition;
import com.bocom.rdss.spark.sdp3x.api.PipelineBuilder;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;

import java.util.Arrays;
import java.util.Collections;

/**
 * Demonstrates how a Spark application can declare and execute a batch-only Java SDP pipeline.
 */
public final class BatchSdp3xExampleJob {
  private static final String SOURCE_TABLE = "sdp3x_orders_source";
  private static final String CLEAN_TABLE = "sdp3x_orders_clean";
  private static final String DAILY_TABLE = "sdp3x_daily_orders";

  /**
   * Prevents instantiation of the utility-style example job.
   */
  private BatchSdp3xExampleJob() {
  }

  /**
   * Runs the example pipeline end-to-end in one Spark application.
   */
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
      .appName("BatchSdp3xExampleJob")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/sdp3x-warehouse")
      .getOrCreate();

    try {
      prepareSourceData(spark);
      PipelineDefinition pipeline = buildPipeline();
      ExecutionOptions options = ExecutionOptions.defaults()
        .withMaxConcurrentFlows(2)
        .withMaterializedViewSaveMode(SaveMode.Overwrite);

      new PipelineOrchestrator().run(pipeline, spark, options);

      spark.table(CLEAN_TABLE).show(false);
      spark.table(DAILY_TABLE).show(false);
    } finally {
      spark.stop();
    }
  }

  /**
   * Seeds one source table that the example pipeline will read from.
   *
   * @param spark shared Spark session for the example run
   */
  private static void prepareSourceData(SparkSession spark) {
    Dataset<OrderRecord> sourceData = spark.createDataset(Arrays.asList(
      new OrderRecord(1L, "east", "2026-04-15", 120.0d),
      new OrderRecord(2L, "east", "2026-04-15", 80.0d),
      new OrderRecord(3L, "west", "2026-04-16", 55.0d)),
      Encoders.bean(OrderRecord.class));
    sourceData.write().mode(SaveMode.Overwrite).saveAsTable(SOURCE_TABLE);
  }

  /**
   * Builds the example pipeline definition using the Java-first DSL.
   */
  private static PipelineDefinition buildPipeline() {
    return new PipelineBuilder("batch_orders_pipeline")
      .addDataset(ImmutableDatasetDefinition.table(CLEAN_TABLE).withFormat("parquet"))
      .addDataset(ImmutableDatasetDefinition.materializedView(DAILY_TABLE).withFormat("parquet"))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "clean_orders",
        CLEAN_TABLE,
        Collections.singleton(SOURCE_TABLE),
        runtime -> runtime.read(SOURCE_TABLE)
          .filter(functions.col("amount").gt(0))
          .withColumn("order_date", functions.to_date(functions.col("orderDate")))))
      .addFlow(ImmutableFlowDefinition.batchFlow(
        "daily_orders",
        DAILY_TABLE,
        Collections.singleton(CLEAN_TABLE),
        runtime -> runtime.read(CLEAN_TABLE)
          .groupBy(functions.col("region"), functions.col("order_date"))
          .agg(
            functions.count(functions.lit(1)).alias("order_count"),
            functions.sum(functions.col("amount")).alias("total_amount"))))
      .build();
  }

  /**
   * Bean-style source record used to construct the example input dataset.
   */
  public static final class OrderRecord {
    private long orderId;
    private String region;
    private String orderDate;
    private double amount;

    /** Creates an empty bean required by Spark's bean encoder. */
    public OrderRecord() {
    }

    /** Creates one source row for the example input table. */
    public OrderRecord(long orderId, String region, String orderDate, double amount) {
      this.orderId = orderId;
      this.region = region;
      this.orderDate = orderDate;
      this.amount = amount;
    }

    /** Returns the order id. */
    public long getOrderId() {
      return orderId;
    }

    /** Sets the order id. */
    public void setOrderId(long orderId) {
      this.orderId = orderId;
    }

    /** Returns the region value. */
    public String getRegion() {
      return region;
    }

    /** Sets the region value. */
    public void setRegion(String region) {
      this.region = region;
    }

    /** Returns the order date in string form. */
    public String getOrderDate() {
      return orderDate;
    }

    /** Sets the order date in string form. */
    public void setOrderDate(String orderDate) {
      this.orderDate = orderDate;
    }

    /** Returns the order amount. */
    public double getAmount() {
      return amount;
    }

    /** Sets the order amount. */
    public void setAmount(double amount) {
      this.amount = amount;
    }
  }
}
