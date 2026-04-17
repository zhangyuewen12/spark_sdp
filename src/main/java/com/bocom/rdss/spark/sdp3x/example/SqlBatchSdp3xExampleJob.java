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

import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.sql.SqlPipelineProjectRunner;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Comparator;

/**
 * Demonstrates the SQL-project MVP with {@code spark-pipeline.yml} and {@code .sql} files.
 */
public final class SqlBatchSdp3xExampleJob {
  private static final String CLEAN_TABLE = "orders_clean";
  private static final String DAILY_TABLE = "daily_orders";

  private SqlBatchSdp3xExampleJob() {
  }

  public static void main(String[] args) {
    Path projectRoot = args.length > 0
      ? Paths.get(args[0]).toAbsolutePath().normalize()
      : Paths.get("examples/sql-batch-pipeline").toAbsolutePath().normalize();
    String metastoreUrl = "jdbc:derby:;databaseName="
      + projectRoot.resolve("target/sql-example-metastore").toAbsolutePath().normalize()
      + ";create=true";

    SparkSession spark = SparkSession.builder()
      .appName("SqlBatchSdp3xExampleJob")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/sql-example-warehouse")
      .config("hive.metastore.uris", "")
      .config("spark.hadoop.hive.metastore.uris", "")
      .config("javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateAll", "true")
      .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.hadoop.hive.metastore.schema.verification", "false")
      .enableHiveSupport()
      .getOrCreate();

    try {
      resetExampleTables(projectRoot, spark);
      new SqlPipelineProjectRunner().run(
        projectRoot,
        spark,
        ExecutionOptions.defaults()
          .withMaxConcurrentFlows(2)
          .withMaterializedViewSaveMode(SaveMode.Overwrite));

      spark.table(CLEAN_TABLE).show(false);
      spark.table(DAILY_TABLE).show(false);
    } finally {
      spark.stop();
    }
  }

  private static void resetExampleTables(Path projectRoot, SparkSession spark) {
    deleteRecursively(projectRoot.resolve("target").resolve("sql-example-metastore"));
    spark.sql("DROP TABLE IF EXISTS orders_source");
    spark.sql("DROP TABLE IF EXISTS orders_clean");
    spark.sql("DROP TABLE IF EXISTS daily_orders");
    Path warehouse = Paths.get("target", "sql-example-warehouse").toAbsolutePath().normalize();
    deleteRecursively(warehouse.resolve("orders_source"));
    deleteRecursively(warehouse.resolve("orders_clean"));
    deleteRecursively(warehouse.resolve("daily_orders"));
  }

  private static void deleteRecursively(Path path) {
    if (!Files.exists(path)) {
      return;
    }
    try {
      Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(current -> {
          try {
            Files.deleteIfExists(current);
          } catch (IOException e) {
            throw new IllegalStateException("Failed to clean SQL example path: " + current, e);
          }
        });
    } catch (IOException e) {
      throw new IllegalStateException("Failed to traverse SQL example path: " + path, e);
    }
  }
}
