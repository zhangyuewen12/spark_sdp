package com.bocom.rdss.spark.sdp3x.testsupport;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public final class SparkTestSupport {
  private SparkTestSupport() {
  }

  public static SparkSession newLocalSparkSession(String appName, Path warehouseDir) {
    createDirectories(warehouseDir);
    return SparkSession.builder()
      .appName(appName)
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
      .getOrCreate();
  }

  public static SparkSession newHiveSparkSession(String appName, Path warehouseDir) {
    createDirectories(warehouseDir);
    String metastoreUrl = "jdbc:derby:;databaseName="
      + warehouseDir.resolve("metastore_db").toAbsolutePath().normalize()
      + ";create=true";
    return SparkSession.builder()
      .appName(appName)
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
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
  }

  public static void stop(SparkSession sparkSession) {
    if (sparkSession != null) {
      sparkSession.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  public static void deleteRecursively(Path path) {
    if (path == null || !Files.exists(path)) {
      return;
    }
    try {
      Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(current -> {
          try {
            Files.deleteIfExists(current);
          } catch (IOException e) {
            throw new IllegalStateException("Failed to delete path: " + current, e);
          }
        });
    } catch (IOException e) {
      throw new IllegalStateException("Failed to traverse path for deletion: " + path, e);
    }
  }

  private static void createDirectories(Path directory) {
    try {
      Files.createDirectories(directory);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create Spark warehouse directory: " + directory, e);
    }
  }
}
