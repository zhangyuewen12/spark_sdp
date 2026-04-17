package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.PipelineOrchestrator;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Spark-aware entrypoint for executing SQL pipeline projects.
 */
public final class SqlPipelineRunApplication {
  private SqlPipelineRunApplication() {
  }

  public static void run(SqlPipelineCliOptions cliOptions) {
    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner();
    PipelineDefinition pipelineDefinition = runner.compile(cliOptions.projectPath());
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName(pipelineDefinition.name())
      .enableHiveSupport();
    boolean localExecution = false;
    Path localWarehouseDirectory = null;
    if (cliOptions.master() != null) {
      sparkBuilder.master(cliOptions.master());
      localExecution = cliOptions.master().startsWith("local");
    } else if (!cliOptions.submittedViaSparkSubmit()) {
      sparkBuilder.master("local[*]");
      localExecution = true;
    }
    if (localExecution) {
      Path projectPath = cliOptions.projectPath().toAbsolutePath().normalize();
      Path projectRoot = Files.isDirectory(projectPath) ? projectPath : projectPath.getParent();
      Path targetDirectory = projectRoot.resolve("target");
      String metastoreDirectory = "metastore_db_" + System.nanoTime();
      String metastoreUrl = "jdbc:derby:;databaseName="
        + targetDirectory.resolve(metastoreDirectory).toAbsolutePath().normalize()
        + ";create=true";
      localWarehouseDirectory = targetDirectory.resolve("spark-warehouse");
      sparkBuilder
        .config("spark.sql.warehouse.dir", localWarehouseDirectory.toString())
        .config("hive.metastore.uris", "")
        .config("spark.hadoop.hive.metastore.uris", "")
        .config("javax.jdo.option.ConnectionURL", metastoreUrl)
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastoreUrl)
        .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
        .config("datanucleus.schema.autoCreateAll", "true")
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
        .config("hive.metastore.schema.verification", "false")
        .config("spark.hadoop.hive.metastore.schema.verification", "false");
    }
    SparkSession sparkSession = sparkBuilder.getOrCreate();

    try {
      if (localExecution && localWarehouseDirectory != null) {
        resetLocalManagedDatasets(sparkSession, pipelineDefinition, localWarehouseDirectory);
      }
      ExecutionReport report = new PipelineOrchestrator().run(
        pipelineDefinition,
        sparkSession,
        ExecutionOptions.defaults());
      System.out.println(
        "Executed " + report.results().size() + " flow(s) for pipeline '" + pipelineDefinition.name() + "'.");
    } finally {
      sparkSession.stop();
    }
  }

  private static void resetLocalManagedDatasets(
      SparkSession sparkSession,
      PipelineDefinition pipelineDefinition,
      Path warehouseDirectory) {
    for (DatasetDefinition datasetDefinition : pipelineDefinition.datasets()) {
      if (datasetDefinition.kind() == DatasetKind.TEMPORARY_VIEW) {
        continue;
      }
      sparkSession.sql("DROP TABLE IF EXISTS " + datasetDefinition.name());
      deleteIfExists(warehouseDirectory.resolve(unqualifiedName(datasetDefinition.name())));
      deleteIfExists(warehouseDirectory.resolve(databaseDirectory(datasetDefinition.name()))
        .resolve(unqualifiedName(datasetDefinition.name())));
    }
  }

  private static String unqualifiedName(String datasetName) {
    String[] parts = datasetName.split("\\.");
    return parts[parts.length - 1];
  }

  private static Path databaseDirectory(String datasetName) {
    String[] parts = datasetName.split("\\.");
    if (parts.length <= 1) {
      return java.nio.file.Paths.get("default.db");
    }
    return java.nio.file.Paths.get(parts[parts.length - 2] + ".db");
  }

  private static void deleteIfExists(Path path) {
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
            throw new IllegalStateException("Failed to delete local warehouse path: " + current, e);
          }
        });
    } catch (IOException e) {
      throw new IllegalStateException("Failed to clean local warehouse path: " + path, e);
    }
  }
}
