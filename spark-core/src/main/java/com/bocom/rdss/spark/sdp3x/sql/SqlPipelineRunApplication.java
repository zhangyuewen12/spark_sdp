package com.bocom.rdss.spark.sdp3x.sql;

import com.bocom.rdss.spark.sdp3x.PipelineOrchestrator;
import com.bocom.rdss.spark.sdp3x.api.DatasetDefinition;
import com.bocom.rdss.spark.sdp3x.api.DatasetKind;
import com.bocom.rdss.spark.sdp3x.api.PipelineDefinition;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionOptions;
import com.bocom.rdss.spark.sdp3x.execution.ExecutionReport;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Spark-aware entrypoint for executing SQL pipeline projects.
 */
public final class SqlPipelineRunApplication {
  static final String DEV_HIVE_SITE_RESOURCE = "dev/hive-site.xml";
  static final String LOCAL_DEV_HIVE_ENABLED_PROPERTY = "spark.sdp.local.dev-hive.enabled";

  private SqlPipelineRunApplication() {
  }

  /** Spark job entrypoint (entrypoint B). Run this main method directly from IDEA for local mode. */
  public static void main(String[] args) {
    SqlPipelineRunOptions options = SqlPipelineRunOptions.parse(args);
    run(options);
  }

  public static void run(SqlPipelineRunOptions cliOptions) {
    SqlPipelineProjectRunner runner = new SqlPipelineProjectRunner();
    PipelineDefinition pipelineDefinition = runner.compile(cliOptions.projectPath());
    SparkSessionContext sparkSessionContext = createSparkSession(cliOptions, pipelineDefinition);
    SparkSession sparkSession = sparkSessionContext.sparkSession();

    try {
      if (sparkSessionContext.localWarehouseDirectory() != null) {
        resetLocalManagedDatasets(
          sparkSession,
          pipelineDefinition,
          sparkSessionContext.localWarehouseDirectory());
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

  static boolean isLocalDevHiveEnabled() {
    return !"false".equalsIgnoreCase(System.getProperty(LOCAL_DEV_HIVE_ENABLED_PROPERTY, "true"));
  }

  static Optional<Map<String, String>> loadPreferredLocalHiveSiteProperties(ClassLoader classLoader) {
    if (!isLocalDevHiveEnabled()) {
      return Optional.empty();
    }
    java.net.URL resource = classLoader.getResource(DEV_HIVE_SITE_RESOURCE);
    if (resource == null) {
      return Optional.empty();
    }
    try (InputStream inputStream = resource.openStream()) {
      return Optional.of(parseHiveSite(inputStream));
    } catch (Exception e) {
      throw new IllegalStateException(
        "Failed to load local Hive configuration from classpath resource '" + DEV_HIVE_SITE_RESOURCE + "'.",
        e);
    }
  }

  private static SparkSessionContext createSparkSession(
      SqlPipelineRunOptions cliOptions,
      PipelineDefinition pipelineDefinition) {
    String localMaster = null;
    if (cliOptions.master() != null) {
      localMaster = cliOptions.master();
    } else if (!cliOptions.submittedViaSparkSubmit()) {
      localMaster = "local[*]";
    }
    boolean localExecution = localMaster != null && localMaster.startsWith("local");
    if (!localExecution) {
      return new SparkSessionContext(newSparkBuilder(cliOptions, pipelineDefinition).getOrCreate(), null);
    }

    Optional<Map<String, String>> preferredHiveSite =
      loadPreferredLocalHiveSiteProperties(SqlPipelineRunApplication.class.getClassLoader());
    if (preferredHiveSite.isPresent()) {
      SparkSession preferredSession = null;
      try {
        SparkSession.Builder preferredBuilder = newSparkBuilder(cliOptions, pipelineDefinition);
        applyHiveSiteProperties(preferredBuilder, preferredHiveSite.get());
        preferredSession = preferredBuilder.getOrCreate();
        preferredSession.catalog().listDatabases().collectAsList();
        System.out.println(
          "Local startup loaded Hive metastore settings from classpath resource '" + DEV_HIVE_SITE_RESOURCE + "'.");
        return new SparkSessionContext(preferredSession, null);
      } catch (RuntimeException e) {
        stopQuietly(preferredSession);
        System.err.println(
          "Local startup could not use classpath resource '" + DEV_HIVE_SITE_RESOURCE
            + "' and will fall back to an embedded Derby metastore. Cause: " + e.getMessage());
      }
    }

    SparkSession.Builder derbyBuilder = newSparkBuilder(cliOptions, pipelineDefinition);
    Path localWarehouseDirectory = applyEmbeddedDerbyMetastore(cliOptions.projectPath(), derbyBuilder);
    return new SparkSessionContext(derbyBuilder.getOrCreate(), localWarehouseDirectory);
  }

  private static SparkSession.Builder newSparkBuilder(
      SqlPipelineRunOptions cliOptions,
      PipelineDefinition pipelineDefinition) {
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName(pipelineDefinition.name())
      .enableHiveSupport();
    if (cliOptions.master() != null) {
      sparkBuilder.master(cliOptions.master());
    } else if (!cliOptions.submittedViaSparkSubmit()) {
      sparkBuilder.master("local[*]");
    }
    return sparkBuilder;
  }

  private static Path applyEmbeddedDerbyMetastore(Path projectPath, SparkSession.Builder sparkBuilder) {
    Path absoluteProjectPath = projectPath.toAbsolutePath().normalize();
    Path projectRoot = Files.isDirectory(absoluteProjectPath)
      ? absoluteProjectPath
      : absoluteProjectPath.getParent();
    Path targetDirectory = projectRoot.resolve("target");
    String metastoreDirectory = "metastore_db_" + System.nanoTime();
    String metastoreUrl = "jdbc:derby:;databaseName="
      + targetDirectory.resolve(metastoreDirectory).toAbsolutePath().normalize()
      + ";create=true";
    Path localWarehouseDirectory = targetDirectory.resolve("spark-warehouse");
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
    return localWarehouseDirectory;
  }

  private static void applyHiveSiteProperties(
      SparkSession.Builder sparkBuilder,
      Map<String, String> hiveSiteProperties) {
    hiveSiteProperties.forEach((key, value) -> {
      sparkBuilder.config(key, value);
      if (!key.startsWith("spark.")) {
        sparkBuilder.config("spark.hadoop." + key, value);
      }
    });
    if (!hiveSiteProperties.containsKey("spark.sql.warehouse.dir")
        && hiveSiteProperties.containsKey("hive.metastore.warehouse.dir")) {
      sparkBuilder.config("spark.sql.warehouse.dir", hiveSiteProperties.get("hive.metastore.warehouse.dir"));
    }
  }

  private static Map<String, String> parseHiveSite(InputStream inputStream) throws Exception {
    Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
    NodeList propertyNodes = document.getElementsByTagName("property");
    LinkedHashMap<String, String> properties = new LinkedHashMap<>();
    for (int index = 0; index < propertyNodes.getLength(); index++) {
      org.w3c.dom.Node propertyNode = propertyNodes.item(index);
      if (!(propertyNode instanceof org.w3c.dom.Element)) {
        continue;
      }
      org.w3c.dom.Element property = (org.w3c.dom.Element) propertyNode;
      String name = childText(property, "name");
      String value = childText(property, "value");
      if (name != null && !name.isEmpty() && value != null) {
        properties.put(name, value);
      }
    }
    return properties;
  }

  private static String childText(org.w3c.dom.Element parent, String tagName) {
    NodeList nodes = parent.getElementsByTagName(tagName);
    if (nodes.getLength() == 0 || nodes.item(0) == null) {
      return null;
    }
    String text = nodes.item(0).getTextContent();
    return text == null ? null : text.trim();
  }

  private static void stopQuietly(SparkSession sparkSession) {
    if (sparkSession == null) {
      return;
    }
    try {
      sparkSession.stop();
    } finally {
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
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

  private static final class SparkSessionContext {
    private final SparkSession sparkSession;
    private final Path localWarehouseDirectory;

    private SparkSessionContext(SparkSession sparkSession, Path localWarehouseDirectory) {
      this.sparkSession = sparkSession;
      this.localWarehouseDirectory = localWarehouseDirectory;
    }

    private SparkSession sparkSession() {
      return sparkSession;
    }

    private Path localWarehouseDirectory() {
      return localWarehouseDirectory;
    }
  }
}
