package com.bocom.rdss.spark.sdp3x.starter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkSubmitStarterTest {
  @TempDir
  Path tempDir;

  @Test
  void shouldBuildYarnClusterSubmitCommandFromJobDirectory() throws Exception {
    Path jobDirectory = Files.createDirectories(tempDir.resolve("daily-job"));
    Files.createDirectories(jobDirectory.resolve("transformations"));
    Files.write(jobDirectory.resolve("transformations/001.sql"),
      "CREATE TEMP VIEW v AS SELECT 1;".getBytes(StandardCharsets.UTF_8));
    Files.write(jobDirectory.resolve("spark-pipeline.yaml"), (
      "name: daily-job\n"
        + "spark-submit:\n"
        + "  master: yarn\n"
        + "  deploy-mode: cluster\n"
        + "  driver.memory: 2g\n"
        + "  executor.memory: 4g\n"
        + "  executor.cores: 2\n"
        + "  executor.num: 3\n"
        + "  conf.spark.sql.adaptive.enabled: true\n").getBytes(StandardCharsets.UTF_8));
    Path jar = Files.createFile(tempDir.resolve("spark-sdp.jar"));

    List<String> command = SparkSubmitStarter.buildCommand(
      SparkSubmitStarter.StarterOptions.parse(new String[] {"--spec", jobDirectory.toString()}), jar);

    assertOption(command, "--master", "yarn");
    assertOption(command, "--deploy-mode", "cluster");
    assertOption(command, "--name", "daily-job");
    assertOption(command, "--driver-memory", "2g");
    assertOption(command, "--executor-memory", "4g");
    assertOption(command, "--executor-cores", "2");
    assertOption(command, "--num-executors", "3");
    assertOption(command, "--class", SparkSubmitStarter.APPLICATION_CLASS);
    assertOption(command, "--conf", "spark.sql.adaptive.enabled=true");
    assertTrue(command.stream().anyMatch(value -> value.endsWith("#spark-sdp-job")));
    assertEquals("spark-sdp-job/spark-pipeline.yaml", command.get(command.size() - 1));
  }

  @Test
  void shouldAppendPropertiesAsSparkConfAndLetYamlOverrideProperties() throws Exception {
    Path jobDirectory = Files.createDirectories(tempDir.resolve("configured-job"));
    Files.write(jobDirectory.resolve("spark-pipeline.yaml"), (
      "name: configured-job\n"
        + "spark-submit:\n"
        + "  conf.spark.sql.catalog: yaml_catalog\n"
        + "  spark.sql.shuffle.partitions: 10\n").getBytes(StandardCharsets.UTF_8));
    Path properties = tempDir.resolve("spark.properties");
    Files.write(properties, (
      "spark.sql.catalog=mysql\n"
        + "spark.sql.catalog.mysql=com.example.MysqlCatalog\n"
        + "spark.sql.catalog.mysql.url=jdbc:mysql://localhost:3306/example?x=a=b\n")
      .getBytes(StandardCharsets.UTF_8));
    Path jar = Files.createFile(tempDir.resolve("configured-spark-sdp.jar"));

    List<String> command = SparkSubmitStarter.buildCommand(
      SparkSubmitStarter.StarterOptions.parse(new String[] {
        "--conf", properties.toString(), "--spec", jobDirectory.toString()
      }), jar);

    assertOption(command, "--conf", "spark.sql.catalog=yaml_catalog");
    assertTrue(command.contains("spark.sql.catalog.mysql=com.example.MysqlCatalog"));
    assertTrue(command.contains(
      "spark.sql.catalog.mysql.url=jdbc:mysql://localhost:3306/example?x=a=b"));
    assertTrue(command.contains("spark.sql.shuffle.partitions=10"));
    assertEquals(1, command.stream().filter("spark.sql.catalog=yaml_catalog"::equals).count());
    assertTrue(!command.contains("spark.sql.catalog=mysql"));
  }

  @Test
  void shouldRejectMissingPropertiesFile() throws Exception {
    Path jobDirectory = Files.createDirectories(tempDir.resolve("missing-conf-job"));
    Files.write(jobDirectory.resolve("spark-pipeline.yaml"),
      "name: missing-conf-job\n".getBytes(StandardCharsets.UTF_8));
    Path jar = Files.createFile(tempDir.resolve("missing-conf-spark-sdp.jar"));

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.buildCommand(
        SparkSubmitStarter.StarterOptions.parse(new String[] {
          "--spec", jobDirectory.toString(), "--conf", tempDir.resolve("missing.properties").toString()
        }), jar));

    assertTrue(error.getMessage().startsWith("Spark properties file does not exist:"));
  }

  @Test
  void shouldPassCommaSeparatedJarsToSparkSubmitAndOverrideYamlJars() throws Exception {
    Path jobDirectory = Files.createDirectories(tempDir.resolve("jars-job"));
    Files.write(jobDirectory.resolve("spark-pipeline.yaml"), (
      "name: jars-job\n"
        + "spark-submit:\n"
        + "  jars: yaml-only.jar\n").getBytes(StandardCharsets.UTF_8));
    Path jar = Files.createFile(tempDir.resolve("jars-spark-sdp.jar"));

    List<String> command = SparkSubmitStarter.buildCommand(
      SparkSubmitStarter.StarterOptions.parse(new String[] {
        "--spec", jobDirectory.toString(),
        "--jars", "/opt/jars/mysql.jar,/opt/jars/catalog.jar"
      }), jar);

    assertOption(command, "--jars",
      "/opt/jars/mysql.jar,/opt/jars/catalog.jar");
    assertTrue(!command.contains("yaml-only.jar"));
  }

  @Test
  void shouldPassFilesToSparkSubmitAndOverrideYamlFiles() throws Exception {
    Path jobDirectory = Files.createDirectories(tempDir.resolve("files-job"));
    Files.write(jobDirectory.resolve("spark-pipeline.yaml"), (
      "name: files-job\n"
        + "spark-submit:\n"
        + "  files: yaml-hive-site.xml\n").getBytes(StandardCharsets.UTF_8));
    Path jar = Files.createFile(tempDir.resolve("files-spark-sdp.jar"));

    List<String> command = SparkSubmitStarter.buildCommand(
      SparkSubmitStarter.StarterOptions.parse(new String[] {
        "--spec", jobDirectory.toString(),
        "--files", "/etc/hive/conf/hive-site.xml"
      }), jar);

    assertOption(command, "--files", "/etc/hive/conf/hive-site.xml");
    assertTrue(!command.contains("yaml-hive-site.xml"));
  }

  @Test
  void shouldRejectInvalidArguments() {
    assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.StarterOptions.parse(new String[] {"job"}));
    assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.StarterOptions.parse(new String[] {"--conf", "spark.properties"}));
    assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.StarterOptions.parse(new String[] {"--spec"}));
    assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.StarterOptions.parse(
        new String[] {"--spec", "job", "--jars", "  "}));
    assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.StarterOptions.parse(
        new String[] {"--spec", "job", "--files", "  "}));
    assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.StarterOptions.parse(new String[] {
        "--spec", "job", "--jars", "a.jar", "--jars", "b.jar"
      }));
  }

  private void assertOption(List<String> command, String option, String expectedValue) {
    int index = command.indexOf(option);
    assertTrue(index >= 0, "Missing option " + option + " in " + command);
    assertEquals(expectedValue, command.get(index + 1));
  }
}
