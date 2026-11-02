package com.bocom.rdss.spark.sdp3x.starter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkSubmitStarterCoverageTest {
  @TempDir Path tempDir;

  @Test
  void archivesOnlySelectedTaskAndPassesSubmissionOptions() throws Exception {
    Path job = Files.createDirectory(tempDir.resolve("job"));
    Path spec = write(job.resolve("spark-pipeline.yaml"),
      "name: compact\njob-type: hudi-compaction\nspark-submit:\n"
        + "  deployMode: client\n  queue: priority\n  driver.cores: 2\n"
        + "  executor.number: 5\n  files: hive-site.xml\n  jars: hudi.jar\n"
        + "  principal: user@REALM\n  keytab: user.keytab\n");
    Path task = write(tempDir.resolve("compaction.sql"), "RUN COMPACTION ON db.table;\n");
    write(job.resolve("unused.txt"), "unused");
    Path jar = write(tempDir.resolve("app.jar"), "");
    List<String> command = SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", spec.toString(), "--task-file", task.toString()}), jar);

    assertOption(command, "--deploy-mode", "client");
    assertOption(command, "--queue", "priority");
    assertOption(command, "--driver-cores", "2");
    assertOption(command, "--num-executors", "5");
    assertOption(command, "--files", "hive-site.xml");
    assertOption(command, "--jars", "hudi.jar");
    assertOption(command, "--principal", "user@REALM");
    assertOption(command, "--keytab", "user.keytab");
    assertOption(command, "--task-file", "spark-sdp-job/task/compaction.sql");

    String archive = command.get(command.indexOf("--archives") + 1).split("#")[0];
    try (FileSystem zip = FileSystems.newFileSystem(URI.create("jar:" + Paths.get(archive).toUri()),
        Collections.emptyMap())) {
      assertTrue(Files.exists(zip.getPath("/spark-pipeline.yaml")));
      assertTrue(Files.exists(zip.getPath("/task/compaction.sql")));
      assertFalse(Files.exists(zip.getPath("/unused.txt")));
    }
  }

  @Test
  void archivesJobDirectoryButSkipsBuildAndGitFiles() throws Exception {
    Path job = Files.createDirectory(tempDir.resolve("whole-job"));
    write(job.resolve("spark-pipeline.yaml"), "name: whole\n");
    write(Files.createDirectories(job.resolve("transformations")).resolve("one.sql"), "SELECT 1");
    write(Files.createDirectories(job.resolve("target")).resolve("generated.txt"), "generated");
    write(Files.createDirectories(job.resolve(".git")).resolve("config"), "git");
    write(Files.createDirectories(job.resolve(".idea")).resolve("settings"), "idea");
    List<String> command = SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", job.resolve("spark-pipeline.yaml").toString(),
        "--jobPath", job.toString()}),
      write(tempDir.resolve("app.jar"), ""));
    String archive = command.get(command.indexOf("--archives") + 1).split("#")[0];
    try (FileSystem zip = FileSystems.newFileSystem(URI.create("jar:" + Paths.get(archive).toUri()),
        Collections.emptyMap())) {
      assertTrue(Files.exists(zip.getPath("/transformations/one.sql")));
      assertFalse(Files.exists(zip.getPath("/target/generated.txt")));
      assertFalse(Files.exists(zip.getPath("/.git/config")));
      assertFalse(Files.exists(zip.getPath("/.idea/settings")));
    }
  }

  @Test
  void rejectsInvalidSpecsTasksAndCompactionWithoutSql() throws Exception {
    Path jar = write(tempDir.resolve("app.jar"), "");
    Path job = Files.createDirectory(tempDir.resolve("invalid-job"));
    assertBuildError(job, jar, "--spec must be a readable YAML file");
    assertBuildError(tempDir.resolve("absent"), jar, "--spec must be a readable YAML file");
    Path txt = write(tempDir.resolve("job.txt"), "name: bad\n");
    assertBuildError(txt, jar, "--spec must be a YAML file");
    Path spec = write(job.resolve("spark-pipeline.yaml"), "job-type: hudi-compaction\n");
    assertBuildError(spec, jar, "requires --task-file");
    Path invalidTask = write(tempDir.resolve("tables.list"), "db.table\n");
    assertThrows(IllegalArgumentException.class, () -> SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", spec.toString(), "--task-file",
        invalidTask.toString()}), jar));
    assertThrows(IllegalArgumentException.class, () -> SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", spec.toString(), "--task-file",
        tempDir.resolve("missing.sql").toString()}), jar));
  }

  @Test
  void handlesEmptyOrScalarYamlAndPropertyOverrides() throws Exception {
    Path job = Files.createDirectory(tempDir.resolve("simple-job"));
    Path spec = write(job.resolve("spark-pipeline.yaml"), "\n");
    Path jar = write(tempDir.resolve("app.jar"), "");
    List<String> empty = SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", spec.toString()}), jar);
    assertOption(empty, "--master", "yarn");
    write(spec, "some string\n");
    assertOption(SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", spec.toString()}), jar), "--master", "yarn");
    write(spec, "spark-submit:\n  master: local[2]\n  files: '  '\n  spark.sql.shuffle.partitions: 3\n");
    assertOption(SparkSubmitStarter.buildCommand(
      StarterOptions.parse(new String[] {"--spec", spec.toString()}), jar), "--master", "local[2]");
  }

  @Test
  void executesSubmittedProcessAndReturnsItsExitCode() throws Exception {
    Path job = Files.createDirectory(tempDir.resolve("run-job"));
    write(job.resolve("spark-pipeline.yaml"), "name: run\n");
    Path jar = write(tempDir.resolve("app.jar"), "");
    Process success = mock(Process.class);
    when(success.waitFor()).thenReturn(0);
    assertEquals(0, SparkSubmitStarter.run(new String[] {"--spec", job.resolve("spark-pipeline.yaml").toString()},
      () -> jar, command -> success));
    Process failure = mock(Process.class);
    when(failure.waitFor()).thenReturn(17);
    assertEquals(17, SparkSubmitStarter.run(new String[] {"--spec", job.resolve("spark-pipeline.yaml").toString()},
      () -> jar, command -> failure));
    SparkSubmitStarter.main(new String[] {"--help"});
  }

  @Test
  void reportsProcessStartupAndInterruptionFailures() throws Exception {
    Path job = Files.createDirectory(tempDir.resolve("failure-job"));
    write(job.resolve("spark-pipeline.yaml"), "name: run\n");
    Path jar = write(tempDir.resolve("app.jar"), "");
    String[] args = {"--spec", job.resolve("spark-pipeline.yaml").toString()};
    assertTrue(assertThrows(IllegalStateException.class,
      () -> SparkSubmitStarter.run(args, () -> jar, command -> {
        throw new IOException("cannot start");
      })).getMessage().contains("Failed to start"));
    Process interrupted = mock(Process.class);
    when(interrupted.waitFor()).thenThrow(new InterruptedException("interrupted"));
    assertTrue(assertThrows(IllegalStateException.class,
      () -> SparkSubmitStarter.run(args, () -> jar, command -> interrupted))
      .getMessage().contains("Interrupted"));
    assertTrue(Thread.interrupted());
  }

  @Test
  void locatesConfiguredApplicationJarAndRejectsDevelopmentClassDirectory() throws Exception {
    String original = System.getProperty("spark.sdp.jar");
    Method locate = SparkSubmitStarter.class.getDeclaredMethod("locateApplicationJar");
    locate.setAccessible(true);
    try {
      System.clearProperty("spark.sdp.jar");
      InvocationTargetException missing = assertThrows(InvocationTargetException.class,
        () -> locate.invoke(null));
      assertTrue(missing.getCause().getMessage().contains("must run from a jar"));
      System.setProperty("spark.sdp.jar", tempDir.resolve("nonexistent.jar").toString());
      assertEquals(tempDir.resolve("nonexistent.jar"), locate.invoke(null));
    } finally {
      if (original == null) System.clearProperty("spark.sdp.jar");
      else System.setProperty("spark.sdp.jar", original);
    }
  }

  @Test
  void rejectsMalformedAndEmptyKeyPropertiesFiles() throws Exception {
    Path job = Files.createDirectory(tempDir.resolve("bad-properties-job"));
    write(job.resolve("spark-pipeline.yaml"), "name: configured\n");
    Path jar = write(tempDir.resolve("app.jar"), "");
    Path malformed = tempDir.resolve("malformed.properties");
    Files.write(malformed, new byte[] {(byte) 0xC3, (byte) 0x28});
    assertTrue(assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.buildCommand(StarterOptions.parse(new String[] {
        "--spec", job.resolve("spark-pipeline.yaml").toString(),
        "--conf", malformed.toString()}), jar))
      .getMessage().contains("Failed to read Spark properties file"));

    Path emptyKey = write(tempDir.resolve("empty-key.properties"), "=value\n");
    assertTrue(assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.buildCommand(StarterOptions.parse(new String[] {
        "--spec", job.resolve("spark-pipeline.yaml").toString(),
        "--conf", emptyKey.toString()}), jar))
      .getMessage().contains("empty key"));
  }

  @Test
  void quotesCommandArgumentsContainingSpacesAndApostrophes() throws Exception {
    Method printable = SparkSubmitStarter.class.getDeclaredMethod("printable", List.class);
    printable.setAccessible(true);
    String command = (String) printable.invoke(null,
      java.util.Arrays.asList("spark-submit", "a b'c.jar"));
    assertTrue(command.startsWith("spark-submit 'a b"));
    assertTrue(command.contains("'\\''"));
    assertTrue(command.endsWith("c.jar'"));
  }

  private void assertBuildError(Path spec, Path jar, String text) {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
      () -> SparkSubmitStarter.buildCommand(
        StarterOptions.parse(new String[] {"--spec", spec.toString()}), jar));
    assertTrue(error.getMessage().contains(text));
  }

  private Path write(Path path, String content) throws IOException {
    Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    return path;
  }

  private void assertOption(List<String> command, String key, String value) {
    int index = command.indexOf(key);
    assertTrue(index >= 0, command.toString());
    assertEquals(value, command.get(index + 1));
  }
}
