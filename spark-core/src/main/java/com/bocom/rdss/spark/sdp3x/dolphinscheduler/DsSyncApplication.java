package com.bocom.rdss.spark.sdp3x.dolphinscheduler;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;

import static com.bocom.rdss.spark.sdp3x.dolphinscheduler.DsConfig.*;

/**
 * DolphinScheduler 工作流生成与发布入口，由平台提供的 ds-sync.sh 调用。
 * 根据环境标识读取 projectHome/properties/application-&lt;env&gt;.properties，
 * 加载 DS 地址、项目、Token 等配置；不读取或接收操作系统环境变量。
 * generate 仅离线生成配置，apply 每次创建新工作流并配置、切换定时。
 */
public final class DsSyncApplication {
  private DsSyncApplication() { }

  /**
   * 命令行入口，四个位置参数依次为 operation、projectHome、workflowHome、env。
   * 示例：generate /opt/spark/my-project ads sit。
   *
   * @param args operation 为 generate/apply；projectHome 为 SQL 工程根目录；workflowHome 为 models 下的分组名，
   *             例如 ads、dws/trade；env 为 sit/uat/prd 等环境标识
   */
  public static void main(String[] args) {
    int result = run(args, Clock.systemUTC());
    if (result != 0) {
      System.exit(result);
    }
  }

  /**
   * 校验入口参数，读取指定环境文件，生成工作流并按需发布。
   *
   * @param args 与 main 相同的四个位置参数，也支持单独传入 --help
   * @param clock 发布版本时间来源；生产使用系统时钟，测试可注入固定时钟
   * @return 成功或帮助返回 0，参数、配置或发布失败返回 1
   */
  static int run(String[] args, Clock clock) {
    if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h"))) {
      usage();
      return 0;
    }
    try {
      require(args.length == 4, "Required arguments: generate/apply projectHome workflowHome env");
      String operation = args[0];
      require(operation.equals("generate") || operation.equals("apply"), "Operation must be generate or apply");
      require(!args[1].trim().isEmpty(), "projectHome must not be empty");
      require(!args[2].trim().isEmpty(), "workflowHome must not be empty");
      String env = args[3];
      require(env.matches("[A-Za-z0-9_-]+"), "env must be a nonempty environment name, for example sit/uat/prd");
      Path root = Paths.get(args[1]).toRealPath();
      require(Files.isDirectory(root), "projectHome must be a project directory");
      String group = args[2];
      DsWorkflow workflow = new DsWorkflow(root, env, group);
      Path output = workflow.root.resolve("target/bocomscheduler").resolve(env).resolve(group);
      Files.createDirectories(output);
      try (FileChannel channel = FileChannel.open(output.resolve(".apply.lock"),
          StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        FileLock lock = operation.equals("apply") ? channel.tryLock() : null;
        require(!operation.equals("apply") || lock != null, "Another apply is running for this environment/workflow");
        try {
          DsArtifact artifact = new DsArtifact(workflow, clock);
          if (operation.equals("apply")) {
            DsApiClient api = new DsApiClient(text(workflow.settings, "api-url", null), workflow.apiToken);
            new DsPublisher(api).apply(artifact);
          }
          System.out.println(operation + " complete: " + artifact.name);
          System.out.println("Artifacts: " + artifact.directory);
        } finally {
          if (lock != null) {
            lock.release();
          }
        }
      }
      return 0;
    } catch (Exception | LinkageError e) {
      System.err.println("ds-sync failed: " + e.getMessage());
      return 1;
    }
  }

  private static void usage() {
    System.out.println("Usage: ds-sync.sh <generate|apply> <projectHome> <workflowHome> <env>");
    System.out.println("Example: ds-sync.sh generate /opt/spark/my-project ads sit");
    System.out.println("env selects projectHome/properties/application-<env>.properties; no default or system environment lookup.");
    System.out.println("generate is offline; every apply creates a new workflow and switches managed schedules.");
  }
}
