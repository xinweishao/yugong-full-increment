package com.taobao.yugong;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.taobao.yugong.common.version.VersionInfo;
import com.taobao.yugong.controller.YuGongController;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.File;

/**
 * Single jar app
 */
@Slf4j
public class YuGongApp {

  @Parameter(names = {"-c", "--config"}, converter = FileConverter.class, required = true)
  private File configFile;

  public static void main(String[] args) {
    YuGongApp yuGongApp = new YuGongApp();

    JCommander.newBuilder().addObject(yuGongApp).build().parse(args);
    PropertiesConfiguration config = new PropertiesConfiguration();
    try {
      config.load(yuGongApp.configFile);
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
    try {
      run(config);
    } catch (Throwable e) {
      log.error("## Something goes wrong when starting up the YuGong:\n{}",
          ExceptionUtils.getFullStackTrace(e));
      System.exit(0);
    }
  }

  private static void run(PropertiesConfiguration config) throws InterruptedException {
    final YuGongController controller = new YuGongController(config);
    log.info("## start the YuGong.");
    controller.start();
    log.info("## the YuGong is running now ......");
    log.info(VersionInfo.getBuildVersion());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (!controller.isStart()) {
        return;
      }
      try {
        log.info("## Stop the YuGong");
        controller.stop();
      } catch (Throwable e) {
        log.warn("## Something goes wrong when stopping YuGong:\n{}",
            ExceptionUtils.getFullStackTrace(e));
      } finally {
        log.info("## YuGong is down.");
      }
    }));
    controller.waitForDone();// 如果所有都完成，则进行退出
    Thread.sleep(3 * 1000); // 等待3s，清理上下文
    log.info("## stop the YuGong");
    if (controller.isStart()) {
      controller.stop();
    }
    log.info("## YuGong is down.");
  }
}
