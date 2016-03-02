package com.taobao.yugong;

import java.io.FileInputStream;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.yugong.common.version.VersionInfo;
import com.taobao.yugong.controller.YuGongController;

public class YuGongLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(YuGongLauncher.class);

    public static void main(String[] args) throws Throwable {
        try {
            String conf = System.getProperty("yugong.conf", "classpath:yugong.properties");
            PropertiesConfiguration config = new PropertiesConfiguration();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                config.load(YuGongLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                config.load(new FileInputStream(conf));
            }

            logger.info("## start the YuGong.");
            final YuGongController controller = new YuGongController(config);
            controller.start();
            logger.info("## the YuGong is running now ......");
            logger.info(VersionInfo.getBuildVersion());
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    if (controller.isStart()) {
                        try {
                            logger.info("## stop the YuGong");
                            controller.stop();
                        } catch (Throwable e) {
                            logger.warn("## something goes wrong when stopping YuGong:\n{}",
                                ExceptionUtils.getFullStackTrace(e));
                        } finally {
                            logger.info("## YuGong is down.");
                        }
                    }
                }

            });

            controller.waitForDone();// 如果所有都完成，则进行退出
            Thread.sleep(3 * 1000); // 等待3s，清理上下文
            logger.info("## stop the YuGong");
            if (controller.isStart()) {
                controller.stop();
            }
            logger.info("## YuGong is down.");
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the YuGong:\n{}",
                ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }
}
