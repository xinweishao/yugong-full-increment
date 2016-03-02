package com.taobao.yugong;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import com.taobao.yugong.controller.YuGongController;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class YuGongControllerTest {

    @Test
    public void testSimple() throws Exception {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.load(YuGongLauncher.class.getClassLoader().getResourceAsStream("yugong.properties"));

        YuGongController controller = new YuGongController(config);
        controller.start();
        controller.waitForDone();
        Thread.sleep(3 * 1000); // 等待3s，清理上下文
        controller.stop();
    }
}
