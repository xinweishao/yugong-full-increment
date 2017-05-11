package com.taobao.yugong;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.taobao.yugong.conf.YugongConfiguration;
import com.taobao.yugong.controller.YuGongController;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.io.File;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class YuGongControllerIT {

  @Test
  public void testSimple() throws Exception {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.load(YuGongControllerIT.class.getClassLoader().getResourceAsStream("yugong.properties"));
    
    YAMLMapper yamlMapper = new YAMLMapper();
    YugongConfiguration configuration = yamlMapper.readValue(
        new File("src/main/resources/yugong.yml"), YugongConfiguration.class);

    YuGongController controller = new YuGongController(config, configuration);
    controller.start();
    controller.waitForDone();
    Thread.sleep(3 * 1000); // 等待3s，清理上下文
    controller.stop();
  }
}
