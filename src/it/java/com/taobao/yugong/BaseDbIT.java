package com.taobao.yugong;

import com.taobao.yugong.common.model.DataSourceConfig;
import com.taobao.yugong.common.model.DbType;

import org.junit.Test;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class BaseDbIT {

  @Test
  public DataSourceConfig getOracleConfig() {
    DataSourceConfig config = new DataSourceConfig();
    config.setUsername("test");
    config.setPassword("test");
    config.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:test");
    config.setEncode("UTF-8");
    config.setType(DbType.ORACLE);

    return config;
  }

  @Test
  public DataSourceConfig getMysqlConfig() {
    DataSourceConfig config = new DataSourceConfig();
    config.setUsername("test");
    config.setPassword("test");
    config.setUrl("jdbc:mysql://127.0.0.1:3306");
    config.setEncode("UTF-8");
    config.setType(DbType.MYSQL);

    return config;
  }
}
