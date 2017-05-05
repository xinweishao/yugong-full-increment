package com.taobao.yugong;

import com.taobao.yugong.common.model.DataSourceConfig;
import com.taobao.yugong.common.model.DbType;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sun.misc.Resource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class BaseDbIT {
  
  private Properties properties;
  
  @Before
  public void setup() throws IOException {
    properties = new Properties();
    properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("it.properties"));
  }

  public DataSourceConfig getOracleConfig() {
    DataSourceConfig config = new DataSourceConfig();
    config.setUsername("test");
    config.setPassword("test");
    config.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:test");
    config.setEncode("UTF-8");
    config.setType(DbType.ORACLE);

    return config;
  }

  public DataSourceConfig getMysqlConfig() {
    DataSourceConfig config = new DataSourceConfig();
    config.setUsername(properties.getProperty("mysql.username"));
    config.setPassword(properties.getProperty("mysql.password"));
    config.setUrl(properties.getProperty("mysql.url"));
    config.setEncode(properties.getProperty("mysql.encode"));
    config.setType(DbType.MYSQL);

    return config;
  }

  public DataSourceConfig getSqlServerConfig() {
    DataSourceConfig config = new DataSourceConfig();
    config.setUsername(properties.getProperty("sqlserver.username"));
    config.setPassword(properties.getProperty("sqlserver.password"));
    config.setUrl(properties.getProperty("sqlserver.url"));
    config.setEncode(properties.getProperty("sqlserver.encode"));
    config.setType(DbType.SqlServer);

    return config;
  }
}
