package com.taobao.yugong.common;

import com.taobao.yugong.BaseDbIT;
import com.taobao.yugong.common.db.DataSourceFactory;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;

import org.junit.Test;

import java.util.Map;

import javax.sql.DataSource;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class TableMetaGeneratorIT extends BaseDbIT {

  @Test
  public void testOracle() {
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.start();

    String schemaName = "test";
    String tableName = "test_all_target";
    DataSource oracle = dataSourceFactory.getDataSource(getOracleConfig());
    Table table = TableMetaGenerator.getTableMeta(oracle, schemaName, tableName);
    System.out.println(table);

    Map<String, String> index = TableMetaGenerator.getTableIndex(oracle, schemaName, tableName);
    System.out.println(index);

    String mlogName = TableMetaGenerator.getMLogTableName(oracle, schemaName, tableName);
    System.out.println(mlogName);

    Table mtable = TableMetaGenerator.getTableMeta(oracle, schemaName, mlogName);
    System.out.println(mtable);

    Map<String, String> mindex = TableMetaGenerator.getTableIndex(oracle, schemaName, mlogName);
    System.out.println(mindex);
    dataSourceFactory.stop();
  }

  @Test
  public void testMysql() {
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.start();

    String schemaName = "test";
    String tableName = "yugong_example_date_pk";
    DataSource mysql = dataSourceFactory.getDataSource(getMysqlConfig());
    Table table = TableMetaGenerator.getTableMeta(mysql, schemaName, tableName);
    System.out.println(table);

    Map<String, String> index = TableMetaGenerator.getTableIndex(mysql, schemaName, tableName);
    System.out.println(index);
    dataSourceFactory.stop();
  }
}
