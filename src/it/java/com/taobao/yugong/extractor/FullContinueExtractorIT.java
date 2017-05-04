package com.taobao.yugong.extractor;

import com.taobao.yugong.common.db.DataSourceFactory;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.sqlserver.SqlServerFullRecordExtractor;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sql.DataSource;

import static org.junit.Assert.*;

public class FullContinueExtractorIT {
  @Test
  public void queryAndSaveToQueueSQLServer() throws Exception {
    YuGongContext context = new YuGongContext();
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.start();
    Properties properties = new Properties();
    properties.setProperty("maxActive", "200");
    DataSource dataSource = dataSourceFactory.getDataSource(
        "jdbc:sqlserver://192.168.21.28:2433", "user_3D", "555555", DbType.SqlServer, properties);
    Table tableMeta = TableMetaGenerator.getTableMeta(DbType.SqlServer, dataSource, "HJ_VIP", 
        "Activities");
    context.setTableMeta(tableMeta);

    context.setSourceDs(dataSource);
    
    FullContinueExtractor extractor = new FullContinueExtractor(
        new SqlServerFullRecordExtractor(context), context, new LinkedBlockingQueue<>());
    extractor.queryAndSaveToQueue();
    dataSourceFactory.stop();
  }

}