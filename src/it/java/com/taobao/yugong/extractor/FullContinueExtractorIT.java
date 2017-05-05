package com.taobao.yugong.extractor;

import com.google.common.collect.ImmutableMap;
import com.taobao.yugong.BaseDbIT;
import com.taobao.yugong.common.db.DataSourceFactory;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.RunMode;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.stats.ProgressTracer;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.sqlserver.SqlServerFullRecordExtractor;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sql.DataSource;

import static org.junit.Assert.*;

public class FullContinueExtractorIT extends BaseDbIT {
  
  @Test
  public void queryAndSaveToQueueSQLServer() throws Exception {
    YuGongContext context = new YuGongContext();
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.start();
    DataSource dataSource = dataSourceFactory.getDataSource(getSqlServerConfig());
    Table tableMeta = TableMetaGenerator.getTableMeta(DbType.SqlServer, dataSource, "HJ_VIP", 
        "Activities");

    context.setTableMeta(tableMeta);
    context.setSourceDs(dataSource);
    context.setOnceCrawNum(200);
    
    FullContinueExtractor extractor = new FullContinueExtractor(
        new SqlServerFullRecordExtractor(context), context, new LinkedBlockingQueue<>());
    extractor.queryAndSaveToQueue();
    Assert.assertTrue(extractor.getQueue().size() >= 17);
    
    dataSourceFactory.stop();
  }

  @Test
  public void queryAndSaveToQueueSQLServerHugeData() throws Exception {
    YuGongContext context = new YuGongContext();
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.start();
    DataSource dataSource = dataSourceFactory.getDataSource(getSqlServerConfig());
    Table tableMeta = TableMetaGenerator.getTableMeta(DbType.SqlServer, dataSource, "HJ_VIP",
        "ShopProduct");

    context.setTableMeta(tableMeta);
    context.setSourceDs(dataSource);
    context.setOnceCrawNum(200);

    FullContinueExtractor extractor = new FullContinueExtractor(
        new SqlServerFullRecordExtractor(context), context, new LinkedBlockingQueue<>());
    extractor.queryAndSaveToQueue();
    Assert.assertEquals(200, extractor.getQueue().size());

    dataSourceFactory.stop();
  }

}