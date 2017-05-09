package com.taobao.yugong.applier;

import com.google.common.base.CaseFormat;
import com.taobao.yugong.BaseDbIT;
import com.taobao.yugong.common.db.DataSourceFactory;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.RunMode;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.stats.ProgressTracer;
import com.taobao.yugong.extractor.sqlserver.SqlServerFullRecordExtractor;
import com.taobao.yugong.translator.NameDataTranslator;

import org.junit.Test;

import java.util.List;

import javax.sql.DataSource;

public class CheckRecordApplierIT extends BaseDbIT {
  
  @Test
  public void doApply() throws Exception {
    YuGongContext context = new YuGongContext();
    DataSourceFactory dataSourceFactory = new DataSourceFactory();
    dataSourceFactory.start();
    DataSource dataSource = dataSourceFactory.getDataSource(getSqlServerConfig());
    Table tableMeta = TableMetaGenerator.getTableMeta(DbType.SqlServer, dataSource, "HJ_VIP",
        "ShopProduct");
    ProgressTracer progressTracer = new ProgressTracer(RunMode.CHECK, 1);
    context.setTableMeta(tableMeta);
    context.setSourceDs(dataSource);
    context.setOnceCrawNum(200);

    SqlServerFullRecordExtractor extractor = new SqlServerFullRecordExtractor(context);
    extractor.setTracer(progressTracer);
    extractor.initContinueExtractor();
//    extractor.getFullContinueExtractor().start(); // get fetch size with blocking queue
    extractor.start();

    List<Record> records = extractor.extract();

    //    YuGongInstance instance = new YuGongInstance(context);

    YuGongContext applierContext = new YuGongContext();
    DataSource applierDataSource = dataSourceFactory.getDataSource(getMysqlConfig());
//    ProgressTracer applierProgressTracer = new ProgressTracer(RunMode.CHECK, 1);
    applierContext.setTargetDs(applierDataSource);
    applierContext.setOnceCrawNum(200);
    CheckRecordApplier applier = new CheckRecordApplier(applierContext);

    NameDataTranslator translator = new NameDataTranslator() {
      @Override
      public boolean translator(Record record) {
        if (record.getSchemaName().equals("HJ_VIP")) {
          record.setSchemaName("hj_product");
        }
        if (record.getTableName().equals("ShopProduct")) {
          record.setTableName("shop_product");
        }
        return super.translator(record);
      }
    };
    translator.setTableCaseFormatFrom(CaseFormat.UPPER_UNDERSCORE);
    translator.setTableCaseFormatTo(CaseFormat.LOWER_UNDERSCORE);
    translator.setColumnCaseFormatFrom(CaseFormat.UPPER_CAMEL);
    translator.setColumnCaseFormatTo(CaseFormat.LOWER_UNDERSCORE);

    applier.start();
    applier.apply(translator.translator(records));
    
//    extractor.start();

//    instance.setExtractor(sqlServerFullRecordExtractor);
//    instance.setApplier(applier);
//    instance.start();
  }

}