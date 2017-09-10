package com.taobao.yugong.extractor.sqlserver;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.model.record.SqlServerIncrementRecord;
import com.taobao.yugong.exception.YuGongException;

import java.util.List;

public class SqlServerCdcExtractor extends AbstractSqlServerExtractor {

  @Override
  public void start() {
    super.start();

    String cdcExtractSql = "";
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.INCING);
  }

  @Override
  public List<Record> extract() throws YuGongException {
    List<SqlServerIncrementRecord> list = Lists.newArrayList();
    List<? extends Record> records2 = list;
    return (List<Record>) records2;
  }

  @Override
  public void stop() {
    super.stop();
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
  }
}
