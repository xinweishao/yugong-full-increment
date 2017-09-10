package com.taobao.yugong.extractor.sqlserver;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.model.record.SqlServerIncrementRecord;
import com.taobao.yugong.exception.YuGongException;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;
import java.util.List;

public class SqlServerCdcExtractor extends AbstractSqlServerExtractor {

  private String cdcGetNetChangesName;

  @Override
  public void start() {
    super.start();
    String schemaName = context.getTableMeta().getSchema();
    String tableName = context.getTableMeta().getName();
    cdcGetNetChangesName = "cdc.fn_cdc_get_net_changes_" + tableName;

    String cdcExtractSql = "";
    // TODO
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.INCING);
  }

  @Override
  public List<Record> extract() throws YuGongException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getSourceDs());
    List<SqlServerIncrementRecord> records = fetchCdcRecord();
    return Lists.newArrayList();
  }
  
  public List<SqlServerIncrementRecord> fetchCdcRecord() {
    String sql = "SELECT * FROM " + cdcGetNetChangesName;
    // XXX
    return Lists.newArrayList();
  }
  
  private byte[] convertStartDateToLsn(Date date) {
    String sql = "sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time)";
    // XXX
    return null;
  }

  private byte[] convertEndDateToLsn(Date date) {
    String sql = "sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time)";
    // XXX
    return null;
  }

  @Override
  public void stop() {
    super.stop();
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
  }
}
