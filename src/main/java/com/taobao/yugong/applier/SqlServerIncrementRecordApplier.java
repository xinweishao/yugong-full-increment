package com.taobao.yugong.applier;

import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.model.record.SqlServerIncrementRecord;
import com.taobao.yugong.exception.YuGongException;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.util.List;


public class SqlServerIncrementRecordApplier extends AbstractRecordApplier {

  protected YuGongContext context;

  public SqlServerIncrementRecordApplier(YuGongContext context) {
    this.context = context;
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
  }

  @Override
  public void apply(List<Record> records) throws YuGongException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
    for (Record record : records) {
      if (!(record instanceof SqlServerIncrementRecord)) {
        logger.info("Record {} is not SqlServerIncrementRecord", record);
        continue;
      }
      applyOneByOne((SqlServerIncrementRecord)record, jdbcTemplate);
    }
  }

  private void applyOneByOne(SqlServerIncrementRecord records, JdbcTemplate jdbcTemplate) {
    String sql = "";
    switch (records.getOperation()) {
      case INSERT:
        sql = "INSERT ";
        break;
      case DELETE:
        break;
      case UPDATE_OLD_VALUE:
        break;
      case UPDATE_NEW_VALUE:
        break;
    }
    jdbcTemplate.execute(sql, (PreparedStatementCallback) ps-> {
      return null;
    });
    
  }
}
