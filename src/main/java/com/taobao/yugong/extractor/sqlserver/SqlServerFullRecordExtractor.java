package com.taobao.yugong.extractor.sqlserver;

import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.AbstractRecordExtractor;

import lombok.Setter;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class SqlServerFullRecordExtractor extends AbstractRecordExtractor {

  private final YuGongContext context;
  @Setter
  private String extractSql;

  public SqlServerFullRecordExtractor(YuGongContext context) {
    this.context = context;
  }

  @Override
  public List<Record> extract() throws YuGongException {
    return null;
  }

  @Override
  public Position ack(List<Record> records) throws YuGongException {
    return null;
  }

  public void setExtractSql(String extractSql) {
    this.extractSql = extractSql;
  }

  public String getExtractSql() {
    return extractSql;
  }

  @Override
  public void start() {
    super.start();
  }

  public class ContinueExtractor extends AbstractYuGongLifeCycle implements Runnable {

    private JdbcTemplate jdbcTemplate;
//    private Object id = 0L;
    private volatile boolean running = true;

    @Override
    public void run() {
      
    }
  }
}
