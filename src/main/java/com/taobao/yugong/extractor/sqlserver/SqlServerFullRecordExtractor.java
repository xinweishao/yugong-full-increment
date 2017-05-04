package com.taobao.yugong.extractor.sqlserver;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.AbstractFullRecordExtractor;

import lombok.Getter;
import lombok.Setter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

public class SqlServerFullRecordExtractor extends AbstractFullRecordExtractor {

  private final YuGongContext context;
  private String extractSql;

  private static final String MIN_PK_FORMAT = "select min({0}) from {1}.{2}";

  public SqlServerFullRecordExtractor(YuGongContext context) {
    this.context = context;
    String primaryKey = context.getTableMeta().getPrimaryKeys().get(0).getName();
    String schemaName = context.getTableMeta().getSchema();
    String tableName = context.getTableMeta().getName();
    this.getMinPkSql = MessageFormat.format(MIN_PK_FORMAT, primaryKey, schemaName, tableName);
  }

  @Override
  public List<Record> extract() throws YuGongException {
    return null;
  }

  @Override
  public Position ack(List<Record> records) throws YuGongException {
    return null;
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public ColumnValue getColumnValue(ResultSet rs, String encoding, ColumnMeta col)
      throws SQLException {
    Object value = rs.getObject(col.getName());
    return new ColumnValue(col.clone(), value);
  }
}
