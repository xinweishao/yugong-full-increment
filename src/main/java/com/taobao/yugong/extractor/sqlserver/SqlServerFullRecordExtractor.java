package com.taobao.yugong.extractor.sqlserver;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.sql.SqlTemplates;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.AbstractFullRecordExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

public class SqlServerFullRecordExtractor extends AbstractFullRecordExtractor {

  private final YuGongContext context;

  private static final String MIN_PK_FORMAT = "select min({0}) from {1}.dbo.{2}";
  private static final String DEFALT_EXTRACT_SQL_FORMAT =
      "select TOP (?) {0} from {1}.dbo.{2} where {3} > ? order by {3} asc;";
  //      "select * from (select {0} from {1}.{2} t where {3} > ? order by {3} asc) where rownum <= ?";
  private static Map<String, Integer> PARAMTER_INDEX_MAP = ImmutableMap.of("id", 2, "limit", 1);

  public SqlServerFullRecordExtractor(YuGongContext context) {
    this.context = context;
    String primaryKey = context.getTableMeta().getPrimaryKeys().get(0).getName();
    String schemaName = context.getTableMeta().getSchema();
    String tableName = context.getTableMeta().getName();
    this.getMinPkSql = MessageFormat.format(MIN_PK_FORMAT, primaryKey, schemaName, tableName);
    this.parameterIndexMap = PARAMTER_INDEX_MAP;

    if (Strings.isNullOrEmpty(extractSql)) {
      String colStr = SqlTemplates.COMMON.makeColumn(context.getTableMeta().getColumnsWithPrimary());
      this.extractSql = MessageFormat.format(DEFALT_EXTRACT_SQL_FORMAT, colStr, schemaName,
          tableName, primaryKey);
    }
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
