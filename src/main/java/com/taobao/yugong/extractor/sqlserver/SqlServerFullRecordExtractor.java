package com.taobao.yugong.extractor.sqlserver;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.taobao.yugong.common.db.sql.SqlTemplates;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.extractor.AbstractFullRecordExtractor;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SqlServerFullRecordExtractor extends AbstractFullRecordExtractor {

  private static final String MIN_PK_FORMAT = "select min({0}) from {1}.dbo.{2}";
  private static final String DEFALT_EXTRACT_SQL_FORMAT =
      "select TOP (?) {0} from {1}.dbo.{2} where {3} > ? order by {3} asc;";
  private static Map<String, Integer> PARAMETER_INDEX_MAP = ImmutableMap.of("id", 2, "limit", 1);

  public SqlServerFullRecordExtractor(YuGongContext context) {
    this.context = context;
    String primaryKey = context.getTableMeta().getPrimaryKeys().get(0).getName();
    String schemaName = context.getTableMeta().getSchema();
    String tableName = context.getTableMeta().getName();
    this.getMinPkSql = MessageFormat.format(MIN_PK_FORMAT, primaryKey, schemaName, tableName);
    this.parameterIndexMap = PARAMETER_INDEX_MAP;

    if (Strings.isNullOrEmpty(extractSql)) {
      String colStr = SqlTemplates.COMMON.makeColumn(context.getTableMeta().getColumnsWithPrimary());
      this.extractSql = MessageFormat.format(DEFALT_EXTRACT_SQL_FORMAT, colStr, schemaName,
          tableName, primaryKey);
    }
    queue = new LinkedBlockingQueue<>(context.getOnceCrawNum() * 2);
  }

}
