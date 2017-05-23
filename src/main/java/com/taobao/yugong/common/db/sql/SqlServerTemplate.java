package com.taobao.yugong.common.db.sql;

import java.util.List;

public class SqlServerTemplate extends SqlTemplate {
  
  @Override
  public String getInsertSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("SET IDENTITY_INSERT %s ON;", tableName));
    sql.append("INSERT INTO ").append(makeFullName("dbo", tableName)).append("(");
    String[] allColumns = buildAllColumns(pkNames, columnNames);
    int size = allColumns.length;
    for (int i = 0; i < size; i++) {
      sql.append(getColumnName(allColumns[i])).append(splitCommea(size, i));
    }

    sql.append(") VALUES (");
    makeColumnQuestions(sql, allColumns);
    sql.append(")");
    return sql.toString().intern();
  }

  @Override
  public String getSelectSql(String schemaName, String tableName, List<String> pkNames, List<String> colNames) {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ");
    String[] allColumns = buildAllColumns(pkNames.toArray(new String[0]),
        colNames.toArray(new String[0]));
    int size = allColumns.length;
    for (int i = 0; i < size; i++) {
      sql.append(getColumnName(allColumns[i])).append(splitCommea(size, i));
    }

    sql.append(" FROM ").append(makeFullName("dbo", tableName)).append(" WHERE ( ");
    if (pkNames.size() > 0) { // 可能没有主键
      makeColumnEquals(sql, pkNames.toArray(new String[0]), "AND");
    } else {
      makeColumnEquals(sql, colNames.toArray(new String[0]), "AND");
    }
    sql.append(" ) ");
    return sql.toString().intern();
  }
  
}
