package com.taobao.yugong.common.db.sql;

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
}
