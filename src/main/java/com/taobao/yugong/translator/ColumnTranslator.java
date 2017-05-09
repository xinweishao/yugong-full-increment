package com.taobao.yugong.translator;

import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnTranslator implements TableTranslator {

  protected Collection<String> includeColumns = new HashSet<>();
  protected Collection<String> excludeColumns = new HashSet<>();
  protected Collection<String> checked = new HashSet<>();
  protected Collection<String> notChecked = new HashSet<>();
  protected Map<String, Set<String>> columnAlias = new HashMap<>();


  /**
   * 包含的同步列
   */
  public ColumnTranslator include(String... columns) {
    for (String column : columns) {
//      includeColumns.add(StringUtils.upperCase(column));
      includeColumns.add(column);
    }
    return this;
  }

  /**
   * 排除的同步列
   */
  public ColumnTranslator exclude(String... columns) {
    for (String column : columns) {
//      excludeColumns.add(StringUtils.upperCase(column));
      excludeColumns.add(column);
    }
    return this;
  }

  /**
   * 需要检查的字段
   */
  public ColumnTranslator check(String... columns) {
    for (String column : columns) {
      checked.add(StringUtils.upperCase(column));
    }
    return this;
  }

  /**
   * 不需要检查的字段
   */
  public ColumnTranslator notCheck(String... columns) {
    for (String column : columns) {
      notChecked.add(StringUtils.upperCase(column));
    }
    return this;
  }

  /**
   * 别名定义
   */
  public ColumnTranslator alias(String srcColumn, String targetColumn) {
    String sourceColumn = StringUtils.upperCase(srcColumn);
    Set<String> targetColumnSet = columnAlias.get(sourceColumn);
    if (targetColumnSet == null) {
      targetColumnSet = new HashSet<>(2);
      columnAlias.put(sourceColumn, targetColumnSet);
    }
    targetColumnSet.add(StringUtils.upperCase(targetColumn));
    return this;
  }

  public Record translator(Record record) {
    if (excludeColumns != null && !excludeColumns.isEmpty()) {
      // 处理列排除
      for (String excludeColumn : excludeColumns) {
        record.removeColumnByName(excludeColumn);
      }
    }

    if (includeColumns != null && !includeColumns.isEmpty()) {
      // 检查一下所有列是否存在
      for (String name : includeColumns) {
        record.getColumnByName(name);
      }

      // 删除掉不需要的列
      List<ColumnValue> pks = record.getPrimaryKeys();
      List<ColumnValue> columns = record.getColumns();
      Set<String> allColumns = new HashSet<String>();
      for (ColumnValue pk : pks) {
        allColumns.add(StringUtils.upperCase(pk.getColumn().getName()));
      }

      for (ColumnValue column : columns) {
        allColumns.add(StringUtils.upperCase(column.getColumn().getName()));
      }

      allColumns.removeAll(includeColumns);
      for (String name : allColumns) {
        record.removeColumnByName(name);
      }
    }

    if (notChecked != null && !notChecked.isEmpty()) {
      // 处理不检查列
      for (String notCheckColumn : notChecked) {
        ColumnValue column = record.getColumnByName(notCheckColumn);
        if (column != null) {
          column.setCheck(false);
        }
      }
    }

    if (checked != null && !checked.isEmpty()) {
      // 处理检查列
      for (String checkColumn : checked) {
        ColumnValue column = record.getColumnByName(checkColumn);
        if (column != null) {
          column.setCheck(true);
        }
      }
    }

    if (columnAlias != null && !columnAlias.isEmpty()) {
      for (Map.Entry<String, Set<String>> entry : columnAlias.entrySet()) {
        String srcColumn = entry.getKey();
        Set<String> targetColumns = entry.getValue();

        ColumnValue column = record.getColumnByName(srcColumn);
        if (column != null && targetColumns.size() >= 1) {
          Iterator<String> iter = targetColumns.iterator();
          String columnName = iter.next();
          column.getColumn().setName(columnName);
          if (iter.hasNext()) {
            ColumnValue newColumn = column.clone();
            newColumn.getColumn().setName(iter.next());
            record.addColumn(newColumn);
          }
        }
      }
    }
    return record;
  }
}
