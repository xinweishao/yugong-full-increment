package com.taobao.yugong.translator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import lombok.Getter;
import lombok.Setter;

public class NameDataTranslator extends AbstractDataTranslator {

  @Getter
  @Setter
  private CaseFormat columnCaseFormatFrom;
  @Getter
  @Setter
  private CaseFormat columnCaseFormatTo;
  @Getter
  @Setter
  private CaseFormat tableCaseFormatFrom;
  @Getter
  @Setter
  private CaseFormat tableCaseFormatTo;

  public NameDataTranslator() {
    ColumnTranslator translator = new ColumnTranslator();
    super.setTranslator(translator);
  }
  
  @VisibleForTesting
  protected String tableCaseConvert(String input) {
    if (tableCaseFormatFrom == null || tableCaseFormatTo == null) {
      return input;
    }
    return tableCaseFormatFrom.to(tableCaseFormatTo, input);
  }

  @VisibleForTesting
  protected String columnCaseConvert(String input) {
    if (columnCaseFormatFrom == null || columnCaseFormatTo == null) {
      return input;
    }
    if (columnCaseFormatFrom == CaseFormat.UPPER_CAMEL) {
      input = input.replace("ID", "Id"); // ugly solution
    }
    return columnCaseFormatFrom.to(columnCaseFormatTo, input);
  }

  @Override
  public boolean translator(Record record) {
    record.setTableName(tableCaseConvert(record.getTableName()));
    if (columnCaseFormatFrom != null && columnCaseFormatTo != null) {
      for (ColumnValue column : record.getColumns()) {
        column.getColumn().setRawName(columnCaseConvert(column.getColumn().getRawName()));
        column.getColumn().setName(columnCaseConvert(column.getColumn().getName()));
      }
    }
    return super.translator(record);
  }
}
