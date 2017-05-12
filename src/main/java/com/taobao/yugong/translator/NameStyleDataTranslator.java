package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.taobao.yugong.common.model.record.Record;

import lombok.Getter;
import lombok.Setter;

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class NameStyleDataTranslator extends AbstractDataTranslator {

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

  public NameStyleDataTranslator() {
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
      input = input.replace("IP", "Ip"); // ugly solution
    }
    return columnCaseFormatFrom.to(columnCaseFormatTo, input);
  }

  @Override
  public boolean translator(Record record) {
    record.setTableName(tableCaseConvert(record.getTableName()));
    if (columnCaseFormatFrom != null && columnCaseFormatTo != null) {
      record.getColumns().forEach(x -> {
        x.getColumn().setName(columnCaseConvert(x.getColumn().getName()));
      });
      record.getPrimaryKeys().forEach(x -> {
        x.getColumn().setName(columnCaseConvert(x.getColumn().getName()));
      });
    }
    return super.translator(record);
  }
}
