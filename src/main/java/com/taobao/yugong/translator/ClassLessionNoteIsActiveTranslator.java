package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import java.util.List;


@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClassLessionNoteIsActiveTranslator implements DataTranslator {

  @Override
  public String translatorSchema() {
    return null;
  }

  @Override
  public String translatorTable() {
    return null;
  }

  @Override
  public boolean translator(Record record) {
    return true;
  }

  @Override
  public List<Record> translator(List<Record> records) {
    records
        .stream()
        .filter(record -> record.getColumnByName("is_active") != null)
        .forEach(
            record -> {
              ColumnValue isActive = record.getColumnByName("is_active");
              isActive.setValue(!(Boolean) isActive.getValue());
            });
    return records;
  }

}
