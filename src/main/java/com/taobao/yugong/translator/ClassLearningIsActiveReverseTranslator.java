package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import java.util.List;


@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClassLearningIsActiveReverseTranslator implements DataTranslator {

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
                .forEach(
                        record -> {
                            ColumnValue isDeleted = record.getColumnByName("is_deleted");
                            isDeleted.setValue(!(Boolean) isDeleted.getValue());
                        });
        return records;
    }

}
