package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import java.sql.Types;
import java.util.List;


@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClassLearningNoteInfoTranslator implements DataTranslator {

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
                            ColumnValue isActive = record.getColumnByName("is_active");
                            ColumnValue isDeleted = new ColumnValue();
                            ColumnMeta isDeletedMeta = new ColumnMeta("is_deleted", Types.BOOLEAN);
                            isDeleted.setColumn(isDeletedMeta);
                            isDeleted.setValue(!(Boolean) isActive.getValue());

                            ColumnValue secondPoint = record.getColumnByName("second_point");
                            ColumnValue isMedia = new ColumnValue();
                            ColumnMeta meta = new ColumnMeta("is_media", Types.BOOLEAN);
                            isMedia.setColumn(meta);
                            if (secondPoint.getValue() == null || secondPoint.getValue().equals(-1)) {
                                isMedia.setValue(0);
                            } else {
                                isMedia.setValue(1);
                            }
                            record.addColumn(isMedia);

                        });
        return records;
    }

}
