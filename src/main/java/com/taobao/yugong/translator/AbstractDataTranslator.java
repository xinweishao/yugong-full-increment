package com.taobao.yugong.translator;

import java.util.List;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.model.record.Record;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class AbstractDataTranslator implements DataTranslator {

    public boolean translator(Record record) {
        return true;
    }

    public List<Record> translator(List<Record> records) {
        List<Record> result = Lists.newArrayList();
        for (Record record : records) {
            if (translator(record)) {
                result.add(record);
            }
        }

        return result;
    }
}
