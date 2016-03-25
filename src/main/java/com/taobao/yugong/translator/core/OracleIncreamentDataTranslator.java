package com.taobao.yugong.translator.core;

import java.util.List;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.IncrementOpType;
import com.taobao.yugong.common.model.record.OracleIncrementRecord;
import com.taobao.yugong.common.model.record.OracleIncrementRecord.DiscardType;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.AbstractDataTranslator;

/**
 * 要忽略oracle基于mlog增量的几种类型
 * 
 * @author agapple 2013-9-27 下午10:57:49
 */
public class OracleIncreamentDataTranslator extends AbstractDataTranslator {

    public List<Record> translator(List<Record> records) {
        List<Record> result = Lists.newArrayList();
        for (Record record : records) {
            if (translator(record)) {
                // 过滤完到这一步的
                // I类型,可能出现拆分字段变更,比如I/U两次,I也能看到拆分字段变更
                // U类型,可能出现拆分字段变更,比如I/U,U/U
                // D类型,数据库里不会有记录,有记录的话应该是被忽略了
                if (record instanceof OracleIncrementRecord) {
                    OracleIncrementRecord oracleRecord = (OracleIncrementRecord) record;
                    List<ColumnValue> beforeExtColumns = oracleRecord.getBeforeExtColumns();
                    List<ColumnValue> afterExtColumns = oracleRecord.getAfterExtColumns();
                    if (beforeExtColumns != null && !beforeExtColumns.isEmpty()) {
                        // 发生拆分建变化
                        OracleIncrementRecord newOracleRecord = (OracleIncrementRecord) record.clone();
                        record.getPrimaryKeys().addAll(beforeExtColumns);
                        oracleRecord.setOpType(IncrementOpType.D); // 将老记录设置为DELETE
                        newOracleRecord.getPrimaryKeys().addAll(afterExtColumns); // 新纪录保持为INSERT/UPDATE
                        // 添加记录
                        result.add(record);
                        result.add(newOracleRecord);
                    } else {
                        // 拆分字段添加为主键,按照主键标准进行处理
                        record.getPrimaryKeys().addAll(afterExtColumns);
                        result.add(record);
                    }
                } else {
                    result.add(record);
                }
            }
        }

        return result;
    }

    public boolean translator(Record record) {
        if (record instanceof OracleIncrementRecord) {
            DiscardType discardType = ((OracleIncrementRecord) record).getDiscardType();
            return discardType.isNotDiscard();// 返回true代表需要同步
        } else {
            return super.translator(record);
        }
    }
}
