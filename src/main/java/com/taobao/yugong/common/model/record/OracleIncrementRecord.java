package com.taobao.yugong.common.model.record;

import java.util.List;

import com.taobao.yugong.common.db.meta.ColumnValue;

/**
 * oracle的数据记录，带着rowId唯一标示
 * 
 * @author agapple 2013-9-16 下午4:11:05
 */
public class OracleIncrementRecord extends IncrementRecord {

    private ColumnValue       rowId;
    private DiscardType       discardType = DiscardType.NONE;

    public OracleIncrementRecord(){
        super();
    }

    public OracleIncrementRecord(String schemaName, String tableName, List<ColumnValue> primaryKeys,
                                 List<ColumnValue> columns){
        super(schemaName, tableName, primaryKeys, columns);
    }

    public ColumnValue getRowId() {
        return rowId;
    }

    public void setRowId(ColumnValue rowId) {
        this.rowId = rowId;
    }

    public DiscardType getDiscardType() {
        return discardType;
    }

    public void setDiscardType(DiscardType discardType) {
        this.discardType = discardType;
    }

    @Override
    public OracleIncrementRecord clone() {
        OracleIncrementRecord record = new OracleIncrementRecord();
        super.clone(record);
        record.setDiscardType(this.discardType);
        record.setRowId(this.rowId);
        return record;
    }

    public static enum DiscardType {
        /** 存在delete，丢弃I/U */
        DELTE_AFTER_IU,
        /** 存在I/U，丢弃delete */
        IU_AFTER_DELETE,
        /** 不丢弃 */
        NONE;

        public boolean isDiscard() {
            return !isNotDiscard();
        }

        public boolean isNotDiscard() {
            return this == DiscardType.NONE;
        }
    }

}
