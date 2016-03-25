package com.taobao.yugong.common.model.record;

import java.util.List;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnValue;

/**
 * oracle的数据记录，带着rowId唯一标示
 * 
 * @author agapple 2013-9-16 下午4:11:05
 */
public class OracleIncrementRecord extends IncrementRecord {

    private ColumnValue       rowId;
    private DiscardType       discardType      = DiscardType.NONE;
    private List<ColumnValue> afterExtColumns  = Lists.newArrayList(); // 当前行记录里的最新值
    private List<ColumnValue> beforeExtColumns = Lists.newArrayList(); // mlog里记录的扩展列,不能和主键混用

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

    public List<ColumnValue> getBeforeExtColumns() {
        return beforeExtColumns;
    }

    public void setBeforeExtColumns(List<ColumnValue> beforeExtColumns) {
        this.beforeExtColumns = beforeExtColumns;
    }

    public List<ColumnValue> getAfterExtColumns() {
        return afterExtColumns;
    }

    public void setAfterExtColumns(List<ColumnValue> afterExtColumns) {
        this.afterExtColumns = afterExtColumns;
    }

}
