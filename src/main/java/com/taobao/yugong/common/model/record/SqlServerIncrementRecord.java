package com.taobao.yugong.common.model.record;

import com.taobao.yugong.common.db.meta.ColumnValue;

import lombok.Data;
import lombok.Getter;

import java.util.List;

@Data
public class SqlServerIncrementRecord extends IncrementRecord {

  private byte[] startLsn;
  private byte[] seqval;
  private CdcOperation operation;
  private byte[] updateMask;
  
  public enum CdcOperation {
    DELETE(1),
    INSERT(2),
    UPDATE_OLD_VALUE(3),
    UPDATE_NEW_VALUE(4);

    @Getter
    private int value;

    CdcOperation(int value) {
      this.value = value;
    }
  }
  
}
