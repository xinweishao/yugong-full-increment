package com.taobao.yugong.extractor;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;

import lombok.Getter;
import lombok.Setter;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class AbstractFullRecordExtractor extends AbstractRecordExtractor {
  @Getter
  protected String getMinPkSql;
  @Setter
  @Getter
  protected String extractSql;

  public abstract ColumnValue getColumnValue(ResultSet rs, String encoding, ColumnMeta col) throws
      SQLException;
}
