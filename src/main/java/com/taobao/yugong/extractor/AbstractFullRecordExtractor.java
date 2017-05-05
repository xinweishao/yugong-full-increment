package com.taobao.yugong.extractor;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;

import lombok.Getter;
import lombok.Setter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public abstract class AbstractFullRecordExtractor extends AbstractRecordExtractor {
  @Getter
  @Setter
  protected String getMinPkSql;
  @Getter
  @Setter
  protected String extractSql;
  @Getter
  protected Map<String, Integer> parameterIndexMap;

  public abstract ColumnValue getColumnValue(ResultSet rs, String encoding, ColumnMeta col) throws
      SQLException;

  public String getExtractSql() {
    return extractSql;
  }

  public void setExtractSql(String extractSql) {
    this.extractSql = extractSql;
  }
}
