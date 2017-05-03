package com.taobao.yugong.extractor.oracle;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.extractor.AbstractRecordExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public abstract class AbstractOracleRecordExtractor extends AbstractRecordExtractor {

  /**
   * 从oracle的resultset中得到value
   *
   * <pre>
   * 1. 对于DATE类型特殊处理成TIMESTAMP类型，否则复制过去会丢掉时间部分
   * 2.  如果为字符串类型，并且需要进行转码，那么进行编码转换。
   * </pre>
   */
  protected ColumnValue getColumnValue(ResultSet rs, String encoding, ColumnMeta col) throws SQLException {
    Object value = null;
    if (col.getType() == Types.DATE) {
      value = rs.getTimestamp(col.getName());
      col = new ColumnMeta(col.getName(), Types.TIMESTAMP);
    } else if (col.getType() == Types.TIMESTAMP) {
      value = rs.getTimestamp(col.getName());
      col = new ColumnMeta(col.getName(), Types.TIMESTAMP);
    } else if (YuGongUtils.isCharType(col.getType())) {
      // byte[] bytes = rs.getBytes(col.getName());
      // if (bytes == null) {
      // value = rs.getObject(col.getName());
      // } else {
      // try {
      // value = new String(bytes, encoding);
      // } catch (UnsupportedEncodingException e) {
      // throw new YuGongException("codec error!!", e);
      // }
      // }
      value = rs.getString(col.getName());
    } else if (YuGongUtils.isClobType(col.getType())) {
      // Clob c = rs.getClob(col.getName());
      // if (c == null) {
      // value = rs.getObject(col.getName());
      // } else {
      // InputStream is = c.getAsciiStream();
      // byte[] bb = new byte[(int) c.length()];
      // try {
      // is.read(bb);
      // } catch (IOException e) {
      // throw new SQLException("read from clob error,column:" +
      // col.getName(), e);
      // }
      //
      // try {
      // value = new String(bb, encoding);
      // } catch (UnsupportedEncodingException e) {
      // throw new RuntimeException("codec error!!", e);
      // }
      // }
      value = rs.getString(col.getName());
    } else if (YuGongUtils.isBlobType(col.getType())) {
      value = rs.getBytes(col.getName());
    } else {
      value = rs.getObject(col.getName());
    }

    // 使用clone对象，避免translator修改了引用
    return new ColumnValue(col.clone(), value);
  }
}
