package com.taobao.yugong.translator;

import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.model.record.Record;

public interface TableMetaTranslator {

  public void translator(Table table);
}
