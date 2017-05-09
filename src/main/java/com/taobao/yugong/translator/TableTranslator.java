package com.taobao.yugong.translator;

import com.taobao.yugong.common.model.record.Record;

public interface TableTranslator {

  public Record translator(Record record);
}
