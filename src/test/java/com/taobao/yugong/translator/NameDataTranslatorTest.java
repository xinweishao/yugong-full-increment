package com.taobao.yugong.translator;

import com.google.common.base.CaseFormat;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class NameDataTranslatorTest {
  @Test
  public void tableCaseConvert() throws Exception {
    NameDataTranslator translator = new NameDataTranslator();
    translator.setTableCaseFormatFrom(CaseFormat.UPPER_UNDERSCORE);
    translator.setTableCaseFormatTo(CaseFormat.LOWER_UNDERSCORE);
    translator.setColumnCaseFormatFrom(CaseFormat.UPPER_CAMEL);
    translator.setColumnCaseFormatTo(CaseFormat.LOWER_UNDERSCORE);
    Assert.assertEquals("hj_vip", translator.tableCaseConvert("HJ_VIP"));
    Assert.assertEquals("product_id", translator.columnCaseConvert("ProductID"));
  }

  @Test
  public void columnCaseConvert() throws Exception {
  }

}