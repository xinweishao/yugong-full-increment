package com.taobao.yugong.translator;

import com.taobao.yugong.common.model.record.Record;

import org.junit.Test;

import static org.junit.Assert.*;

public class UserRouterMapShardingTranslatorTest {
  @Test
  public void newRouteMapRecord() throws Exception {
    UserRouterMapShardingTranslator translator = new UserRouterMapShardingTranslator();
    Record record = translator.newRouteMapRecord(RouteMapType.USER_NAME, "hello", 123);
    assertEquals("hello", record.getColumnByName("Content"));
  }

}