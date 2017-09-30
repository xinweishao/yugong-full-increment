package com.taobao.yugong.translator;

import com.taobao.yugong.common.model.record.Record;

import org.junit.Test;

import static org.junit.Assert.*;

public class UserRouterMapShardingTranslatorTest {
  
  @Test
  public void calculateShardingKey() throws Exception {
    UserRouterMapShardingTranslator translator = new UserRouterMapShardingTranslator();
    assertEquals(32, translator.calculateShardingKey("1122334455"));
    assertEquals(35, translator.calculateShardingKey("aaa_kao"));
    assertEquals(34, translator.calculateShardingKey("hello"));
  }

  @Test
  public void newRouteMapRecord() throws Exception {
    UserRouterMapShardingTranslator translator = new UserRouterMapShardingTranslator();
    Record record = translator.newRouteMapRecord(RouteMapType.USER_NAME, "hello", 123);
    assertEquals("hello", record.getColumnByName("Content").getValue());
  }

}