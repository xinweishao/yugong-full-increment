package com.taobao.yugong.translator;

public enum RouteMapType {
//  USER_NAME("USER_NAME"),
//  EMAIL("EMAIL"),
//  MOBILE("MOBILE");
    USER_NAME(1),
    EMAIL(2),
    MOBILE(3);

  private final int value;

  RouteMapType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
