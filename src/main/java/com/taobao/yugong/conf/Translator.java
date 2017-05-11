package com.taobao.yugong.conf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import lombok.Data;

import java.util.Map;


@JsonNaming(value = PropertyNamingStrategy.SnakeCaseStrategy.class)
@Data
public class Translator {
  @JsonProperty("class")
  String clazz;
  Map<String, Object> properties;
}
