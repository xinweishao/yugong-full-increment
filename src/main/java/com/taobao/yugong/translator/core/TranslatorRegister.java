package com.taobao.yugong.translator.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableMap;
import com.taobao.yugong.conf.TranslatorConf;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.translator.ColumnFixDataTranslator;
import com.taobao.yugong.translator.DataTranslator;
import com.taobao.yugong.translator.NameStyleDataTranslator;
import com.taobao.yugong.translator.NameTableMetaTranslator;
import com.taobao.yugong.translator.TableMetaTranslator;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class TranslatorRegister {
  private static YAMLMapper MAPPER = new YAMLMapper();

  //  @Getter
  private static Map<String, Class<?>> tableMetaTranslatorRegister = ImmutableMap.<String, Class<?>>builder()
      .put(NameTableMetaTranslator.class.getCanonicalName(), NameTableMetaTranslator.class)
      .build();

  private static Map<String, Class<?>> dataTranslatorRegister = ImmutableMap.<String, Class<?>>builder()
      .put(NameStyleDataTranslator.class.getCanonicalName(), NameStyleDataTranslator.class)
      .put(ColumnFixDataTranslator.class.getCanonicalName(), ColumnFixDataTranslator.class)
      .build();


  public static TableMetaTranslator newTableMetaTranslator(TranslatorConf conf) {
    Class<TableMetaTranslator> clazz = (Class<TableMetaTranslator>) tableMetaTranslatorRegister
        .get(conf.getClazz());
    if (clazz == null) {
      throw new YuGongException("Cannot read value of Translator: {}", conf.getClazz());
    }
    TableMetaTranslator translator = null;
    String confYaml;
    try {
      confYaml = MAPPER.writeValueAsString(conf.getProperties());
    } catch (JsonProcessingException e) {
      throw new YuGongException(e);
    }
    try {
      translator = MAPPER.readValue(confYaml, clazz);
    } catch (IOException e) {
      log.error("Cannot read value of Translator: {}", conf.getClazz());
    }

    return translator;
  }

  public static DataTranslator newDataTranslator(TranslatorConf conf) {
    Class<DataTranslator> clazz = (Class<DataTranslator>) dataTranslatorRegister
        .get(conf.getClazz());
    if (clazz == null) {
      throw new YuGongException("Cannot read value of Translator: {}", conf.getClazz());
    }
    DataTranslator translator = null;
    String confYaml;
    try {
      confYaml = MAPPER.writeValueAsString(conf.getProperties());
    } catch (JsonProcessingException e) {
      throw new YuGongException(e);
    }
    try {
      translator = MAPPER.readValue(confYaml, clazz);
    } catch (IOException e) {
      log.error("Cannot read value of Translator: {}", conf.getClazz());
    }

    return translator;
  }

}
