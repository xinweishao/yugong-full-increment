package com.taobao.yugong.conf;

import lombok.Data;

/**
 * Next generation for yugong configuration, based YAML
 */
@Data
public class YugongConfiguration {
  private Datebases databases;
  private Table table;
  private Extractor extractor;
  private Appiler appiler;
  private Translators translators;
}
