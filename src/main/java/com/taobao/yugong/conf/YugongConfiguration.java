package com.taobao.yugong.conf;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Next generation for yugong configuration, based YAML
 */
@Data
public class YugongConfiguration {
  private Datebase database;
  private Table table;
  private Extractor extractor;
  private Appiler appiler;
  private Translators translators;
}
