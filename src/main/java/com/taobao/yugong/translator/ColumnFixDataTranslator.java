package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import lombok.Data;
import lombok.Getter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ColumnFixDataTranslator extends AbstractDataTranslator {
  
//  private Collection<String> includeColumns = new HashSet<>();
//  private Collection<String> excludeColumns = new HashSet<>();
//  private Collection<String> checked = new HashSet<>();
//  private Collection<String> notChecked = new HashSet<>();
  private final ColumnTranslator translator;

  public ColumnFixDataTranslator() {
    translator = new ColumnTranslator();
//    translator.setIncludeColumns(includeColumns);
//    translator.setExcludeColumns(excludeColumns);
//    translator.setChecked(checked);
//    translator.setNotChecked(notChecked);
    super.setTranslator(translator);
  }
  
  public void setColumnAlias(Map<String, Set<String>> columnAlias) {
    this.translator.setColumnAlias(columnAlias);
  }

  public Map<String, Set<String>> getColumnAlias() {
    return this.translator.getColumnAlias();
  }
}

