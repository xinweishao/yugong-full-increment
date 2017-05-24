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
  
  private final ColumnTranslator translator;

  public ColumnFixDataTranslator() {
    translator = new ColumnTranslator();
    super.setTranslator(translator);
  }
  
  public void setColumnAlias(Map<String, Set<String>> columnAlias) {
    this.translator.setColumnAlias(columnAlias);
  }

  public Map<String, Set<String>> getColumnAlias() {
    return this.translator.getColumnAlias();
  }

  public void setColumnReplace(Map<String, String> columnReplace) {
    this.translator.setColumnReplace(columnReplace);
  }

  public Map<String, String> getColumnReplace() {
    return this.translator.getColumnReplace();
  }

  public void setExcludeColumns(Collection<String> excludeAlias) {
    this.translator.setExcludeColumns(excludeAlias);
  }

  public Collection<String> getExcludeColumns() {
    return this.translator.getExcludeColumns();
  }
  
}

