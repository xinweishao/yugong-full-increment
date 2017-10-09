package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.modules.pass.UserRouterMapUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class UserRouterMapShardingTranslator implements DataTranslator {

  @Override
  public String translatorSchema() {
    return null;
  }

  @Override
  public String translatorTable() {
    return null;
  }

  @Override
  public boolean translator(Record record) {
    return true;
  }

  @VisibleForTesting
  Record newRouteMapRecord(RouteMapType type, String input, int userId) {
    return UserRouterMapUtil.newRouteMapRecord(type, input, userId);
  }

  @VisibleForTesting
  Record newUserNameRecord(Record record) {
    ColumnValue inputColumn = record.getColumnByName("UserName");
    ColumnValue userIdColumn = record.getColumnByName("UserID");
    return UserRouterMapUtil.newRouteMapRecord(RouteMapType.USER_NAME, (String) inputColumn.getValue(),
        (int) userIdColumn.getValue());
  }

  @VisibleForTesting
  Optional<Record> newUserEmailRecord(Record record) {
    ColumnValue inputColumn = record.getColumnByName("UserEmail");
    if (inputColumn == null) {
      return Optional.empty();
    }
    if (inputColumn.getValue() == null) {
      return Optional.empty();
    }
    if (Strings.isNullOrEmpty((String)inputColumn.getValue())) {
      return Optional.empty();
    }
    ColumnValue userIdColumn = record.getColumnByName("UserID");
    return Optional.of(UserRouterMapUtil.newRouteMapRecord(RouteMapType.EMAIL, (String) inputColumn.getValue(),
        (int) userIdColumn.getValue()));
  }

  @Override
  public List<Record> translator(List<Record> records) {
    ArrayList<Record> newRecords = Lists.newArrayList();
    records.forEach(record -> {
      newRecords.add(newUserNameRecord(record));
      Optional<Record> emailRecordOpt = newUserEmailRecord(record);
      emailRecordOpt.ifPresent(newRecords::add);
    });
    newRecords.addAll(records);
    return newRecords;
  }

}
