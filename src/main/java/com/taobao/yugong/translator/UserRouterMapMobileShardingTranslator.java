package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.modules.pass.UserRouterMapUtil;

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class UserRouterMapMobileShardingTranslator implements DataTranslator {

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
  Optional<Record> newMobileNumRecord(Record record) {
    ColumnValue inputColumn = record.getColumnByName("MobileNum");
    ColumnValue isBindedColumn = record.getColumnByName("IsBinded");
    if (isBindedColumn == null) {
      return Optional.empty();
    }
    if (isBindedColumn.getValue() == null) {
      return Optional.empty();
    }
    if (!(boolean)isBindedColumn.getValue()) {
      return Optional.empty();
    }
    if (inputColumn == null) {
      return Optional.empty();
    }
    if (inputColumn.getValue() == null) {
      return Optional.empty();
    }
    ColumnValue userIdColumn = record.getColumnByName("UserId");
    return Optional.of(UserRouterMapUtil.newRouteMapRecord(RouteMapType.MOBILE, (String) inputColumn.getValue(),
        (int) userIdColumn.getValue()));
  }

  @Override
  public List<Record> translator(List<Record> records) {
    ArrayList<Record> newRecords = Lists.newArrayList();
    records.forEach(record -> {
      Optional<Record> mobileNumRecordOpt = newMobileNumRecord(record);
      mobileNumRecordOpt.ifPresent(newRecords::add);
    });
    newRecords.addAll(records);
    return newRecords;
  }

}
