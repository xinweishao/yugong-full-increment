package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
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
  int calculateShardingKey(String input) {
    String sha1 = DigestUtils.sha1Hex(input);
    BigInteger sharding = new BigInteger(
        sha1.substring(0, sha1.length() < 16 ? sha1.length() : 16), 16);
    return sharding.mod(BigInteger.valueOf(64)).intValue();
  }

  @VisibleForTesting
  Record newRouteMapRecord(RouteMapType type, String input, int userId) {
    ColumnMeta idColumn = new ColumnMeta("Id", Types.INTEGER);
    ColumnMeta contentColumn = new ColumnMeta("Content", Types.VARCHAR);
    ColumnMeta typeColumn = new ColumnMeta("Type", Types.INTEGER);
    ColumnMeta userIdColumn = new ColumnMeta("User_Id", Types.INTEGER);
    ColumnMeta createdColumn = new ColumnMeta("Created", Types.DATE);

    List<ColumnValue> primaryKeys = Lists.newArrayList();
    List<ColumnValue> columns = Lists.newArrayList(
        new ColumnValue(contentColumn, input),
        new ColumnValue(typeColumn, type.getValue()),
        new ColumnValue(userIdColumn, userId),
        new ColumnValue(createdColumn, new Date())
    );
    Record record = new Record();
    record.setTableName("LoginRouteMap_" + calculateShardingKey(input));
    record.setPrimaryKeys(primaryKeys);
    record.setColumns(columns);
    return record;
  }

  @VisibleForTesting
  Record newUserNameRecord(Record record) {
    ColumnValue inputColumn = record.getColumnByName("UserName");
    ColumnValue userIdColumn = record.getColumnByName("UserID");
    return newRouteMapRecord(RouteMapType.USER_NAME, (String) inputColumn.getValue(),
        (int) userIdColumn.getValue());
  }

  @VisibleForTesting
  Optional<Record> newUserEmailRecord(Record record) {
    ColumnValue inputColumn = record.getColumnByName("UserEmail");
    if (inputColumn == null) {
      return Optional.empty();
    }
    ColumnValue userIdColumn = record.getColumnByName("UserID");
    return Optional.of(newRouteMapRecord(RouteMapType.EMAIL, (String) inputColumn.getValue(),
        (int) userIdColumn.getValue()));
  }

  @VisibleForTesting
  Optional<Record> newMobileNumRecord(Record record) {
    ColumnValue inputColumn = record.getColumnByName("Phone"); // XXX
    if (inputColumn == null) {
      return Optional.empty();
    }
    if (inputColumn.getValue() == null) {
      return Optional.empty();
    }
    ColumnValue userIdColumn = record.getColumnByName("UserID");
    return Optional.of(newRouteMapRecord(RouteMapType.MOBILE, (String) inputColumn.getValue(),
        (int) userIdColumn.getValue()));
  }

  @Override
  public List<Record> translator(List<Record> records) {
    ArrayList<Record> newRecords = Lists.newArrayList();
    records.forEach(record -> {
      newRecords.add(newUserNameRecord(record));
      Optional<Record> emailRecordOpt = newUserEmailRecord(record);
      emailRecordOpt.ifPresent(newRecords::add);
      Optional<Record> mobileNumRecordOpt = newMobileNumRecord(record);
      mobileNumRecordOpt.ifPresent(newRecords::add);
    });
    newRecords.addAll(records);
    return newRecords;
  }

}
