package com.taobao.yugong.extractor.sqlserver;

import com.google.common.collect.Lists;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.IncrementOpType;
import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;

import org.joda.time.DateTime;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;

public class SqlServerCdcExtractor extends AbstractSqlServerExtractor {

  public static final int CDC_MIN_DURATION = 10 * 60;
  private final String cdcGetNetChangesName;
  private final String schemaName;
  private final String tableName;
  private Table tableMeta;
  private List<ColumnMeta> primaryKeyMetas;
  private List<ColumnMeta> columnsMetas;
  private YuGongContext context;
  private DateTime start;
  private int noUpdateSleepTime;
  private int stepTime;

  public SqlServerCdcExtractor(YuGongContext context, DateTime start, int noUpdateSleepTime,
      int stepTime) {
    this.context = context;
    this.schemaName = context.getTableMeta().getSchema();
    this.tableName = context.getTableMeta().getName();
    this.cdcGetNetChangesName = "cdc.fn_cdc_get_all_changes_" + tableName;
    this.start = start;
    this.noUpdateSleepTime = noUpdateSleepTime;
    this.stepTime = stepTime;
  }

  @Override
  public void start() {
    super.start();
    tableMeta = context.getTableMeta();
    primaryKeyMetas = tableMeta.getPrimaryKeys();
    columnsMetas = tableMeta.getColumns();
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.INCING);
  }

  @Override
  public List<Record> extract() {
    DateTime now = DateTime.now();
    DateTime end = start.plusSeconds(stepTime);
    if (end.isAfter(now)) {
      setStatus(ExtractStatus.CATCH_UP);
      tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
      try {
        Thread.sleep(noUpdateSleepTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();// 传递下去
        return Lists.newArrayList();
      }
      return Lists.newArrayList();
    }

    JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getSourceDs());
    List<IncrementRecord> records = Lists.newArrayList();
    try {
      records = fetchCdcRecord(jdbcTemplate, primaryKeyMetas,
          columnsMetas, start, end);
    }
    catch (BadSqlGrammarException e) {
      if (e.getCause().getMessage().equals("An insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_ ... .")) {
        logger.info("An insufficient number counter, ignore");
      } else {
        logger.error("message is: {}", e.getMessage());
        throw new YuGongException(e);
      }
    }
    start = end;

    return (List<Record>) (List<? extends Record>) records;
  }

  @Override
  public Position ack(List<Record> records) {
    return null;
  }

  List<IncrementRecord> fetchCdcRecord(JdbcTemplate jdbcTemplate,
      List<ColumnMeta> primaryKeysM, List<ColumnMeta> columnsM, DateTime start, DateTime end)
  throws BadSqlGrammarException {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String sql = String.format(
        "DECLARE @begin_time datetime, @end_time datetime, @begin_lsn binary(10), @end_lsn binary(10);\n"
            + "SET @begin_time = '%s';\n"
            + "SET @end_time   = '%s';\n"
            + "SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @begin_time);\n"
            + "SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than', @end_time);\n"
            + "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_%s(@begin_lsn, @end_lsn, 'all');\n",
        format.format(start.toDate()),
        format.format(end.toDate()),
        tableName
    );
    List<IncrementRecord> records = Lists.newArrayList();
    jdbcTemplate.execute(sql, (PreparedStatement ps) -> {
      ResultSet resultSet = ps.executeQuery();
      while (resultSet.next()) {
        List<ColumnValue> columnValues = Lists.newArrayList();
        List<ColumnValue> primaryKeys = Lists.newArrayList();
        for (ColumnMeta primaryKey : primaryKeysM) {
          ColumnValue columnValue = YuGongUtils.getColumnValue(resultSet, null, primaryKey);
          primaryKeys.add(columnValue);
        }

        for (ColumnMeta column : columnsM) {
          ColumnValue columnValue = YuGongUtils.getColumnValue(resultSet, null, column);
          columnValues.add(columnValue);
        }

        int operationValue = (Integer)(YuGongUtils.getColumnValue(resultSet, null,
            new ColumnMeta("__$operation", Types.INTEGER)).getValue());
        Optional<IncrementOpType> operation = IncrementOpType.ofSqlServerCdc(operationValue);
        if (!operation.isPresent()) {
          continue;
        }
        IncrementRecord record = new IncrementRecord(
            context.getTableMeta().getSchema(),
            context.getTableMeta().getName(), primaryKeys, columnValues, operation.get());
        records.add(record);
      }

      return null;
    });
    return records;
  }

  @Override
  public void stop() {
    super.stop();
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
  }

}
