package com.taobao.yugong.extractor.sqlserver;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.model.record.SqlServerIncrementRecord;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;

import org.joda.time.DateTime;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class SqlServerCdcExtractor extends AbstractSqlServerExtractor {

  private final String cdcGetNetChangesName;
  private final String schemaName;
  private final String tableName;
  private Table tableMeta;
  private List<ColumnMeta> primaryKeyMetas;
  private List<ColumnMeta> columnsMetas;
  private YuGongContext context;
  private DateTime start;

  public SqlServerCdcExtractor(YuGongContext context) {
    this.context = context;
    schemaName = context.getTableMeta().getSchema();
    tableName = context.getTableMeta().getName();
    cdcGetNetChangesName = "cdc.fn_cdc_get_all_changes_" + tableName;
    start = new DateTime(2017, 9, 11, 14, 3, 0); // XXX
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
  public List<Record> extract() throws YuGongException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getSourceDs());
    DateTime end = start.plusSeconds(context.getOnceCrawNum());
    List<SqlServerIncrementRecord> records = fetchCdcRecord(jdbcTemplate, primaryKeyMetas,
        columnsMetas, start, end);
    start = end;
    
    return (List<Record>) (List<? extends Record>) records;
  }

  @Override
  public Position ack(List<Record> records) throws YuGongException {
    return null;
  }

  List<SqlServerIncrementRecord> fetchCdcRecord(JdbcTemplate jdbcTemplate,
      List<ColumnMeta> primaryKeysM, List<ColumnMeta> columnsM, DateTime start, DateTime end) {
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
    List<SqlServerIncrementRecord> records = Lists.newArrayList();
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

        SqlServerIncrementRecord.CdcOperation operation = SqlServerIncrementRecord.CdcOperation
            .of((Integer)YuGongUtils.getColumnValue(
                resultSet, null, new ColumnMeta("__$operation", Types.INTEGER)).getValue());
        SqlServerIncrementRecord record = new SqlServerIncrementRecord(
            context.getTableMeta().getSchema(),
            context.getTableMeta().getName(), primaryKeys, columnValues, operation);
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
