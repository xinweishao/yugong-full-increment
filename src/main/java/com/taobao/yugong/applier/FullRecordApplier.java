package com.taobao.yugong.applier;

import com.google.common.collect.Lists;
import com.google.common.collect.MigrateMap;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.db.sql.SqlTemplates;
import com.taobao.yugong.common.db.sql.TypeMapping;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 全量同步appiler
 *
 * <pre>
 * 1. 行记录同步，目标库存在则更新，没有则插入
 * </pre>
 *
 * @author agapple 2013-9-23 下午5:27:02
 */
public class FullRecordApplier extends AbstractRecordApplier {

  protected static final Logger logger = LoggerFactory.getLogger(FullRecordApplier.class);
  protected Map<List<String>, TableSqlUnit> applierSqlCache;
  protected YuGongContext context;
  protected DbType sourceDbType;
  protected DbType targetDbType;
  protected boolean useMerge = false;

  public FullRecordApplier(YuGongContext context) {
    this.context = context;
  }

  public void start() {
    super.start();
    sourceDbType = YuGongUtils.judgeDbType(context.getSourceDs());
    targetDbType = YuGongUtils.judgeDbType(context.getTargetDs());
    applierSqlCache = MigrateMap.makeMap();
  }

  public void stop() {
    super.stop();
  }

  /**
   * default batch insert
   */
  public void apply(List<Record> records) throws YuGongException {
    // no one,just return
    if (YuGongUtils.isEmpty(records)) {
      return;
    }

    doApply(records);
  }

  protected void doApply(List<Record> records) {
    Map<List<String>, List<Record>> buckets = MigrateMap.makeComputingMap(
        names -> Lists.newArrayList());

    // 根据目标库的不同，划分为多个bucket
    for (Record record : records) {
      buckets.get(Arrays.asList(record.getSchemaName(), record.getTableName())).add(record);
    }

    JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getTargetDs());
    for (final List<Record> batchRecords : buckets.values()) {
      TableSqlUnit sqlUnit = getSqlUnit(batchRecords.get(0));
      if (context.isBatchApply()) {
        applierByBatch(jdbcTemplate, batchRecords, sqlUnit);
      } else {
        applyOneByOne(jdbcTemplate, batchRecords, sqlUnit);
      }
    }
  }

  /**
   * batch处理支持
   */
  protected void applierByBatch(JdbcTemplate jdbcTemplate, final List<Record> batchRecords,
      TableSqlUnit sqlUnit) {
    boolean redoOneByOne = false;
    try {
      final Map<String, Integer> indexs = sqlUnit.applierIndexs;
      jdbcTemplate.execute(sqlUnit.applierSql, (PreparedStatementCallback) ps -> {
        for (Record record : batchRecords) {
          // 先加字段，后加主键
          List<ColumnValue> cvs = record.getColumns();
          for (ColumnValue cv : cvs) {
            int type = TypeMapping.map(sourceDbType, targetDbType, cv.getColumn().getType());
            ps.setObject(getIndex(indexs, cv), cv.getValue(), type);
          }

          // 添加主键
          List<ColumnValue> pks = record.getPrimaryKeys();
          for (ColumnValue pk : pks) {
            int type = TypeMapping.map(sourceDbType, targetDbType, pk.getColumn().getType());
            ps.setObject(getIndex(indexs, pk), pk.getValue(), type);
          }

          ps.addBatch();
        }

        ps.executeBatch();
        return null;
      });
    } catch (Exception e) {
      // catch the biggest exception,no matter how, rollback it;
      redoOneByOne = true;
      // conn.rollback();
    }

    // batch cannot pass the duplicate entry exception,so
    // if executeBatch throw exception,rollback it, and
    // redo it one by one
    if (redoOneByOne) {
      applyOneByOne(jdbcTemplate, batchRecords, sqlUnit);
    }
  }

  /**
   * 一条条记录串行处理
   */
  protected void applyOneByOne(JdbcTemplate jdbcTemplate, final List<Record> records,
      TableSqlUnit sqlUnit) {
    final Map<String, Integer> indexs = sqlUnit.applierIndexs;
    jdbcTemplate.execute(sqlUnit.applierSql, (PreparedStatementCallback) ps -> {
      for (Record record : records) {
        List<ColumnValue> pks = record.getPrimaryKeys();
        // 先加字段，后加主键
        List<ColumnValue> cvs = record.getColumns();
        for (ColumnValue cv : cvs) {
          int type = TypeMapping.map(sourceDbType, targetDbType, cv.getColumn().getType());
          ps.setObject(getIndex(indexs, cv), cv.getValue(), type);
        }

        // 添加主键
        for (ColumnValue pk : pks) {
          int type = TypeMapping.map(sourceDbType, targetDbType, pk.getColumn().getType());
          ps.setObject(getIndex(indexs, pk), pk.getValue(), type);
        }

        try {
          ps.execute();
        } catch (SQLException e) {
          if (context.isSkipApplierException()) {
            logger.warn("skiped record data : " + record.toString(), e);
          } else {
            if (e.getMessage().contains("Duplicate entry")
                || e.getMessage().startsWith("ORA-00001: 违反唯一约束条件")) {
              logger.warn("skiped record data ,maybe transfer before,just continue:"
                  + record.toString());
            } else if (e.getMessage().contains("Invalid JSON text")) {
              logger.warn("skiped record data ,maybe JSON data error,just continue:"
                  + record.toString());
            } else {
              throw new SQLException("failed Record Data : " + record.toString(), e);
            }
          }
        }
      }

      return null;
    });

  }

  /**
   * 基于当前记录生成sqlUnit
   */
  protected TableSqlUnit getSqlUnit(Record record) {
    List<String> names = Arrays.asList(record.getSchemaName(), record.getTableName());
    TableSqlUnit sqlUnit = applierSqlCache.get(names);
    if (sqlUnit == null) {
      synchronized (names) {
        sqlUnit = applierSqlCache.get(names);
        if (sqlUnit == null) { // double-check
          sqlUnit = new TableSqlUnit();
          String applierSql = null;
          Table meta = TableMetaGenerator.getTableMeta(targetDbType, context.getTargetDs(),
              context.isIgnoreSchema() ? null : names.get(0),
              names.get(1));

          String[] primaryKeys = getPrimaryNames(record);
          String[] columns = getColumnNames(record);
          if (useMerge) {
            if (targetDbType == DbType.MYSQL) {
              applierSql = SqlTemplates.MYSQL.getMergeSql(meta.getSchema(),
                  meta.getName(),
                  primaryKeys,
                  columns,
                  true);
            } else if (targetDbType == DbType.DRDS) {
              applierSql = SqlTemplates.MYSQL.getMergeSql(meta.getSchema(),
                  meta.getName(),
                  primaryKeys,
                  columns,
                  false);
            } else if (targetDbType == DbType.ORACLE) {
              applierSql = SqlTemplates.ORACLE.getMergeSql(meta.getSchema(),
                  meta.getName(),
                  primaryKeys,
                  columns);
            }
          } else {
            if (targetDbType == DbType.MYSQL) {
              // 如果mysql，全主键时使用insert ignore
              applierSql = SqlTemplates.MYSQL.getInsertNomalSql(meta.getSchema(),
                  meta.getName(),
                  primaryKeys,
                  columns);
            } else if (targetDbType == DbType.SQL_SERVER) {
                applierSql = SqlTemplates.SQL_SERVER.getInsertSql(meta.getSchema(),
                    meta.getName(),
                    primaryKeys,
                    columns);
            } else {
              applierSql = SqlTemplates.ORACLE.getInsertSql(meta.getSchema(),
                  meta.getName(),
                  primaryKeys,
                  columns);
            }
          }

          int index = 1;
          Map<String, Integer> indexs = new HashMap<>();
          for (String column : columns) {
            indexs.put(column, index);
            index++;
          }

          for (String column : primaryKeys) {
            indexs.put(column, index);
            index++;
          }
          // 检查下是否少了列
          checkIndexColumns(meta, indexs);

          sqlUnit.applierSql = applierSql;
          sqlUnit.applierIndexs = indexs;
          applierSqlCache.put(names, sqlUnit);
        }
      }
    }

    return sqlUnit;
  }
}
