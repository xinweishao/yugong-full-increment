package com.taobao.yugong.extractor.oracle;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.sql.SqlTemplates;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.RunMode;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.IdPosition;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.common.utils.thread.NamedThreadFactory;
import com.taobao.yugong.exception.YuGongException;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * oracle单数字主键的提取
 *
 * @author agapple 2013-9-12 下午4:49:25
 */
public class OracleFullRecordExtractor extends AbstractOracleRecordExtractor {

  // private static final String FORMAT =
  // "select /*+index(t {0})*/ {1} from {2}.{3} t where {4} > ? and rownum <= ? order by {4} asc";
  private static final String FORMAT = "select * from (select {0} from {1}.{2} t where {3} > ? order by {3} asc) where rownum <= ?";
  private static final String MIN_PK_FORMAT = "select min({0}) from {1}.{2}";
  private YuGongContext context;
  private LinkedBlockingQueue<Record> queue;
  private String extractSql;
  private Thread extractorThread = null;
  private String getMinPkSql;

  public OracleFullRecordExtractor(YuGongContext context) {
    this.context = context;
  }

  public void start() {
    super.start();
    String primaryKey = context.getTableMeta().getPrimaryKeys().get(0).getName();
    String schemaName = context.getTableMeta().getSchema();
    String tableName = context.getTableMeta().getName();

    if (StringUtils.isEmpty(extractSql)) {
      // 获取索引
      // Map<String, String> index =
      // TableMetaGenerator.getTableIndex(context.getSourceDs(),
      // schemaName, tableName);
      String colStr = SqlTemplates.COMMON.makeColumn(context.getTableMeta().getColumnsWithPrimary());
      this.extractSql = new MessageFormat(FORMAT).format(new Object[]{colStr, schemaName, tableName, primaryKey});
      // logger.info("table : {} \n\t extract sql : {}",
      // context.getTableMeta().getFullName(), extractSql);
    }

    if (getMinPkSql == null && StringUtils.isNotBlank(primaryKey)) {
      this.getMinPkSql = new MessageFormat(MIN_PK_FORMAT).format(new Object[]{primaryKey, schemaName, tableName});
    }

    extractorThread = new NamedThreadFactory(this.getClass().getSimpleName() + "-"
        + context.getTableMeta().getFullName()).newThread(new ContinueExtractor(context));
    extractorThread.start();

    queue = new LinkedBlockingQueue<Record>(context.getOnceCrawNum() * 2);
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.FULLING);
  }

  public void stop() {
    super.stop();

    extractorThread.interrupt();
    try {
      extractorThread.join(2 * 1000);
    } catch (InterruptedException e) {
      // ignore
    }
    tracer.update(context.getTableMeta().getFullName(), ProgressStatus.SUCCESS);
  }

  public Position ack(List<Record> records) throws YuGongException {
    if (YuGongUtils.isNotEmpty(records)) {
      // 之前在选择Extractor时已经做过主键类型判断，一定是number类型
      Record record = records.get(records.size() - 1);
      IdPosition position = (IdPosition) context.getLastPosition();
      if (position == null || context.getRunMode() == RunMode.FULL) { // 如果是full模式，不记录历史
        position = new IdPosition();
      }
      position.setCurrentProgress(ProgressStatus.FULLING);
      List<ColumnValue> pks = record.getPrimaryKeys();
      if (YuGongUtils.isNotEmpty(pks)) {
        Object value = pks.get(0).getValue();
        if (value instanceof Number) {
          position.setId((Number) value);// 更新一下id
        }
      }
      return position;
    }

    return null;
  }

  public List<Record> extract() throws YuGongException {
    List<Record> records = Lists.newArrayListWithCapacity(context.getOnceCrawNum());
    for (int i = 0; i < context.getOnceCrawNum(); i++) {
      Record r = queue.poll();
      if (r != null) {
        records.add(r);
      } else if (status() == ExtractStatus.TABLE_END) {
        // 验证下是否已经结束了
        Record r1 = queue.poll();
        if (r1 != null) {
          records.add(r1);
        } else {
          // 已经取到低了，没有数据了
          break;
        }
      } else {
        // 没取到数据
        i--;
        continue;
      }
    }

    return records;
  }

  public class ContinueExtractor extends AbstractYuGongLifeCycle implements Runnable {

    private JdbcTemplate jdbcTemplate;
    private Object id = 0L;
    private volatile boolean running = true;

    public ContinueExtractor(YuGongContext context) {
      jdbcTemplate = new JdbcTemplate(context.getSourceDs());

      Position position = context.getLastPosition();
      if (position != null) {
        IdPosition idPosition = ((IdPosition) position);
        if (idPosition.getCurrentProgress() == ProgressStatus.FULLING) {
          id = idPosition.getId();
        }

        if (id == null) {
          id = getMinId();
        }
      } else {
        id = getMinId();
      }

      logger.info(context.getTableMeta().getFullName() + " start postion:" + id);
    }

    private Object getMinId() {
      if (jdbcTemplate != null && StringUtils.isNotBlank(getMinPkSql)) {
        Object min = jdbcTemplate.execute(getMinPkSql, new PreparedStatementCallback() {

          @Override
          public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
            ResultSet rs = ps.executeQuery();
            Object re = null;
            while (rs.next()) {
              re = rs.getObject(1);
              break;
            }
            return re;
          }
        });

        if (min != null) {
          if (min instanceof Number) {
            min = Long.valueOf(String.valueOf(min)) - 1;
          } else {
            min = "";
          }
        } else {
          if (min instanceof Number) {
            min = 0;
          } else {
            min = "";
          }
        }

        return min;
      } else {
        throw new YuGongException("jdbcTemplate or getMinPkSql is null while getMinId");
      }
    }

    public void run() {
      while (running) {
        jdbcTemplate.execute(extractSql, new PreparedStatementCallback() {

          public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
            ps.setObject(1, id);
            ps.setInt(2, context.getOnceCrawNum());
            ps.setFetchSize(200);
            ResultSet rs = ps.executeQuery();

            List<Record> result = Lists.newArrayListWithCapacity(context.getOnceCrawNum());
            while (rs.next()) {
              List<ColumnValue> cms = new ArrayList<ColumnValue>();
              List<ColumnValue> pks = new ArrayList<ColumnValue>();

              for (ColumnMeta pk : context.getTableMeta().getPrimaryKeys()) {
                ColumnValue cv = getColumnValue(rs, context.getSourceEncoding(), pk);
                pks.add(cv);

                id = cv.getValue(); // 肯定只有一个主键，更新一下
              }

              for (ColumnMeta col : context.getTableMeta().getColumns()) {
                ColumnValue cv = getColumnValue(rs, context.getSourceEncoding(), col);
                cms.add(cv);
              }

              Record re = new Record(context.getTableMeta().getSchema(),
                  context.getTableMeta().getName(),
                  pks,
                  cms);

              result.add(re);
            }

            if (result.size() < 1) {
              setStatus(ExtractStatus.TABLE_END);
              running = false;
            }

            for (Record record : result) {
              try {
                queue.put(record);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 传递
                throw new YuGongException(e);
              }
            }
            return null;
          }
        });
      }
    }
  }

  public void setExtractSql(String extractSql) {
    // if (StringUtils.isNotEmpty(extractSql) &&
    // !StringUtils.contains(extractSql, "{3}")) {
    // throw new YuGongException("extracSql is not valid . eg :" + FORMAT);
    // }
    this.extractSql = extractSql;
  }
}
