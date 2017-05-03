package com.taobao.yugong.extractor;

import com.google.common.collect.Lists;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.IdPosition;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.oracle.OracleFullRecordExtractor;

import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ContinueExtractor extends AbstractYuGongLifeCycle implements Runnable {

  private OracleFullRecordExtractor oracleFullRecordExtractor;
  private JdbcTemplate jdbcTemplate;
  private Object id = 0L;
  private YuGongContext context;
  private BlockingQueue<Record> queue;
  private volatile boolean running = true;

  public ContinueExtractor(OracleFullRecordExtractor oracleFullRecordExtractor,
      YuGongContext context, BlockingQueue<Record> queue) {
    this.oracleFullRecordExtractor = oracleFullRecordExtractor;
    this.context = context;
    this.queue = queue;
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
    if (jdbcTemplate == null
        || !StringUtils.isNotBlank(oracleFullRecordExtractor.getGetMinPkSql())) {
      throw new YuGongException("jdbcTemplate or getMinPkSql is null while getMinId");
    }
    Object min = jdbcTemplate.execute(oracleFullRecordExtractor.getGetMinPkSql(),
        (PreparedStatementCallback) ps -> {
          ResultSet rs = ps.executeQuery();
          Object re = null;
          while (rs.next()) {
            re = rs.getObject(1);
            break;
          }
          return re;
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
  }

  public void run() {
    while (running) {
      jdbcTemplate.execute(oracleFullRecordExtractor.getExtractSql(),
          (PreparedStatementCallback) ps -> {
            ps.setObject(1, id);
            ps.setInt(2, context.getOnceCrawNum());
            ps.setFetchSize(200);
            ResultSet rs = ps.executeQuery();

            List<Record> result = Lists.newArrayListWithCapacity(context.getOnceCrawNum());
            while (rs.next()) {
              List<ColumnValue> cms = new ArrayList<>();
              List<ColumnValue> pks = new ArrayList<>();

              for (ColumnMeta pk : context.getTableMeta().getPrimaryKeys()) {
                ColumnValue cv = oracleFullRecordExtractor
                    .getColumnValue(rs, context.getSourceEncoding(), pk);
                pks.add(cv);

                id = cv.getValue(); // 肯定只有一个主键，更新一下
              }

              for (ColumnMeta col : context.getTableMeta().getColumns()) {
                ColumnValue cv = oracleFullRecordExtractor
                    .getColumnValue(rs, context.getSourceEncoding(), col);
                cms.add(cv);
              }

              Record re = new Record(context.getTableMeta().getSchema(),
                  context.getTableMeta().getName(),
                  pks,
                  cms);

              result.add(re);
            }

            if (result.size() < 1) {
              oracleFullRecordExtractor.setStatus(ExtractStatus.TABLE_END);
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
          });
    }
  }
}
