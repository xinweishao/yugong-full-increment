package com.taobao.yugong.extractor.sqlserver;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.sql.SqlTemplates;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.extractor.AbstractFullRecordExtractor;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Types;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class SqlServerFullRecordExtractor extends AbstractFullRecordExtractor {

  public static final String CONVERT_VARCHAR = "CONVERT(varchar(256), {0})";

  private static final String MIN_PK_FORMAT = "select min({0}) from {1}.dbo.{2}";

  //select min(pk) from (select pk=CONVERT(BIGINT, hashbytes('MD5', CONVERT(varchar(256), OrderId)+CONVERT(varchar(256), ProductId))) from ShopOrderDetail) ShopOrderDetail;
  private static final String MIN_COMPOSITE_INDEXS_FORMAT = "select min(_hashed_pk) from (select _hashed_pk=CONVERT(BIGINT, hashbytes(''MD5'', {0})) from {1}.dbo.{2}) {2}";

  private static final String DEFALT_EXTRACT_SQL_FORMAT =
      "select TOP (?) {0} from {1}.dbo.{2} where {3} > ? order by {3} asc;";

  //select *, pk=CONVERT(BIGINT, hashbytes('MD5', CONVERT(varchar(256), OrderID)+CONVERT(varchar(256), ProductID))) from TEST_MSSQL.dbo.ShopOrderDetail order by pk
  public static final String DEFAULT_EXTRACT_COMPOSITE_INDEXS_SQL_FORMAT =
          "select * from (select TOP (?) {0}, _hashed_pk=CONVERT(BIGINT, hashbytes(''MD5'', {3})) from {1}.dbo.{2}  order by _hashed_pk asc) {2} where _hashed_pk > ?";

  private static Map<String, Integer> PARAMETER_INDEX_MAP = ImmutableMap.of("id", 2, "limit", 1);

  public SqlServerFullRecordExtractor(YuGongContext context) {
    this.context = context;
  }

  @Override
  public void init() {
    super.init();
    //重新处理主键
    if(context.getIgnorePkInspection().length > 0
            && ArrayUtils.contains(context.getIgnorePkInspection(), context.getTableMeta().getName())){

      String schemaName = context.getTableMeta().getSchema();
      String tableName = context.getTableMeta().getName();

      this.getMinPkSql = MessageFormat.format(
              MIN_COMPOSITE_INDEXS_FORMAT,
              mergeHashingColumns(context.getSpecifiedPks().get(tableName)),
              schemaName,
              tableName
      );
      this.parameterIndexMap = PARAMETER_INDEX_MAP;

      //TODO: 暂不支持extractSql的自定义

      String colStr = SqlTemplates.COMMON.makeColumn(context.getTableMeta().getColumnsWithPrimary());
      this.extractSql = MessageFormat.format(
              DEFAULT_EXTRACT_COMPOSITE_INDEXS_SQL_FORMAT,
              colStr,
              schemaName,
              tableName,
              mergeHashingColumns(context.getSpecifiedPks().get(tableName))
      );

      //上下文主键替换
      List<ColumnMeta> pks = context.getTableMeta().getPrimaryKeys();
      context.getTableMeta().getColumns().addAll(pks);
      pks.clear();
      pks.add(new ColumnMeta("_hashed_pk", Types.BIGINT));

    }else{
      String primaryKey = context.getTableMeta().getPrimaryKeys().get(0).getName();
      String schemaName = context.getTableMeta().getSchema();
      String tableName = context.getTableMeta().getName();
      this.getMinPkSql = MessageFormat.format(MIN_PK_FORMAT, primaryKey, schemaName, tableName);
      this.parameterIndexMap = PARAMETER_INDEX_MAP;

      if (Strings.isNullOrEmpty(extractSql)) {
        String colStr = SqlTemplates.COMMON.makeColumn(context.getTableMeta().getColumnsWithPrimary());
        this.extractSql = MessageFormat.format(DEFALT_EXTRACT_SQL_FORMAT, colStr, schemaName,
                tableName, primaryKey);
      }
    }
    queue = new LinkedBlockingQueue<>(context.getOnceCrawNum() * 2);
  }


  /**
   * 合并需要hash的字段
   * @param columns
   * @return
   */
  private String mergeHashingColumns(String[] columns){
    //CONVERT(varchar(256), OrderID)+CONVERT(varchar(256), ProductID)
    StringBuilder convertedCols = new StringBuilder();

    for(String column : columns){
      convertedCols
              .append(MessageFormat.format(CONVERT_VARCHAR, column))
              .append("+");
    }
    return convertedCols.toString().replaceAll("\\+$", "");
  }
}
