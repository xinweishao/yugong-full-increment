package com.taobao.yugong.controller;

import com.google.common.collect.Lists;
import com.taobao.yugong.applier.AllRecordApplier;
import com.taobao.yugong.applier.CheckRecordApplier;
import com.taobao.yugong.applier.FullRecordApplier;
import com.taobao.yugong.applier.IncrementRecordApplier;
import com.taobao.yugong.applier.MultiThreadCheckRecordApplier;
import com.taobao.yugong.applier.MultiThreadFullRecordApplier;
import com.taobao.yugong.applier.MultiThreadIncrementRecordApplier;
import com.taobao.yugong.applier.RecordApplier;
import com.taobao.yugong.common.YuGongConstants;
import com.taobao.yugong.common.alarm.AlarmService;
import com.taobao.yugong.common.alarm.LogAlarmService;
import com.taobao.yugong.common.alarm.MailAlarmService;
import com.taobao.yugong.common.db.DataSourceFactory;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.db.meta.TableMetaGenerator;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.DataSourceConfig;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.RunMode;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.stats.ProgressTracer;
import com.taobao.yugong.common.stats.StatAggregation;
import com.taobao.yugong.common.utils.LikeUtil;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.common.utils.compile.JdkCompiler;
import com.taobao.yugong.common.utils.thread.NamedThreadFactory;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.AbstractRecordExtractor;
import com.taobao.yugong.extractor.RecordExtractor;
import com.taobao.yugong.extractor.oracle.AbstractOracleRecordExtractor;
import com.taobao.yugong.extractor.oracle.OracleAllRecordExtractor;
import com.taobao.yugong.extractor.oracle.OracleFullRecordExtractor;
import com.taobao.yugong.extractor.oracle.OracleMaterializedIncRecordExtractor;
import com.taobao.yugong.extractor.oracle.OracleOnceFullRecordExtractor;
import com.taobao.yugong.extractor.oracle.OracleRecRecordExtractor;
import com.taobao.yugong.extractor.sqlserver.SqlServerFullRecordExtractor;
import com.taobao.yugong.positioner.FileMixedRecordPositioner;
import com.taobao.yugong.positioner.MemoryRecordPositioner;
import com.taobao.yugong.positioner.RecordPositioner;
import com.taobao.yugong.translator.DataTranslator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

/**
 * 整个迁移流程调度控制
 *
 * @author agapple 2013-9-17 下午3:15:29
 */
public class YuGongController extends AbstractYuGongLifeCycle {

  private DataSourceFactory dataSourceFactory = new DataSourceFactory();
  private JdkCompiler compiler = new JdkCompiler();
  private Configuration config;

  private RunMode runMode;
  private YuGongContext globalContext;
  private DbType sourceDbType = DbType.ORACLE;
  private DbType targetDbType = DbType.MYSQL;
  private File translatorDir;
  private AlarmService alarmService;

  private TableController tableController;
  private ProgressTracer progressTracer;
  private List<YuGongInstance> instances = Lists.newArrayList();
  private ScheduledExecutorService schedule;
  // 全局的工作线程池
  private ThreadPoolExecutor extractorExecutor = null;
  private ThreadPoolExecutor applierExecutor = null;
  private boolean onceFull;
  private boolean extractorDump;
  private boolean applierDump;
  private int statBufferSize;
  private int statPrintInterval;
  private boolean concurrent;
  private String alarmReceiver;
  private int retryTimes;
  private int retryInterval;

  public YuGongController(Configuration config) {
    this.config = config;

    // 设置下运行模式
    String mode = config.getString("yugong.table.mode");
    if (StringUtils.isEmpty(mode)) {
      throw new YuGongException("yugong.table.mode should not be empty");
    }
    this.runMode = RunMode.valueOf(mode);
    this.sourceDbType = DbType.valueOf(StringUtils.upperCase(
        config.getString("yugong.database.source.type")));
    this.targetDbType = DbType.valueOf(StringUtils.upperCase(
        config.getString("yugong.database.target.type")));
    this.translatorDir = new File(config.getString("yugong.translator.dir", "../conf/translator"));
    this.alarmService = initAlarmService();
    onceFull = config.getBoolean("yugong.extractor.once", false);
    extractorDump = config.getBoolean("yugong.extractor.dump", true);
    applierDump = config.getBoolean("yugong.applier.dump", true);
    statBufferSize = config.getInt("yugong.stat.buffer.size", 16384);
    statPrintInterval = config.getInt("yugong.stat.print.interval", 5);
    // 是否并行执行concurrent
    concurrent = config.getBoolean("yugong.table.concurrent.enable", false);
    alarmReceiver = config.getString("yugong.alarm.receiver", "");
    retryTimes = config.getInt("yugong.table.retry.times", 3);
    retryInterval = config.getInt("yugong.table.retry.interval", 1000);
  }

  @Override
  public void start() {
    MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
    super.start();
    if (!dataSourceFactory.isStart()) {
      dataSourceFactory.start();
    }
    this.globalContext = initGlobalContext();

    Collection<TableHolder> tableMetas = initTables();
    int threadSize = 1; // 默认1，代表串行
    if (concurrent) {
      threadSize = config.getInt("yugong.table.concurrent.size", 5); // 并行执行的table数

    }

    tableController = new TableController(tableMetas.size(), threadSize);
    progressTracer = new ProgressTracer(runMode, tableMetas.size());

    int noUpdateThresoldDefault = -1;
    if (threadSize < tableMetas.size()) { // 如果是非一次性并发跑，默认为3次noUpdate
      noUpdateThresoldDefault = 3;
    }
    int noUpdateThresold = config.getInt("yugong.extractor.noupdate.thresold",
        noUpdateThresoldDefault);
    boolean useExtractorExecutor = config.getBoolean("yugong.extractor.concurrent.global", false);
    boolean useApplierExecutor = config.getBoolean("yugong.applier.concurrent.global", false);
    if (useExtractorExecutor) {
      int extractorSize = config.getInt("yugong.extractor.concurrent.size", 5);
      extractorExecutor = new ThreadPoolExecutor(extractorSize,
          extractorSize,
          60,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(extractorSize * 2),
          new NamedThreadFactory("Global-Extractor"),
          new ThreadPoolExecutor.CallerRunsPolicy());
    }

    if (useApplierExecutor) {
      int applierSize = config.getInt("yugong.applier.concurrent.size", 5);
      applierExecutor = new ThreadPoolExecutor(applierSize,
          applierSize,
          60,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(applierSize * 2),
          new NamedThreadFactory("Global-Applier"),
          new ThreadPoolExecutor.CallerRunsPolicy());
    }
    for (TableHolder tableHolder : tableMetas) {
      YuGongContext context = buildContext(globalContext, tableHolder.table, tableHolder.ignoreSchema);

      RecordPositioner positioner = choosePositioner(tableHolder);
      RecordExtractor extractor = chooseExtractor(tableHolder, context, runMode, positioner);
      RecordApplier applier = chooseApplier(tableHolder, context, runMode);
      // 可能在装载DRDS时,已经加载了一次translator处理
      DataTranslator translator = tableHolder.translator;
      if (translator == null) {
        translator = choseTranslator(tableHolder);
      }
      YuGongInstance instance = new YuGongInstance(context);
      StatAggregation statAggregation = new StatAggregation(statBufferSize, statPrintInterval);
      instance.setExtractor(extractor);
      instance.setApplier(applier);
      instance.setTranslator(translator);
      instance.setPositioner(positioner);
      instance.setTableController(tableController);
      instance.setAlarmService(alarmService);
      instance.setAlarmReceiver(alarmReceiver);
      instance.setExtractorDump(extractorDump);
      instance.setApplierDump(applierDump);
      instance.setStatAggregation(statAggregation);
      instance.setRetryTimes(retryTimes);
      instance.setRetryInterval(retryInterval);
      instance.setTargetDbType(targetDbType);
      instance.setProgressTracer(progressTracer);
      instance.setNoUpdateThresold(noUpdateThresold);
      // 设置translator的并发数
      instance.setThreadSize(config.getInt("yugong.extractor.concurrent.size", 5));
      instance.setExecutor(extractorExecutor);
      instances.add(instance);
    }

    logger.info("## prepare start tables[{}] with concurrent[{}]", instances.size(), threadSize);
    int progressPrintInterval = config.getInt("yugong.progress.print.interval", 1);
    schedule = Executors.newScheduledThreadPool(2);
    schedule.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {
        try {
          progressTracer.print(true);
        } catch (Throwable e) {
          logger.error("print progress failed", e);
        }
      }
    }, progressPrintInterval, progressPrintInterval, TimeUnit.MINUTES);
    schedule.execute(() -> {
      while (true) {
        try {
          YuGongInstance instance = tableController.takeDone();
          if (instance.isStart()) {
            instance.stop();
          }
        } catch (InterruptedException e) {
          // do nothging
          return;
        } catch (Throwable e) {
          logger.error("stop failed", e);
        }
      }
    });

    for (YuGongInstance instance : instances) {
      instance.start();
      if (!concurrent) {
        // 如果非并发，则串行等待其返回
        try {
          instance.waitForDone();
        } catch (Exception e) {
          processException(instance.getContext().getTableMeta(), e);
        }

        instance.stop();
      }
    }

    MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
  }

  public void waitForDone() throws InterruptedException {
    tableController.waitForDone();
  }

  @Override
  public void stop() {
    super.stop();
    for (YuGongInstance instance : instances) {
      if (instance.isStart()) {
        instance.stop();
      }
    }
    schedule.shutdownNow();
    MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
    progressTracer.print(true);
    if (dataSourceFactory.isStart()) {
      dataSourceFactory.stop();
    }
    MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
  }

  private RecordExtractor chooseExtractor(TableHolder tableHolder, YuGongContext context, RunMode runMode,
      RecordPositioner positioner) {
    if (runMode == RunMode.FULL || runMode == RunMode.CHECK) {
      String tablename = tableHolder.table.getName();
      String fullName = tableHolder.table.getFullName();
      // 优先找tableName
      String extractSql = config.getString("yugong.extractor.sql." + tablename);
      if (StringUtils.isEmpty(extractSql)) {
        extractSql = config.getString("yugong.extractor.sql." + fullName,
            config.getString("yugong.extractor.sql"));
      }

      // 优先找tableName
      boolean isTableExtracOnce = config.getBoolean("yugong.extractor.once." + tablename, false);
      boolean forceFull = !isTableExtracOnce && StringUtils.isNotEmpty(extractSql);
      if (onceFull) {
        if (sourceDbType == DbType.ORACLE) {
          OracleOnceFullRecordExtractor recordExtractor = new OracleOnceFullRecordExtractor(
              context);
          recordExtractor.setExtractSql(extractSql);
          recordExtractor.setTracer(progressTracer);
          return recordExtractor;
        } else {
          throw new YuGongException("OnceFullRecordExtractor, unsupport " + sourceDbType);
        }
      } else {
        if (!forceFull && (!isOnlyPkIsNumber(tableHolder.table) || isTableExtracOnce || !StringUtils
            .isEmpty(extractSql))) {
          throw new YuGongException("FullRecordExtractor Condition Error, table: "
              + tableHolder.table);
        }
        if (sourceDbType == DbType.ORACLE) {
          OracleFullRecordExtractor recordExtractor = new OracleFullRecordExtractor(context);
          recordExtractor.setExtractSql(extractSql);
          recordExtractor.setTracer(progressTracer);
          return recordExtractor;
        } else if (sourceDbType == DbType.SQL_SERVER) {
          SqlServerFullRecordExtractor recordExtractor = new SqlServerFullRecordExtractor(context);
          recordExtractor.setExtractSql(extractSql);
          recordExtractor.setTracer(progressTracer);
          return recordExtractor;
        } else {
          throw new YuGongException("unsupport " + sourceDbType);
        }
      }
    } else if (runMode == RunMode.INC) {
      if (sourceDbType == DbType.ORACLE) {
        OracleMaterializedIncRecordExtractor recordExtractor =
            new OracleMaterializedIncRecordExtractor(context);
        recordExtractor.setConcurrent(config.getBoolean("yugong.extractor.concurrent.enable",
            true));
        recordExtractor.setSleepTime(config.getLong("yugong.extractor.noupdate.sleep", 1000L));
        recordExtractor.setThreadSize(config.getInt("yugong.extractor.concurrent.size", 5));
        recordExtractor.setExecutor(extractorExecutor);
        recordExtractor.setTracer(progressTracer);
        return recordExtractor;
      } else {
        throw new YuGongException("unsupport " + sourceDbType);
      }
    } else if (runMode == RunMode.MARK || runMode == RunMode.CLEAR) {
      if (sourceDbType == DbType.ORACLE) {
        return new OracleRecRecordExtractor(context);
      } else {
        throw new YuGongException("unsupport " + sourceDbType);
      }
    } else {
      // 不会有并发问题，所以共用一份context
      AbstractRecordExtractor markExtractor = (AbstractRecordExtractor) chooseExtractor(tableHolder,
          context,
          RunMode.MARK,
          positioner);
      AbstractRecordExtractor fullExtractor = (AbstractRecordExtractor) chooseExtractor(tableHolder,
          context,
          RunMode.FULL,
          positioner);
      AbstractRecordExtractor incExtractor = (AbstractRecordExtractor) chooseExtractor(tableHolder,
          context,
          RunMode.INC,
          positioner);
      fullExtractor.setTracer(progressTracer);
      incExtractor.setTracer(progressTracer);
      if (sourceDbType == DbType.ORACLE) {
        OracleAllRecordExtractor allExtractor = new OracleAllRecordExtractor(context);
        allExtractor.setMarkExtractor((AbstractOracleRecordExtractor) markExtractor);
        allExtractor.setFullExtractor((AbstractOracleRecordExtractor) fullExtractor);
        allExtractor.setIncExtractor((AbstractOracleRecordExtractor) incExtractor);
        allExtractor.setPositioner(positioner);
        return allExtractor;
      } else {
        throw new YuGongException("unsupport " + sourceDbType);
      }
    }
  }

  private RecordApplier chooseApplier(TableHolder tableHolder, YuGongContext context,
      RunMode runMode) {
    boolean concurrent = config.getBoolean("yugong.applier.concurrent.enable", true);
    int threadSize = config.getInt("yugong.applier.concurrent.size", 5);
    int splitSize = context.getOnceCrawNum() / threadSize;
    if (splitSize > 100 || splitSize <= 0) {
      splitSize = 100;
    }

    if (runMode == RunMode.FULL) {
      if (concurrent) {
        return new MultiThreadFullRecordApplier(context, threadSize, splitSize, applierExecutor);
      } else {
        return new FullRecordApplier(context);
      }
    } else if (runMode == RunMode.INC) {
      if (concurrent) {
        return new MultiThreadIncrementRecordApplier(context, threadSize, splitSize, applierExecutor);
      } else {
        return new IncrementRecordApplier(context);
      }
    } else if (runMode == RunMode.ALL) {
      // 不会有并发问题，所以共用一份context
      RecordApplier fullApplier = chooseApplier(tableHolder, context, RunMode.FULL);
      RecordApplier incApplier = chooseApplier(tableHolder, context, RunMode.INC);
      AllRecordApplier allApplier = new AllRecordApplier(context);
      allApplier.setFullApplier(fullApplier);
      allApplier.setIncApplier(incApplier);
      return allApplier;
    } else if (runMode == RunMode.CHECK) {
      if (concurrent) {
        return new MultiThreadCheckRecordApplier(context, threadSize, splitSize, applierExecutor);
      } else {
        return new CheckRecordApplier(context);
      }
    } else {
      return new FullRecordApplier(context);// 其他情况返回一个full
    }
  }

  private DataTranslator choseTranslator(TableHolder tableHolder) {
    try {
      return buildTranslator(tableHolder.table.getName());
    } catch (Exception e) {
      throw new YuGongException(e);
    }
  }

  /**
   * read java class and compile to Translator for specific table
   */
  private DataTranslator buildTranslator(String tableName) throws Exception {
    String tableNameFormated = YuGongUtils.toPascalCase(tableName);
    String translatorName = tableNameFormated + "DataTranslator";
    String packageName = DataTranslator.class.getPackage().getName();
    Class clazz = null;
    try {
      clazz = Class.forName(packageName + "." + translatorName);
    } catch (ClassNotFoundException e) {
      File file = new File(translatorDir, translatorName + ".java");
      if (!file.exists()) {
        // 兼容下表名
        file = new File(translatorDir, tableNameFormated + ".java");
        if (!file.exists()) {
          return null;
        }
      }

      String javaSource = StringUtils.join(IOUtils.readLines(new FileInputStream(file)), "\n");
      clazz = compiler.compile(javaSource);
    }

    return (DataTranslator) clazz.newInstance();
  }

  private RecordPositioner choosePositioner(TableHolder tableHolder) {
    try {
      String mode = config.getString("yugong.run.positioner", "FILE");
      if (StringUtils.equalsIgnoreCase("FILE", mode)) {
        FileMixedRecordPositioner positioner = new FileMixedRecordPositioner();
        positioner.setDataDir(new File("../conf/positioner")); // 使用了../相对目录，启动脚本会确保user.dir为bin目录
        positioner.setDataFileName(
            tableHolder.table.getSchema() + "_" + tableHolder.table.getName() + ".dat");
        return positioner;
      } else {
        RecordPositioner positioner = new MemoryRecordPositioner();
        return positioner;
      }
    } catch (Exception e) {
      throw new YuGongException(e);
    }
  }

  private YuGongContext buildContext(YuGongContext globalContext, Table table,
      boolean ignoreSchema) {
    YuGongContext result = globalContext.cloneGlobalContext();
    result.setTableMeta(table);
    if (ignoreSchema) {  // 自动识别table是否为无shcema定义
      result.setIgnoreSchema(ignoreSchema);
    }
    return result;
  }

  private YuGongContext initGlobalContext() {
    YuGongContext context = new YuGongContext();
    logger.info("check source database connection ...");
    context.setSourceDs(initDataSource("source"));
    logger.info("check source database is ok");

    // if (sourceDbType.isOracle() && runMode.isAll()) {
    // preCheckMlogGrant(context.getSourceDs());
    // }

    logger.info("check target database connection ...");
    context.setTargetDs(initDataSource("target"));
    logger.info("check target database is ok");
    context.setSourceEncoding(config.getString("yugong.database.source.encode", "UTF-8"));
    context.setTargetEncoding(config.getString("yugong.database.target.encode", "UTF-8"));
    context.setBatchApply(config.getBoolean("yugong.table.batchApply", true));
    context.setOnceCrawNum(config.getInt("yugong.table.onceCrawNum", 200));
    context.setTpsLimit(config.getInt("yugong.table.tpsLimit", 2000));
    context.setIgnoreSchema(config.getBoolean("yugong.table.ignoreSchema", false));
    context.setSkipApplierException(config.getBoolean("yugong.table.skipApplierException", false));
    context.setRunMode(runMode);
    return context;
  }

  private DataSource initDataSource(String type) {
    String username = config.getString("yugong.database." + type + ".username");
    String password = config.getString("yugong.database." + type + ".password");
    DbType dbType = DbType.valueOf(config.getString("yugong.database." + type + ".type"));
    String url = config.getString("yugong.database." + type + ".url");
    String encode = config.getString("yugong.database." + type + ".encode");
    String poolSize = config.getString("yugong.database." + type + ".poolSize");

    Properties properties = new Properties();
    if (poolSize != null) {
      properties.setProperty("maxActive", poolSize);
    } else {
      properties.setProperty("maxActive", "200");
    }
    if (dbType.isMysql()) {  // mysql的编码直接交给驱动去做
      properties.setProperty("characterEncoding", encode);
    }

    DataSourceConfig dsConfig = new DataSourceConfig(url, username, password, dbType, properties);
    return dataSourceFactory.getDataSource(dsConfig);
  }

  private Collection<TableHolder> initTables() {
    logger.info("check source tables read privileges ...");
    List tableWhiteList = config.getList("yugong.table.white");
    List tableBlackList = config.getList("yugong.table.black");
    boolean isEmpty = true;
    for (Object table : tableWhiteList) {
      isEmpty &= StringUtils.isBlank((String) table);
    }

    List<TableHolder> tables = Lists.newArrayList();
    DbType targetDbType = YuGongUtils.judgeDbType(globalContext.getTargetDs());
    if (isEmpty) {
      List<Table> metas = TableMetaGenerator.getTableMetasWithoutColumn(
          globalContext.getSourceDs(), null, null);
      for (Table table : metas) {
        if (!isBlackTable(table.getName(), tableBlackList)
            && !isBlackTable(table.getFullName(), tableBlackList)) {
          table = TableMetaGenerator.getTableMeta(sourceDbType, globalContext.getSourceDs(),
              table.getSchema(), table.getName());
          // 构建一下拆分条件
          DataTranslator translator = buildExtKeys(table, null, targetDbType);
          TableHolder holder = new TableHolder(table);
          holder.translator = translator;
          if (!tables.contains(holder)) {
            tables.add(holder);
          }
        }
      }
    } else {
      for (Object obj : tableWhiteList) {
        String whiteTable = getTable((String) obj);
        // 先粗略判断一次
        if (!isBlackTable(whiteTable, tableBlackList)) {
          String[] strs = StringUtils.split(whiteTable, ".");
          List<Table> whiteTables;
          boolean ignoreSchema = false;
          if (strs.length == 1) {
            whiteTables = TableMetaGenerator.getTableMetasWithoutColumn(globalContext.getSourceDs(),
                null,
                strs[0]);
            ignoreSchema = true;
          } else if (strs.length == 2) {
            whiteTables = TableMetaGenerator.getTableMetasWithoutColumn(globalContext.getSourceDs(),
                strs[0],
                strs[1]);
          } else {
            throw new YuGongException("table[" + whiteTable + "] is not valid");
          }

          if (whiteTables.isEmpty()) {
            throw new YuGongException("table[" + whiteTable + "] is not found");
          }

          for (Table table : whiteTables) {
            // 根据实际表名处理一下
            if (!isBlackTable(table.getName(), tableBlackList)
                && !isBlackTable(table.getFullName(), tableBlackList)) {
              table = TableMetaGenerator.getTableMeta(sourceDbType, globalContext.getSourceDs(),
                  table.getSchema(), table.getName());
              // 构建一下拆分条件
              DataTranslator translator = buildExtKeys(table, (String) obj, targetDbType);
              TableHolder holder = new TableHolder(table);
              holder.ignoreSchema = ignoreSchema;
              holder.translator = translator;
              if (!tables.contains(holder)) {
                tables.add(holder);
              }
            }
          }
        }
      }
    }

    // List<String> noPkTables = Lists.newArrayList();
    // for (TableHolder tableHolder : tables) {
    // if (YuGongUtils.isEmpty(tableHolder.table.getPrimaryKeys())) {
    // noPkTables.add(tableHolder.table.getFullName());
    // }
    // }
    //
    // if (YuGongUtils.isNotEmpty(noPkTables)) {
    // throw new YuGongException("Table[" +
    // StringUtils.join(noPkTables.toArray()) +
    // "] has no pks , pls check!");
    // }
    logger.info("check source tables is ok.");
    return tables;
  }

  private boolean isBlackTable(String table, List tableBlackList) {
    for (Object tableBlack : tableBlackList) {
      if (LikeUtil.isMatch((String) tableBlack, table)) {
        return true;
      }
    }

    return false;
  }

  /**
   * 尝试构建拆分字段,如果tableStr指定了拆分字段则读取之,否则在目标库找对应的拆分字段
   */
  private DataTranslator buildExtKeys(Table table, String tableStr, DbType targetDbType) {
    DataTranslator translator = null;
    String extKey = getExtKey(tableStr);
    if (targetDbType.isDRDS()) {
      // 只针对目标为DRDS时处理
      try {
        translator = buildTranslator(table.getName());
      } catch (Exception e) {
        throw new YuGongException(e);
      }

      String schemaName = table.getSchema();
      String tableName = table.getName();

      if (translator != null) {
        // 使用源表的表名查询一次拆分表名
        String tschemaName = translator.translatorSchema();
        String ttableName = translator.translatorTable();
        if (tschemaName != null) {
          schemaName = tschemaName;
        }

        if (ttableName != null) {
          tableName = ttableName;
        }
      }
      String drdsExtKey = TableMetaGenerator.getShardKeyByDRDS(
          globalContext.getTargetDs(), schemaName, tableName);
      if (extKey != null && !StringUtils.equalsIgnoreCase(drdsExtKey, extKey)) {
        logger.warn("table:[{}] is not matched drds shardKey:[{}]", tableStr, drdsExtKey);
      }

      extKey = drdsExtKey;
    }

    if (extKey != null) {
      // 以逗号切割
      String[] keys = StringUtils.split(StringUtils.replace(extKey, "|", ","), ",");
      List<String> newExtKeys = new ArrayList<String>();
      for (String key : keys) {
        boolean found = false;
        for (ColumnMeta meta : table.getPrimaryKeys()) {
          if (meta.getName().equalsIgnoreCase(key)) {
            found = true;
            break;
          }
        }

        if (!found) {
          // 只增加非主键的字段
          newExtKeys.add(key);
        }
      }

      if (newExtKeys.size() > 0) {
        extKey = StringUtils.join(newExtKeys, ",");
        table.setExtKey(extKey);

        // 调整一下原始表结构信息,将extKeys当做主键处理
        // 主要为简化extKeys变更时,等同于主键进行处理
        List<ColumnMeta> primaryKeys = table.getPrimaryKeys();
        List<ColumnMeta> newColumns = Lists.newArrayList();
        for (ColumnMeta column : table.getColumns()) {
          boolean exist = false;
          for (String key : newExtKeys) {
            if (column.getName().equalsIgnoreCase(key)) {
              primaryKeys.add(column);
              exist = true;
              break;
            }
          }

          if (!exist) {
            newColumns.add(column);
          }
        }

        table.setPrimaryKeys(primaryKeys);
        table.setColumns(newColumns);
      }
    }

    return translator;
  }

  private AlarmService initAlarmService() {
    String emailPassword = config.getString("yugong.alarm.email.password");
    if (StringUtils.isNotEmpty(emailPassword)) {
      MailAlarmService alarmService = new MailAlarmService();
      alarmService.setEmailPassword(emailPassword);
      alarmService.setEmailHost(config.getString("yugong.alarm.email.host"));
      alarmService.setEmailUsername(config.getString("yugong.alarm.email.username"));
      alarmService.setStmpPort(config.getInt("yugong.alarm.email.stmp.port", 465));
      alarmService.setSslSupport(config.getBoolean("yugong.alarm.email.ssl.support", true));
      alarmService.start();
      return alarmService;
    } else {
      return new LogAlarmService();
    }
  }

  @SuppressWarnings("unused")
  private boolean isOnlyOnePk(Table table) {
    return table.getPrimaryKeys() != null && table.getPrimaryKeys().size() == 1;
  }

  /**
   * is only one primary key, and this key is number
   */
  private boolean isOnlyPkIsNumber(Table table) {
    if (table.getPrimaryKeys() != null && table.getPrimaryKeys().size() == 1) {
      return YuGongUtils.isNumber(table.getPrimaryKeys().get(0).getType());
    }

    return false;
  }

  private void processException(Table table, Exception exception) {
    MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
    abort("process table[" + table.getFullName() + "] has error!", exception);
    System.exit(-1);// 串行时，出错了直接退出jvm
  }

  /**
   * 从表白名单中得到shardKey
   *
   * @param tableName 带有shardkey的表, 例子 yugong_example_oracle#pk|name
   */
  private String getExtKey(String tableName) {
    if (StringUtils.isEmpty(tableName)) {
      return null;
    }

    String[] paramArray = tableName.split("#");
    if (paramArray.length == 1) {
      return null;
    } else if (paramArray.length == 2) {
      return StringUtils.trim(paramArray[1]);
    } else {
      // 其他情况
      return null;
    }
  }

  private String getTable(String tableName) {
    String[] paramArray = tableName.split("#");
    if (paramArray.length >= 1 && !"".equals(paramArray[0])) {
      return paramArray[0];
    } else {
      return null;
    }
  }

  private static class TableHolder {

    public TableHolder(Table table) {
      this.table = table;
    }

    Table table;
    boolean ignoreSchema = false;
    DataTranslator translator = null;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((table == null) ? 0 : table.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      TableHolder other = (TableHolder) obj;
      if (table == null) {
        if (other.table != null) return false;
      } else if (!table.equals(other.table)) return false;
      return true;
    }

  }

  @SuppressWarnings("unused")
  private void preCheckMlogGrant(DataSource ds) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
    String mlogName = "migrate" + System.nanoTime();
    logger.info("check mlog privileges ...");
    jdbcTemplate.execute("CREATE MATERIALIZED VIEW " + mlogName + " AS SELECT SYSDATE FROM DUAL");
    jdbcTemplate.execute("DROP MATERIALIZED VIEW " + mlogName);
    logger.info("check mlog privileges is ok");
  }
}
