package com.taobao.yugong.common.db;

import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.lifecycle.YuGongLifeCycle;
import com.taobao.yugong.common.model.DataSourceConfig;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.exception.YuGongException;

/**
 * 获取数据库连接
 * 
 * @author agapple 2013年2月25日 下午8:00:41
 * @since 1.0.0
 */
public class DataSourceFactory extends AbstractYuGongLifeCycle implements YuGongLifeCycle {

    private static final Logger               logger      = LoggerFactory.getLogger(DataSourceFactory.class);
    private int                               maxWait     = 5 * 1000;
    private int                               minIdle     = 0;
    private int                               initialSize = 0;
    private int                               maxActive   = 32;

    private Map<DataSourceConfig, DataSource> dataSources;

    public void start() {
        super.start();
        dataSources = new MapMaker().makeComputingMap(new Function<DataSourceConfig, DataSource>() {

            public DataSource apply(DataSourceConfig config) {
                return createDataSource(config.getUrl(),
                    config.getUsername(),
                    config.getPassword(),
                    config.getType(),
                    config.getProperties());
            }
        });

    }

    public void stop() {
        super.stop();

        for (DataSource source : dataSources.values()) {
            DruidDataSource basicDataSource = (DruidDataSource) source;
            basicDataSource.close();
        }

        dataSources.clear();
    }

    public DataSource getDataSource(DataSourceConfig config) {
        return dataSources.get(config);
    }

    public DataSource getDataSource(String url, String userName, String password, DbType dbType, Properties props) {
        return dataSources.get(new DataSourceConfig(url, userName, password, dbType, props));
    }

    private DataSource createDataSource(String url, String userName, String password, DbType dbType, Properties props) {
        try {
            int maxActive = Integer.valueOf(props.getProperty("maxActive", String.valueOf(this.maxActive)));
            if (maxActive < 0) {
                maxActive = 200;
            }
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setUseUnfairLock(true);
            dataSource.setNotFullTimeoutRetryCount(2);
            dataSource.setInitialSize(initialSize);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxActive(maxActive);
            dataSource.setMaxWait(maxWait);
            dataSource.setDriverClassName(dbType.getDriver());
            // 动态的参数
            if (props != null && props.size() > 0) {
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    dataSource.addConnectionProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            }

            if (dbType.isOracle()) {
                dataSource.addConnectionProperty("restrictGetTables", "true");
                dataSource.addConnectionProperty("oracle.jdbc.V8Compatible", "true");
                dataSource.setValidationQuery("select 1 from dual");
                dataSource.setExceptionSorter("com.alibaba.druid.pool.vendor.OracleExceptionSorter");
            } else if (dbType.isMysql() || dbType.isDRDS()) {
                dataSource.addConnectionProperty("useServerPrepStmts", "false");
                dataSource.addConnectionProperty("rewriteBatchedStatements", "true");
                dataSource.addConnectionProperty("allowMultiQueries", "true");
                dataSource.addConnectionProperty("readOnlyPropagatesToServer", "false");
                dataSource.setValidationQuery("select 1");
                dataSource.setExceptionSorter("com.alibaba.druid.pool.vendor.MySqlExceptionSorter");
                dataSource.setValidConnectionCheckerClassName("com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker");
            } else {
                logger.error("Unknow database type");
            }
            return dataSource;
        } catch (Throwable e) {
            throw new YuGongException("create dataSource error!", e);
        }
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

}
