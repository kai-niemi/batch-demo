package io.cockroachdb.batch;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

/**
 * @author Kai Niemi
 */
public class DataSourceConfig {
    public static final String SQL_TRACE_LOGGER = "io.cockroachdb.batch.SQL_TRACE";

    public DataSource createDataSource(Map<String, String> params) {
        String url = params.getOrDefault("url",
                "jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable");
        String username = params.getOrDefault("user", "root");
        String password = params.get("password");
        int poolSize = Integer.parseInt(params.getOrDefault("pool-size", "400"));
        String isolationLevel = params.getOrDefault("isolation", "TRANSACTION_SERIALIZABLE");

        boolean traceSQL = Boolean.parseBoolean( params.getOrDefault("trace", "false"));

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setAutoCommit(true);
        dataSource.setMaximumPoolSize(poolSize);
        dataSource.setMinimumIdle(0);
        dataSource.setInitializationFailTimeout(-1);
        dataSource.setConnectionTimeout(TimeUnit.SECONDS.toMillis(5));
        dataSource.setValidationTimeout(TimeUnit.SECONDS.toMillis(20));
        dataSource.setMaxLifetime(TimeUnit.MINUTES.toMillis(3));
        dataSource.setIdleTimeout(TimeUnit.SECONDS.toMillis(60));
        dataSource.setTransactionIsolation(isolationLevel);
        dataSource.addDataSourceProperty("reWriteBatchedInserts", "true");
        dataSource.addDataSourceProperty("application_name", "batch-demo");

        return traceSQL ? loggingProxy(dataSource) : dataSource;
    }

    private DataSource loggingProxy(DataSource dataSource) {
        DefaultQueryLogEntryCreator creator = new DefaultQueryLogEntryCreator();
        creator.setMultiline(false);

        SLF4JQueryLoggingListener listener = new SLF4JQueryLoggingListener();
        listener.setQueryLogEntryCreator(creator);
        listener.setLogger(SQL_TRACE_LOGGER);
        listener.setLogLevel(SLF4JLogLevel.TRACE);
        listener.setWriteConnectionId(true);
        listener.setWriteIsolation(true);

        return ProxyDataSourceBuilder
                .create(dataSource)
                .name("SQL-Trace")
                .asJson()
                .listener(listener)
                .build();
    }
}
