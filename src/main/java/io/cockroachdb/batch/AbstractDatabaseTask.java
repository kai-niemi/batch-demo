package io.cockroachdb.batch;

import javax.sql.DataSource;

/**
 * Base for database tasks with a supplied pooled datasource.
 *
 * @author Kai Niemi
 */
public abstract class AbstractDatabaseTask implements BatchTask, DataSourceAware {
    private DataSource dataSource;

    @Override
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected DataSource getDataSource() {
        return dataSource;
    }
}

