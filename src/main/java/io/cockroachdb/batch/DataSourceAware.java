package io.cockroachdb.batch;

import javax.sql.DataSource;

/**
 * Interface to be implemented by any task that wishes to be supplied
 * with a {@link DataSource} to use.
 *
 * @author Kai Niemi
 */
public interface DataSourceAware {
    void setDataSource(DataSource dataSource);
}
