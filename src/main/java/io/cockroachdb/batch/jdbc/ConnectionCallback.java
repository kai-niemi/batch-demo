package io.cockroachdb.batch.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Callback for operating on a JDBC connection.
 *
 * @param <T>
 */
@FunctionalInterface
public interface ConnectionCallback<T> {
    T process(Connection connection) throws SQLException;
}
