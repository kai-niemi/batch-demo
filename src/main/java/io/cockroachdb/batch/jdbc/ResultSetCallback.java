package io.cockroachdb.batch.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Callback for operating on a JDBC ResultSet.
 *
 * @param <T>
 */
@FunctionalInterface
public interface ResultSetCallback<T> {
    /**
     * An implementation should call next prior to reading from the {@link ResultSet}.
     *
     * @param resultSet the ResultSet of a query statement
     * @return any value extracted or computed from the result
     * @throws SQLException on any SQL exception
     * @see ResultSet#next()
     */
    T process(ResultSet resultSet) throws SQLException;
}
