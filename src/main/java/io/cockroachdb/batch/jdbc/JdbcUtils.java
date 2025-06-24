package io.cockroachdb.batch.jdbc;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

public abstract class JdbcUtils {
    private JdbcUtils() {
    }

    public static <T> T queryForEntity(DataSource dataSource,
                                       String sql,
                                       ResultSetCallback<T> action)
            throws DataAccessException {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    return action.process(rs);
                }
            }
        } catch (SQLException ex) {
            throw new DataAccessException(ex);
        }
    }

    /**
     * Support method for executing one implicit (auto-commit) transaction.
     *
     * @param ds     the data source
     * @param action the connection callback
     * @param <T>    the result entity type
     * @return any result entity from the execution
     */
    public static <T> T executeImplicit(DataSource ds,
                                        ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            if (!conn.getAutoCommit()) {
                throw new IllegalStateException("Connection auto-commit is false!");
            }

            try {
                return action.process(conn);
            } catch (RuntimeException | Error ex) {
                throw ex;
            } catch (Throwable ex) {
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    /**
     * Support method for executing one explicit (BEGIN .. COMMIT/ROLLBACK) transaction.
     *
     * @param ds     the data source
     * @param action the connection callback
     * @param <T>    the result entity type
     * @return any result entity from the execution
     */
    public static <T> T executeExplicit(DataSource ds,
                                        ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            T result;
            try {
                result = action.process(conn);
            } catch (RuntimeException | Error ex) {
                conn.rollback();
                throw ex;
            } catch (Throwable ex) {
                conn.rollback();
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
            conn.commit();
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }
}
