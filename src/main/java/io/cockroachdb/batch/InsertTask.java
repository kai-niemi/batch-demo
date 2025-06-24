package io.cockroachdb.batch;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.batch.jdbc.ConnectionCallback;
import io.cockroachdb.batch.jdbc.DataAccessException;
import io.cockroachdb.batch.jdbc.JdbcUtils;
import io.cockroachdb.batch.util.Assert;
import io.cockroachdb.batch.util.RandomData;

public class InsertTask extends AbstractDatabaseTask {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String TABLE_NAME = "t_test";

    private static final String DDL_TEMPLATE = """
            create table if not exists %s
            (
                id int not null primary key default unordered_unique_rowid(),
                %s
            )
            """;

    private static final String DML_TEMPLATE = """
            insert into %s (%s) values (%s)
            """;

    private int numCols;

    private int colSize;

    private boolean implicitTxn;

    private String insertSql;

    private String tableName;

    @Override
    public void prepareTask(Map<String, String> params) {
        this.tableName = params.getOrDefault("tableName", TABLE_NAME);

        String prefix = tableName.equals(TABLE_NAME) ? "" : tableName + ".";

        this.numCols = Integer.parseInt(params.getOrDefault(prefix + "numCols", "10"));
        this.colSize = Integer.parseInt(params.getOrDefault(prefix + "colSize", "64"));
        this.implicitTxn = Boolean.parseBoolean(params.getOrDefault(prefix + "implicitTxn", "false"));

        Assert.isTrue(numCols > 0, "numCols must be > 0");
        Assert.isTrue(colSize > 0, "colSize must be > 0");

        {
            List<String> cols = new ArrayList<>();
            List<String> values = new ArrayList<>();

            IntStream.rangeClosed(1, numCols).forEach(value -> {
                cols.add("col%d".formatted(value));
                values.add("?");
            });

            this.insertSql = DML_TEMPLATE.formatted(
                    tableName,
                    String.join(",", cols),
                    String.join(",", values));
        }

        logger.debug("Task parameters for %s".formatted(getClass().getSimpleName()));
        logger.debug("\ttableName: %s".formatted(tableName));
        logger.debug("\t%s: %s".formatted(prefix + "numCols", numCols));
        logger.debug("\t%s: %s".formatted(prefix + "colSize", colSize));
        logger.debug("\t%s: %s".formatted(prefix + "implicitTxn", implicitTxn));
        logger.debug("\tinsertSql: %s".formatted(insertSql));

        JdbcUtils.executeImplicit(getDataSource(), connection -> {
            List<String> cols = new ArrayList<>();

            IntStream.rangeClosed(1, numCols).forEach(value -> {
                cols.add("col%d varchar(%d) null".formatted(value, colSize));
            });

            try (Statement statement = connection.createStatement()) {
                String sql = DDL_TEMPLATE.formatted(tableName, String.join(",", cols));
                statement.execute(sql);
            }

            return null;
        });
    }

    @Override
    public void executeOne(int batchSize) {
        ConnectionCallback<Void> action = connection -> {
            try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
                for (int row = 1; row <= batchSize; row++) {
                    for (int parameterIndex = 1; parameterIndex <= numCols; parameterIndex++) {
                        ps.setString(parameterIndex, RandomData.randomString(colSize));

                    }
                    ps.addBatch();
                }

                Arrays.stream(ps.executeLargeBatch())
                        .forEach(value -> {
                            if (value == Statement.EXECUTE_FAILED) {
                                throw new DataAccessException("Rows affected: " + value);
                            }
                        });
            }
            return null;
        };

        if (implicitTxn) {
            JdbcUtils.executeImplicit(getDataSource(), action);
        } else {
            JdbcUtils.executeExplicit(getDataSource(), action);
        }
    }
}

