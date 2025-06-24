package io.cockroachdb.batch;

import java.util.Map;

/**
 * Functional interface for batch tasks.
 *
 * @author Kai Niemi
 */
@FunctionalInterface
public interface BatchTask {
    /**
     * Invoked once prior to task execution providing an opportunity to initialize things.
     *
     * @param params command-line parameters
     */
    default void prepareTask(Map<String, String> params) {

    }

    /**
     * Execute a single batch.
     *
     * @param batchSize the size of the batch, always > 0
     */
    void executeOne(int batchSize);

    /**
     * Invoked once post execution providing an opportunity to teardown any side effects of the task.
     */
    default void teardownTask() {

    }
}
