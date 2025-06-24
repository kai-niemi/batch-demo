package io.cockroachdb.batch;

import java.util.concurrent.ExecutorService;

public interface ExecutorAware {
    void setExecutorService(ExecutorService executorService);
}
