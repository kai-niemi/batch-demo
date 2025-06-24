package io.cockroachdb.batch;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import io.cockroachdb.batch.workload.RecoverableException;

/**
 * A fake task that just sleeps to simulate I/O wait
 * and optionally throws exceptions based on probability.
 *
 * @author Kai Niemi
 */
public class FakeTask implements BatchTask {
    private long minWaitMillis;

    private long maxWaitMillis;

    private final double transientErrorProbability = 0;

    private final double nonTransientErrorProbability = 0;

    private final int num = 3;

    @Override
    public void prepareTask(Map<String, String> params) {
        this.minWaitMillis = Integer.parseInt(params.getOrDefault("minWait", "1"));
        this.maxWaitMillis = Integer.parseInt(params.getOrDefault("maxWait", "5"));
    }

    @Override
    public void executeOne(int batchSize) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        if (r.nextDouble(0, 1.0) < transientErrorProbability) {
            throw new RecoverableException("Fake recoverable exception for #" + num);
        } else if (r.nextDouble(0, 1.0) < nonTransientErrorProbability) {
            throw new IllegalStateException("Fake non-recoverable exception for #" + num);
        } else {
            try {
                long t = r.nextLong(minWaitMillis, maxWaitMillis);
                TimeUnit.MILLISECONDS.sleep(t);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
