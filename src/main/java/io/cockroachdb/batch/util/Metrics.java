package io.cockroachdb.batch.util;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

/**
 * A collector of method call metrics with aggregation functions.
 * Tracks the outcome of invocations and execution time percentiles
 * by periodic sampling.
 *
 * @author Kai Niemi
 */
public class Metrics {
    public static Metrics empty() {
        return new Metrics();
    }

    public static Metrics copy(Metrics from) {
        Metrics m = new Metrics();
        m.updateTime = from.updateTime;
        m.success = from.getSuccess();
        m.transientFail = from.getTransientFail();
        m.nonTransientFail = from.getNonTransientFail();
        m.opsPerSec = from.getOpsPerSec();
        m.opsPerMin = from.getOpsPerMin();
        m.avgTime = from.getAvgTime();
        m.minTime = from.getMinTime();
        m.maxTime = from.getMaxTime();
        m.p50 = from.getP50();
        m.p90 = from.getP90();
        m.p95 = from.getP95();
        m.p99 = from.getP99();
        m.p999 = from.getP999();
        return m;
    }

    private static double percentile(List<Long> sortedDurations, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException(">=0 N <=1");
        }
        if (!sortedDurations.isEmpty()) {
            int index = (int) Math.ceil(percentile * sortedDurations.size());
            return sortedDurations.get(index - 1);
        }
        return 0;
    }

    private final LinkedList<Pair<Instant, Pair<Duration, Integer>>> timeSeries = new LinkedList<>();

    private final Instant startTime = Instant.now();

    private Instant updateTime;

    private int success;

    private int transientFail;

    private int nonTransientFail;

    private double opsPerSec;

    private double opsPerMin;

    private double avgTime;

    private double minTime;

    private double maxTime;

    private double p50;

    private double p90;

    private double p95;

    private double p99;

    private double p999;

    private final int samplePeriod = 120;

    Metrics() {
        this.updateTime = Instant.now();
    }

    public void markSuccess(Duration duration) {
        success++;
        update(duration);
    }

    public void markFail(Duration duration, boolean isTransient) {
        if (isTransient) {
            transientFail++;
        } else {
            nonTransientFail++;
        }
        update(duration);
    }

    private void update(Duration duration) {
        updateTime = Instant.now();

        // Purge items by time range
        timeSeries.removeIf(item -> item.getFirst()
                .isBefore(updateTime.minusSeconds(samplePeriod)));

        timeSeries.add(Pair.of(updateTime, Pair.of(duration, success)));

        // Latency percentiles and mean
        {
            List<Long> sortedMillis = timeSeries
                    .stream()
                    .map(p -> p.getSecond().getFirst())
                    .toList()
                    .stream()
                    .mapToLong(Duration::toMillis)
                    .sorted()
                    .boxed()
                    .toList();

            p50 = percentile(sortedMillis, .5);
            p90 = percentile(sortedMillis, .9);
            p95 = percentile(sortedMillis, .95);
            p99 = percentile(sortedMillis, .99);
            p999 = percentile(sortedMillis, .999);

            avgTime = sortedMillis
                    .stream()
                    .mapToDouble(value -> value)
                    .average()
                    .orElse(0);
            minTime = sortedMillis
                    .stream()
                    .mapToDouble(value -> value)
                    .min()
                    .orElse(0);
            maxTime = Math.max(maxTime, sortedMillis
                    .stream()
                    .mapToDouble(value -> value)
                    .max()
                    .orElse(0));
        }

        // Ops per time unit
        {
            List<Instant> samples = timeSeries
                    .stream()
                    .map(Pair::getFirst)
                    .filter(instant -> instant.isAfter(updateTime.minusSeconds(samplePeriod)))
                    .toList();

            Instant oldestTime = samples.stream()
                    .findFirst()
                    .orElse(updateTime);

            opsPerSec = samples.size() /
                        Math.max(1, Duration.between(oldestTime, updateTime).toMillis() / 1000.0);

            opsPerMin = opsPerSec * 60;
        }
    }

    public double getExecutionTimeSeconds() {
        return Duration.between(startTime, Instant.now()).toMillis() / 1000.0;
    }

    public int getSuccess() {
        return success;
    }

    public int getTransientFail() {
        return transientFail;
    }

    public int getNonTransientFail() {
        return nonTransientFail;
    }

    public double getAvgTime() {
        return avgTime;
    }

    public double getMaxTime() {
        return maxTime;
    }

    public double getMinTime() {
        return minTime;
    }

    public double getOpsPerSec() {
        return opsPerSec;
    }

    public double getOpsPerMin() {
        return opsPerMin;
    }

    public double getP50() {
        return p50;
    }

    public double getP90() {
        return p90;
    }

    public double getP95() {
        return p95;
    }

    public double getP99() {
        return p99;
    }

    public double getP999() {
        return p999;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Metrics instance;

        private Builder() {
            this.instance = new Metrics();
        }

        public Builder withUpdateTime(Instant updateTime) {
            instance.updateTime = updateTime;
            return this;
        }

        public Builder withMeanTimeMillis(double meanTimeMillis) {
            instance.avgTime = meanTimeMillis;
            return this;
        }

        public Builder withMinTimeMillis(double min) {
            instance.minTime = min;
            return this;
        }

        public Builder withMaxTimeMillis(double max) {
            instance.maxTime = max;
            return this;
        }

        public Builder withOps(double opsPerSec, double opsPerMin) {
            instance.opsPerSec = opsPerSec;
            instance.opsPerMin = opsPerMin;
            return this;
        }

        public Builder withP50(double p50) {
            instance.p50 = p50;
            return this;
        }

        public Builder withP90(double p90) {
            instance.p90 = p90;
            return this;
        }

        public Builder withP95(double p9999) {
            instance.p95 = p9999;
            return this;
        }

        public Builder withP999(double p999) {
            instance.p999 = p999;
            return this;
        }

        public Builder withP99(double p99) {
            instance.p99 = p99;
            return this;
        }

        public Builder withSuccess(int success) {
            instance.success = success;
            return this;
        }

        public Builder withFails(int transientFail, int nonTransientFail) {
            instance.transientFail = transientFail;
            instance.nonTransientFail = nonTransientFail;
            return this;
        }

        public Metrics build() {
            return instance;
        }
    }
}
