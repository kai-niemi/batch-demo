package io.cockroachdb.batch.workload;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.cockroachdb.batch.BatchTask;
import io.cockroachdb.batch.util.Metrics;

/**
 * A call metrics console printer.
 *
 * @author Kai Niemi
 */
public class MetricsPrinter {
    private final ScheduledExecutorService scheduledExecutorService
            = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private final WorkloadManager workloadManager;

    public MetricsPrinter(WorkloadManager workloadManager) {
        this.workloadManager = workloadManager;
    }

    private void printHeader() {
        System.out.printf("%4s %-25s %9s %9s %7s %7s | %5s %5s %5s %5s | %7s %7s %7s %s\n",
                "id", "name", "op/s", "op/m", "time", "mean",
                "p50", "p95", "p99", "p999",
                "success", "retry", "error", "status"
        );
        System.out.println(new String(new char[125]).replace('\0', '-'));
    }

    public void scheduleWithPeriod(int interval, TimeUnit timeUnit) {
        scheduledExecutorService.scheduleAtFixedRate(
                () -> printMetrics(25), interval, interval, timeUnit);
    }

    public void printMetrics(int limit) {
        AtomicInteger i = new AtomicInteger();

        final List<Workload<BatchTask>> workloads = workloadManager.getWorkloads(WorkloadStatus.RUNNING);

        workloads.stream()
                .limit(limit)
                .forEach(workload -> {
                    if (i.getAndIncrement() % 10 == 0) {
                        printHeader();
                    }
                    Metrics m = workload.getMetrics();
                    System.out.printf(
                            "%4d %-25s %9.1f %9.1f %7.1f %7.1f | %5.0f %5.0f %5.0f %5.0f | %7d %7d %7d %s\n",
                            workload.getId(),
                            workload.getName(),
                            m.getOpsPerSec(),
                            m.getOpsPerMin(),
                            m.getExecutionTimeSeconds(),
                            m.getAvgTime(),
                            m.getP50(),
                            m.getP95(),
                            m.getP99(),
                            m.getP999(),
                            m.getSuccess(),
                            m.getTransientFail(),
                            m.getNonTransientFail(),
                            workload.getStatus()
                    );
                });

        if (workloads.size() > 1) {
            Metrics m = workloadManager.getMetricsAggregate(WorkloadStatus.RUNNING);
            System.out.printf("%4s %-25s %9.1f %9.1f %7.1f %7.1f | %5.0f %5.0f %5.0f %5.0f | %7d %7d %7d\n",
                    "Σ",
                    "",
                    m.getOpsPerSec(),
                    m.getOpsPerMin(),
                    m.getExecutionTimeSeconds(),
                    m.getAvgTime(),
                    m.getP50(),
                    m.getP95(),
                    m.getP99(),
                    m.getP999(),
                    m.getSuccess(),
                    m.getTransientFail(),
                    m.getNonTransientFail());
        }
    }

    public void printSummary(int batchSize) {
        Metrics m = workloadManager.getMetricsAggregate(WorkloadStatus.COMPLETED);

        System.out.println("=== Summary ===");
        System.out.printf("Total batches: %,d\n",
                m.getSuccess());
        System.out.printf("Total rows inserted: %,d\n",
                m.getSuccess() * batchSize);
        System.out.printf("Total failed batches: %,d\n",
                m.getNonTransientFail());
        System.out.printf("Total retried batches: %,d\n",
                m.getTransientFail());
        System.out.printf("Avg batch latency: %5.2f ms\n",
                m.getAvgTime());
        System.out.printf("Avg per-row latency: %5.4f ms\n",
                m.getAvgTime() / batchSize);
        System.out.printf("Min batch time: %.2f ms\n",
                m.getMinTime());
        System.out.printf("Max batch time: %.2f ms\n",
                m.getMaxTime());
    }
}
