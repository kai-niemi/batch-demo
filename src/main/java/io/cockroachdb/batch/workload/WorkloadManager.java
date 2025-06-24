package io.cockroachdb.batch.workload;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.batch.Task;
import io.cockroachdb.batch.jdbc.DataAccessException;
import io.cockroachdb.batch.util.Metrics;
import io.cockroachdb.batch.util.Problem;

/**
 * A simple workload manager that submit tasks to an executor service
 * and collects time-series call metrics / stats with aggregation.
 *
 * @author Kai Niemi
 */
public class WorkloadManager {
    private static void backoffDelayWithJitter(int calls) {
        try {
            TimeUnit.MILLISECONDS.sleep(
                    Math.min((long) (Math.pow(2, calls) + Math.random() * 1000), 5000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Throwable getMostSpecificCause(Throwable original) {
        Throwable rootCause = getRootCause(original);
        return rootCause != null ? rootCause : original;
    }

    private static Throwable getRootCause(Throwable original) {
        if (original == null) {
            return null;
        } else {
            Throwable rootCause = null;
            for (Throwable cause = original.getCause();
                 cause != null && cause != rootCause;
                 cause = cause.getCause()) {
                rootCause = cause;
            }
            return rootCause;
        }
    }

    private static final ExceptionClassifier EXCEPTION_CLASSIFIER = new ExceptionClassifier() {
    };

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicInteger monotonicId = new AtomicInteger();

    private final List<Workload<Task>> workloads = new LinkedList<>();

    private final ExecutorService executorService;

    public WorkloadManager(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void submitWorkload(Task task, int batchSize,
                               Predicate<Integer> completion, String name) {
        final Metrics metrics = Metrics.empty();

        final LinkedList<Problem> problems = new LinkedList<>();

        final Future<Task> future = executorService.submit(() -> {
            AtomicInteger totalCalls = new AtomicInteger();
            AtomicInteger fails = new AtomicInteger();

            while (completion.test(totalCalls.incrementAndGet())) {
                if (Thread.interrupted()) {
                    logger.warn("Thread interrupted - bailing out");
                    break;
                }

                final Instant invocationTime = Instant.now();

                try {
                    task.executeOne(batchSize);
                    metrics.markSuccess(Duration.between(invocationTime, Instant.now()));
                    fails.set(0);
                } catch (Throwable ex) {
                    final Duration callTime = Duration.between(invocationTime, Instant.now());

                    if (problems.size() >= 20) {
                        problems.removeLast();
                    }
                    problems.addFirst(Problem.from(ex));

                    Throwable cause = getMostSpecificCause(ex);

                    boolean isTransient = false;
                    if (cause instanceof SQLException) {
                        String sqlState = ((SQLException) cause).getSQLState();
                        if (EXCEPTION_CLASSIFIER.isTransient((SQLException) cause)) {
                            logger.warn("Transient SQL exception in %s: [%s]: [%s]"
                                    .formatted(name, sqlState, cause));
                            isTransient = true;
                        } else {
                            logger.error("Non-transient SQL exception in %s: [%s]: [%s]"
                                    .formatted(name, sqlState, cause));
                        }
                    } else if (ex instanceof RecoverableException) {
                        logger.warn("Recoverable exception in %s: [%s]"
                                .formatted(name, ex));
                        isTransient = true;
                    } else {
                        throw new DataAccessException(ex);
                    }

                    metrics.markFail(callTime, isTransient);

                    backoffDelayWithJitter(fails.incrementAndGet());
                }
            }
            return task;
        });

        workloads.add(new Workload<>(monotonicId.incrementAndGet(), name, future, metrics, problems));
    }

    public void shutdownAndWait() {
        executorService.shutdown();

        workloads.forEach((workload) -> {
            try {
                workload.getFuture().get().teardownTask();
                workload.setCompletion(Optional.empty());
                logger.info("Finished %s successfully".formatted(workload.getName()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workload.setCompletion(Optional.of(Problem.from(e)));
                logger.warn("Finished %s prematurely due to interrupt".formatted(workload.getName()), e);
            } catch (ExecutionException e) {
                workload.setCompletion(Optional.of(Problem.from(e.getCause())));
                logger.warn("Finished %s prematurely due to error".formatted(workload.getName()), e.getCause());
            }
        });

        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public List<Workload<Task>> getWorkloads() {
        return new LinkedList<>(workloads)
                .stream()
                .toList();
    }

    public List<Workload<Task>> getWorkloads(WorkloadStatus status) {
        return new LinkedList<>(workloads)
                .stream()
                .filter(taskWorkload -> taskWorkload.getStatus().equals(status))
                .toList();
    }

    public Metrics getMetricsAggregate(WorkloadStatus status) {
        List<Metrics> metrics = getWorkloads(status)
                .stream()
                .map(Workload::getMetrics)
                .toList();
        return Metrics.builder()
                .withUpdateTime(Instant.now())
                .withMeanTimeMillis(metrics.stream()
                        .mapToDouble(Metrics::getAvgTime).average().orElse(0))
                .withMinTimeMillis(metrics.stream()
                        .mapToDouble(Metrics::getMinTime).average().orElse(0))
                .withMaxTimeMillis(metrics.stream()
                        .mapToDouble(Metrics::getMaxTime).average().orElse(0))
                .withOps(metrics.stream().mapToDouble(Metrics::getOpsPerSec).sum(),
                        metrics.stream().mapToDouble(Metrics::getOpsPerMin).sum())
                .withP50(metrics.stream().mapToDouble(Metrics::getP50).average().orElse(0))
                .withP90(metrics.stream().mapToDouble(Metrics::getP90).average().orElse(0))
                .withP95(metrics.stream().mapToDouble(Metrics::getP95).average().orElse(0))
                .withP99(metrics.stream().mapToDouble(Metrics::getP99).average().orElse(0))
                .withP999(metrics.stream().mapToDouble(Metrics::getP999).average().orElse(0))
                .withSuccess(metrics.stream().mapToInt(Metrics::getSuccess).sum())
                .withFails(metrics.stream().mapToInt(Metrics::getTransientFail).sum(),
                        metrics.stream().mapToInt(Metrics::getNonTransientFail).sum())
                .build();
    }
}
