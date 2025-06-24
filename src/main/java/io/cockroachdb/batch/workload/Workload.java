package io.cockroachdb.batch.workload;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;

import io.cockroachdb.batch.util.Metrics;
import io.cockroachdb.batch.util.Problem;

/**
 * Wrapper for a background task pending completion.
 *
 * @param <T>
 * @author Kai Niemi
 */
public class Workload<T> {
    private final Integer id;

    private final String name;

    private final Future<T> future;

    private final Metrics metrics;

    private final LinkedList<Problem> problems;

    private boolean failed;

    Workload(Integer id,
             String name,
             Future<T> future,
             Metrics metrics,
             LinkedList<Problem> problems) {
        this.id = id;
        this.name = name;
        this.future = future;
        this.metrics = metrics;
        this.problems = problems;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public WorkloadStatus getStatus() {
        if (failed) {
            return WorkloadStatus.FAILED;
        } else if (isRunning()) {
            return WorkloadStatus.RUNNING;
        } else if (isCancelled()) {
            return WorkloadStatus.CANCELLED;
        } else {
            return WorkloadStatus.COMPLETED;
        }
    }

    public void setCompletion(Optional<Problem> failed) {
        this.failed = failed.isPresent();
        failed.ifPresent(this.problems::addFirst);
    }

    public Metrics getMetrics() {
        return isRunning() ? metrics : Metrics.copy(metrics);
    }

    public boolean isRunning() {
        return !future.isDone();
    }

    public boolean isCancelled() {
        return future.isCancelled();
    }

    public Future<T> getFuture() {
        return future;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Workload workload = (Workload) o;
        return Objects.equals(id, workload.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
