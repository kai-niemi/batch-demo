package io.cockroachdb.batch.workload;

public class RecoverableException extends RuntimeException {
    public RecoverableException(String message) {
        super(message);
    }
}
