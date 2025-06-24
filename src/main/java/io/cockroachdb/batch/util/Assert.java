package io.cockroachdb.batch.util;

/**
 * @author Kai Niemi
 */
public abstract class Assert {
    private Assert() {
    }

    public static void isTrue(boolean x) {
        if (!x) {
            throw new IllegalStateException(x + " is false");
        }
    }

    public static void isTrue(boolean x, String message) {
        if (!x) {
            throw new IllegalStateException(message);
        }
    }

    public static void isFalse(boolean x) {
        if (x) {
            throw new IllegalStateException(x + " is true");
        }
    }

    public static void isFalse(boolean x, String message) {
        if (x) {
            throw new IllegalStateException(message);
        }
    }
}
