package za.co.fnb.dcre.prg.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Bounded retry for CockroachDB serialization aborts (SQLSTATE 40001), which
 * Spring surfaces as TransientDataAccessException subclasses. 40001 aborts are
 * NORMAL under contention (persistence.md): retry with backoff, never skip.
 * The op MUST open its own transaction per attempt: an aborted CRDB
 * transaction rejects every further statement (25P02) until rolled back.
 */
final class CrdbRetry {

    static final int MAX_ATTEMPTS = 5;

    private static final long BASE_BACKOFF_MS = 100;
    private static final Logger log = LoggerFactory.getLogger(CrdbRetry.class);

    private CrdbRetry() {
    }

    static void run(final String op, final Runnable body) {
        get(op, () -> {
            body.run();
            return null;
        });
    }

    /** Value-returning variant (SCRUM-55): retried slice reads hand their rows back. */
    static <T> T get(final String op, final Supplier<T> body) {
        for (int attempt = 1; ; attempt++) {
            try {
                return body.get();
            } catch (final TransientDataAccessException e) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw e;
                }
                final long backoffMs = (BASE_BACKOFF_MS << (attempt - 1))
                        + ThreadLocalRandom.current().nextLong(BASE_BACKOFF_MS);
                log.warn("retrying stage=PRG op={} attempt={}/{} after {} backoffMs={}",
                        op, attempt, MAX_ATTEMPTS, e.getClass().getSimpleName(), backoffMs);
                sleep(backoffMs);
            }
        }
    }

    private static void sleep(final long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during CRDB retry backoff", interrupted);
        }
    }
}
