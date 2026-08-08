package za.co.fnb.dcre.prg.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;
import za.co.fnb.dcre.prg.data.model.PrgReportEntity;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.model.UnknownRow;
import za.co.fnb.dcre.prg.data.repo.PrgDeliveryLedgerRepo;
import za.co.fnb.dcre.prg.data.repo.PrgReportRepo;
import za.co.fnb.dcre.prg.data.repo.PrgWatermarkRepo;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Business tier: per-client delta PSR emission on clock windows.
 * PSR flat file layout is a SYNTHETIC-CONTRACT (R-35): header
 * "PSR|client|window", one "TX|e2e|status" per row, trailer "END|count".
 * Zero delta rows = a zero-valued HEARTBEAT file (SCRUM-55, below); resend
 * re-emits all current reportable statuses.
 *
 * <p>SCRUM-42 load fix: whole-book reads plus a full in-heap render blew
 * CRDB's sql memory budget (2116569420 bytes on the 30M-tx book) and the JVM.
 * Every read is now a bounded keyset slice (ORDER BY e2e LIMIT sliceSize) and
 * the PSR streams to disk slice by slice; nothing holds more than one slice
 * of rows in heap at once.
 *
 * <p>R-29 order preserved: the WHOLE file becomes visible first (streamed tmp
 * + ATOMIC_MOVE), then watermarks advance in per-slice REQUIRES_NEW
 * transactions. Review crg-12 honesty: the advance phase re-reads the
 * COMMITTED target in slice-sized batches and ledgers/watermarks EXACTLY its
 * TX lines, never a fresh delta re-read (a status that moved between the
 * stream and the advance was never in the emitted file: it stays unledgered
 * at the new status and rides the next window). A crash between the two
 * phases replays as skip-existing-file + the same advance-from-file.
 *
 * <p>SCRUM-55: every scheduled emission registers a prg_report row (restart
 * reuses it, file_name is unique) and the advance phase ledgers each advanced
 * line in the same per-slice transaction as its watermark: the delivery
 * ledger is the authority on what was externally reported. Lines an
 * IMMEDIATE report already ledgered never reach a scheduled delta (immediate
 * reports advance the watermark), so the ON CONFLICT no-op is only the
 * resend/replay path.
 *
 * <p>SCRUM-55 heartbeat: a quiet SCHEDULED window (zero delta) emits a
 * zero-valued normal-format PSR so the consumer can tell "no movement" from
 * "PRG dead". Only this class (the SCHEDULED path) heartbeats; the
 * immediate/manual paths stay file-less no-ops. The HB placeholder is a
 * SYNTHETIC-CONTRACT (register A-item): the real legacy response copybook
 * and its exact zero-placeholder bytes are unrecovered. A same-window replay
 * that finds a standing heartbeat but a late delta (kill-resume) defers the
 * delta to the NEXT window: it is never ledgered or watermark-advanced
 * against a file that carries no TX lines.
 */
@Service
public class PsrReportService {

    /** Above this, unknown-status exclusions collapse to ONE summary WARN (R-38 at scale, CRW's hybrid). */
    static final int UNKNOWN_DETAIL_WARN_LIMIT = 100;

    /** Heartbeat zero placeholder, DCRE + 29 zeros = 33 chars (Max35-safe). SYNTHETIC-CONTRACT (A-57). */
    static final String HB_PLACEHOLDER = "DCRE" + "0".repeat(29);

    private static final Logger log = LoggerFactory.getLogger(PsrReportService.class);

    private final PrgWatermarkRepo watermarks;
    private final PrgReportRepo reports;
    private final PrgDeliveryLedgerRepo ledger;
    private final ExchangeLayout layout;
    private final TransactionTemplate watermarkTx;
    private final int sliceSize;

    public PsrReportService(final PrgWatermarkRepo watermarks, final PrgReportRepo reports,
                            final PrgDeliveryLedgerRepo ledger, final ExchangeLayout layout,
                            final PlatformTransactionManager txManager,
                            @Value("${dcre.prg.psr-slice-size:50000}") final int sliceSize) {
        this.watermarks = watermarks;
        this.reports = reports;
        this.ledger = ledger;
        this.layout = layout;
        // Each watermark-advance attempt needs its OWN transaction: a CRDB
        // 40001 abort poisons the surrounding transaction (25P02 on any further
        // statement), so retrying inside the step transaction can never succeed.
        this.watermarkTx = new TransactionTemplate(txManager);
        this.watermarkTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.sliceSize = sliceSize;
    }

    /** Job-less convenience (direct-service callers/tests): no batch execution, so job_name stays null. */
    public Optional<Path> window(final String client, final String windowKey, final boolean resend)
            throws IOException {
        return window(client, windowKey, resend, null);
    }

    /**
     * @param jobName SCRUM-58 clock-scoped trace anchor (env JOB_NAME or
     *                {@code local-prg-<executionId>}), stamped on the report row.
     * @return the emitted PSR file path: the windowed delta, or the zero-valued heartbeat on zero delta.
     */
    public Optional<Path> window(final String client, final String windowKey, final boolean resend,
                                 final String jobName) throws IOException {
        warnUnknown(client);
        final List<StatusRow> first = readSlice(client, resend, "");
        final Path target = layout.resolve(client, ExchangeChannel.ONHOST_RESP, ExchangeSub.OUT)
                .resolve(client + "_PSR_" + windowKey + ".txt");
        if (first.isEmpty()) {
            return Optional.of(heartbeat(client, windowKey, target, jobName));
        }
        if (heartbeatStands(target)) {
            // SCRUM-55 kill-resume guard (review crg-11): a heartbeat already stands
            // for this window (R-24: never rewritten). Ledgering/advancing the late
            // delta against a zero-TX file would record lines as externally delivered
            // that never reached the client (permanently suppressing e.g. a terminal
            // RJCT); the delta flows untouched to the NEXT window instead.
            log.info("report stage=PRG type=SCHEDULED client={} window={} reason=HEARTBEAT_STANDS"
                    + " action=defer-delta", client, windowKey);
            return Optional.of(heartbeat(client, windowKey, target, jobName));
        }
        if (!Files.exists(target)) {
            // R-24 restart no-op contract: an existing target means a prior
            // emission stands; only a fresh window streams a new file.
            streamPsr(client, windowKey, resend, target, first);
        }
        // R-29: the whole file is visible by now, so replaying the advance is safe
        // (upsert keyed (client, e2e)); WriteTooOldError under load is a routine 40001.
        advanceFromEmittedFile(client, target, openReport(client, "SCHEDULED", windowKey, target, jobName));
        return Optional.of(target);
    }

    /**
     * SCRUM-55 kill-resume guard: true when this window's standing artifact
     * is a heartbeat: registry row type HEARTBEAT, or (crash in the gap
     * between the heartbeat ATOMIC_MOVE and the registry insert) a standing
     * file whose trailer is END|0. A real delta file always counts at least
     * one TX line, so END|0 uniquely classifies a heartbeat; the trailer
     * check is a bounded tail read and only runs when no registry row exists.
     */
    private boolean heartbeatStands(final Path target) throws IOException {
        final var standing = reports.findByFileName(target.getFileName().toString());
        if (standing.isPresent()) {
            return "HEARTBEAT".equals(standing.get().getType());
        }
        return Files.exists(target) && "END|0".equals(StreamedPsrWrite.trailer(target));
    }

    /**
     * SCRUM-55 heartbeat: zero-valued normal-format PSR for a quiet window.
     * HB carries the SYNTHETIC-CONTRACT zero placeholders, PD the client's
     * prg_sla_pending count; neither is a TX line, so the trailer stays
     * END|0. Registry row type HEARTBEAT; the watermark and the delivery
     * ledger are NEVER touched (nothing was externally reported). A crash
     * between the move and the registry insert replays as skip-existing-file
     * + find-or-save.
     */
    private Path heartbeat(final String client, final String windowKey, final Path target,
                           final String jobName) throws IOException {
        if (!Files.exists(target)) { // R-24: a standing emission is never rewritten
            try (StreamedPsrWrite psr = StreamedPsrWrite.begin(target,
                    "PSR|%s|%s".formatted(client, windowKey))) {
                psr.writeInfo("HB|%s|%s|0|0.00".formatted(HB_PLACEHOLDER, HB_PLACEHOLDER));
                psr.writeInfo("PD|%d".formatted(watermarks.countSlaPending(client)));
                psr.commit();
            }
        }
        openReport(client, "HEARTBEAT", windowKey, target, jobName);
        log.info("report stage=PRG type=HEARTBEAT client={} window={} file={}",
                client, windowKey, target.getFileName());
        return target;
    }

    /** SCRUM-55 report registry: find-or-save on the unique file_name so restarts reuse the row. */
    private UUID openReport(final String client, final String reportType, final String windowKey,
                            final Path target, final String jobName) {
        final String fileName = target.getFileName().toString();
        return watermarkTx.execute(s -> reports.findByFileName(fileName)
                .orElseGet(() -> reports.save(PrgReportEntity.of(
                        client, reportType, "CLOCK", windowKey, null, fileName, jobName)))
                .getId());
    }

    /**
     * R-38 exclusion visibility without the row-returning whole-book scan
     * that blew the sql memory budget: aggregate count per client, per-row
     * WARN detail only at small counts (mirrors CRW's hybrid choice).
     */
    private void warnUnknown(final String client) {
        final long unknown = watermarks.countUnknown(client);
        if (unknown == 0) {
            return;
        }
        if (unknown <= UNKNOWN_DETAIL_WARN_LIMIT) {
            for (final UnknownRow row : watermarks.findUnknownDetail(client, UNKNOWN_DETAIL_WARN_LIMIT)) {
                log.warn("excluded stage=PRG arrival={} seq={} e2e={} reason=STATUS_UNKNOWN",
                        row.arrivalId(), row.sequence(), row.e2e());
            }
            return;
        }
        log.warn("excluded stage=PRG client={} count={} reason=STATUS_UNKNOWN", client, unknown);
    }

    /** Streams header, every slice's TX lines and the counted trailer, then moves tmp to target atomically. */
    private void streamPsr(final String client, final String windowKey, final boolean resend,
                           final Path target, final List<StatusRow> first) throws IOException {
        try (StreamedPsrWrite psr = StreamedPsrWrite.begin(target, "PSR|" + client + "|" + windowKey)) {
            List<StatusRow> slice = first;
            while (true) {
                for (final StatusRow row : slice) {
                    psr.writeTx("TX|" + row.e2e() + "|" + row.status());
                }
                if (slice.size() < sliceSize) {
                    break;
                }
                slice = readSlice(client, resend, slice.getLast().e2e());
            }
            psr.commit();
        }
    }

    /**
     * R-29 second phase, review crg-12 honesty: the emitted file is the ONLY
     * truth of what was externally reported, so the advance streams the
     * committed target back (bounded: one slice-sized batch of TX lines in
     * heap at a time) and ledgers/watermarks exactly those (e2e, status)
     * tuples in per-batch REQUIRES_NEW transactions (fresh tx per CrdbRetry
     * attempt). A fresh delta re-read here could ledger a status that moved
     * AFTER the file was written and was therefore never delivered; such
     * rows now stay unledgered and ride the next window's delta. The same
     * path replays after a crash: a standing target is advanced as written.
     */
    private void advanceFromEmittedFile(final String client, final Path target, final UUID reportId)
            throws IOException {
        try (BufferedReader psr = Files.newBufferedReader(target)) {
            final List<StatusRow> batch = new ArrayList<>(sliceSize);
            String line;
            while ((line = psr.readLine()) != null) {
                if (!line.startsWith("TX|")) {
                    continue; // header/trailer; heartbeats never reach this phase
                }
                final String[] parts = line.split("\\|", 3);
                batch.add(new StatusRow(parts[1], parts[2]));
                if (batch.size() == sliceSize) {
                    advanceSlice(client, reportId, List.copyOf(batch));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                advanceSlice(client, reportId, batch);
            }
        }
    }

    /** Ledger + watermark per advanced line, atomically per slice (the ledger is the delivery authority). */
    private void advanceSlice(final String client, final UUID reportId, final List<StatusRow> slice) {
        CrdbRetry.run("watermark-advance client=%s from=%s".formatted(client, slice.getFirst().e2e()),
                () -> watermarkTx.executeWithoutResult(status -> {
                    for (final StatusRow row : slice) {
                        ledger.record(reportId, client, row.e2e(), row.status(), null);
                        watermarks.upsertWatermark(client, row.e2e(), row.status());
                    }
                }));
    }

    private List<StatusRow> readSlice(final String client, final boolean resend, final String afterE2e) {
        return resend
                ? watermarks.findRangeSlice(client, afterE2e, sliceSize)
                : watermarks.findDeltaSlice(client, afterE2e, sliceSize);
    }
}
