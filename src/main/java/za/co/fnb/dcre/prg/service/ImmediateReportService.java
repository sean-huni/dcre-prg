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
import za.co.fnb.dcre.prg.data.model.LedgerRow;
import za.co.fnb.dcre.prg.data.model.PrgReportEntity;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.repo.PrgDeliveryLedgerRepo;
import za.co.fnb.dcre.prg.data.repo.PrgReportRepo;
import za.co.fnb.dcre.prg.data.repo.PrgWatermarkRepo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Business tier (SCRUM-55): parent-scoped IMMEDIATE and MANUAL PSR reports.
 * IMMEDIATE sends the parent's ledger-guarded reportable delta; MANUAL regen
 * (manualRef set) resends all current reportable statuses, bypassing the guard
 * but still ledgering with the manual_ref;
 * replay re-renders one historical report byte-for-line from its ledger rows.
 *
 * <p>Every read+ledger slice runs in its OWN REQUIRES_NEW transaction: the
 * caller is a Batch tasklet whose step transaction pins one CRDB snapshot, so
 * in-step reads would never observe the slice commits and the ledger-drop-out
 * pagination of {@code findUnreportedForParent} would loop forever.
 *
 * <p>Crash windows: ledger slices commit before the file's atomic move, so a
 * kill mid-stream leaves ledgered lines with no visible file. A restart of
 * the SAME window seeds the render from the report's own ledger rows before
 * appending the unledgered remainder (resume-same-logical-work): without the
 * seed those lines are marked delivered everywhere (ledger, watermark,
 * prg_report_due all exclude them) yet never reach any file. Immediate and
 * manual reports DO advance the watermark (the scheduled delta must not
 * repeat their lines); replay never touches it (historical statuses must not
 * regress last_status).
 *
 * <p>Deliberate 5-dependency aggregator: this service owns the whole report
 * flow (read, registry, ledger, layout, transactions).
 */
@Service
public class ImmediateReportService {

    private static final Logger log = LoggerFactory.getLogger(ImmediateReportService.class);

    private final PrgWatermarkRepo watermarks;
    private final PrgReportRepo reports;
    private final PrgDeliveryLedgerRepo ledger;
    private final ExchangeLayout layout;
    private final TransactionTemplate freshTx;
    private final int sliceSize;

    public ImmediateReportService(final PrgWatermarkRepo watermarks, final PrgReportRepo reports,
                                  final PrgDeliveryLedgerRepo ledger, final ExchangeLayout layout,
                                  final PlatformTransactionManager txManager,
                                  @Value("${dcre.prg.psr-slice-size:50000}") final int sliceSize) {
        this.watermarks = watermarks;
        this.reports = reports;
        this.ledger = ledger;
        this.layout = layout;
        this.freshTx = new TransactionTemplate(txManager);
        this.freshTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.sliceSize = sliceSize;
    }

    /** Job-less convenience (IMMEDIATE, direct callers/tests): no batch execution, so job_name stays null. */
    public Optional<Path> reportParent(final String client, final String sourceMsgId,
                                       final String windowKey) throws IOException {
        return reportParent(client, sourceMsgId, windowKey, null, null);
    }

    /**
     * Job-less convenience: MANUAL regen when manualRef is set (all current
     * reportable statuses, guard bypassed, still ledgered); job_name stays null.
     */
    public Optional<Path> reportParent(final String client, final String sourceMsgId,
                                       final String windowKey, final String manualRef) throws IOException {
        return reportParent(client, sourceMsgId, windowKey, manualRef, null);
    }

    /**
     * IMMEDIATE (manualRef null) or MANUAL regen (manualRef set): ledger-guarded
     * or guard-bypassed parent delta.
     *
     * @param jobName SCRUM-58 clock-scoped trace anchor (env JOB_NAME or
     *                {@code local-prg-<executionId>}), stamped on the report row.
     * @return the emitted file, or empty when nothing is unreported.
     */
    public Optional<Path> reportParent(final String client, final String sourceMsgId,
                                       final String windowKey, final String manualRef,
                                       final String jobName) throws IOException {
        final Path target = outDir(client).resolve(client + "_PSR_" + windowKey + ".txt");
        final List<StatusRow> probe = freshTx.execute(s -> read(client, sourceMsgId, manualRef, ""));
        if (probe == null || probe.isEmpty()) {
            return recoverStanding(client, sourceMsgId, manualRef, target);
        }
        final PrgReportEntity report = openReport(client, sourceMsgId, windowKey, manualRef, target, jobName);
        stream(report, client, sourceMsgId, manualRef, target);
        advanceWatermarks(client, report.getId());
        return Optional.of(target);
    }

    /** Job-less convenience: replay audited with a null job_name. */
    public Path replay(final UUID reportId) throws IOException {
        return replay(reportId, null);
    }

    /** Exact replay of one report from its ledger rows; audited as a new MANUAL prg_report row. */
    public Path replay(final UUID reportId, final String jobName) throws IOException {
        final PrgReportEntity original = reports.findById(reportId).orElseThrow(
                () -> new IllegalArgumentException("no prg_report row for report.id=" + reportId));
        final List<LedgerRow> rows = ledger.rowsForReport(reportId);
        final Path target = outDir(original.getClient()).resolve(
                original.getFileName() + ".replay-" + Instant.now().getEpochSecond());
        final PrgReportEntity audit = freshTx.execute(s -> reports.save(PrgReportEntity.of(
                original.getClient(), "MANUAL", "REPLAY", original.getWindowKey(),
                original.getParentSourceMsgId(), target.getFileName().toString(), jobName)));
        try (StreamedPsrWrite psr = StreamedPsrWrite.begin(target,
                "PSR|" + original.getClient() + "|" + original.getWindowKey())) {
            for (final LedgerRow row : rows) {
                psr.writeTx("TX|" + row.e2e() + "|" + row.status());
            }
            psr.commit();
        }
        final String ref = "replay-" + reportId;
        CrdbRetry.run("replay-ledger report=%s".formatted(audit.getId()),
                () -> freshTx.executeWithoutResult(s -> rows.forEach(r ->
                        ledger.record(audit.getId(), original.getClient(), r.e2e(), r.status(), ref))));
        log.info("report stage=PRG type=MANUAL trigger=REPLAY client={} report={} lines={} file={}",
                original.getClient(), reportId, rows.size(), target.getFileName());
        return target;
    }

    /**
     * No unreported delta, but a report row for this window may stand: the
     * kill-resume window. File present = killed between the ATOMIC_MOVE and
     * the advance: finish the advance. File absent with ledgered rows =
     * killed between a ledger slice commit and the move: re-render the file
     * from the ledger, then advance. No standing row, or a standing row with
     * nothing ledgered yet, is a plain no-op (nothing was lost).
     */
    private Optional<Path> recoverStanding(final String client, final String sourceMsgId,
                                           final String manualRef, final Path target) throws IOException {
        final Optional<PrgReportEntity> standing = reports.findByFileName(target.getFileName().toString());
        if (standing.isEmpty()
                || (!Files.exists(target) && ledger.countForReport(standing.get().getId()) == 0)) {
            return Optional.empty();
        }
        stream(standing.get(), client, sourceMsgId, manualRef, target); // existing file = stream no-op
        advanceWatermarks(client, standing.get().getId());
        return Optional.of(target);
    }

    /** find-or-save keyed on the unique file_name: a Batch restart of the same window reuses the row. */
    private PrgReportEntity openReport(final String client, final String sourceMsgId,
                                       final String windowKey, final String manualRef, final Path target,
                                       final String jobName) {
        final String fileName = target.getFileName().toString();
        return freshTx.execute(s -> reports.findByFileName(fileName).orElseGet(() -> {
            final String trigger = watermarks.dueReason(client, sourceMsgId)
                    .orElse(manualRef == null ? "ADHOC" : "MANUAL");
            return reports.save(PrgReportEntity.of(client, manualRef == null ? "IMMEDIATE" : "MANUAL",
                    trigger, windowKey, sourceMsgId, fileName, jobName));
        }));
    }

    /** Streams header + TX lines + counted trailer; every written line was ledgered in its slice's own tx. */
    private void stream(final PrgReportEntity report, final String client, final String sourceMsgId,
                        final String manualRef, final Path target) throws IOException {
        if (Files.exists(target)) {
            return; // R-24 restart no-op: the prior emission stands, its lines are already ledgered
        }
        try (StreamedPsrWrite psr = StreamedPsrWrite.begin(target,
                "PSR|" + client + "|" + report.getWindowKey())) {
            String after = seedLedgered(psr, report.getId());
            while (true) {
                final String resume = after;
                final List<StatusRow> slice = CrdbRetry.get(
                        "immediate-slice client=%s parent=%s".formatted(client, sourceMsgId),
                        () -> freshTx.execute(s -> ledgerSlice(report.getId(), client, sourceMsgId,
                                manualRef, resume)));
                for (final StatusRow row : slice) {
                    psr.writeTx("TX|" + row.e2e() + "|" + row.status());
                }
                if (slice.size() < sliceSize) {
                    break;
                }
                after = slice.getLast().e2e();
            }
            psr.commit();
            log.info("report stage=PRG type={} client={} parent={} window={} lines={} file={}",
                    report.getType(), client, sourceMsgId, report.getWindowKey(),
                    psr.count(), target.getFileName());
        }
    }

    /**
     * Kill-resume seed: lines a PRIOR crashed attempt of THIS report already
     * ledgered (slice txs commit before the ATOMIC_MOVE) render first, or
     * they would never reach any file. A fresh report has zero rows (no-op);
     * returning the last seeded e2e resumes the MANUAL keyset without minting
     * duplicate manual_ref rows.
     */
    private String seedLedgered(final StreamedPsrWrite psr, final UUID reportId) throws IOException {
        final List<LedgerRow> recovered = ledger.rowsForReport(reportId);
        for (final LedgerRow row : recovered) {
            psr.writeTx("TX|" + row.e2e() + "|" + row.status());
        }
        return recovered.isEmpty() ? "" : recovered.getLast().e2e();
    }

    /** One slice: read (auto = ledger drop-out pagination, manual = keyset) + ledger in the SAME fresh tx. */
    private List<StatusRow> ledgerSlice(final UUID reportId, final String client, final String sourceMsgId,
                                        final String manualRef, final String afterE2e) {
        final List<StatusRow> rows = read(client, sourceMsgId, manualRef, afterE2e);
        for (final StatusRow row : rows) {
            ledger.record(reportId, client, row.e2e(), row.status(), manualRef);
        }
        return rows;
    }

    private List<StatusRow> read(final String client, final String sourceMsgId, final String manualRef,
                                 final String afterE2e) {
        return manualRef == null
                ? watermarks.findUnreportedForParent(client, sourceMsgId, sliceSize)
                : watermarks.findCurrentForParent(client, sourceMsgId, afterE2e, sliceSize);
    }

    private void advanceWatermarks(final String client, final UUID reportId) {
        CrdbRetry.run("immediate-advance client=%s report=%s".formatted(client, reportId),
                () -> freshTx.executeWithoutResult(s -> {
                    for (final LedgerRow row : ledger.rowsForReport(reportId)) {
                        watermarks.upsertWatermark(client, row.e2e(), row.status());
                    }
                }));
    }

    private Path outDir(final String client) {
        return layout.resolve(client, ExchangeChannel.ONHOST_RESP, ExchangeSub.OUT);
    }
}
