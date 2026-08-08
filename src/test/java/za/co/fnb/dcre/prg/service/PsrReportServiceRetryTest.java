package za.co.fnb.dcre.prg.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.repo.PrgDeliveryLedgerRepo;
import za.co.fnb.dcre.prg.data.repo.PrgReportRepo;
import za.co.fnb.dcre.prg.data.repo.PrgWatermarkRepo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CRDB 40001 survival on the watermark advance (the observed WriteTooOldError
 * under 300k-tx load): bounded retry, R-29 order preserved (file first, so a
 * retried or even exhausted advance never loses the emission).
 */
class PsrReportServiceRetryTest {

    private static final String CLIENT = "FNBRF01";

    /** Larger than the stubbed delta so both phases finish on the first slice. */
    private static final int SLICE_SIZE = 10;

    private static final CannotAcquireLockException ABORT = new CannotAcquireLockException(
            "PreparedStatementCallback; ERROR: restart transaction: TransactionRetryWithProtoRefreshError:"
                    + " WriteTooOldError");

    @TempDir
    Path root;

    private PrgWatermarkRepo watermarks;
    private PsrReportService service;

    @BeforeEach
    void setUp() {
        watermarks = mock(PrgWatermarkRepo.class);
        // SCRUM-55 registry/ledger collaborators: benign mocks; the retry contract under test is unchanged.
        PrgReportRepo reports = mock(PrgReportRepo.class);
        when(reports.findByFileName(anyString())).thenReturn(Optional.empty());
        when(reports.save(any())).thenAnswer(inv -> inv.getArgument(0));
        PrgDeliveryLedgerRepo ledger = mock(PrgDeliveryLedgerRepo.class);
        ExchangeLayout layout = new ExchangeLayout(root, Map.of(CLIENT,
                Map.of(ExchangeChannel.ONHOST_RESP, Map.of(ExchangeSub.OUT, "fnbrf01/onhost-resp/out"))));
        service = new PsrReportService(watermarks, reports, ledger, layout,
                new ResourcelessTransactionManager(), SLICE_SIZE);
        when(watermarks.countUnknown(CLIENT)).thenReturn(0L);
        // SCRUM-42 sliced reads: the stream phase AND the R-29 advance phase each
        // read the first (and only) keyset slice.
        when(watermarks.findDeltaSlice(CLIENT, "", SLICE_SIZE)).thenReturn(List.of(
                new StatusRow("E2E1", "ACSC"), new StatusRow("E2E2", "RJCT")));
    }

    @Test
    void windowCompletesWhenUpsertThrowsTransientTwiceThenSucceeds() throws Exception {
        doThrow(ABORT).doThrow(ABORT).doNothing()
                .when(watermarks).upsertWatermark(CLIENT, "E2E1", "ACSC");

        Optional<Path> emitted = service.window(CLIENT, "w1", false);

        assertTrue(emitted.isPresent(), "two transient aborts must not fail the window");
        assertEquals(List.of("PSR|" + CLIENT + "|w1", "TX|E2E1|ACSC", "TX|E2E2|RJCT", "END|2"),
                Files.readAllLines(emitted.get()), "file written exactly once, before the advance");
        verify(watermarks, times(3)).upsertWatermark(CLIENT, "E2E1", "ACSC");
        verify(watermarks, times(1)).upsertWatermark(CLIENT, "E2E2", "RJCT");
    }

    @Test
    void windowPropagatesAfterAttemptBudgetOnPersistentAborts() {
        doThrow(ABORT).when(watermarks).upsertWatermark(CLIENT, "E2E1", "ACSC");

        assertThrows(CannotAcquireLockException.class, () -> service.window(CLIENT, "w1", false),
                "persistent 40001 aborts exhaust the budget and propagate");

        verify(watermarks, times(CrdbRetry.MAX_ATTEMPTS)).upsertWatermark(CLIENT, "E2E1", "ACSC");
        assertTrue(Files.exists(root.resolve("fnbrf01/onhost-resp/out/" + CLIENT + "_PSR_w1.txt")),
                "R-29: the PSR file stands even when the advance exhausts (replay is a no-op + advance)");
    }
}
