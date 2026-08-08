package za.co.fnb.dcre.prg.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Scheduled-path honesty (review crg-12): the ledger and the watermark must
 * record EXACTLY the (e2e, status) lines the emitted PSR file carries. A row
 * whose status moves between the streamed delta read and the advance phase
 * was never externally reported at the new status: it stays unledgered at
 * that status and rides the next window's delta.
 */
class PsrReportServiceAdvanceHonestyTest {

    private static final String CLIENT = "FNBRF01";

    /** Larger than the stubbed delta so both phases finish on the first slice. */
    private static final int SLICE_SIZE = 10;

    @TempDir
    Path root;

    private PrgWatermarkRepo watermarks;
    private PrgDeliveryLedgerRepo ledger;
    private PsrReportService service;

    @BeforeEach
    void setUp() {
        watermarks = mock(PrgWatermarkRepo.class);
        PrgReportRepo reports = mock(PrgReportRepo.class);
        when(reports.findByFileName(anyString())).thenReturn(Optional.empty());
        when(reports.save(any())).thenAnswer(inv -> inv.getArgument(0));
        ledger = mock(PrgDeliveryLedgerRepo.class);
        ExchangeLayout layout = new ExchangeLayout(root, Map.of(CLIENT,
                Map.of(ExchangeChannel.ONHOST_RESP, Map.of(ExchangeSub.OUT, "fnbrf01/onhost-resp/out"))));
        service = new PsrReportService(watermarks, reports, ledger, layout,
                new ResourcelessTransactionManager(), SLICE_SIZE);
        when(watermarks.countUnknown(CLIENT)).thenReturn(0L);
    }

    @Test
    void statusMovingBetweenStreamAndAdvanceIsLedgeredAtTheEmittedFileStatus() throws Exception {
        // The stream phase reads ACSP and writes it to the file; by advance time
        // the book has moved to ACSC (a row mutating mid-run).
        when(watermarks.findDeltaSlice(CLIENT, "", SLICE_SIZE))
                .thenReturn(List.of(new StatusRow("E2E1", "ACSP")))
                .thenReturn(List.of(new StatusRow("E2E1", "ACSC")));

        Optional<Path> emitted = service.window(CLIENT, "w1", false);

        assertTrue(emitted.isPresent(), "the window emits the streamed delta");
        assertEquals(List.of("PSR|" + CLIENT + "|w1", "TX|E2E1|ACSP", "END|1"),
                Files.readAllLines(emitted.get()), "the file carries the streamed status");
        verify(watermarks).upsertWatermark(CLIENT, "E2E1", "ACSP");
        verify(watermarks, never()).upsertWatermark(CLIENT, "E2E1", "ACSC");
        verify(ledger).record(any(), eq(CLIENT), eq("E2E1"), eq("ACSP"), isNull());
        verify(ledger, never()).record(any(), eq(CLIENT), eq("E2E1"), eq("ACSC"), any());
    }
}
