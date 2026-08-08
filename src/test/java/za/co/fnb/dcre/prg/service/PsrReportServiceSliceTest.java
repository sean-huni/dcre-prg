package za.co.fnb.dcre.prg.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-42 bounded-scan proofs against a real CRDB. The 30M-tx sweep killed
 * the PRG window with "sql: memory budget exceeded" on the whole-book R-38
 * scan while Java materialized the full delta in heap, so all reads are now
 * keyset slices (size 10 here) and the PSR streams to disk:
 * (1) a multi-slice delta produces ONE correct PSR file (header, ordered TX
 * rows, trailer count) with every watermark advanced and no stale tmp;
 * (2) a restart with the target already emitted skips the rewrite (R-24
 * no-op) but still advances the missing watermarks (R-29 replay);
 * (3) an unknown-status count above the detail limit collapses to ONE
 * summary WARN instead of a per-row WARN flood (CRW's hybrid choice).
 * Clients come from the FNBT pool top end; the BDD scenarios draw from 00 up.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.prg.psr-slice-size=10",
        "dcre.exchange-root=build/test-exchange", "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class PsrReportServiceSliceTest {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @Autowired
    PsrReportService service;

    @Autowired
    JdbcTemplate jdbc;

    ListAppender<ILoggingEvent> warns;

    @BeforeEach
    void attachWarnAppender() {
        warns = new ListAppender<>();
        warns.start();
        ((Logger) LoggerFactory.getLogger(PsrReportService.class)).addAppender(warns);
    }

    @AfterEach
    void detachWarnAppender() {
        ((Logger) LoggerFactory.getLogger(PsrReportService.class)).detachAppender(warns);
    }

    /** Seeds one arrival with {@code known} CTV_PASS rows (zero-padded e2e) + {@code unknown} verdict-less rows. */
    void seed(String client, int known, int unknown) {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                + " initg_pty, business_date, client_token, layout_version)"
                + " VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, "MSG" + client, "MSG" + client, "20260712080000", known + unknown,
                client, "20260712", client, 2);
        for (int i = 1; i <= known + unknown; i++) {
            jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                    + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                    arrival, i, "DC", e2e(client, i), e2e(client, i), "62000000010", "ZAR", "1000", 10.00);
            if (i <= known) {
                jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')",
                        arrival, i);
            }
        }
    }

    String e2e(String client, int i) {
        return "%sE2E%03d".formatted(client, i);
    }

    Path target(String client, String window) {
        return Path.of("build/test-exchange", client.toLowerCase(), "onhost-resp", "out",
                client + "_PSR_" + window + ".txt");
    }

    void cleanExchange(String client, String window) throws Exception {
        Files.deleteIfExists(target(client, window));
        Files.deleteIfExists(target(client, window).resolveSibling(client + "_PSR_" + window + ".txt.tmp"));
    }

    List<String> exclusionWarns() {
        return warns.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("excluded stage=PRG"))
                .toList();
    }

    @Test
    void multiSliceDeltaStreamsOneCorrectPsrAndAdvancesAllWatermarks() throws Exception {
        String client = "FNBT15";
        cleanExchange(client, "s1");
        seed(client, 25, 0); // slice size 10 -> 3 slices (10 + 10 + 5)

        Optional<Path> emitted = service.window(client, "s1", false);

        assertTrue(emitted.isPresent(), "a 3-slice delta emits exactly one PSR file");
        List<String> lines = Files.readAllLines(emitted.get());
        assertEquals(27, lines.size(), "header + 25 TX rows + trailer");
        assertEquals("PSR|" + client + "|s1", lines.getFirst(), "PSR header");
        assertEquals("END|25", lines.getLast(), "trailer counts ALL rows across slices");
        for (int i = 1; i <= 25; i++) {
            assertEquals("TX|" + e2e(client, i) + "|CTV_PASS", lines.get(i),
                    "slice boundaries neither drop nor duplicate row " + i);
        }
        assertFalse(Files.exists(emitted.get().resolveSibling(client + "_PSR_s1.txt.tmp")),
                "no stale tmp survives a committed stream");
        assertEquals(25, jdbc.queryForObject(
                "SELECT count(*) FROM prg_watermark WHERE client=? AND last_status='CTV_PASS'",
                Integer.class, client), "every slice's watermarks advanced after the move");
    }

    @Test
    void restartWithExistingTargetAdvancesExactlyTheStandingFileAndDefersTheDelta() throws Exception {
        String client = "FNBT14";
        cleanExchange(client, "s1");
        cleanExchange(client, "s2");
        seed(client, 5, 0);
        // Crash simulation: prior run emitted the file (R-24: existing target = prior
        // emission) and died before any watermark advance. The sentinel line proves
        // the replay advances the FILE's content, never a fresh delta re-read
        // (review crg-12 honesty: the ledger records what was externally reported).
        Files.createDirectories(target(client, "s1").getParent());
        List<String> sentinel = List.of("PSR|" + client + "|s1", "TX|sentinel|X", "END|1");
        Files.write(target(client, "s1"), sentinel);

        Optional<Path> emitted = service.window(client, "s1", false);

        assertEquals(Optional.of(target(client, "s1")), emitted, "the standing emission is reported");
        assertEquals(sentinel, Files.readAllLines(target(client, "s1")),
                "restart never rewrites an existing target (R-24 no-op)");
        assertEquals(List.of("sentinel|X"), jdbc.queryForList(
                "SELECT concat(e2e, '|', last_status) FROM prg_watermark WHERE client=?",
                String.class, client),
                "the replay advances EXACTLY the standing file's lines (R-29 second phase)");
        assertEquals(List.of("sentinel|X"), jdbc.queryForList(
                "SELECT concat(e2e, '|', status) FROM prg_delivery_ledger WHERE client=?",
                String.class, client),
                "the ledger holds only what a visible file carried");

        // the 5 seeded rows were in NO emitted file: they ride the next window
        Optional<Path> next = service.window(client, "s2", false);
        assertTrue(next.isPresent(), "the deferred delta emits on the next window");
        assertEquals(5, Files.readAllLines(next.get()).stream().filter(l -> l.startsWith("TX|")).count(),
                "all 5 deferred rows reach the next window's file");
        assertEquals(6, jdbc.queryForObject(
                "SELECT count(*) FROM prg_watermark WHERE client=?", Integer.class, client),
                "sentinel + the 5 deferred rows are advanced after the next window");
    }

    @Test
    void unknownCountAboveDetailLimitEmitsSingleSummaryWarn() throws Exception {
        String client = "FNBT13";
        cleanExchange(client, "s1");
        seed(client, 0, 101); // one past the per-row detail limit

        Optional<Path> emitted = service.window(client, "s1", false);

        // SCRUM-55: an unknown-only book carries no reportable delta -> the zero-valued heartbeat
        assertTrue(emitted.isPresent(), "a zero-delta window emits the heartbeat PSR");
        assertEquals("END|0", Files.readAllLines(emitted.get()).getLast(),
                "the heartbeat carries no TX lines");
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM prg_watermark WHERE client=?", Integer.class, client),
                "the heartbeat never advances the watermark");
        assertEquals(List.of("excluded stage=PRG client=" + client + " count=101 reason=STATUS_UNKNOWN"),
                exclusionWarns(), "101 unknowns collapse to ONE summary WARN, no per-row flood");
    }
}
