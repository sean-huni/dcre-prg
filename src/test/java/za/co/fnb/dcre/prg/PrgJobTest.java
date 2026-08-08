package za.co.fnb.dcre.prg;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import za.co.fnb.dcre.prg.service.PsrReportService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class PrgJobTest {

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
    Job prgJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    /** Tables exist via the 001-bootstrap-sources-crg guards; seed data only. */
    void seed(UUID arrival, String client) {
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                + " initg_pty, business_date, client_token, layout_version)"
                + " VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, "DCRERFPRG01", "DCRERFPRG01", "20260712080000", 4, "FNBRF01", "20260712", client, 2);
        for (int i = 1; i <= 4; i++) {
            jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                    + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                    arrival, i, "DC", "E2EPRG" + i, "E2EPRG" + i, "62000000010", "ZAR", "1000", 10.00 * i);
            jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')",
                    arrival, i);
        }
        // Response legs: tx1 + tx2 settled (PBSR ACSC), tx3 rejected (PBSR RJCT), tx4 only CTV PASS.
        pbsr("E2EPRG1", "ACSC", null);
        pbsr("E2EPRG2", "ACSC", null);
        pbsr("E2EPRG3", "RJCT", "AM04");
        // tx5: mid-DAG state, no CTV verdict yet -> status NULL -> never reportable.
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, 5, "DC", "E2EPRG5", "E2EPRG5", "62000000010", "ZAR", "1000", 50.00);
    }

    void pbsr(String e2e, String status, String reason) {
        jdbc.update("INSERT INTO pbsr_resp (response_file, orgnl_msg_id, e2e, status, reason)"
                + " VALUES (?,?,?,?,?)", "PBSR_20260712.xml", "DCRERFPRG01", e2e, status, reason);
    }

    JobParameters window(String client, String window, boolean resend) {
        JobParametersBuilder builder = new JobParametersBuilder()
                .addString("client", client, true)
                .addString("window", window, true);
        if (resend) {
            builder.addString("resend", "true", false);
        }
        return builder.toJobParameters();
    }

    List<String> txLines(Path file) throws Exception {
        return Files.readAllLines(file).stream().filter(l -> l.startsWith("TX|")).toList();
    }

    @Test
    void emitsDeltaPsrPerClockWindow() throws Exception {
        UUID arrival = UUID.randomUUID();
        // SCRUM-42: a real configured client token; PSR now lands under the per-client /out leaf.
        String client = "FNBRF01";
        seed(arrival, client);
        Path dir = Path.of("build/test-exchange/fnbrf01/onhost-resp/out");

        Logger psrLogger = (Logger) LoggerFactory.getLogger(PsrReportService.class);
        ListAppender<ILoggingEvent> warns = new ListAppender<>();
        warns.start();
        psrLogger.addAppender(warns);

        // (a) w1: full first delta, 4 rows with the deepest-leg status each (R-17).
        JobExecution w1 = jobOperator.start(prgJob, window(client, "w1", false));
        assertEquals(BatchStatus.COMPLETED, w1.getStatus());

        // R-38 exclusion visibility: tx5 has no verdict (status NULL) -> exactly one WARN in w1.
        List<String> exclusionWarns = warns.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("excluded stage=PRG"))
                .toList();
        assertEquals(1, exclusionWarns.size(), "exactly one excluded tx in w1 (R-38)");
        assertEquals("excluded stage=PRG arrival=" + arrival + " seq=5 e2e=E2EPRG5"
                + " reason=STATUS_UNKNOWN", exclusionWarns.get(0), "uniform R-38 WARN shape");
        psrLogger.detachAppender(warns);
        Path file1 = dir.resolve(client + "_PSR_w1.txt");
        List<String> lines = Files.readAllLines(file1);
        assertEquals("PSR|" + client + "|w1", lines.get(0));
        assertEquals("END|4", lines.get(lines.size() - 1));
        assertEquals(List.of("TX|E2EPRG1|ACSC", "TX|E2EPRG2|ACSC", "TX|E2EPRG3|RJCT",
                "TX|E2EPRG4|CTV_PASS"), txLines(file1));
        assertEquals(4, jdbc.queryForObject(
                "SELECT count(*) FROM prg_watermark WHERE client=?", Integer.class, client));

        // (b) w2: nothing moved -> zero-valued heartbeat PSR (SCRUM-55), watermark untouched.
        JobExecution w2 = jobOperator.start(prgJob, window(client, "w2", false));
        assertEquals(BatchStatus.COMPLETED, w2.getStatus());
        assertEquals(List.of("PSR|" + client + "|w2",
                        "HB|DCRE00000000000000000000000000000|DCRE00000000000000000000000000000|0|0.00",
                        "PD|0", "END|0"),
                Files.readAllLines(dir.resolve(client + "_PSR_w2.txt")),
                "an unchanged window emits the zero-valued heartbeat PSR");
        assertEquals(4, jdbc.queryForObject(
                "SELECT count(*) FROM prg_watermark WHERE client=?", Integer.class, client),
                "the heartbeat never advances the watermark");

        // (c) w3: one status flips, the file carries exactly that one row.
        jdbc.update("UPDATE pbsr_resp SET status='RJCT', reason='MS03' WHERE e2e='E2EPRG1'");
        JobExecution w3 = jobOperator.start(prgJob, window(client, "w3", false));
        assertEquals(BatchStatus.COMPLETED, w3.getStatus());
        assertEquals(List.of("TX|E2EPRG1|RJCT"), txLines(dir.resolve(client + "_PSR_w3.txt")));

        // (d) w4 resend: watermark ignored, all 4 current rows re-emitted.
        JobExecution w4 = jobOperator.start(prgJob, window(client, "w4", true));
        assertEquals(BatchStatus.COMPLETED, w4.getStatus());
        assertEquals(List.of("TX|E2EPRG1|RJCT", "TX|E2EPRG2|ACSC", "TX|E2EPRG3|RJCT",
                "TX|E2EPRG4|CTV_PASS"), txLines(dir.resolve(client + "_PSR_w4.txt")));
        assertTrue(Files.readAllLines(dir.resolve(client + "_PSR_w4.txt")).contains("END|4"));
    }

    /**
     * SCRUM-42 fail-closed: an unconfigured client with a reportable row makes the layout
     * throw rather than emit to a shared/wrong directory, so the batch job fails closed.
     */
    @Test
    void failsClosedWhenClientHasNoConfiguredExchangeDir() throws Exception {
        UUID arrival = UUID.randomUUID();
        String client = "FNBZZ99"; // absent from dcre-exchange-layout.yml
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                + " initg_pty, business_date, client_token, layout_version)"
                + " VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, "DCREZZPRG01", "DCREZZPRG01", "20260712080000", 1, client, "20260712", client, 2);
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, 1, "DC", "E2EZZ1", "E2EZZ1", "62000000010", "ZAR", "1000", 10.00);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')",
                arrival, 1);

        JobExecution failed = jobOperator.start(prgJob, window(client, "w1", false));

        assertEquals(BatchStatus.FAILED, failed.getStatus(), "unconfigured client must fail closed");
        assertTrue(failed.getAllFailureExceptions().stream()
                        .anyMatch(t -> t instanceof IllegalArgumentException && t.getMessage().contains(client)),
                "failure is the fail-closed IllegalArgumentException naming the unconfigured client");
        assertFalse(Files.exists(Path.of("build/test-exchange/fnbzz99/onhost-resp/out/" + client + "_PSR_w1.txt")),
                "no PSR file is written for an unconfigured client");
    }
}
