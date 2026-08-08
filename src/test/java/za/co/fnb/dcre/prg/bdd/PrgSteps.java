package za.co.fnb.dcre.prg.bdd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import za.co.fnb.dcre.prg.service.PsrReportService;

/** Glue for the PSR window projection feature; scenario-scoped (fresh instance per scenario). */
public class PrgSteps {

    // SCRUM-42: each scenario draws a distinct configured client from the isolation pool
    // (FNBT00..FNBT15 in the test dcre-exchange-layout.yml). A unique client per scenario
    // keeps the client-partitioned view, the e2e response joins, and the (client, window)
    // batch job identity from colliding across scenarios sharing one container.
    private static final AtomicInteger POOL_SEQ = new AtomicInteger();

    @Autowired
    Job prgJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    private String client;
    private UUID arrival;
    private String msgId;
    private int nextSequence;
    private final Map<String, String> e2eByLabel = new HashMap<>();
    private final Map<String, Integer> seqByLabel = new HashMap<>();
    private JobExecution lastRun;
    private ListAppender<ILoggingEvent> warns;
    private Map<String, String> watermarkSnapshot;

    @Before
    public void attachWarnAppender() {
        warns = new ListAppender<>();
        warns.start();
        ((Logger) LoggerFactory.getLogger(PsrReportService.class)).addAppender(warns);
    }

    @After
    public void detachWarnAppender() {
        ((Logger) LoggerFactory.getLogger(PsrReportService.class)).detachAppender(warns);
    }

    @Given("a PRG client with a new arrival")
    public void aPrgClientWithANewArrival() {
        client = "FNBT%02d".formatted(POOL_SEQ.getAndIncrement());
        arrival = UUID.randomUUID();
        msgId = "MSG" + client;
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                + " initg_pty, business_date, client_token, layout_version)"
                + " VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260712080000", 9, client, "20260712", client, 2);
    }

    @Given("transaction {string} has SBSR status {string} and PBSR status {string}")
    public void hasSbsrAndPbsr(String label, String sbsrStatus, String pbsrStatus) {
        int seq = seedEntry(label);
        pass(seq);
        respond("sbsr_resp", label, sbsrStatus);
        respond("pbsr_resp", label, pbsrStatus);
    }

    @Given("transaction {string} has only ISR status {string}")
    public void hasOnlyIsr(String label, String isrStatus) {
        int seq = seedEntry(label);
        pass(seq);
        respond("isr_resp", label, isrStatus);
    }

    @Given("transaction {string} has only a CTV PASS verdict")
    public void hasOnlyCtvPass(String label) {
        pass(seedEntry(label));
    }

    @Given("transaction {string} has no verdict yet")
    public void hasNoVerdictYet(String label) {
        seedEntry(label);
    }

    @Given("the PRG window {string} has already emitted")
    public void windowAlreadyEmitted(String window) throws Exception {
        runWindow(window, false);
        assertEquals(BatchStatus.COMPLETED, lastRun.getStatus());
        assertTrue(Files.exists(psrFile(window)), "expected the prior window's PSR file");
        watermarkSnapshot = readWatermarks();
    }

    @When("the PRG window {string} runs")
    public void windowRuns(String window) throws Exception {
        runWindow(window, false);
    }

    @When("the PRG window {string} runs as a resend")
    public void windowRunsAsResend(String window) throws Exception {
        runWindow(window, true);
    }

    @When("the PBSR status of transaction {string} flips to {string}")
    public void pbsrStatusFlips(String label, String status) {
        jdbc.update("UPDATE pbsr_resp SET status=? WHERE e2e=?", status, e2eByLabel.get(label));
    }

    @Then("the PRG job completes")
    public void jobCompletes() {
        assertEquals(BatchStatus.COMPLETED, lastRun.getStatus());
    }

    @Then("the PSR file for window {string} reports exactly:")
    public void psrReportsExactly(String window, DataTable table) throws Exception {
        List<String> lines = Files.readAllLines(psrFile(window));
        List<String> expectedTx = table.asMaps().stream()
                .map(row -> "TX|" + e2eByLabel.get(row.get("transaction")) + "|" + row.get("status"))
                .toList();
        assertEquals("PSR|" + client + "|" + window, lines.get(0), "PSR header");
        assertEquals("END|" + expectedTx.size(), lines.get(lines.size() - 1), "PSR trailer count");
        assertEquals(expectedTx, lines.stream().filter(l -> l.startsWith("TX|")).toList());
    }

    /** SCRUM-55: a quiet window emits the exact zero-valued heartbeat (HB placeholder = DCRE + 29 zeros). */
    @Then("the PSR file for window {string} is a zero-valued heartbeat")
    public void psrFileIsZeroValuedHeartbeat(String window) throws Exception {
        assertEquals(List.of("PSR|" + client + "|" + window,
                "HB|DCRE00000000000000000000000000000|DCRE00000000000000000000000000000|0|0.00",
                "PD|0", "END|0"), Files.readAllLines(psrFile(window)),
                "an unchanged window emits the zero-valued heartbeat PSR");
        assertEquals("HEARTBEAT", jdbc.queryForObject(
                "SELECT type FROM prg_report WHERE file_name=?", String.class,
                psrFile(window).getFileName().toString()), "heartbeat registry row");
    }

    @Then("the client watermark is unchanged by the heartbeat")
    public void watermarkUnchangedByHeartbeat() {
        assertFalse(watermarkSnapshot.isEmpty(), "the prior window advanced at least one watermark");
        assertEquals(watermarkSnapshot, readWatermarks(), "heartbeats never advance the watermark");
    }

    @Then("exactly one WARN reports transaction {string} excluded for stage {string} with reason {string}")
    public void exactlyOneExclusionWarn(String label, String stage, String reason) {
        List<String> matches = warns.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("excluded stage=" + stage) && m.contains("arrival=" + arrival))
                .toList();
        assertEquals(1, matches.size(), "exactly one exclusion WARN for this arrival (R-38)");
        assertEquals("excluded stage=" + stage + " arrival=" + arrival + " seq=" + seqByLabel.get(label)
                + " e2e=" + e2eByLabel.get(label) + " reason=" + reason, matches.get(0));
    }

    @Then("the client watermark holds exactly:")
    public void watermarkHoldsExactly(DataTable table) {
        Map<String, String> expected = new HashMap<>();
        table.asMaps().forEach(row ->
                expected.put(e2eByLabel.get(row.get("transaction")), row.get("status")));
        assertEquals(expected, readWatermarks(), "watermarks advance only for emitted rows");
    }

    private Map<String, String> readWatermarks() {
        Map<String, String> rows = new HashMap<>();
        jdbc.query("SELECT e2e, last_status FROM prg_watermark WHERE client=?",
                rs -> { rows.put(rs.getString(1), rs.getString(2)); }, client);
        return rows;
    }

    private int seedEntry(String label) {
        int seq = ++nextSequence;
        String e2e = client + label;
        e2eByLabel.put(label, e2e);
        seqByLabel.put(label, seq);
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, seq, "DC", e2e, e2e, "62000000010", "ZAR", "1000", 10.00 * seq);
        return seq;
    }

    private void pass(int sequence) {
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')",
                arrival, sequence);
    }

    private void respond(String table, String label, String status) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status)"
                + " VALUES (?,?,?,?)",
                table.toUpperCase() + "_" + client + ".xml", msgId, e2eByLabel.get(label), status);
    }

    private void runWindow(String window, boolean resend) throws Exception {
        JobParametersBuilder builder = new JobParametersBuilder()
                .addString("client", client, true)
                .addString("window", window, true);
        if (resend) {
            builder.addString("resend", "true", false);
        }
        lastRun = jobOperator.start(prgJob, builder.toJobParameters());
    }

    private Path psrFile(String window) {
        // SCRUM-42 per-client leaf: <root>/<base>/onhost-resp/out/<client>_PSR_<window>.txt.
        return Path.of("build/test-exchange", client.toLowerCase(), "onhost-resp", "out",
                client + "_PSR_" + window + ".txt");
    }
}
