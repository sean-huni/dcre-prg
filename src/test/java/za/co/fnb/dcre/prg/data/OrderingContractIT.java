package za.co.fnb.dcre.prg.data;

import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ORDERING CONTRACT stated in {@code db.changelog-master.xml}, executed rather than asserted in
 * a comment.
 *
 * <p>Until 2026-08-08 this class was {@code BootstrapOrderIT} and it proved the opposite property:
 * that PRG could migrate FIRST on an empty {@code dcre_pay} because it pre-created the nine
 * relations it reads behind {@code onFail="CONTINUE"} preconditions. That pre-create made PRG a
 * second writer of relations owned by six sibling services, and CONTINUE skips silently, so
 * whichever service migrated second inherited the other's shape permanently and invisibly. CRG
 * retired the identical pattern; PRG was forked before that retirement and kept it.
 *
 * <p>The contract that replaces it has to be tested from BOTH directions, because deleting a
 * pre-create without reproducing an ordering guarantee is how a schema-ownership violation becomes
 * a reader with no creator. That is the silent-zero shape this project has paid for twice
 * ({@code ais_verdict}, {@code pai_verdict}), so the negative direction is the important half:
 *
 * <ol>
 *   <li>{@link #theProductionMasterFailsLoudlyWhenItRunsBeforeItsPeers} proves a premature
 *       migration CRASHES and names the missing relation. It cannot report success, and it cannot
 *       leave a queryable view that returns nothing, because CockroachDB resolves a view body at
 *       CREATE time and simply refuses to create it;</li>
 *   <li>{@link #theProductionMasterSucceedsOnceItsPeersHaveMigrated} proves the same changelog then
 *       applies cleanly and every view executes, so the failure above is genuinely an ordering
 *       failure and not a broken changelog.</li>
 * </ol>
 *
 * <p>Both run the SHIPPED master with the SHIPPED history-table names. The peers are stood up by
 * the test fixture the shipped changelog no longer contains.
 */
class OrderingContractIT {

    private static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    private static final String PRODUCTION_MASTER = "classpath:db/changelog/db.changelog-master.xml";
    private static final String TEST_MASTER = "classpath:db/changelog/db.changelog-test-master.xml";

    /** Every object the SHIPPED changelog is responsible for producing, named so a miss is legible. */
    private static final List<String> OWNED_TABLES =
            List.of("prg_status_class", "prg_watermark", "prg_report", "prg_delivery_ledger");

    /** Relations PRG READS and does not own. Nothing in the shipped changelog may create these. */
    private static final List<String> PEER_TABLES =
            List.of("tx_header", "tx_entry", "validation_log", "isr_resp", "sbsr_resp", "pbsr_resp",
                    "prw_emission_group", "prw_emission", "prw_emission_member");

    private static final List<String> VIEWS =
            List.of("prg_isr_pick", "prg_sbsr_pick", "prg_pbsr_pick", "ext_tx_status",
                    "prg_member_status", "prg_report_due", "prg_sla_pending", "prg_status_exception");

    /**
     * The negative direction, and the reason this class exists in this form. A premature migration
     * must be LOUD.
     *
     * <p>The assertion names the missing relation rather than merely requiring an exception: a bare
     * throw assertion would also pass if the container were dead, the database absent or the
     * changelog unparseable, which are three ways to succeed without testing anything.
     */
    @Test
    void theProductionMasterFailsLoudlyWhenItRunsBeforeItsPeers() {
        final DataSource ds = freshDatabase("peers_absent");

        assertThatThrownBy(() -> migrate(ds, PRODUCTION_MASTER))
                .as("a PRG migration that beats its peers must crash and say what is missing,"
                        + " never report success over relations nothing created")
                .hasMessageFindingMatch("(?is)tx_header|isr_resp|prw_emission|relation.*does not exist");

        final JdbcTemplate jdbc = new JdbcTemplate(ds);
        for (final String table : PEER_TABLES) {
            assertThat(relationExists(jdbc, table))
                    .as("PRG created %s, which it does not own: the retired bootstrap guard is back",
                            table)
                    .isFalse();
        }
        for (final String view : VIEWS) {
            assertThat(relationExists(jdbc, view))
                    .as("view %s exists after a failed migration, so a reader could query it and"
                            + " get an empty result instead of an error", view)
                    .isFalse();
        }
    }

    /** The positive direction: with the peers present, the SAME changelog applies and works. */
    @Test
    void theProductionMasterSucceedsOnceItsPeersHaveMigrated() throws Exception {
        final DataSource ds = freshDatabase("peers_present");
        final JdbcTemplate jdbc = new JdbcTemplate(ds);
        migrate(ds, TEST_MASTER);

        for (final String table : OWNED_TABLES) {
            assertThat(relationExists(jdbc, table)).as("PRG-owned table %s is absent", table).isTrue();
        }
        for (final String view : VIEWS) {
            assertThat(relationExists(jdbc, view)).as("view %s is absent", view).isTrue();
            // Existing is not the same as working: a view can be created and still be unqueryable
            // once CockroachDB resolves its dependencies, so every one is actually executed.
            assertThatCode(() -> jdbc.queryForObject("SELECT count(*) FROM " + view, Long.class))
                    .as("view %s exists but does not execute", view)
                    .doesNotThrowAnyException();
        }

        // The catalogue is reference data the projection depends on, not decoration: an empty one
        // makes every status unreportable and every PSR empty, with no error anywhere.
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_status_class", Long.class))
                .as("the v1 status catalogue must seed all fourteen recognised codes")
                .isEqualTo(14L);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_class WHERE sla_suppressed", Long.class))
                .as("ACWC and ACWP are the two SLA-suppressed codes")
                .isEqualTo(2L);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_class WHERE NOT reportable", Long.class))
                .as("no v1 code is unreportable: unknown codes fail closed by ABSENCE from the"
                        + " catalogue, not by a row saying so")
                .isZero();

        // Batch metadata is in the same changelog and is just as load-bearing: without it the
        // persistent JobRepository silently has nowhere to write. It became typed tags on
        // 2026-08-08, so the sequences are asserted alongside the tables: the external SQL file it
        // replaced created three of them and a typed transcription is exactly where one goes missing.
        assertThat(relationExists(jdbc, "prg_batch_job_instance"))
                .as("PRG_BATCH_ metadata is absent").isTrue();
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.sequences"
                        + " WHERE sequence_schema = current_schema()"
                        + " AND sequence_name LIKE 'prg\\_batch\\_%'", Long.class))
                .as("the three PRG_BATCH_ sequences must survive the typed-tag conversion")
                .isEqualTo(3L);
    }

    /**
     * The v1 baseline carries none of CRG's re-run guards on the schema PRG designs, on the argument
     * that DATABASECHANGELOG alone makes a re-run safe from a clean baseline. That argument is
     * executed here rather than asserted in a comment.
     */
    @Test
    void reRunningTheWholeChangelogChangesNothingAndRaisesNothing() throws Exception {
        final DataSource ds = freshDatabase("rerun_safety");
        final JdbcTemplate jdbc = new JdbcTemplate(ds);
        migrate(ds, TEST_MASTER);

        final Long applied = jdbc.queryForObject(
                "SELECT count(*) FROM prg_databasechangelog", Long.class);

        assertThatCode(() -> migrate(ds, TEST_MASTER))
                .as("a second run of the v1 baseline must be a no-op; if this throws, the"
                        + " missing re-run guards were load-bearing after all")
                .doesNotThrowAnyException();

        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_databasechangelog", Long.class))
                .as("the second run logged additional changesets, so something re-executed")
                .isEqualTo(applied);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_status_class", Long.class))
                .as("the seed changeset re-inserted, so the catalogue is now duplicated")
                .isEqualTo(14L);
    }

    // --- plumbing -----------------------------------------------------------------------------

    private DataSource freshDatabase(final String database) {
        new JdbcTemplate(dataSourceFor(CRDB.getDatabaseName())).execute("CREATE DATABASE " + database);
        return dataSourceFor(database);
    }

    private DataSource dataSourceFor(final String database) {
        final DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl("jdbc:postgresql://%s:%d/%s?sslmode=disable"
                .formatted(CRDB.getHost(), CRDB.getMappedPort(26257), database));
        ds.setUsername(CRDB.getUsername());
        ds.setPassword(CRDB.getPassword());
        return ds;
    }

    /** Runs a master changelog with the SHIPPED history-table names, not a test copy. */
    private void migrate(final DataSource ds, final String changeLog) throws Exception {
        final SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setDataSource(ds);
        liquibase.setChangeLog(changeLog);
        liquibase.setDatabaseChangeLogTable("prg_databasechangelog");
        liquibase.setDatabaseChangeLogLockTable("prg_databasechangeloglock");
        liquibase.setResourceLoader(new DefaultResourceLoader());
        liquibase.afterPropertiesSet();
    }

    private boolean relationExists(final JdbcTemplate jdbc, final String name) {
        final Long found = jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.tables"
                        + " WHERE table_schema = current_schema() AND table_name = ?",
                Long.class, name);
        return found != null && found > 0;
    }
}
