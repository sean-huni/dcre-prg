package za.co.fnb.dcre.prg.data;

import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The v1 baseline claims two things that are only claims until something runs them.
 *
 * <ol>
 *   <li>PRG is CLOCK-launched, so on a brand-new dcre_pay it can migrate BEFORE PRR/PTV/PRW/PIX/PSX/PPX
 *       have ever run. Its view stack must therefore stand up against nothing at all. A-79 is what
 *       happens when this is assumed instead of tested: a view left uncreated meant no arrival could
 *       ever complete, silently, with no alert.</li>
 *   <li>The changelog carries none of CRG's re-run guards on the schema PRG designs, on the argument
 *       that DATABASECHANGELOG alone makes a re-run safe from a clean baseline. That argument is
 *       executed here rather than asserted in a comment.</li>
 * </ol>
 *
 * <p>The third case is the honest one: the 001 bootstrap guard DUPLICATES nine tables it does not
 * own, and onFail="CONTINUE" means whichever service runs second silently keeps the first one's
 * shape. That is a real cost of the pattern, so it is pinned as observable behaviour rather than
 * left as a hazard nobody has looked at. BootstrapSourceParityTest is the other half: it compares
 * this module's copy against the owners' originals.
 */
class BootstrapOrderIT {

    private static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    /** Every object the changelog is responsible for producing, named so a miss is legible. */
    private static final List<String> OWNED_TABLES =
            List.of("prg_status_class", "prg_watermark", "prg_report", "prg_delivery_ledger");

    private static final List<String> BOOTSTRAPPED_TABLES =
            List.of("tx_header", "tx_entry", "validation_log", "isr_resp", "sbsr_resp", "pbsr_resp",
                    "prw_emission_group", "prw_emission", "prw_emission_member");

    private static final List<String> VIEWS =
            List.of("prg_isr_pick", "prg_sbsr_pick", "prg_pbsr_pick", "ext_tx_status",
                    "prg_member_status", "prg_report_due", "prg_sla_pending", "prg_status_exception");

    @Test
    void migratesFromNothingWhenPrgIsTheFirstServiceToTouchTheDatabase() throws Exception {
        final JdbcTemplate jdbc = migrateInto("bootstrap_first");

        for (final String table : BOOTSTRAPPED_TABLES) {
            assertThat(relationExists(jdbc, table))
                    .as("bootstrap guard did not create %s, so the view stack had no source", table)
                    .isTrue();
        }
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
        // persistent JobRepository silently has nowhere to write.
        assertThat(relationExists(jdbc, "prg_batch_job_instance"))
                .as("PRG_BATCH_ metadata is absent").isTrue();
    }

    @Test
    void reRunningTheWholeChangelogChangesNothingAndRaisesNothing() throws Exception {
        final DataSource ds = freshDatabase("rerun_safety");
        final JdbcTemplate jdbc = new JdbcTemplate(ds);
        migrate(ds);

        final Long applied = jdbc.queryForObject(
                "SELECT count(*) FROM prg_databasechangelog", Long.class);

        assertThatCode(() -> migrate(ds))
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

    /**
     * The duplication hazard of the 001 guard, made observable. A table the OWNER already created
     * is left exactly as the owner made it, including a shape PRG's own copy does not describe.
     * That is what onFail="CONTINUE" buys and also what it costs: if the two DDL copies ever drift,
     * whichever service runs second loses silently and nothing anywhere reports it.
     */
    @Test
    void aSourceTableThatAlreadyExistsIsLeftAloneRatherThanRewritten() throws Exception {
        final DataSource ds = freshDatabase("owner_first");
        final JdbcTemplate jdbc = new JdbcTemplate(ds);

        // Stand in for PRR having migrated first, with a column PRG's copy has never heard of.
        jdbc.execute("""
                CREATE TABLE tx_header (
                    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
                    arrival_id UUID NOT NULL,
                    msg_id_raw VARCHAR(35) NOT NULL,
                    msg_id VARCHAR(35) NOT NULL,
                    created_ts VARCHAR(14) NOT NULL,
                    tx_count INT NOT NULL,
                    initg_pty VARCHAR(35) NOT NULL,
                    business_date VARCHAR(8) NOT NULL,
                    client_token VARCHAR(16),
                    layout_version INT NOT NULL,
                    owner_only_marker VARCHAR(8),
                    version BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT uq_pay_header_arrival UNIQUE (arrival_id))""");

        migrate(ds);

        assertThat(columnExists(jdbc, "tx_header", "owner_only_marker"))
                .as("the guard overwrote the owner's table instead of standing down")
                .isTrue();
        for (final String view : VIEWS) {
            assertThat(relationExists(jdbc, view))
                    .as("view %s failed to build over the owner's pre-existing table", view)
                    .isTrue();
        }
    }

    // --- plumbing -----------------------------------------------------------------------------

    private JdbcTemplate migrateInto(final String database) throws Exception {
        final DataSource ds = freshDatabase(database);
        migrate(ds);
        return new JdbcTemplate(ds);
    }

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

    /** Runs the SHIPPED master changelog with the SHIPPED history-table names, not a test copy. */
    private void migrate(final DataSource ds) throws Exception {
        final SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setDataSource(ds);
        liquibase.setChangeLog("classpath:db/changelog/db.changelog-master.xml");
        liquibase.setDatabaseChangeLogTable("prg_databasechangelog");
        liquibase.setDatabaseChangeLogLockTable("prg_databasechangeloglock");
        liquibase.setResourceLoader(new org.springframework.core.io.DefaultResourceLoader());
        liquibase.afterPropertiesSet();
    }

    private boolean relationExists(final JdbcTemplate jdbc, final String name) {
        final Long found = jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.tables"
                        + " WHERE table_schema = current_schema() AND table_name = ?",
                Long.class, name);
        return found != null && found > 0;
    }

    private boolean columnExists(final JdbcTemplate jdbc, final String table, final String column) {
        final Long found = jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.columns"
                        + " WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
                Long.class, table, column);
        return found != null && found > 0;
    }
}
