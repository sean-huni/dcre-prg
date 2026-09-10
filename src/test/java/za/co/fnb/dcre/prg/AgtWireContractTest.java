package za.co.fnb.dcre.prg;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The datasource environment variable is a CROSS-REPOSITORY WIRE CONTRACT with AGT, and
 * this test pins THIS side of it.
 *
 * <p><b>It is a ONE-SIDED test and it does not prove the contract holds.</b> AGT injects an
 * environment variable into every stage Job it launches
 * ({@code JobLauncher.DB_URL_ENV = "DCRE_DB_URL"}), routing the VALUE per family. This test
 * asserts only that PRG READS that name. Nothing here can see AGT, so if AGT renames its
 * half tomorrow this test stays green and the service goes back to failing to connect. A
 * green run here means "PRG still reads the agreed name", never "PRG and AGT agree".
 *
 * <p>Reading AGT's source across the repository boundary was considered and rejected: it
 * would break every clean clone and every CI checkout of this repository alone. The durable
 * fix is GENERATION of both sides from one source, which is out of scope today and is
 * recorded as the real remedy rather than implied to be solved by this file.
 *
 * <p>Why it exists at all: PRG read {@code DCRE_PAY_DB_URL}, a name nothing in AGT or
 * dcre-infra ever set. In a pod it fell back to its committed {@code localhost:26257}
 * default, which is the pod itself, and could not reach any database. Both sides were
 * individually green the whole time.
 *
 * <p>Deliberate, minimal deviation from the sibling {@code AgtWireContractTest} in PAI: the
 * offender sweep below strips {@code #} comments before matching. PAI's version matches the
 * bare token in the raw file, which the explanatory comment above the datasource block in
 * PRG, PRW and six other payments services would satisfy as prose. A sweep a comment can
 * trip is a sweep that fires on documentation rather than on configuration.
 */
class AgtWireContractTest {

    /** The literal AGT injects. Changing it here without changing AGT breaks every pod. */
    private static final String AGT_INJECTED_NAME = "DCRE_DB_URL";

    private static final Path APPLICATION_YML = Path.of("src/main/resources/application.yml");

    /**
     * The datasource url must be a placeholder whose VARIABLE NAME is exactly the name AGT
     * injects. Asserted by extracting the name out of the placeholder rather than by
     * substring-matching the whole line, so that a service reading some OTHER variable that
     * merely happens to contain this one as a prefix or suffix still fails.
     */
    @Test
    void datasourceUrlReadsExactlyTheVariableNameAgtInjects() throws Exception {
        final String url = datasourceUrlLine();
        final Matcher placeholder = Pattern.compile("\\$\\{([A-Z0-9_]+):").matcher(url);
        assertThat(placeholder.find())
                .as("control: the datasource url is a ${VAR:default} placeholder at all,"
                        + " so the name extracted below is a real read and not an empty match")
                .isTrue();
        assertThat(placeholder.group(1))
                .as("PRG must read the variable AGT injects into every stage Job. A name only"
                        + " this repo knows is a name nothing in a pod ever sets, and the"
                        + " service then falls back to its localhost default, which in a pod"
                        + " is the pod itself")
                .isEqualTo(AGT_INJECTED_NAME);
    }

    /**
     * 12FactorApp Alignment (https://12factor.net/): a fresh clone with NO {@code .env} runs
     * on committed defaults, so the default must still name a working local database, and it
     * must name the PAYMENTS one. The database IS the family discriminator now, so a write
     * aimed at the collections database is a perfectly valid write to the wrong family and
     * never errors.
     */
    @Test
    void committedDefaultIsAWorkingLocalPaymentsDatabase() throws Exception {
        final String url = datasourceUrlLine();
        assertThat(url)
                .as("the committed default must name dcre_pay: the database IS the family"
                        + " discriminator now, and a valid write to the wrong database never"
                        + " errors")
                .contains("/dcre_pay?");
        assertThat(url)
                .as("no clean clone may be pointed at the collections database")
                .doesNotContain("dcre_col");
    }

    /**
     * The DRIFT SHAPE itself, swept over every shipped resource rather than over the one line
     * above. A per-family variable NAME is the thing that created this defect: AGT routes ONE
     * name per family, so a second name is a name nobody injects.
     */
    @Test
    void noShippedResourceReadsAPerFamilyDatabaseVariable() throws Exception {
        final List<Path> offenders;
        try (var paths = Files.walk(Path.of("src/main/resources"))) {
            offenders = paths.filter(Files::isRegularFile)
                    .filter(p -> activeBody(p).contains("DCRE_PAY_DB_")
                            || activeBody(p).contains("DCRE_COL_DB_")
                            || activeBody(p).contains("DCRE_MAN_DB_"))
                    .toList();
        }
        assertThat(offenders)
                .as("one variable routed per family, never one variable name per family:"
                        + " AGT injects DCRE_DB_URL and resolves the value from the stage")
                .isEmpty();
    }

    /**
     * Control for the sweep above. A walk that silently reached nothing would make
     * {@link #noShippedResourceReadsAPerFamilyDatabaseVariable} pass for the wrong reason,
     * which is the failure mode that let the original defect through: a green result that
     * proves the search ran, not that the thing is absent.
     */
    @Test
    void theResourceSweepCanActuallyFindSomething() throws Exception {
        final List<Path> found;
        try (var paths = Files.walk(Path.of("src/main/resources"))) {
            found = paths.filter(Files::isRegularFile)
                    // The PLACEHOLDER form, not the bare name, and comments stripped: prose
                    // mentioning the variable would otherwise satisfy this control, and a
                    // control a comment can satisfy is not a control.
                    .filter(p -> activeBody(p).contains("${" + AGT_INJECTED_NAME + ":"))
                    .toList();
        }
        assertThat(found)
                .as("control: the sweep reads real files, so an empty offender list above is"
                        + " an absence and not a walk that reached nothing")
                .isNotEmpty();
    }

    /** The datasource url line, comments stripped so prose can never satisfy an assertion. */
    private static String datasourceUrlLine() throws Exception {
        return Files.readAllLines(APPLICATION_YML).stream()
                .map(AgtWireContractTest::stripComment)
                .filter(l -> l.trim().startsWith("url:"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no datasource url line in " + APPLICATION_YML));
    }

    /** A file's CONFIGURATION, with every {@code #} comment removed. */
    private static String activeBody(final Path path) {
        try {
            return Files.readAllLines(path).stream()
                    .map(AgtWireContractTest::stripComment)
                    .reduce("", (a, b) -> a + "\n" + b);
        } catch (Exception e) {
            throw new IllegalStateException(path.toString(), e);
        }
    }

    private static String stripComment(final String line) {
        return line.replaceAll("#.*$", "");
    }
}
