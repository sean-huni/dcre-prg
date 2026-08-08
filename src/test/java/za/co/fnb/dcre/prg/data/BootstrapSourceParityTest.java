package za.co.fnb.dcre.prg.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The test fixture is a SECOND copy of nine tables PRG does not own, so drift between the two
 * copies is invisible: no error, no log line, just a suite that keeps passing against a shape its
 * owner has already changed. "Two places holding one fact: one is stale and nothing tells you
 * which."
 *
 * <p>This test is the thing that tells you which. It reads the OWNERS' changelogs off disk and
 * compares them, column by column and constraint by constraint, against this module's copy. It is
 * deliberately not a comment in the changelog saying the shapes match.
 *
 * <p>It survived the 2026-08-08 retirement of the shipped bootstrap guard, and matters MORE
 * afterwards rather than less. Before, a shape mismatch produced a wrong table in production;
 * now it produces a test fixture that lies about production, which is the failure mode nobody
 * looks for. The one it caught immediately: {@code prw_emission_group.client} narrowed 35 -> 16
 * when A-43 made {@code client_token} the authority.
 *
 * <p>It can only run inside the monorepo working tree, where the sibling modules exist beside this
 * one. A standalone clone of dcre-prg has no siblings to compare against, so the test ASSUMES its
 * way out with a stated reason rather than passing quietly, and the surviving guard for that case
 * is BootstrapOrderIT.
 */
class BootstrapSourceParityTest {

    /** Sibling module directories, resolved from this module rather than from a fixed root. */
    private static final Path PAYMENTS = Path.of("").toAbsolutePath().getParent();

    private static final Map<String, String> OWNER_OF = Map.of(
            "tx_header", "prr",
            "tx_entry", "prr",
            "validation_log", "ptv",
            "isr_resp", "pix",
            "sbsr_resp", "psx",
            "pbsr_resp", "ppx",
            "prw_emission_group", "prw",
            "prw_emission", "prw",
            "prw_emission_member", "prw");

    /**
     * The TEST FIXTURE, since 2026-08-08. This file used to be a shipped bootstrap guard at
     * src/main/resources/db/changelog/2026/08/001-pay-report-sources.xml; retiring it moved the
     * mirror under src/test but did NOT remove the drift class, because the fixture is still a
     * hand-maintained copy of other repositories' DDL and PRG's suite can still stay green against
     * a shape its owner has already changed. CRG's equivalent fixture has only a comment saying so.
     */
    private static final Path OWN_GUARD =
            Path.of("src/test/resources/db/changelog/test/001-read-sources.xml");

    @Test
    void everyMirroredTableMatchesItsOwnersDeclarationColumnForColumn() throws Exception {
        assumeMonorepo();
        final Document mine = parse(OWN_GUARD);

        // Control (verification.md 11a): an empty comparison would pass vacuously, and the whole
        // point of this test is that an absence must be distinguishable from "nothing was compared".
        assertThat(OWNER_OF).as("nothing to compare").isNotEmpty();

        for (final Map.Entry<String, String> entry : new TreeMap<>(OWNER_OF).entrySet()) {
            final String table = entry.getKey();
            final Path ownerFile = ownerChangelogFor(table, entry.getValue());
            final Map<String, String> theirs = columnsOf(parse(ownerFile), table);
            final Map<String, String> ours = columnsOf(mine, table);

            assertThat(theirs)
                    .as("no createTable for %s found in %s: this test can no longer see what it"
                            + " exists to guard", table, ownerFile)
                    .isNotEmpty();
            assertThat(ours)
                    .as("%s owns %s but the PRG test fixture does not declare it, so this"
                            + " module's integration tests have no source for their views",
                            entry.getValue(), table)
                    .containsExactlyInAnyOrderEntriesOf(theirs);
        }
    }

    @Test
    void everyMirroredTableCarriesItsOwnersConstraintNames() throws Exception {
        assumeMonorepo();
        final Document mine = parse(OWN_GUARD);

        for (final Map.Entry<String, String> entry : new TreeMap<>(OWNER_OF).entrySet()) {
            final String table = entry.getKey();
            final Path ownerFile = ownerChangelogFor(table, entry.getValue());
            final Map<String, String> theirs = uniqueConstraintsOf(parse(ownerFile), table);
            final Map<String, String> ours = uniqueConstraintsOf(mine, table);

            assertThat(ours)
                    .as("constraint names on %s differ from %s's; a name mismatch means the"
                            + " owner's own guarded changeset can no longer recognise its work",
                            table, entry.getValue())
                    .containsExactlyInAnyOrderEntriesOf(theirs);
        }
    }

    private void assumeMonorepo() {
        Assumptions.assumeTrue(Files.isDirectory(PAYMENTS.resolve("prr")),
                "sibling payments modules are absent (standalone clone), so the owners'"
                        + " declarations cannot be read; OrderingContractIT still covers the"
                        + " ordering contract itself");
    }

    /** Finds the owner's changelog by SEARCHING for the createTable, never by a hardcoded path. */
    private Path ownerChangelogFor(final String table, final String module) throws IOException {
        final Path root = PAYMENTS.resolve(module).resolve("src/main/resources/db/changelog");
        try (Stream<Path> files = Files.walk(root)) {
            final Optional<Path> found = files
                    .filter(p -> p.toString().endsWith(".xml"))
                    .filter(p -> declares(p, table))
                    .findFirst();
            return found.orElseThrow(() -> new AssertionError(
                    "no changelog under %s declares %s".formatted(root, table)));
        }
    }

    private boolean declares(final Path file, final String table) {
        try {
            return !columnsOf(parse(file), table).isEmpty();
        } catch (final Exception e) {
            return false;
        }
    }

    /** Column name to normalised type, from the createTable body for one table. */
    private Map<String, String> columnsOf(final Document doc, final String table) {
        final Map<String, String> columns = new LinkedHashMap<>();
        for (final Element createTable : elements(doc, "createTable")) {
            if (!table.equals(createTable.getAttribute("tableName"))) {
                continue;
            }
            for (final Element column : childElements(createTable, "column")) {
                columns.put(column.getAttribute("name"),
                        column.getAttribute("type").toUpperCase(Locale.ROOT).replace(" ", ""));
            }
        }
        return columns;
    }

    /**
     * Unique constraint name to its normalised column list, gathered from BOTH spellings: the
     * standalone addUniqueConstraint tag and the inline uniqueConstraintName on a column's
     * constraints. The owners use both, so comparing only one would miss half the surface.
     */
    private Map<String, String> uniqueConstraintsOf(final Document doc, final String table) {
        final Map<String, String> constraints = new TreeMap<>();
        for (final Element added : elements(doc, "addUniqueConstraint")) {
            if (table.equals(added.getAttribute("tableName"))) {
                constraints.put(added.getAttribute("constraintName"),
                        added.getAttribute("columnNames").replace(" ", ""));
            }
        }
        for (final Element createTable : elements(doc, "createTable")) {
            if (!table.equals(createTable.getAttribute("tableName"))) {
                continue;
            }
            for (final Element column : childElements(createTable, "column")) {
                for (final Element c : childElements(column, "constraints")) {
                    final String name = c.getAttribute("uniqueConstraintName");
                    if (!name.isEmpty()) {
                        constraints.put(name, column.getAttribute("name"));
                    }
                }
            }
        }
        for (final Element index : elements(doc, "createIndex")) {
            if (table.equals(index.getAttribute("tableName"))
                    && "true".equals(index.getAttribute("unique"))) {
                constraints.put(index.getAttribute("indexName"),
                        String.join(",", childElements(index, "column").stream()
                                .map(c -> c.getAttribute("name")).toList()));
            }
        }
        return constraints;
    }

    private List<Element> elements(final Document doc, final String tag) {
        return nodesToElements(doc.getElementsByTagName(tag));
    }

    private List<Element> childElements(final Element parent, final String tag) {
        return nodesToElements(parent.getElementsByTagName(tag));
    }

    private List<Element> nodesToElements(final NodeList nodes) {
        return java.util.stream.IntStream.range(0, nodes.getLength())
                .mapToObj(nodes::item)
                .filter(n -> n.getNodeType() == Node.ELEMENT_NODE)
                .map(Element.class::cast)
                .toList();
    }

    private Document parse(final Path file) throws Exception {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        // The changelogs reference the Liquibase XSD by URL; resolving it would make this test
        // depend on the network, and it is not validating anything here.
        factory.setValidating(false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        return factory.newDocumentBuilder().parse(file.toFile());
    }
}
