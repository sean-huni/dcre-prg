# dcre-prg

Payment Report Generator: the clock-windowed status reporter that terminates the DCRE **ENDO
payments** response leg. Sheet: `design-register/docs/diagrams/dcre-payments-res.png`.

```
Fint Resp ──▶ PARALLEL ──▶ [PIX] ──▶ isr_resp   ┐
                       ├─▶ [PSX] ──▶ sbsr_resp  ├─▶ PARALLEL ──▶ [PRG] ──▶ OnHost Resp
                       └─▶ [PPX] ──▶ pbsr_resp  ┘
```

## What it does

PRG projects per-transaction external status (`ext_tx_status`, over the payments spine, PTV
verdicts and the ISR/SBSR/PBSR response legs, deepest leg wins per R-17) and emits delta Payment
Status Report (PSR) files per client on clock windows. Each run diffs `ext_tx_status` against
`prg_watermark` for one client, streams the delta as a PSR flat file into that client's
`onhost-resp/out` exchange directory, then advances the watermark in bounded slices. It is not
file-triggered: AGT's clock instantiates `prgJob` per (client, window) as a short-lived Kubernetes
Job, and PRG reports whatever `ext_tx_status` holds behind the watermark. The owner's statement of
intent is "PRG generates all the reports for all the clients at specified times (of when they
prefer to receive their Payment Reports)"; the window interval is that clock.

**PRG serves the ENDO Payments flow only.** It reads and writes `dcre_pay` and nothing else. The
DC Collections flow has its own report generator, `crg`, against `dcre_col`. One service serving
both flows was the SOLID violation the family split exists to remove.

## Forked from `collections/crg`, and everything that differs

Family consistency is the rule: a payments service copies the collections sibling that already
solves the job, and every structural difference carries the burden of proof. There are six, and
all but one are forced by decisions other payments services already made.

| # | Difference | Why it is not drift |
|---|---|---|
| 1 | Reads `dcre_pay`; `DCRE_DB_URL` | Database per family. The database IS the discriminator now, so there is no flow column to branch on. |
| 2 | Emission registry is `prw_emission*`, with **no `run_date`** | PRW's own ruling: payments has no CDE, no collection day and no warehousing, so a run date is not part of any identity. |
| 3 | Replies correlate by `orgnl_msg_id = prw_emission.outbound_msg_id`, never by an `emission_id` column | PIX/PSX/PPX deliberately dropped CIX's emission FK. CRG's three correlation paths collapse to two here (see below). |
| 4 | No `man_collection_outcome` view | The mandate gate is DC-only (R-19). A payment carries no bank-registered mandate, so there is nothing for MSR to read here. `tx_entry.mandate_ref` exists in the shared physical layout and is expected NULL on every ENDO row. |
| 5 | Paketo `bootBuildImage`, no Dockerfile | Estate mandate. A brand-new repo with no published image is the cheapest adoption point, as PRR/PRW/PIX/PSX/PPX each concluded. |
| 6 | `prg_report.type`, not `prg_report.report_type` | A column never repeats its own table name. A v1 baseline on an empty database is the only moment this is free. This is the one difference that is a choice rather than a consequence. |

### The correlation change, in detail

CRG resolves a reply to its outbound batch three ways, in precedence order: a resolved
`emission_id` column, a legacy arm binding `orgnl_msg_id = crw_emission.outbound_msg_id`, and a
no-emission family fallback against `tx_header.msg_id`. PRG has two, because the first collapses
into the second:

1. `orgnl_msg_id = prw_emission.outbound_msg_id`. Exact, not heuristic: `outbound_msg_id` is
   globally unique (`uq_prw_emission_outbound_msg`).
2. `orgnl_msg_id = tx_header.msg_id`, or a split child `msg_id_N`, for arrivals with no emission
   row at all.

The `prg_*_pick` views are therefore a single window pass rather than CRG's `UNION ALL` of a
resolved arm and a legacy arm, and the `resolved DESC` tiebreaker is gone with it: nothing can
outrank anything. Latest-row selection within a `(emission, e2e)` is `created_at DESC` with
`response_file DESC` as the deterministic tiebreaker.

Path 2 in payments is a fixture / no-registry safety net rather than a legacy-data path: PRW writes
its registry row before the file becomes visible, so a real-flow reply always has an emission to
bind to. It is kept because the failure it prevents is silent (an uncorrelatable reply projects as
`CTV_PASS` and the transaction looks merely un-responded, forever), and because dropping an arm the
sibling has is exactly the local cleverness that produces drift.

## Architecture and principles

- **SOLID, 3-tier**: one responsibility per class along `PsrTasklet` (thin Spring Batch entry
  adapter) to `PsrReportService` (business tier) to `PrgWatermarkRepo` (Spring Data JDBC).
  Supporting single-purpose units: `StreamedPsrWrite` (staged streaming boundary write),
  `CrdbRetry` (bounded SQLSTATE 40001 retry). Layer-first packages: `config/`, `service/`,
  `data/model/`, `data/repo/`.
- **12FactorApp Alignment - https://12factor.net/**: config strictly from the environment with
  committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process
  (the JVM exit code carries the Batch verdict via `ExitCodeMain`, R-34), CockroachDB and the
  exchange directory as attached resources.
- **Idempotent restart semantics**: job identity is the identifying parameter pair (client, window)
  (R-16); `resend` is non-identifying. R-29 order: the WHOLE file becomes visible first (streamed
  tmp plus `ATOMIC_MOVE`), then watermarks advance in per-slice `REQUIRES_NEW` transactions. An
  existing target file is a restart no-op (R-24); a crash between file and watermark replays as
  skip-existing-file plus watermark advance, neither skipping nor duplicating.
  `StaleExecutionSweeper.abandonStale(ds, "PRG_BATCH_", 60)` runs as an `@Order(-10)`
  `ApplicationRunner` so a killed pod never strands a STARTED execution (A-39a).
- **CRDB-correct upserts**: watermark writes are `INSERT ... ON CONFLICT (client, e2e) DO UPDATE`,
  never `UPSERT INTO` (CRDB arbitrates UPSERT on the primary key only; the business identity is
  (client, e2e)). Serialization aborts (40001) retry up to 5 attempts with jittered backoff in a
  fresh transaction per attempt.
- **Bounded scale (SCRUM-42)**: whole-book reads plus a full in-heap render blew CRDB's sql memory
  budget on the 30M-tx book. Every read is a keyset slice (`ORDER BY e2e LIMIT :limit`, default
  50000) and the PSR streams to disk slice by slice; nothing holds more than one slice in heap.

### Fintegrate status classification

`prg_status_class` is the runtime authority (R-21: a reference table, never an enum). All fourteen
recognised codes are explicit and fully classified at v1:

- Terminal success: `ACSC`, `ACCC`.
- Terminal non-success: `RJCT`, `CANC`.
- Accepted non-terminal: `ACSP`, `ACTC`, `ACCP`, `ACFC`.
- Pending/interim: `RCVD`, `PDNG`, `PART`, `PATC`.
- Accepted warehoused, SLA-suppressed: `ACWP` future-dated, `ACWC` auto-bumped.

`ACWP`/`ACWC` are RMB DebiCheck codes for warehoused COLLECTIONS and are not expected on this leg,
since payments has no collection day. They are catalogued anyway: an unlisted code that did arrive
would be silently suppressed from every PSR as an unknown, and classifying a code you never see
costs nothing.

Unknown Fintegrate codes are preserved unchanged but fail closed as non-terminal, non-reportable
protocol exceptions, by ABSENCE from the catalogue rather than by a row saying so. They appear in
`prg_status_exception`, remain visible to the 20h/24h pending-SLA path, and never enter scheduled,
resend, immediate or manual-regeneration PSRs. DCRE never translates them into a supported code and
never instructs OnHost to resubmit until Fintegrate confirms that no processing or settlement
occurred.

### Job contract

One job `prgJob`, one tasklet step `psrStep`.

- Scheduled run: delta selection, reportable rows whose `ext_tx_status.status` moved past
  `prg_watermark.last_status` (or have no watermark row yet). Unknown response codes are excluded.
  Zero delta rows emits a zero-valued heartbeat PSR so the consumer can tell "no movement" from
  "PRG dead".
- Resend run (`resend=true`, non-identifying): all current reportable-status rows, watermark
  ignored; re-projects CURRENT state, not the original report (A-8).
- R-38 exclusion visibility: mid-DAG rows with `status IS NULL` are never reportable. Up to 100 each
  gets a WARN in the uniform shape
  `excluded stage=PRG arrival=<uuid> seq=<n> e2e=<e2e> reason=STATUS_UNKNOWN`; above 100 they
  collapse to one summary WARN per client.
- File: `<exchange-root>/<client-base>/onhost-resp/out/<CLIENT>_PSR_<window>.txt`; layout is
  SYNTHETIC-CONTRACT (R-35): header `PSR|client|window`, one `TX|e2e|status` per row ordered by
  e2e, trailer `END|count`. An unconfigured client fails the job closed
  (`IllegalArgumentException` from the layout) rather than writing to a wrong directory.
- Outcome seam: on COMPLETED, `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>`
  (`OutcomeFileWriter`, R-33).

### SYNTHETIC-CONTRACT: heartbeat layout

A SCHEDULED window with zero delta emits a zero-valued, normal-format PSR instead of no file:

```text
PSR|<client>|<window>
HB|DCRE00000000000000000000000000000|DCRE00000000000000000000000000000|0|0.00
PD|<pendingCount>
END|0
```

- The HB placeholder is `DCRE` plus 29 zeros (33 chars, Max35-safe). **SYNTHETIC-CONTRACT: the real
  legacy Payment Report response copybook and its exact zero-placeholder bytes are unrecovered;
  this shape is invented and is a critical future update once the copybook is attested
  (design-register A-57).**
- `PD|<pendingCount>` counts the client's `prg_sla_pending` rows (members of VISIBLE outbound
  batches whose current status is non-terminal); zero pending prints `PD|0`. HB and PD are not TX
  lines, so the trailer stays `END|0`.
- A heartbeat registers a `prg_report` row (type `HEARTBEAT`) but NEVER touches the delivery ledger
  or the watermark: nothing was externally reported. Only the SCHEDULED path heartbeats;
  IMMEDIATE/MANUAL no-ops stay file-less.

## Database

Liquibase, pure XML, per-service history tables (`prg_databasechangelog` / `...lock`) on the shared
`dcre_pay` database. Calendar layout under `db/changelog/2026/08/`.

**This is a V1 BASELINE, not a port of CRG's history.** Every DCRE database is being dropped and cut
over directly, so there is no historic state to migrate and no deployed schema to protect. Each
object is declared once in its final shape, and the re-run guards CRG's 53-changeset history needed
are absent on purpose: no `validCheckSum ANY` on schema this project designs, no `MARK_RAN`
preconditions over objects that cannot exist on an empty database, no defensive `IF NOT EXISTS`
retrofits. `DATABASECHANGELOG` is what makes a re-run safe from a clean baseline, and
`OrderingContractIT` executes that claim rather than asserting it. There are no exceptions left:
the batch metadata was the last one and became typed tags on 2026-08-08.

| File | Contents |
|---|---|
| `002-pay-reporting.xml` | `prg_status_class` (+14 seeded codes), `prg_watermark`, `prg_report`, `prg_delivery_ledger`, and PRG's read-path indexes on relations it reads |
| `003-pay-status-views.xml` | `prg_isr_pick` / `prg_sbsr_pick` / `prg_pbsr_pick`, `ext_tx_status`, `prg_member_status`, `prg_report_due`, `prg_sla_pending`, `prg_status_exception` |
| `004-batch-metadata.xml` | Spring Batch 6 metadata under prefix `PRG_BATCH_` |

### The ordering contract (2026-08-08)

PRG reads nine relations it does not own and **creates none of them**: `tx_header` + `tx_entry`
(PRR), `validation_log` (PTV), `isr_resp` / `sbsr_resp` / `pbsr_resp` (PIX / PSX / PPX), and
`prw_emission_group` / `prw_emission` / `prw_emission_member` (PRW).

Until 2026-08-08 `001-pay-report-sources.xml` PRE-CREATED all nine behind `onFail="CONTINUE"`
preconditions, because PRG is clock-launched and can migrate before any of those services has run.
CRG carried the identical guard and retired it; PRG was forked from CRG before that retirement and
inherited the anti-pattern without the fix. The guard made PRG a second writer of nine relations it
does not own, it skipped SILENTLY so whichever service migrated second inherited the other's shape
permanently and invisibly, and a PRG mint winning the race would crashloop the owner, whose own
baseline creates those tables unguarded.

**What guarantees the relations exist now.** `002`'s read-path indexes and `003`'s views name their
relations by identifier, and CockroachDB resolves a view body at CREATE time, so a premature PRG
migration FAILS, names the missing relation and leaves its changesets unapplied; the pod restarts,
retries and converges the moment the owner has migrated. That is **not** the silent-zero shape
(A-76, A-79): a view that was never created cannot be queried and return nothing. A `MARK_RAN`
precondition WOULD reintroduce the silent shape by recording the skip permanently, which is why
there is none. `OrderingContractIT` runs both directions: the shipped master against an empty
database must throw and create no view, and the same master with the peers present must apply and
execute every view.

PRG's own integration tests get the nine relations from
`src/test/resources/db/changelog/test/001-read-sources.xml`, reached via
`db.changelog-test-master.xml`, which runs the fixture and then the PRODUCTION master by its
production classpath path, so every production changeset keeps its exact identity. That fixture is
still a hand-maintained mirror of other repositories' DDL, so `BootstrapSourceParityTest` reads the
owners' changelogs off disk and compares them column by column and constraint by constraint. It
runs only inside the monorepo working tree and says so when it skips.

**Dependency worth naming:** `payments/pai` currently runs its Liquibase against `dcre_col` and
creates `account`, `tx_header`, `tx_entry` and `pai_verdict` there. PRG reads `tx_header` and
`tx_entry` in `dcre_pay` and depends on PRR creating them, not PAI; but until PAI's datasource is
corrected, the payments database's spine has an owner writing into the wrong database, and that fix
is a precondition for the ordering contract above being true in the cluster rather than only in the
tests.

### Platform library dependencies (mavenLocal, 0.1.0)

| Module | Used for |
|---|---|
| `za.co.fnb.dcre:platform-persistence` | `BaseEntity` (on `PrgWatermarkEntity`), `JdbcConfig` |
| `za.co.fnb.dcre:platform-files` | `ExchangeLayout` / `ExchangeChannel` / `ExchangeSub` (per-client exchange resolution, fail-closed) |
| `za.co.fnb.dcre:platform-batch` | `ExitCodeMain`, `OutcomeSeamListener`, `OutcomeFileWriter`, `StaleExecutionSweeper`, `HeartbeatWriter`; ships the `dcre-exchange-layout.yml` classpath resource imported via `spring.config.import` |

## Prerequisites

- Java 25 (`.sdkmanrc` pins `25-tem`; Gradle 9.5.1 wrapper included)
- Docker (Testcontainers CockroachDB in tests, image build)
- Platform libs published to Maven Local: run `./gradlew publishToMavenLocal` in
  `dcre-platform-model`, then `dcre-platform-files`, then `dcre-platform-batch` (each
  `api`-exposes the previous), and in `dcre-platform-persistence` (standalone)
- A reachable CockroachDB and exchange directory for a real run (the `dcre-infra` kind cluster
  locally)

## Quickstart

Clean clone, no `.env` needed (committed dev defaults):

```bash
./gradlew build

# one scheduled window against a reachable CockroachDB (defaults: localhost:26257/dcre_pay)
java -jar build/libs/prg-2.0.jar client=FNBRF01 window=w1

# resend override: non-identifying parameter, re-emits all current rows
java -jar build/libs/prg-2.0.jar client=FNBRF01 window=w1-manual resend=true,java.lang.String,false
```

## Configuration

Precedence: yml default < environment variable. All defaults are committed in `application.yml`.

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_pay?sslmode=disable` | CockroachDB via pgwire |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | HeartbeatWriter liveness stamp on `agt_ops.launch_intent` |
| `DCRE_AGTOPS_DB_USER` | `root` | agt_ops user |
| `DCRE_AGTOPS_DB_PASSWORD` | (empty) | agt_ops password |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root: PSR output tree + outcome seam |
| `DCRE_PRG_PSR_SLICE_SIZE` | `50000` | Keyset slice size for reads, streaming emission and watermark advances |
| `DCRE_AMOUNT_SCALE` | `2` | Fleet-wide flag; not read by PRG sources |
| `DCRE_V1_ENABLED` | `false` | Fleet-wide flag; not read by PRG sources |
| `DCRE_FLOW_DC` | `false` | Fleet-wide flag; not read by PRG sources. `false` here because PRG is the ENDO leg |
| `JOB_NAME` | `local-<executionId>` | K8s-injected identity for the outcome seam |

Per-client exchange directories bind from the `dcre-exchange-layout.yml` classpath resource
(shipped in `platform-batch`, imported via `spring.config.import`); Batch metadata uses table
prefix `PRG_BATCH_`.

## Testing

```bash
./gradlew test
```

Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3` (Docker required).

- `PrgJobTest`: end-to-end window sequence (first delta with deepest-leg statuses plus exactly one
  R-38 WARN, unchanged window emits the zero-valued heartbeat, single status flip emits exactly
  that row, resend re-emits all current rows) plus the SCRUM-42 fail-closed unconfigured-client
  case.
- `OrderingContractIT`: the ordering contract, executed in both directions. The SHIPPED master
  against an empty database must THROW, name the missing relation, and leave no peer table and no
  view behind (a view that exists after a failed migration is a reader that returns zero rows
  instead of an error); the same master with the peers present applies cleanly and every view is
  actually queried, the catalogue seeds fourteen codes, and the three `PRG_BATCH_` sequences
  survive the typed-tag conversion. A second full run is a no-op.
- `BootstrapSourceParityTest`: the nine mirrored table declarations in the TEST FIXTURE compared
  against the owners' changelogs, column by column and constraint by constraint. It matters more
  after the retirement, not less: a stale mirror now produces a fixture that lies about production.
  Skips, loudly, outside the monorepo.
- `ClientAuthorityIT`: the only fixture in this module where `initg_pty` and `client_token` differ,
  so the only one that can see which column `prw_emission_group.client` carries (A-43).
- `PsrReportServiceSliceTest`: bounded-scan proofs (multi-slice delta to one correct streamed PSR,
  restart-with-existing-target still advances watermarks, summary WARN above the detail limit).
- `PsrReportServiceRetryTest` / `PsrReportServiceAdvanceHonestyTest`: 40001 retry semantics of the
  watermark advance, and that the ledger records what was externally reported rather than a fresh
  re-read.
- `ReportingSchemaIT` / `ImmediateReportIT`: status classes, delivery-ledger guard, batch-scoped
  `ext_tx_status`, due/SLA views; IMMEDIATE/MANUAL report modes with kill-resume proofs.
- `HeartbeatIT`: the zero-valued heartbeat (exact file shape, `prg_sla_pending` PD count, HEARTBEAT
  registry row, watermark and ledger untouched, R-24 restart no-op).
- `ReportDuePerfIT`: `prg_report_due` must stay set-based. The correlated-scan shape it forbids hung
  the live AGT trigger scan for 7+ minutes on a 12,001-tx parent.
- `ConfigPlaceholderBindingTest`: every `${dcre.*}` placeholder in main sources resolves against the
  committed yml, the inherited `dcre.crg` prefix is gone from both sides, and the relative
  exchange-root default keeps its six-level depth.
- Cucumber BDD suite: `src/test/resources/features/psr-window-projection.feature`.

## Local cluster deployment

```bash
./gradlew bootBuildImage          # Paketo, BP_JVM_VERSION=25, produces dcre-prg:2.0
kind load docker-image --name dcre-dev dcre-prg:2.0
```

**AGT is not yet wired for this service and PRG cannot run in-cluster until it is.** `Stage.PRG`
exists in AGT today but points at the COLLECTIONS report generator image against `dcre_col`, for
both flows, which is the coupling this split removes. The required AGT changes are listed in the
SCRUM-107 handover and are deliberately not made from this repository: several agents edit AGT in
one pass, and concurrent edits have already caused conflicts twice on this project.

Fleet version switching: `dcre-infra` `scripts/switch-version.sh`; cluster bring-up:
`scripts/kind-up.sh` (kind cluster `dcre-dev`); clean slate: `scripts/env-reset.sh`. Topology gate:
`scripts/verify-topology.sh`.

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Payments family: [dcre-prr](https://github.com/sean-huni/dcre-prr),
  [dcre-ptv](https://github.com/sean-huni/dcre-ptv), [dcre-pai](https://github.com/sean-huni/dcre-pai),
  [dcre-prw](https://github.com/sean-huni/dcre-prw), [dcre-pir](https://github.com/sean-huni/dcre-pir),
  [dcre-pix](https://github.com/sean-huni/dcre-pix), [dcre-psx](https://github.com/sean-huni/dcre-psx),
  [dcre-ppx](https://github.com/sean-huni/dcre-ppx)
- Collections sibling: [dcre-crg](https://github.com/sean-huni/dcre-crg)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model),
  [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files),
  [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch),
  [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Infra and tooling: [dcre-infra](https://github.com/sean-huni/dcre-infra),
  [dcre-design-register](https://github.com/sean-huni/dcre-design-register)
