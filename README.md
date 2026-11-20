# Business Registry Consensus API

`GET /v1/gov/business/registration`

A multi-source, consensus-verified data service for agent runtimes. It ingests company registration facts live from three independent registers (UK Companies House, GLEIF, OpenCorporates), cross-verifies them, scores its own confidence, stores everything in PostgreSQL JSONB under a TTL, and serves one standardised envelope.

**Stack:** Python · FastAPI · Pydantic · PostgreSQL 16 (JSONB, GIN indexes) · asyncio/httpx · Docker Compose · pytest (127 tests, no network or database needed)

> **Context.** Built as Assignment 1 of AI4012 Multi-Agent Systems (FAST-NUCES, Fall 2026), catalog task #84
> *Government & Public Data / Public Records / Business registration lookup*. "The brief" below refers to that
> assignment specification.

---

## Why this task

The brief demands **at least two distinct online providers** for the same fact. Most catalog rows have exactly one credible free source, which would make "consensus" theatre. This implementation integrates three independent corporate registers:

| Source | ID | Coverage | Weight | Role |
|---|---|---|---|---|
| UK Companies House | `SRC-CH-001` | UK (`GB`) | 0.45 | The statutory UK register |
| GLEIF Global LEI Index | `SRC-GLEIF-001` | Global (incl. `PK`, `GB`) | 0.30 | Independent global LEI register |
| OpenCorporates | `SRC-OC-001` | Global (incl. `PK`, `GB`) | 0.25 | Open corporate database aggregating statutory filings |

### Multi-Source Consensus & Jurisdiction Coverage
* **Pakistan (`PK`) Queries** (Assigned Catalog Specification): Answered collaboratively by **GLEIF** and **OpenCorporates**, delivering genuine dual-source consensus, independent verification, and full confidence for Pakistani corporate entities.
* **UK (`GB`) Queries**: Answered across all three registers. With three independent sources responding, **outlier rejection** is fully functional in production.

---

## Quick start

```bash
pip install -r requirements.txt
python -m pytest -q            # 127 tests — no database, no API key, no network
```

That is the full test suite. It runs anywhere.

### Running the service for real

```bash
# On Windows PowerShell:
Copy-Item .env.example .env    # then paste your Companies House API key in
docker compose up -d           # PostgreSQL 16 on :5432
uvicorn app.main:app --port 8000
```

The `.env` file is read automatically at startup, so nothing needs exporting by hand; a real environment variable still overrides it, and the file is ignored entirely under `APP_ENV=test` so the test suite stays reproducible.

Get a free Companies House key at <https://developer.company-information.service.gov.uk/> — register the application as a **REST API key**, not OAuth. GLEIF needs no credentials. OpenCorporates supports an optional API key via `OPENCORPORATES_API_KEY`.

The schema in `sql/schema.sql` is idempotent and applied automatically on boot.

### Try it

```bash
# UK statutory lookup:
curl "http://localhost:8000/v1/gov/business/registration?country=GB&company_name=Barclays%20Bank%20UK%20PLC" | jq

# Pakistan catalog specification query:
curl "http://localhost:8000/v1/gov/business/registration?country=PK&company_name=Habib%20Bank%20Limited" | jq

# Health & Purge worker status:
curl "http://localhost:8000/readyz" | jq
```

Interactive docs at <http://localhost:8000/docs>.

### Live provider check

```bash
# 1. Full three-source agreement (verified: true):
python scripts/smoke_live.py "Barclays Bank UK PLC"

# 2. Inexact identity gate demonstration (parent vs subsidiary detected; verified: false):
python scripts/smoke_live.py "Monzo Bank Limited"

# 3. Pakistan multi-source consensus:
python scripts/smoke_live.py "Habib Bank Limited" --country PK
```

Probes each provider separately, then runs the full pipeline and prints the envelope with a verdict. Use this before a demo — the pytest suite is mocked by design and will not catch an upstream schema change.

---

## How the five requirements are met

### 1. Live ingestion and multi-source consensus

Ingestion runs on two paths:

- **Continuous background polling** (`app/ingestion.py`). A scheduler re-fetches every watched query from all providers on a fixed interval (`REFRESH_INTERVAL_SECONDS`, default 900 s) with **no HTTP traffic required**. The watchlist is seeded from the `WATCHLIST` environment variable at boot (including UK and Pakistan benchmarks), and any query an agent asks for is auto-subscribed so it is kept fresh from then on. It is bounded (`WATCHLIST_MAX`, least-recently-requested evicted) so a probing agent cannot turn the poller into an unbounded crawler. `POST /internal/refresh` triggers a sweep manually; `/readyz` reports cycle and failure counts.
- **On-demand** for anything not yet warm.

All eligible providers are called **concurrently** (`asyncio.gather`) on every cache miss. Consensus is a **weighted field-level majority vote** (`app/consensus.py`):

- Each field is voted on independently, so a sparse source cannot drag a field toward null.
- Names are compared after normalisation — `BARCLAYS BANK UK PLC` and `Barclays Bank UK Plc` are recognised as the same entity, not a disagreement.
- Status vocabularies from all registers are mapped onto one enum (`active` / `inactive` / `dissolved` / `unknown`).
- **Outlier rejection** drops a source that dissents on most fields when three or more sources responded.
- `confidence = field-weighted agreement × source coverage`. Authority is dynamically budgeted against providers eligible for the queried jurisdiction.
- `quality_score = weighted completeness × freshness decay`.
- **Identity is a precondition, not just another field.** Before any attribute comparison counts, the sources must be describing the *same legal entity*. When registry name search returns near neighbours (e.g. parent vs subsidiary), each adapter picks the exact-name match out of the returned page, reporting `name_match_exact: false` when no exact match exists.
- `verified` requires high confidence (≥0.75) **and** at least two sources **and** unanimous status **and** confirmed identity.

Freshness measures when we last confirmed a fact, not when the registry last edited its row. The registry's own edit date is still reported as `provenance[].source_modified_at`.

`meta.provenance` carries additive fields beyond the brief's sample — `status`, `weight`, `error_code`, `source_modified_at`, and `name_match_exact`. They make every provider's outcome visible.

### 2. No silent fallbacks

Every failure is a named, typed object (`app/errors.py`) that travels to the caller:

- A failed provider appears in `meta.provenance` with `status: "failed"` and its `error_code`, and again in `meta.warnings`.
- Timeouts, 429s, 401/403s, 5xx, malformed JSON, and unconfigured credentials each map to a distinct code — never one vague "upstream error".
- **All providers down → HTTP 503 with no `data` block.** Expired rows are never substituted.
- A stored record past its TTL is **not served**; the system re-ingests. If re-ingestion fails, the request fails.
- `age_seconds` is always measured against the *upstream* timestamp, never the moment we cached it.

### 3. Storage and lifecycle

`sql/schema.sql` — three tables plus an audit log:

| Table | Purpose | TTL |
|---|---|---|
| `raw_ingest` | Immutable per-source evidence, incl. failures | 48 h |
| `consensus_record` | The fused answer (serving cache) | 24 h |
| `registration_rollup` | Daily historical summary | permanent |
| `lifecycle_audit` | Proof the purger ran | permanent |

Raw evidence outlives the serving cache so the audit trail does not vanish with it. Indexes: `GIN (… jsonb_path_ops)` on every JSONB column, plus btrees on `expires_at` and `(product_id, query_key, computed_at DESC)`.

Expiry is **summarise-then-purge**: `purge_expired()` rolls expired consensus rows into one row per (query, day) in `registration_rollup` *before* deleting them, so history survives TTL. A background worker (`app/lifecycle.py`) calls it every 5 minutes; a `pg_cron` schedule is included commented-out as an in-database alternative.

### 4. SLA and reliability

`tests/test_sla.py` asserts:

- **Availability** — 200 requests at concurrency 20, all 200 OK.
- **Latency** — two paths, budgeted separately and honestly. The **warm path** (consensus record inside TTL, no upstream call) meets p95 < 200 ms; this is the steady state under sustained polling precisely because the background poller keeps watched queries fresh. The **cold path** calls live providers (200–700 ms floor) where our internal overhead is asserted to stay under 60 ms.
- **Freshness** — `age_seconds <= ttl_seconds`; anything older is flagged `stale: true` and warned about, never disguised.
- **Failover** — one provider down still serves, visibly degraded; recovery restores verification on its own.
- The advertised `rate_limit` is the one actually enforced.

### 5. Code architecture and testing

127 tests in seven suites:

| Suite | Covers |
|---|---|
| `test_consensus.py` | Voting, Pakistan multi-source consensus, 3-source outlier rejection, scoring bounds |
| `test_envelope_spec.py` | Literal compliance with Section 3 of the brief |
| `test_failover.py` | Every failure class, degradation, recovery |
| `test_sla.py` | Availability, p95, freshness, caching, rate limit |
| `test_storage_lifecycle.py` | TTL, summarisation, purge, audit, Postgres, `.env` loading |
| `test_sources.py` | Companies House, GLEIF, and OpenCorporates adapter normalisation |
| `test_ingestion.py` | Continuous polling, watchlist bounds, refresh failures |

Providers sit behind one `BaseSource` interface (`base.py`, `companies_house.py`, `gleif.py`, `opencorporates.py`). The app is built by a factory (`create_app`) so each test gets a fully isolated instance.

---

## Layout

```
app/
  config.py       env-driven settings, source weights, TTLs
  errors.py       typed error taxonomy
  models.py       Pydantic transcription of the response envelope
  consensus.py    the voting engine
  pipeline.py     ingest → fuse → persist → serve
  storage.py      PostgresStore + MemoryStore test double
  ingestion.py    continuous background poller (live re-ingestion)
  lifecycle.py    background TTL worker
  ratelimit.py    fixed-window limiter
  main.py         FastAPI app factory
  sources/        base.py, companies_house.py, gleif.py, opencorporates.py
sql/schema.sql    tables, indexes, purge + summarise functions
scripts/          smoke_live.py, build_report_pdf.py
tests/            127 automated tests
report/           two-page submission report and typeset PDF
```

## A note on the in-memory store

`MemoryStore` mirrors the SQL semantics (including summarise-then-purge) so the suite runs without a database. It is **refused at startup outside `APP_ENV=test`** — `build_store()` raises rather than quietly demoting. A test double must never become a production fallback; that would be exactly the failure mode the brief prohibits.
