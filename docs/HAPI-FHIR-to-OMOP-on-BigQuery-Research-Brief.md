# Moving HAPI FHIR (Roche LIMS) into OMOP CDM on BigQuery

**A research brief for the Chile team**
_Prepared April 8, 2026 · Audience: informaticians, lab scientists, and data engineers_

---

## TL;DR — the recommendation in one paragraph

Keep HAPI FHIR as the system of record for the daily Roche LIMS feed. Mirror it nightly into a **Google Cloud Healthcare API FHIR store** (or export directly to GCS as NDJSON if you prefer to keep HAPI as the only FHIR store), land the raw FHIR in BigQuery using the **Analytics V2 schema**, and then run the **FHIR → OMOP transformation as a dbt project in BigQuery**, taking the HL7/OHDSI **FHIR-to-OMOP Implementation Guide (2025 ballot)** as the canonical design spec and the **NACHC-CAD `fhir-to-omop`** codebase as the reference implementation to borrow mapping logic from. Orchestrate the daily run with **Cloud Composer (Airflow)**, store the OMOP Vocabulary (LOINC, SNOMED, UCUM) loaded from **Athena** in its own BigQuery dataset, and validate the CDM with **OHDSI Achilles / DataQualityDashboard**. This gives you a 100% GCP-managed pipeline, stays OHDSI-native on the transformation side, and avoids the two traps that most teams fall into: (a) choosing a FHIR-to-OMOP tool that doesn't actually target BigQuery, and (b) using a tool that outputs OMOP but isn't maintained.

---

## 1. Understanding the starting point

### What you have

- **Source:** Roche LIMS (most likely `cobas infinity` and/or `navify Lab Operations`). Roche's platforms emit HL7 v2 natively; newer `navify` components also expose FHIR-based APIs.
- **Landing zone:** a **HAPI FHIR** server that is populated **daily** from the LIMS. This is already doing the hard work of translating Roche's representation into FHIR resources — primarily `Patient`, `Encounter`, `Observation` (lab results), `DiagnosticReport` (grouped lab reports), and `Specimen`.
- **Destination:** OMOP CDM in **BigQuery** on GCP.
- **Cadence:** daily batch is acceptable.

### What OMOP needs from a LIMS feed

For a lab-dominant dataset, OMOP is actually a relatively narrow transformation. You are mostly populating five tables:

1. `PERSON` (demographics from `Patient`)
2. `VISIT_OCCURRENCE` / `VISIT_DETAIL` (from `Encounter`, or synthesized from `DiagnosticReport.effective`)
3. `MEASUREMENT` (the overwhelming majority of rows — every lab result `Observation`)
4. `SPECIMEN` (from `Specimen`)
5. `OBSERVATION` (for anything that isn't quantitative — e.g., a cancelled test, a microbiology organism identification)

Everything else in OMOP (DRUG_EXPOSURE, CONDITION_OCCURRENCE, DEATH, etc.) will likely be empty or sparsely populated unless the LIMS exports more than pure lab data. Your team's effort should concentrate on getting the MEASUREMENT transformation **right**, not on breadth.

---

## 2. Tooling landscape — what actually exists in April 2026

The FHIR-to-OMOP space has ~6 named tools. Most of them are either (a) maintained but don't speak BigQuery, or (b) speak BigQuery but don't output OMOP. The table below is the condensed version of that survey.

| Tool | Maintained? | Outputs OMOP? | BigQuery-native? | OHDSI-native? | Verdict |
|---|---|---|---|---|---|
| **HL7/OHDSI FHIR-to-OMOP IG** | Yes — 2025 ballot | Design only (StructureMaps, logical models) | N/A | Yes (Vulcan WG) | **Use as the spec** |
| **NACHC-CAD `fhir-to-omop`** | Yes — commits through 2025 | Yes, full | No — writes to Postgres / MSSQL | Endorsed by OHDSI IG contributors | **Borrow the mapping logic** |
| **Google `fhir-data-pipes`** | Yes — active | **No** — outputs SQL-on-FHIR / Parquet | Yes — DataflowRunner → BQ/Parquet | No | **Use for ingestion/flattening, not OMOP transform** |
| **OHDSI `ETL-German-FHIR-Core`** | Stale (last release Sept 2023) | Yes | No | Yes | Avoid — German-MI-specific, abandoned |
| **OHDSI `FhirToCdm`** | Minimal activity | Yes | No | Yes | Avoid — sparse docs, no BQ |
| **`pyomop`** | Active | Yes | No | Community | Dev/test only |
| **Google Cloud Healthcare API (managed)** | Yes | No (FHIR store only) | Yes — native Analytics V2 streaming to BQ | No | **Use for ingestion** |
| **Healthcare Data Engine / Odysseus partnership** | Active, commercial | Partial — via Odysseus | Yes | Indirectly | Consider only if you want a managed transform |

### The key insight

**No single open-source tool does "HAPI FHIR → OMOP on BigQuery" natively.** Every working approach is a _composition_ of ingestion + transformation. The decision is about where you draw the line between "pre-built tool" and "SQL you own."

Three credible compositions:

- **Composition A (recommended): GCP-managed ingestion + dbt-in-BigQuery transform.** Most GCP-native. Most maintainable. Largest up-front SQL effort.
- **Composition B: GCP-managed ingestion + NACHC tool in a VM + BQ load.** Least SQL. Adds a Postgres intermediary and a Java runtime you have to babysit.
- **Composition C: `fhir-data-pipes` + custom Parquet-to-OMOP transform.** Most "cloud native" in the Beam/Dataflow sense but you end up writing the OMOP transformation layer anyway.

The brief recommends **Composition A**, but Composition B is a reasonable fallback if your team doesn't want to own the SQL.

---

## 3. Recommended reference architecture (Composition A)

```
┌────────────────────┐   HL7 v2 / FHIR  ┌────────────────────┐
│   Roche LIMS       │ ───────────────▶ │   HAPI FHIR        │
│ (cobas / navify)   │                  │ (system of record) │
└────────────────────┘                  └─────────┬──────────┘
                                                  │  nightly
                                                  │  $export (bulk NDJSON)
                                                  ▼
┌─────────────────────────────────────────────────────────────┐
│  Cloud Storage (GCS) — landing bucket                       │
│  gs://<proj>-fhir-landing/YYYY-MM-DD/{Resource}.ndjson      │
└─────────────────────────────────┬───────────────────────────┘
                                  │
          ┌───────────────────────┴──────────────────────┐
          │                                              │
          ▼                                              ▼
┌─────────────────────────┐             ┌───────────────────────────┐
│ Cloud Healthcare API    │             │ (Optional) fhir-data-pipes │
│ FHIR Store (mirror)     │             │ Dataflow → Parquet views   │
│ → BigQuery streaming    │             │ (SQL-on-FHIR v2)           │
│   (Analytics V2 schema) │             └───────────────────────────┘
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│  BigQuery — RAW layer                                       │
│  dataset: fhir_raw                                          │
│  tables: Patient, Observation, DiagnosticReport, Specimen…  │
│  (one table per FHIR resource, Analytics V2 schema)         │
└─────────────────────────────────┬───────────────────────────┘
                                  │
                                  ▼  dbt-bigquery (SQL transforms
                                  │  informed by HL7 FHIR-to-OMOP
                                  │  IG + NACHC reference logic)
                                  ▼
┌─────────────────────────────────────────────────────────────┐
│  BigQuery — STAGING layer                                   │
│  dataset: omop_stg                                          │
│  tables: stg_person, stg_visit, stg_measurement, …          │
│  (typed, de-duplicated, LOINC/UCUM joined)                  │
└─────────────────────────────────┬───────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────┐
│  BigQuery — OMOP CDM v5.4                                   │
│  dataset: omop_cdm                                          │
│  tables: PERSON, VISIT_OCCURRENCE, MEASUREMENT, SPECIMEN,   │
│          OBSERVATION, CARE_SITE, LOCATION, CDM_SOURCE, …    │
└─────────────┬───────────────────────────────┬───────────────┘
              │                               │
              ▼                               ▼
┌───────────────────────┐     ┌───────────────────────────────┐
│ OMOP Vocabulary       │     │ OHDSI Achilles /              │
│ dataset: omop_vocab   │     │ DataQualityDashboard /        │
│ (loaded from Athena)  │     │ ATLAS (on BigQuery)           │
└───────────────────────┘     └───────────────────────────────┘

Orchestration: Cloud Composer (managed Airflow)
Monitoring:    Cloud Logging + BQ information_schema + dbt tests
Region:        southamerica-west1 (Santiago) — keeps data in-country
```

### Layer-by-layer notes

**Layer 1 — Ingestion from HAPI.** HAPI FHIR implements the FHIR Bulk Data Access (`$export`) operation, which writes NDJSON files per resource type. A nightly cron-triggered HTTP POST kicks it off; the resulting files can be written straight to GCS using the HAPI "S3/GCS" sink, or pulled by a Cloud Run job that polls the `$export` status endpoint. NDJSON is the format everything downstream expects.

**Layer 2 — Mirror to Cloud Healthcare API FHIR store (optional but recommended).** The Cloud Healthcare API FHIR store supports `StreamConfigs` with `schemaType = ANALYTICS_V2` that automatically projects every resource change into BigQuery in near-real-time. If you mirror HAPI → Cloud Healthcare API (via the FHIR-to-FHIR import operation), you get a first-class, GCP-managed raw analytics layer for free and avoid writing your own NDJSON flattener. The downside is you now run two FHIR servers. If that feels like a tax, skip this step and use `fhir-data-pipes` (Apache Beam) to flatten NDJSON → BigQuery instead.

**Layer 3 — RAW BigQuery dataset.** One table per resource type, Analytics V2 schema (normalized JSON columns with typed fields). This is the "source of truth" for anything downstream and should be append-only + partitioned on `meta.lastUpdated`.

**Layer 4 — dbt transform to OMOP.** This is where the real work lives. The structure of the dbt project should mirror the OMOP IG: `stg_*` models that flatten and type-cast, `int_*` models that resolve vocabulary joins, and `omop_*` models that are the final CDM tables. See §4 for the concrete lab-mapping logic.

**Layer 5 — OMOP Vocabulary.** Download the vocabulary bundle from [Athena](https://athena.ohdsi.org/) (select LOINC, SNOMED, UCUM, Gender, Race, Ethnicity, Visit at minimum), load into `omop_vocab` dataset. Refresh quarterly or whenever OHDSI publishes a new release.

**Layer 6 — Quality checks.** Run OHDSI **DataQualityDashboard** (DQD) against the BigQuery CDM on every nightly run — it writes a JSON report you can land in GCS and surface via Looker Studio. Run **Achilles** weekly for descriptive statistics. Both work against BigQuery via the DatabaseConnector R package.

**Orchestration.** Cloud Composer (Airflow) is the pragmatic choice because it already has operators for HTTP `$export` kickoff, GCS sensors, BigQuery jobs, dbt Cloud, and dbt-core. Alternatively Cloud Workflows + Cloud Scheduler is cheaper if you don't need the Airflow ergonomics.

**Region.** Deploy everything in `southamerica-west1` (Santiago). This keeps patient data in Chile and is the most defensible posture under Chile's updated data protection law (Ley 21.719, effective December 2026). It also avoids egress costs.

---

## 4. The lab-specific mapping — the only part that really matters

Because this pipeline is LIMS-dominant, the quality of your CDM is basically the quality of your **`Observation` → `MEASUREMENT`** mapping. Here is the concrete field-by-field transformation, with the places most teams get it wrong.

### FHIR `Observation` (category = `laboratory`) → OMOP `MEASUREMENT`

| OMOP field | FHIR source | Notes / gotchas |
|---|---|---|
| `measurement_id` | synthesized (hash of Observation.id) | Keep a `fact_relationship` back to the FHIR id as `measurement_source_value` for traceability |
| `person_id` | `Observation.subject.reference` → PERSON | Resolve the FHIR patient reference to your PERSON.person_id at the staging layer |
| `measurement_concept_id` | `Observation.code.coding[system="loinc"].code` | Join to `omop_vocab.concept` where `vocabulary_id='LOINC'` and `standard_concept='S'`. **If the LOINC is non-standard, use `concept_relationship` ('Maps to') to resolve.** Fall back to `0` (unknown concept) and log. |
| `measurement_date` / `measurement_datetime` | `Observation.effectiveDateTime` or `effectivePeriod.start` | Use `effectiveDateTime` for point-in-time tests. For timed collections, use `effectivePeriod.start`. |
| `measurement_type_concept_id` | constant | Use the "Lab result" Type Concept (historically `44818702`; confirm the current standard concept in your Athena download under `vocabulary_id='Type Concept'` — the exact concept has shifted between OMOP vocabulary releases). |
| `operator_concept_id` | `Observation.valueQuantity.comparator` | Look up the standard concepts for `<`, `<=`, `=`, `>=`, `>` in `omop_vocab.concept` (vocabulary_id = 'Meas Value Operator'). Build a small lookup table in your dbt seed layer so the exact concept IDs are documented and version-controlled. |
| `value_as_number` | `Observation.valueQuantity.value` | Only populated when the value is numeric. |
| `value_as_concept_id` | `Observation.valueCodeableConcept.coding` | For categorical results (e.g., "Positive", "Negative", organism names). Resolve via `concept` where `vocabulary_id='SNOMED'` ideally. |
| `unit_concept_id` | `Observation.valueQuantity.code` (UCUM) | Join to `omop_vocab.concept` where `vocabulary_id='UCUM'`. **If the LIMS emits non-UCUM units (common with legacy Roche feeds!), you need a `source_to_concept_map` for units.** |
| `range_low` / `range_high` | `Observation.referenceRange[0].low.value` / `.high.value` | Use the first reference range; keep additional ranges as a JSON column at staging if needed. |
| `provider_id` | `Observation.performer[0]` | Only if a Practitioner reference is resolvable to PROVIDER |
| `visit_occurrence_id` | `Observation.encounter.reference` → VISIT | Nullable — many lab results have no encounter |
| `measurement_source_value` | `Observation.code.coding[0].code` | Keep the original code exactly as received |
| `measurement_source_concept_id` | `concept_id` of the source code (even if not standard) | Set to `0` if it doesn't exist in Athena |
| `unit_source_value` | `Observation.valueQuantity.unit` | The human-readable unit (e.g., "mmol/L") |
| `value_source_value` | `Observation.valueQuantity.value` or `valueString` | Keep the textual representation for audit |

### Common traps

- **LOINC coverage is usually 60–80%, not 100%.** Plan from day one for a `source_to_concept_map` to cover the tail of Roche-proprietary test codes. Every unmapped test is a row with `measurement_concept_id = 0`.
- **UCUM units vs. Roche units.** Roche instruments often emit local unit strings ("mg/dl" lowercase, "x10^9/L" formatted oddly). Build a unit normalization lookup table early.
- **Reference ranges are sex- and age-specific.** FHIR expresses this via multiple `referenceRange[]` entries with `appliesTo` and `age`. OMOP MEASUREMENT has only one `range_low`/`range_high`. Either pick the applicable range at ETL time (needs person age/sex) or store the whole ranges array in a custom extension table.
- **`DiagnosticReport` is a grouping concept, not an OMOP table.** Don't try to force it somewhere. The accepted pattern is: the DiagnosticReport becomes a synthesized `VISIT_DETAIL` (so that all its child Observations share a visit_detail_id), and the report PDF/narrative, if present, goes into `NOTE`.
- **`Specimen` is its own OMOP table.** Populate it, and set `MEASUREMENT.specimen_id` — this is what researchers will actually query on for LIMS-heavy CDMs.
- **`Observation.interpretation` (H, L, HH, LL, N) is not `value_as_concept_id`.** It belongs in `MEASUREMENT.value_as_concept_id` only for categorical results. For numeric labs, interpretation is usually just discarded or kept in a custom extension column.

### Vocabulary setup

Download from [Athena](https://athena.ohdsi.org/) with at least these vocabularies selected: SNOMED, LOINC, UCUM, Gender, Race, Ethnicity, Visit, Visit Type, Domain, Concept Class, Vocabulary, Relationship, ICD10CM (for any diagnosis data), and CDM v5.4. Load as CSV → BigQuery using `bq load` or the Python BigQuery client. The full set is ~3 GB and fits comfortably in BigQuery at negligible cost.

---

## 5. Why not just use one of the tools directly?

A brief honest assessment of the tempting shortcuts:

**"Just use NACHC `fhir-to-omop`."** It works, and if your team doesn't want to write SQL this is the path of least resistance. But: it targets PostgreSQL/MSSQL and you'd have to stand up a Cloud SQL instance, run the Java tool inside a Cloud Run job or VM, and then re-ship the data into BigQuery. That's three extra moving parts vs. a BigQuery-native dbt project, and each is an ops burden. Its mapping logic is, however, the best open-source reference and you should absolutely read it to inform your dbt code.

**"Just use Google `fhir-data-pipes`."** It's well-maintained and GCP-native, but it outputs **SQL-on-FHIR v2 / Parquet views** — not OMOP. You'd still need to write the OMOP transformation layer yourself. It's a great option if you want to put the intermediate flattened-FHIR in Parquet on GCS and query from BigQuery via external tables, but it's not a FHIR-to-OMOP tool.

**"Just use the Cloud Healthcare API's BigQuery streaming."** Same issue — the Analytics V2 schema is flattened FHIR, not OMOP. It's the **best** option for getting raw FHIR into BigQuery continuously, which is why it's Layer 2 in the recommended architecture. But it stops there.

**"Just use `ETL-German-FHIR-Core`."** Abandoned (last commit 2023) and specific to the German Medical Informatics Initiative profile. Avoid.

**"Just use the HL7 FHIR-to-OMOP IG StructureMaps directly."** The IG publishes machine-readable FHIR Mapping Language (FML) StructureMaps. In theory you run them through a FHIR Mapping Language engine (e.g., `matchbox`, HAPI's StructureMap execution). In practice the tooling is immature, the performance on bulk data is poor, and debugging is hard. Use the IG as a **design spec**, not an execution engine.

---

## 6. Build plan and rough effort

Assuming a small team (1 data engineer, 1 informatician, 0.5 clinical lead):

| Phase | Duration | Deliverable |
|---|---|---|
| 0 — Foundation | 1 week | GCP project, VPC-SC perimeter, BigQuery datasets, Athena vocabulary loaded, region set to `southamerica-west1` |
| 1 — Ingestion | 2 weeks | Nightly HAPI `$export` → GCS → Cloud Healthcare API mirror → BigQuery raw layer |
| 2 — Vocabulary mapping study | 1 week | Inventory of distinct LIMS test codes and units; coverage report against LOINC/UCUM; source_to_concept_map populated |
| 3 — Core ETL (dbt) | 4–6 weeks | `stg_*` + `int_*` + `omop_*` models for PERSON, VISIT_OCCURRENCE, MEASUREMENT, SPECIMEN, OBSERVATION, CARE_SITE, LOCATION, CDM_SOURCE |
| 4 — Orchestration | 1 week | Airflow DAG on Cloud Composer running the full chain nightly, with alerting |
| 5 — Data quality | 2 weeks | DataQualityDashboard integration; Achilles baseline; Looker Studio board for the clinicians |
| 6 — User acceptance | 2 weeks | Clinical review of a sample patient round-trip (LIMS report → FHIR → OMOP) |

**Total: ~13–15 weeks** to a production-grade v1. The heaviest single workstream is Phase 3 (the dbt models), and within that the heaviest single model is `omop_measurement`. Plan accordingly.

---

## 7. Cost sanity check (order of magnitude, not a quote)

For a hospital-scale LIMS (say 500k lab results per day = ~180M rows/year):

- **BigQuery storage:** ~30 GB/year for MEASUREMENT → <$1/month.
- **BigQuery query:** dbt nightly runs scanning partitioned raw FHIR → a few $/month on on-demand, or negligible on a small slot reservation.
- **Cloud Composer:** ~$350/month for a small environment. This is the largest line item; consider Cloud Workflows if budget is tight.
- **Cloud Healthcare API FHIR store:** storage + streaming fees, on the order of $50–150/month at this scale.
- **GCS landing bucket:** negligible.
- **Dataflow (if you use `fhir-data-pipes`):** depends on batch size; typically tens of $ per day at this volume.

Total order of magnitude: **$500–800/month** for the full pipeline. The marginal cost of keeping the CDM refreshed daily is essentially the Composer line.

---

## 8. Governance quick notes (not the focus, but flagging)

- **Chile's Ley 21.719** (data protection, effective December 2026) aligns the country with GDPR-style obligations. Keep processing in `southamerica-west1` and document the legal basis for each use of the OMOP CDM.
- **No PHI should leave the CDM layer.** If researchers need access, give them a **de-identified derivative** dataset with dates shifted per patient and the `NOTE` table redacted. BigQuery row- and column-level ACLs make this straightforward.
- **Audit logging.** Enable BigQuery audit logs and Cloud Healthcare API audit logs from day one. Ship both to a dedicated Cloud Logging sink owned by the security team.
- **OMOP is pseudonymous by design** — there's no direct identifiers in the core tables — but `PERSON.person_source_value` typically holds an MRN. Hash it with a project-wide pepper before it enters BigQuery.

---

## 9. Next concrete steps the Chile team can take this week

1. **Count the distinct LIMS test codes** the HAPI FHIR server currently carries (one SQL query against HAPI's staging DB, or a `GET /Observation?_summary=count&_groupby=code`). This number is the single biggest predictor of how hard the MEASUREMENT mapping will be.
2. **Download a 7-day NDJSON sample** via HAPI `$export`, put it on a developer laptop, and manually hand-map ten Observations end-to-end to OMOP MEASUREMENT rows. This surfaces 90% of the hard questions before any code is written.
3. **Stand up an empty GCP project** in `southamerica-west1` with BigQuery, Cloud Healthcare API, and Cloud Composer enabled. Load the OMOP Vocabulary from Athena into `omop_vocab`. This unblocks Phase 3.
4. **Clone the NACHC `fhir-to-omop` repo** and read its `MeasurementMapper` class — it's the best single reference for the tricky edge cases (units, ranges, non-numeric values).
5. **Decide**: Composition A (dbt in BigQuery) or Composition B (NACHC tool + Cloud SQL). Both are defensible; A is what this brief recommends.

---

## Sources

### Tooling
- [HL7/OHDSI FHIR-to-OMOP Implementation Guide (2025 ballot)](https://github.com/HL7/fhir-omop-ig)
- [NACHC-CAD `fhir-to-omop` (reference implementation)](https://github.com/NACHC-CAD/fhir-to-omop)
- [Google `fhir-data-pipes` (SQL-on-FHIR pipelines)](https://github.com/google/fhir-data-pipes)
- [Google Open Health Stack — FHIR Data Pipes docs](https://developers.google.com/open-health-stack/fhir-analytics/data-pipes)
- [OHDSI `ETL-German-FHIR-Core`](https://github.com/OHDSI/ETL-German-FHIR-Core)
- [OHDSI `FhirToCdm`](https://github.com/OHDSI/FhirToCdm)
- [OHDSI `WhiteRabbit` / Rabbit-in-a-Hat](https://github.com/OHDSI/WhiteRabbit)
- [`pyomop`](https://github.com/dermatologist/pyomop)

### GCP ingestion and BigQuery
- [Cloud Healthcare API — Streaming FHIR resource changes to BigQuery](https://cloud.google.com/healthcare-api/docs/how-tos/fhir-bigquery-streaming)
- [Cloud Healthcare API — Batch export FHIR to BigQuery](https://cloud.google.com/healthcare-api/docs/how-tos/fhir-export-bigquery)
- [Cloud Healthcare API — Import/export FHIR via Cloud Storage](https://cloud.google.com/healthcare-api/docs/how-tos/fhir-import-export)
- [HL7 FHIR Bulk Data Access IG (`$export`)](https://hl7.org/fhir/uv/bulkdata/export/index.html)
- [HAPI FHIR project site](https://hapifhir.io/)

### OMOP vocabulary and lab mapping
- [OHDSI Athena (vocabulary download)](https://athena.ohdsi.org/)
- [The Book of OHDSI — Standardized Vocabularies](https://ohdsi.github.io/TheBookOfOhdsi/StandardizedVocabularies.html)
- [US Core DiagnosticReport Profile for Laboratory Results](https://hl7.org/fhir/us/core/STU5/StructureDefinition-us-core-diagnosticreport-lab.html)
- [SNOMED/LOINC Implementation Guide — FHIR and Laboratory Data](https://docs.snomed.org/implementation-guides/loinc-implementation-guide/information-models-and-terminology-binding/5.3-hl7-fhir-and-laboratory-data)
- [OHDSI Forums — measurement.value_as_concept_id mapping](https://forums.ohdsi.org/t/mapping-laboratory-results-measurement-value-as-concept-id-dealing-with-answer-of-relationship/9588)
- [OMOP vs FHIR comparison (OHDSI 2022 symposium)](https://www.ohdsi.org/wp-content/uploads/2022/10/39-Andrey_Soares_OMOPvFHIR_2022Symposium-Lisa-S.pdf)

### SQL-on-FHIR
- [SQL on FHIR v2 IG (current build)](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/)
- [SQL on FHIR — Tabular views of FHIR data using FHIRPath (npj Digital Medicine, 2025)](https://www.nature.com/articles/s41746-025-01708-w)

### Roche LIMS interfaces
- [Roche `navify` Lab Operations](https://diagnostics.roche.com/us/en/products/instruments/navify-lab-operations-ins-4113.html)
- [Roche `cobas infinity` laboratory solution](https://diagnostics.roche.com/be/en/products/instruments/cobas-infinity-laboratory-solution.html)
- [Roche `cobas liat` HL7 Host Interface Manual](https://diagnostics.roche.com/content/dam/diagnostics/us/en/products/c/cobas-liat-support/cobas-liat-system-him-hl7_sw-ver.-3.3_ver.-8.2.pdf)

### OHDSI tools for CDM validation
- [OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard)
- [OHDSI Achilles](https://github.com/OHDSI/Achilles)
