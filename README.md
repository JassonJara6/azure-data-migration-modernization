# AdventureWorks Azure Data Platform

A production-style, portfolio-sized data platform that will move the AdventureWorks OLTP data through Azure Data Factory, ADLS Gen2, and Azure Databricks into a dimensional model. The complete data-processing path can also run locally with OSS PySpark and Delta Lake, without Azure.

> **Current status:** Bronze R1 has one real COMMITTED parent with eleven reconciled loads (operator evidence); Azure R2 remains pending. **Silver and Gold now have an offline PySpark/Delta workflow**: eleven-source Silver, a sales star schema, three business aggregates, recovery and no-write replay checks. See [local execution](local/README.md) and [validation evidence](docs/validation/local/REPORT.md). The local acceptance dataset is explicitly **synthetic**; it does not demonstrate materialization of the eleven real Silver tables or Gold in Azure. Azure Free Trial is retained; real Azure acceptance is a separate, pending stage. Historical `phase-3-*` files refer to Bronze and keep their original names.

## Target architecture

```text
AdventureWorks SQL Server
        │  full + watermark-based incremental extraction
        ▼
Azure Data Factory ──► ADLS Gen2 / Bronze ──► Databricks / Silver ──► Gold
                              raw                  validated              dimensional
```

For the deployed workspace and assigned metastore, follow [Unity Catalog and Silver through the workspace profile](automation/silver/UNITY-CATALOG-WORKSPACE.md). UC foundation uses delegated workspace/UC privileges without Account Console or mandatory Account ID; the Job still requires an existing service principal and verified RUN AS delegation. The [identity/bootstrap guide](automation/silver/BOOTSTRAP.md) covers initial Azure setup.

Existing Bronze uses Azure Key Vault and published Linked Services. Silver uses Unity Catalog identities and volumes, without embedded credentials. GitHub Actions tests Bronze, Silver and Gold locally; this repository does not automatically deploy them to Azure.

## Phase 1 quick start

### Prerequisites

- Docker Engine with Docker Compose v2
- `curl`
- At least 4 GB of memory available to Docker and roughly 2 GB of free disk space

SQL Server is exposed only on the local machine by default. Copy the environment template and replace the example password (SQL Server requires a strong password):

```bash
cp .env.example .env
# Edit MSSQL_SA_PASSWORD in .env
make source-up
make source-setup
make source-explore
```

`source-setup` downloads Microsoft's AdventureWorks 2022 OLTP backup into the ignored `data/` directory, waits for SQL Server, and performs an idempotent restore. `source-explore` profiles the approved 11-table scope and writes query results to `artifacts/source-profile.txt` (also ignored). See [the Phase 1 runbook](docs/phase-1-local-source.md) and [source contract](docs/source-contract.md) for loading decisions, connection details, and completion criteria.

Stop the container without deleting its database volume with `make source-down`. Use `make source-reset` only when you deliberately want to delete all local source state.

## Repository layout

```text
.
├── docker-compose.yml          # Reproducible local SQL Server
├── scripts/                    # Local automation
├── sql/source/                 # Restore and profiling SQL
├── sql/metadata/               # Bronze control tables, seed, and validation
├── docs/                       # Architecture decisions and source notes
├── terraform/                  # Existing bootstrap and dev infrastructure
├── adf/                        # Bronze pipelines, reconciliation, and guarded SQL commit
├── databricks/                 # Silver/Gold packages, contracts, bundle and tests
├── local/                      # Offline runner, explicit fixtures and persistent Delta lake
├── tests/                      # Automated tests added with each phase
└── .github/workflows/          # Local Bronze, Silver and Gold test workflows
```

Empty phase directories contain `.gitkeep` files solely to make the planned boundaries visible; they do not represent implemented Azure work.

## Delivery roadmap (current plan)

- [x] **Phase 1:** local source, source contract and platform foundation.
- [x] **Phase 2 — Bronze R1:** real parent COMMITTED with eleven reconciled loads, per operator evidence; [R2 acceptance](adf/docs/bronze-end-to-end-rollout.md) remains pending. The ADF confirmation block is retained; external SQL receipts represent confirmation.
- [x] **Silver local validation:** eleven synthetic sources, FULL/incremental MERGE, interrupted-run recovery and replay without additional Delta versions. [Historical Silver evidence](docs/validation/silver/INFORME-VALIDACION-LOCAL-SILVER.md) and [current integrated evidence](docs/validation/local/REPORT.md) are distinguished.
- [ ] **Silver Azure integration:** real eleven-table [one-Job acceptance](databricks/silver/README.md) remains pending. The operator reports that the v4 cluster started and runner grants were verified; that does not yet establish successful receipt/ADLS processing or materialization. No Azure runs are needed for the local path.
- [x] **Gold local implementation:** product/customer/date dimensions, order-line fact, three aggregates, quality/reconciliation and idempotent snapshot MERGE ([model](databricks/gold/README.md)).
- [ ] **Real AdventureWorks Silver/Gold acceptance and Azure execution:** pending; local fixtures are not real source-data evidence.

The repository retains historical phase numbers in source/metadata/ADF and Terraform runbooks. Current Silver documentation governs this increment and does not rename or reset earlier work. Existing infrastructure definitions remain documented in the [bootstrap](terraform/bootstrap/README.md) and [dev Terraform root](terraform/environments/dev/README.md).

Silver release/verification: [publish current artifacts without starting compute](automation/silver/SILVER-RELEASE.md).

## Run the entire local data path

Python 3.11 and Java 17 are required. These commands never call Azure or Databricks:

```bash
python -m pip install './databricks/silver[test]' './databricks/gold'
export SILVER_DELTA_JARS="$(python databricks/silver/tests/download_delta_jars.py work/delta-jars)"
python local/run.py --fixture --root work/local-lake
```

This processes actual synthetic Parquet files for all eleven Bronze contracts,
reuses the existing Silver transformations, writes seven Gold Delta tables and
verifies a replay without changing any of the 22 business/control Delta versions.
Reports include input/target/rejected row counts and exact reconciled sales totals.
Follow [the local guide](local/README.md) for a second process, an updated second
parent, existing real local receipts/Parquet mirrors and tests. No real-looking
receipts are invented in operator-provided data mode.
