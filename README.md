# AdventureWorks Azure Data Platform

A production-style, portfolio-sized data platform that will move the AdventureWorks OLTP data through Azure Data Factory, ADLS Gen2, and Azure Databricks into a dimensional model.

> **Current plan:** Phase 1 foundation and **Phase 2 Bronze R1 are complete**: one real COMMITTED parent with eleven reconciled loads, reported by the operator (`20d65511-8190-41a0-8ad2-23d7568363d2`); Azure R2 remains pending. **Phase 3 Silver is validated locally: 50/50 tests passed** with Python 3.11, Java 17, PySpark 3.5.3 and Delta Lake 3.2.1 ([report and evidence](docs/validation/silver/INFORME-VALIDACION-LOCAL-SILVER.md)). **Silver integration/acceptance in Azure remains pending because of compute-capacity restrictions; Azure Free Trial is retained.** These local results do not demonstrate materialization of the eleven real Silver tables in Azure. Historical `phase-3-*` documents refer to Bronze and retain their original names.

## Target architecture

```text
AdventureWorks SQL Server
        │  full + watermark-based incremental extraction
        ▼
Azure Data Factory ──► ADLS Gen2 / Bronze ──► Databricks / Silver ──► Gold
                              raw                  validated              dimensional
```

For the deployed workspace and assigned metastore, follow [Unity Catalog and Silver through the workspace profile](automation/silver/UNITY-CATALOG-WORKSPACE.md). UC foundation uses delegated workspace/UC privileges without Account Console or mandatory Account ID; the Job still requires an existing service principal and verified RUN AS delegation. The [identity/bootstrap guide](automation/silver/BOOTSTRAP.md) covers initial Azure setup.

Existing Bronze uses Azure Key Vault and published Linked Services. Silver uses Unity Catalog identities and volumes, without embedded credentials. GitHub Actions tests Bronze and Silver locally; this repository does not automatically deploy them to Azure.

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
├── databricks/                 # Silver package, Job bundle, contracts, and tests
├── tests/                      # Automated tests added with each phase
└── .github/workflows/          # Local Bronze and Silver test workflows
```

Empty phase directories contain `.gitkeep` files solely to make the planned boundaries visible; they do not represent implemented Azure work.

## Delivery roadmap (current plan)

- [x] **Phase 1:** local source, source contract and platform foundation.
- [x] **Phase 2 — Bronze R1:** real parent COMMITTED with eleven reconciled loads, per operator evidence; [R2 acceptance](adf/docs/bronze-end-to-end-rollout.md) remains pending. The ADF confirmation block is retained; external SQL receipts represent confirmation.
- [x] **Phase 3 — Silver local validation:** 50/50 tests passed; eleven synthetic sources, FULL/incremental MERGE, interrupted-run recovery and replay without new Delta versions ([evidence](docs/validation/silver/INFORME-VALIDACION-LOCAL-SILVER.md)).
- [ ] **Phase 3 — Silver Azure integration:** real [one-Job acceptance](databricks/silver/README.md) remains blocked by compute capacity under **Azure Free Trial**. Local validation does not certify real Azure data, Unity Catalog or runner permissions.
- [ ] Gold and later delivery phases remain outside this change.

The repository retains historical phase numbers in source/metadata/ADF and Terraform runbooks. Current Silver documentation governs this increment and does not rename or reset earlier work. Existing infrastructure definitions remain documented in the [bootstrap](terraform/bootstrap/README.md) and [dev Terraform root](terraform/environments/dev/README.md).

Silver release/verification: [publish current artifacts without starting compute](automation/silver/SILVER-RELEASE.md).
