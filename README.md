# AdventureWorks Azure Data Platform

A production-style, portfolio-sized data platform that will move the AdventureWorks OLTP data through Azure Data Factory, ADLS Gen2, and Azure Databricks into a dimensional model.

> **Current plan:** Phase 1 source/platform foundation is complete. Phase 2 Bronze has one real COMMITTED parent with eleven loads, reported by the operator (`20d65511-8190-41a0-8ad2-23d7568363d2`); Azure R2 is still pending. **Phase 3 Silver** now has [PySpark/Delta code, Unity Catalog configuration, a single Databricks Job and local tests](databricks/silver/README.md); real Databricks validation is pending. Historical `phase-3-*` documents refer to Bronze and retain their original names.

## Target architecture

```text
AdventureWorks SQL Server
        │  full + watermark-based incremental extraction
        ▼
Azure Data Factory ──► ADLS Gen2 / Bronze ──► Databricks / Silver ──► Gold
                              raw                  validated              dimensional
```

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
- [ ] **Phase 3 — Silver:** implementation and automated local validation in this repository; [one-Job Databricks acceptance](databricks/silver/README.md) pending deployment/configuration and a real run.
- [ ] Gold and later delivery phases remain outside this change.

The repository retains historical phase numbers in source/metadata/ADF and Terraform runbooks. Current Silver documentation governs this increment and does not rename or reset earlier work. Existing infrastructure definitions remain documented in the [bootstrap](terraform/bootstrap/README.md) and [dev Terraform root](terraform/environments/dev/README.md).
