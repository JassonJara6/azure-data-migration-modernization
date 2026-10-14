# AdventureWorks Azure Data Platform

A production-style, portfolio-sized data platform that will move the AdventureWorks OLTP data through Azure Data Factory, ADLS Gen2, and Azure Databricks into a dimensional model.

> **Current scope:** Phase 1 source setup is complete; Phase 2 Terraform infrastructure is implemented. Phase 3 now includes the SQL ingestion metadata foundation; [dynamic ADF Bronze landing artifacts](docs/phase-3-adf-bronze.md) are implemented but unpublished, with watermark confirmation blocked.

## Target architecture

```text
AdventureWorks SQL Server
        │  full + watermark-based incremental extraction (future Phase 3)
        ▼
Azure Data Factory ──► ADLS Gen2 / Bronze ──► Databricks / Silver ──► Gold
                              raw                  validated              dimensional
```

Secrets will be held in Azure Key Vault. Azure resources are defined with Terraform; GitHub Actions CI/CD remains planned for a later phase.

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
├── sql/metadata/               # Phase 3 control tables, seed, and validation
├── docs/                       # Architecture decisions and source notes
├── terraform/                  # Phase 2 bootstrap and dev infrastructure
├── adf/                        # Phase 3 datasets, pipelines, and read-only SQL planner
├── databricks/                 # Reserved for Phases 4–6
├── tests/                      # Automated tests added with each phase
└── .github/workflows/          # CI/CD added in Phase 7
```

Empty phase directories contain `.gitkeep` files solely to make the planned boundaries visible; they do not represent implemented Azure work.

## Delivery roadmap

- [x] **Phase 1:** local source, exploration, and source contract
- [x] **Phase 2:** Azure infrastructure with Terraform (bootstrap and dev definitions)
- [ ] **Phase 3:** metadata-driven Bronze ingestion ([metadata foundation implemented](docs/phase-3-ingestion-metadata.md); [ADF landing prototype](docs/phase-3-adf-bronze.md) unpublished, safe confirmation pending)
- [ ] Phase 4: Silver transformations and data quality
- [ ] Phase 5: Gold dimensional model
- [ ] Phase 6: observability, reconciliation, and error handling
- [ ] Phase 7: CI/CD and automated testing
- [ ] Phase 8: documentation and portfolio presentation

Phase 1 was completed after the restore and 11-table profile were validated locally. Phase 2 currently includes the [remote-state bootstrap](terraform/bootstrap/README.md), ADLS Gen2, an empty Azure Data Factory with SHIR and Bronze-only access, and an RBAC-enabled Key Vault for future SQL secrets, documented in the [dev Terraform root](terraform/environments/dev/README.md).
