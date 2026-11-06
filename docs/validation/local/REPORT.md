# Local Silver and Gold acceptance

This report covers the offline implementation in this PR. Data provenance is
**SYNTHETIC_FIXTURES**, not the real Azure Bronze parent. Test and CLI results below
are populated only from actual executions; Azure acceptance remains pending.

## Scope

The eleven source schemas, primary keys and five INCREMENTAL/six FULL modes are
reused from `databricks/silver/src/admm_silver/contract.json`, itself tested against
the existing SQL metadata seed. Silver business transformations were not rewritten.

| Source | Mode | Key |
| --- | --- | --- |
| Person.Person | INCREMENTAL | BusinessEntityID |
| Person.Address | INCREMENTAL | AddressID |
| Person.StateProvince | FULL | StateProvinceID |
| Person.CountryRegion | FULL | CountryRegionCode |
| Production.Product | INCREMENTAL | ProductID |
| Production.ProductCategory | FULL | ProductCategoryID |
| Production.ProductSubcategory | FULL | ProductSubcategoryID |
| Sales.Customer | FULL | CustomerID |
| Sales.SalesOrderHeader | INCREMENTAL | SalesOrderID |
| Sales.SalesOrderDetail | INCREMENTAL | SalesOrderID + SalesOrderDetailID |
| Sales.SalesTerritory | FULL | TerritoryID |

The fixture generator writes real local Parquet files with the full typed source
schemas, seven-digit timestamps, Bronze metadata, actual file hashes/row counts
and explicitly synthetic COMMITTED-shaped manifests. These are test inputs, NOT
SQL-exported receipts or evidence that any SQL cursor was confirmed. Fixture
parents `30000000-...0001/0002` do not match the real Azure parent.

Gold implements three current-state dimensions, an order-line sales fact and
three daily/monthly aggregates. See [model definitions](../../../databricks/gold/README.md).
The report includes rejects, duplicate/null keys, FKs, header/line totals and
aggregate totals. There are no silent deduplications or discarded source rows.

## Real-data availability and limits

`data/AdventureWorks2022.bak` exists locally (209,838,080 bytes), but no restored
SQL Server or real local Bronze Parquet mirror is accessible. Docker daemon
inspection returned permission denied on `/var/run/docker.sock`; `sqlcmd` is not
installed. The backup alone cannot be read as Parquet. This is the real-data
validation blocker; no missing result is filled with invented real metrics.

The runner supports an existing authoritative receipt/local mirror through
`--receipt`, `--bronze-root` and `--account`, in an isolated output lake. It does
not fabricate evidence, access a cloud account, restore databases or change
Bronze/SQL watermarks. Operator-provided provenance is labelled as such and is
not independently attested by this runner.

Local Spark/Delta is not Unity Catalog, ADLS, DBR or a permissions emulator.
Native Windows is not validated; Linux/WSL2 is the reproducible local target.
Azure Free Trial, all Azure resources, permissions and Jobs remain unchanged.
No Gold Azure deployment/configuration has been introduced.

## Publication and recovery limits

Delta transactions are per table. Existing Silver progress/commit markers are
reused; Gold adds its own progress and batch audit. A synthetic crash after a
Gold Delta write and before checkpoint is resumed without rewriting that table.
Existing Silver tests independently exercise the equivalent Silver interruption.
No cross-table atomic transaction or concurrent writer support is claimed.

Completed-parent replay compares **22 Delta histories** (eleven Silver targets,
two Silver controls, seven Gold targets, two Gold controls), not just final row
counts. A standalone second Python process re-registers existing local Delta
paths and verifies persistence/no additional versions. A second synthetic parent
then exercises partial-window updates/inserts with the composite detail key and FULL deletion
of an unused customer. Repeating completed work must create no new Delta commits.

## Reproduction

Follow [the local execution guide](../../../local/README.md). The CI workflow
runs both test suites and the same initial/replay/updated CLI sequence using
Python 3.11, Java 17, PySpark 3.5.3 and Delta Lake 3.2.1. Dependencies may need
free package downloads on first setup; processing uses only local files.

## Executed results

| Validation | Result | Evidence |
| --- | --- | --- |
| Silver | **PASS: 72/72**, 287.21 s | [JUnit](silver-junit.xml) |
| Gold | **PASS: 16/16**, 399.70 s | [JUnit](gold-junit.xml) |
| Automation | **PASS: 439/439**, 26.49 s | existing offline CLI/provider/wheel regression suite |
| Initial CLI + in-process replay | **PASS** | [25-row source metrics](fixture-first.json) |
| Partial incremental CLI + replay | **PASS** | [18 incoming → 27 current Silver rows](fixture-updated.json) |
| Independent process completed-parent replay | **PASS**, all 22 histories identical | [version maps](cross-process-replay.json) |
| Gold package wheel matches current source | **PASS** | locally built `admm_gold-0.1.0-py3-none-any.whl` |
| Python/Bash/YAML/Markdown/scope checks | **PASS** | local static checks |
| Actual AdventureWorks rows | **NOT VERIFIED** | backup inaccessible as SQL/Parquet in this environment |
| Azure / Databricks execution / UC permissions | **NOT VERIFIED** | not invoked by this task |
| Native Windows Spark | **NOT VERIFIED** | use the validated Linux/WSL2 path |

No final-suite failures or skipped cases. JUnit files retain actual test timing;
only the execution host's hostname attribute was removed. Initial development
runs exposed the deliberately strict model-hash guard when source files changed
while tests ran; the final suite was executed against frozen model code. The
final partial fixture and CLI evidence above are the version included in this PR.

### Observed fixture results

| Metric | Initial parent | Updated parent |
| --- | --- | --- |
| Bronze input rows | 25 | 18 |
| Current Silver rows | 25 | 27 |
| dim_product | 3 | 4 |
| dim_customer | 4 | 4 |
| dim_date | 3 | 3 |
| fact_sales | 5 | 6 |
| agg_sales_daily | 2 | 2 |
| agg_product_month | 3 | 4 |
| agg_customer_month | 3 | 3 |
| Sales quantity | 8 | 11 |
| Net line sales (source amount, not asserted USD) | 115.500000 | 135.500000 |

All eleven Silver sources have zero rejected rows. All seven Gold outputs have
zero duplicate/null-key/rejected rows; all FK checks and header/subtotal mismatch
counts are zero. All three aggregates reconcile to the fact's net/quantity totals.

For example, the second detail window contains **two incoming rows** while its
Silver table has **six current rows**: four unchanged rows survive the MERGE.
The updated snapshot changes existing line quantity, inserts composite key (2,6),
adds product 4 and replaces unused FULL customer 4 with 5. The unchanged date
dimension remains at Delta version 0 even for the new parent (`delta_written=false`).
Successful replays leave both business and control histories unchanged.

Environment: Python 3.11.17, Java 17.0.17, PySpark 3.5.3, Delta Lake 3.2.1,
PyArrow 19.0.1 on Linux, local[2]. Small synthetic fixtures validate correctness,
not performance, production scale or real AdventureWorks completeness. No Azure
or real-data acceptance is implied by these results. No raw data, credentials,
secret configuration or COMMITTED real receipt is published in this directory.
