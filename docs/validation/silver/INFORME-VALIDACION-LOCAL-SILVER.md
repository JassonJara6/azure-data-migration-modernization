# Validación local Silver — 27 de octubre de 2026

**Resultado: 50 pruebas aprobadas, 0 fallos, 0 errores, 0 omitidas.** Duración pytest: 179,59 s (2 min 59 s).

Inicio de la ejecución: 27 de octubre de 2026, 13:24:29 (America/Lima, UTC−05:00), según el timestamp original del JUnit (18:24:29 UTC). Este documento consolida esa ejecución; no representa una nueva prueba ni una ejecución Azure.

## Entorno y alcance

- Python 3.11.17 en venv aislado; Java Temurin 17.0.17+10 (JRE).
- PySpark 3.5.3 / Spark local[2], Delta Lake 3.2.1, PyArrow 19.0.1, pytest 8.4.2.
- Archivos del checkout idénticos al main remoto `3ca288146253efba090bbb3fbf67c0c1f2b620d0` (HEAD local `5b64ad5d4c6e16e600175e029ec16fdda066cbe3`).
- Se descargaron runtimes/dependencias públicos y se usaron jars Delta locales. No se utilizó ninguna identidad o recurso Azure.
- Datos: Parquet y recibos sintéticos generados por la suite; almacenamiento Delta real local, sin mocks de MERGE.
- Durante la ejecución validada no hubo cambios de código, commits ni PR. El observador temporal externo al repositorio registró versiones sin alterar la implementación. Se archiva ahora como [silver_validation_probe.py](silver_validation_probe.py), únicamente para trazabilidad y reproducción; no se importa ni activa en la aplicación.

## Verificaciones solicitadas

| Comprobación | Resultado | Evidencia |
| --- | --- | --- |
| Once fuentes | PASS | test_registry_matches_existing_bronze_contract + once casos test_each_table_preserves_business_and_100ns |
| FULL e INCREMENTAL | PASS | Seis FULL / cinco INCREMENTAL del contrato; snapshot, updates/inserts y borrado de ausentes en FULL |
| MERGE real | PASS | test_delta_full_snapshot_and_incremental_merge; filas finales verificadas en Delta local |
| Recuperación | PASS | Interrupción inyectada después de escribir Delta y antes de guardar progreso; reanudación produce SUCCEEDED y once métricas |
| Idempotencia mismo parent | PASS | test_complete_parent_and_replay_without_delta_writes: replayed=true |
| Sin nuevas versiones Delta al replay | PASS | Historial antes/después idéntico para once targets y dos controles |
| Segundo parent | PASS | Solo sintético: incrementales vacíos y recarga FULL; no R2 Bronze real |
| Calidad y errores | PASS | PK nula/compuesta duplicada, esquema, hashes, límites de cursor, relaciones, precisión temporal/decimal, cuarentena y errores seguros |

## Fuentes

**INCREMENTAL (5):** Person.Person, Person.Address, Production.Product, Sales.SalesOrderHeader, Sales.SalesOrderDetail.

**FULL (6):** Person.StateProvince, Person.CountryRegion, Production.ProductCategory, Production.ProductSubcategory, Sales.Customer, Sales.SalesTerritory.

## Versiones Delta medidas en el replay

| Tabla | Antes | Después |
| --- | ---: | ---: |
| person_person | 1 | 1 |
| person_address | 0 | 0 |
| person_stateprovince | 0 | 0 |
| person_countryregion | 0 | 0 |
| production_product | 0 | 0 |
| production_productcategory | 0 | 0 |
| production_productsubcategory | 0 | 0 |
| sales_customer | 0 | 0 |
| sales_salesorderheader | 0 | 0 |
| sales_salesorderdetail | 0 | 0 |
| sales_salesterritory | 0 | 0 |
| _silver_batches | 3 | 3 |
| _silver_progress | 10 | 10 |

El parent recuperado produjo once métricas y cero rechazos. Las diferencias de versiones iniciales entre targets son compatibles con la interrupción/reanudación; la garantía comprobada aquí es que el replay no añadió versiones.

## Reproducción local

Prerrequisitos: Linux/WSL2, Python 3.11 con `venv`, Java 17 seleccionado mediante `JAVA_HOME`/`PATH`, y acceso HTTPS para instalar dependencias y descargar los jars públicos de Delta. No requiere Azure CLI, credenciales, Databricks CLI ni recursos Azure. La ejecución registrada utilizó Linux; no acredita Windows nativo.

Desde la raíz del repositorio:

```bash
python3.11 -m venv work/silver-local-validation/.venv
source work/silver-local-validation/.venv/bin/activate
python -m pip install pytest==8.4.2 pyspark==3.5.3 delta-spark==3.2.1 pyarrow==19.0.1 pyyaml==6.0.3
export SILVER_DELTA_JARS="$(python databricks/silver/tests/download_delta_jars.py "$PWD/work/silver-local-validation/jars")"
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python" SPARK_LOCAL_IP=127.0.0.1
cp docs/validation/silver/silver_validation_probe.py work/silver-local-validation/silver_validation_probe.py
PYTHONPATH="$PWD/work/silver-local-validation" python -m pytest -p silver_validation_probe -v databricks/silver/tests --durations=10 -rs --junitxml=work/silver-local-validation/silver-junit.xml
```

El observador exige Python 3.11 y Java 17, lee historiales reales de Delta local y escribe su JSON junto a su propia copia, en `work/silver-local-validation/`. No ejecutar el observador directamente desde el directorio de evidencias: así se conservan los archivos históricos versionados. La nueva ejecución tendrá sus propios timestamps, tiempos y resultados; no deben presentarse como los de esta ejecución archivada.

La ejecución original empleó ese mismo comando pytest con el observador temporal en el `PYTHONPATH` y un destino JUnit externo al repositorio. Se usaron los dos jars Delta 3.2.1 ya cacheados y las versiones de entorno indicadas arriba. El cambio de rutas de reproducción no representa una ejecución adicional. `pytest` ejecutó toda `databricks/silver/tests`, sin selección de casos.

Sin el observador, `python -m pytest -v databricks/silver/tests` repite las 50 pruebas existentes, incluida la comprobación de versiones de los once targets. El observador añade las dos tablas de control a la observación del replay; no sustituye MERGE por mocks ni simula sus resultados. Su instrumentación se limita al test de recuperación/replay conocido (cuatro llamadas al pipeline), por lo que requiere revisión si cambia ese test.

## Limitaciones y pendientes Azure

- Valida lógica y persistencia Delta OSS local con fixtures pequeños; no valida el dataset Bronze real ni rendimiento/memoria a escala.
- La interrupción es simulada en un punto concreto; no representa todas las posibles caídas de proceso, disco o concurrencia.
- El pipeline tiene transacciones por tabla, no una transacción atómica sobre las once tablas. Requiere un solo escritor; no se probó concurrencia multiwriter.
- No acredita Databricks Runtime 15.4, instalación remota del wheel, permisos del runner, Unity Catalog, FUSE /Volumes, red o acceso ADLS real.
- La elegibilidad/cuota de Standard_D4ds_v4 en Azure Free Trial sigue pendiente. No se cambió suscripción, SKU, política o permisos.
- Aceptación Azure pendiente: Job real con parent COMMITTED, once métricas reconciliadas, cero rechazos y replay verificado. No se ejecutaron Jobs, clusters, despliegues, ADF/R2 ni escrituras de watermarks.

## Evidencias publicadas y sanitización

- [silver-junit.xml](silver-junit.xml): 50 casos, tiempos y timestamp de la ejecución original. Se eliminó únicamente el atributo `hostname` interno del ejecutor y se normalizó la indentación XML. Todos los demás atributos, casos y resultados se conservaron.
- [replay-evidence.json](replay-evidence.json): copia exacta del original; once métricas, versiones antes/después de trece tablas y `azure_execution=NOT_PERFORMED`. Son métricas sintéticas, no resultados del parent Bronze real.
- [silver_validation_probe.py](silver_validation_probe.py): copia exacta del observador usado en aquella ejecución, archivada como soporte de reproducción. Solo lee historiales y usa las llamadas de la prueba local existente; no es código de despliegue ni de transformación.
- Este informe adapta el original para publicar enlaces y comandos reproducibles sin rutas internas del entorno. Las fuentes, versiones y resultados no se alteraron.

No se publican el log bruto, `environment.json`, datos Parquet, recibos reales, archivos de configuración de `work/`, tokens, contraseñas ni identificadores del host interno. Los archivos publicados fueron revisados en busca de credenciales, URLs autenticadas y cadenas de conexión; no se encontraron. Los nombres de tests que mencionan errores de permisos/secretos son nombres de casos sintéticos, no valores de credenciales.

### Integridad y procedencia

SHA-256 calculados sobre los archivos originales recuperados:

| Archivo original | SHA-256 |
| --- | --- |
| INFORME-VALIDACION-LOCAL-SILVER.md | `61d7838ea0916f6320373344c6df7c953fd52d2f76078a04180ba242fd745ab3` |
| replay-evidence.json | `8707a28ef3afa522d7842cd78d81df563d626aa22ea0c646e880f83c34464abb` |
| silver-junit.xml | `a9798dc3bfc276b9f9caff320ad151ec743ac401cf85c45b36631501c990c930` |
| silver_validation_probe.py | `377f8d42f00bcb88369bf6dd0235af2c9934da6b4a1301235e27cc866e46056c` |

El JSON y el observador publicados conservan esos hashes. El informe y el JUnit se adaptaron/sanitizaron como se describe arriba; sus hashes publicados difieren de los originales.

**La validación local solicitada queda aprobada. La aceptación real de Silver en Azure continúa pendiente por capacidad, manteniendo obligatoriamente Azure Free Trial. No se afirma que las once tablas reales estén materializadas en Azure.**
