# Silver: primaria D4ds_v4, Azure Free Trial, sin fallback

Este cambio prepara configuración y controles; **no acredita una ejecución Azure**.
El operador confirmó en East US `cores` y `standardDDSv4Family`: límite 4, uso 0,
libres 4. El informe anterior mostró D4ds_v4 en el catálogo Databricks y ARM con
`restrictions: []`, 4 vCPU, 16 GiB, x64. El estado Databricks estaba ausente: se
conserva como `NOT_REPORTED`, nunca se convierte en disponibilidad inventada.
Los 50 tests Silver locales y sus límites están [archivados](../../docs/validation/silver/INFORME-VALIDACION-LOCAL-SILVER.md).

## Configuración propuesta

- Mismo workspace, metastore, catálogo, volúmenes, Job **210884055971910**, política,
  Service Principal y permisos. No crea ni adopta ninguno.
- Primaria y driver `Standard_D4ds_v4`, DBR 15.4 LTS, SINGLE_USER, Spark local,
  cero workers, On Demand. Concurrencia 1, timeout 7200 s, reintentos 0, sin triggers.
- `driver_node_type_flexibility.alternate_node_type_ids: []` **y** equivalente
  worker explícitos. Omitir los campos permitiría fallback automático del workspace.
  La política fija ambas listas vacías (representación CSV vacía de la política).
- Las rutas antiguas v5 conservan su comportamiento; v4 tiene una ruta separada
  de una sola SKU, sin exigir un fallback ficticio o utilizar las v5 rechazadas.
  Solo se cambia el root Terraform **workspace**, no el modo legacy de cuenta.

## Evidencia de elegibilidad y cuotas

El checker y launcher comparten `single_node_v4.py`:

1. Cuenta y tenant del proyecto, suscripción Enabled, ARM
   `quotaId=FreeTrial_2014-09-01` y `spendingLimit=On`. No cambia de suscripción,
   oferta, cuota o configuración de gasto; otra oferta se rechaza.
2. Binding ARM workspace/ID Databricks y East US. SKU única en el catálogo,
   dimensiones correctas, sin GPU, oculta o deprecada.
3. Lectura **viva** Resource SKUs de esa suscripción con el filtro documentado
   por ubicación. Paginación completa, máximo 100 páginas / 180 s totales,
   cada lectura máximo 60 s; solo enlaces management.azure.com del mismo recurso.
   El fallo/timeout mantiene el bloqueo. No hay flags de bypass ni snapshots
   locales aceptados como autorización.
4. SKU ARM exacta, familia standardDDSv4Family, East US, x64, 4 vCPU/16 GiB,
   Premium IO, `restrictions` explícitamente vacío. Restricción incluso zonal,
   dato omitido, duplicado o contradictorio bloquea esta ruta conservadora.
5. Flags explícitos Databricks de rechazo **siempre bloquean**, aunque ARM esté
   libre. Un campo ausente sigue siendo desconocido; la evidencia positiva se
   obtiene separadamente de ARM, catálogo y cuotas, con su procedencia indicada.
6. Cuotas regional y familia con al menos 4 vCPU libres cada una. Acepta números
   y cadenas numéricas, `currentValue` o `current_value`; conserva originales.
   Ausencia, duplicados, alias contradictorios, booleanos, NaN o fracciones bloquean.
7. Política efectiva fija de v4 y listas vacías; no se aceptan familias de políticas
   no resueltas ni reglas nuevas sin revisión. Se conservan preflights de UC,
   permisos de entrada, identidad y RUN AS. SHA de wheel/notebook no se elimina.

`ELIGIBLE_SNAPSHOT` significa elegibilidad/cuota nominal en la lectura, **no stock
físico ni permiso para ejecutar**. Los créditos Free Trial pueden agotarse y una
asignación puede fallar por capacidad u otras restricciones del proveedor.

## Migración exacta, pendiente de aprobación remota

`migrate_v4.py` es independiente de `launch.py --deploy`: **nunca** ejecuta el Job,
publica/reconstruye artefactos, exporta SQL, sube receipts o modifica ACL/identidades.
No necesita Account Admin. Requiere la configuración privada existente,
`work/silver-deployment/job-id.json`, el bundle local y su estado `.databricks`.
No borrar/adoptar estado para superar un bloqueo.

El modo por defecto solo consulta y genera plan/evidencia privada en `work/`.
`--apply` exige dos aprobaciones independientes:

- Terraform: SHA-256 de **todo el binario guardado**, con las verificaciones
  existentes antes de aplicar ese mismo archivo. Solo admite `update` de
  `databricks_cluster_policy.silver[0]`: node_type_id v5→v4, driver fijo v4 y dos
  listas de fallback vacías. Se exige el mismo ID, reglas restantes iguales y
  únicamente el cambio node_type_id del output deployment. Rechaza creaciones,
  reemplazos, destrucciones, permisos, identidades o cualquier otro recurso/cambio.
- Job: SHA de la transición con Job ID/host/policy ID, settings anteriores completos,
  clusters finales y SHA del YAML local. Solo acepta el origen v5 con fallback
  **exactamente [D4ds_v4]**, o el destino v4 ya configurado. No acepta D4as_v5,
  otros fallback ni cambios genéricos de tamaño, runtime, runner, triggers o pools.
  La llamada Jobs Update contiene **solo `new_settings.job_clusters`**. Mantiene
  tasks, parámetros, wheel/notebook, permisos y demás settings. Después comprueba
  igualdad completa con el resultado esperado.

Durante las aprobaciones vuelve a comprobar RUN AS, ausencia de runs activos,
settings, YAML local, cuotas y ARM; antes del POST verifica también que la política
no cambió durante la aprobación. No hay reintentos automáticos de escrituras.
Tras verificar el destino sincroniza únicamente los job_clusters del YAML local
(conserva artefactos y estado DAB) y node_type_id de la configuración privada.
No se reescribe release.json ni acceptance.json como si hubiera una ejecución nueva.

**No es una transacción distribuida:** si se aplica la política y luego falla la
revisión/API/local sync del Job, puede quedar política v4 y Job v5. Se detiene sin
iniciar cómputo. Volver a previsualizar permite revisar el estado real y completar
la transición; no hay rollback automático. Si un POST termina en timeout, su
resultado es incierto: leer/revisar antes de repetir. Se requiere un solo operador
sin ediciones concurrentes; las APIs no ofrecen aquí un compare-and-swap atómico.

### Procedimiento Git Bash después de fusionar

Prerrequisitos existentes: Python 3.11+, Terraform 1.9.8, Azure CLI autenticado,
Databricks CLI v1.22.1 OAuth `admm-workspace`, estados/backend originales accesibles.
El intérprete empaquetado de Azure CLI MSI se usa en Windows para evitar que
az.cmd interprete `&`/`%` de las URL REST; `-X utf8` evita depender de cp1252 incluso en modo aislado. Si no se encuentra ese intérprete, se bloquea.

**Previsualización de solo lectura remota (sin aplicar ni ejecutar):**

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1 PYTHONIOENCODING=utf-8
python automation/silver/migrate_v4.py --config work/silver-dev.json
```

Revisar el binario/plan.txt/review.json y v4-job-transition-<sha>.json de ESTA
invocación. Las vistas pueden contener metadatos privados; no subir work/ a Git.
La previsualización termina V4_PLAN_ONLY y no cambia la configuración privada.

**Solo con aprobación expresa posterior para actualizar política/Job existentes:**

```bash
winpty python automation/silver/migrate_v4.py --config work/silver-dev.json --apply
```

Regenera y exige aprobar el plan de esa ejecución; no acepta hashes antiguos.
Efectos remotos permitidos: **un update de la política existente y un update del
cómputo del Job existente**. No Terraform apply de la plataforma, nuevos recursos,
permisos, metastore ni identidades. No arranca compute. Si el destino ya coincide,
no vuelve a escribir el Job. No usar un `bundle deploy` antiguo que revierta el YAML.

**Lecturas posteriores, sin ejecución:**

```bash
python automation/silver/check_capacity.py --profile admm-workspace --job-id 210884055971910
bash automation/silver/collect_workspace.sh --profile admm-workspace
```

Exigir V4_JOB_CONFIGURATION_VERIFIED, ELIGIBLE_SNAPSHOT y ARTIFACT_BYTES_VERIFIED,
revisar el Job/política real con ambas listas vacías y los mismos runner/artefactos.
La lectura de un receipt como operador no demuestra lectura por el runner.

**Ejecución posterior NO autorizada por este PR:** solo tras esa verificación y
nueva autorización de consumo de créditos, usar el launcher normal con stage silver,
uc-mode workspace y config ya v4. Su plan UC debe tener cero cambios inesperados;
conserva aprobaciones y validación SHA. La rama v4 no concede ACL de artefactos:
si faltan permisos, bloquea sin repararlos. Debe conseguir SUCCESS, once métricas,
cero rechazos, parent COMMITTED exacto y replay sin versiones nuevas. Un fallo de
stockout se investiga sin bucle automático, sin cambiar Free Trial ni pedir cuota.
No ejecutar ADF/R2 ni tocar watermarks Bronze.

## Pruebas y límites de compatibilidad

Las regresiones cubren cuotas/formato original, elegibilidad positiva ARM con
status Databricks ausente, rechazos explícitos, Free Trial, binding, paginación,
plan estricto y transición, carreras/tampering, reanudación y modo preview sin POST.
El CLI **real v1.22.1** se prueba contra HTTP loopback estricto para Jobs Update;
su esquema real confirma las listas explícitas vacías. La credencial loopback es
sintética y no se utiliza en Azure. Terraform se valida con planes/proveedor mock,
no con backend ni recursos reales. La suite Silver local permanece sin cambios.

Pendiente real: plan con estado original, representación de las reglas de listas
vacías en la política remota, persistencia de listas vacías en Jobs GET, OAuth/RBAC,
capacidad física y aceptación de once tablas. Si el servicio omite campos y no puede
verificarse el destino, se bloquea; no se inventan valores para superar el control.

Fuentes oficiales: [Resource SKUs](https://learn.microsoft.com/en-us/rest/api/compute/resource-skus/list?view=rest-compute-2021-07-01),
[cuotas](https://learn.microsoft.com/en-us/azure/virtual-machines/quotas),
[fallback explícitamente desactivado](https://learn.microsoft.com/en-us/azure/databricks/compute/flexible-node-types#disable-flexible-node-types-on-a-compute-resource),
[Jobs Update](https://docs.databricks.com/api/workspace/jobs/update),
[políticas](https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition).

## Resultados locales de este PR

- Automatización completa: **405 passed**, sin omisiones; incluye 88 casos nuevos,
  builds reales reproducibles, OAuth de proveedor y CLI real contra loopback.
- Silver/Spark/Delta: **50 passed** en **173,48 s**, Python 3.11.17, Java 17.0.17,
  Spark 3.5.3 y Delta 3.2.1 locales. No cambió ninguna transformación.
- Terraform workspace: validate/fmt y **5 pruebas plan/mock aprobadas**; no backend
  real ni apply. La nueva prueba exige v4, cero workers y ambas reglas sin fallback.
- Sintaxis Python/Bash, enlaces Markdown y git diff --check: PASS.
- Windows nativo, plan con estado real, API Databricks real y capacidad física:
  **NOT VERIFIED**. No se ejecutaron cambios remotos ni compute.

Comandos de automatización ejecutados con CLI 1.22.1 fijado y Terraform 1.9.8:

```bash
SILVER_DATABRICKS_BIN=/ruta/al/databricks SILVER_PROVIDER_AUTH_TESTS=1 SILVER_WHEEL_BUILD_TESTS=1 SILVER_TERRAFORM_BIN=/ruta/al/terraform python -m pytest -q automation/silver/tests
```

Las primeras pruebas detectaron la estructura real `oneOf` y referencias anidadas
del esquema CLI (se corrigió el fixture), y una mutación de política durante la
aprobación (se congela ahora su SHA antes de esperar). La ausencia inicial del CLI
en el cache temporal se resolvió descargando el binario oficial y verificando SHA.
No quedan fallos locales pendientes; estos resultados no son evidencia Azure real.
