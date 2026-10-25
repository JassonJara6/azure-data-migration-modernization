# Verificación FILE/AUTO y estado real de Silver

## Causa confirmada y corrección

PR #36 exportaba el wheel, objeto Workspace `FILE`, con `format=RAW`.
El servicio rechazó ese formato. La publicación ya había ocurrido; este fallo
no demuestra un problema de upload, instalación o transformación.

Se conserva Workspace API **GET `/api/2.0/workspace/export`**, pero con **AUTO**.
La [documentación oficial](https://docs.databricks.com/api/workspace-files/v1/export)
explica que AUTO incluye workspace files; `FILE` no es el objeto legacy `LIBRARY`
(no exportable). No se usa Files API de Volumes para una ruta Workspace.
En [CLI v1.22.1](https://github.com/databricks/cli/blob/v1.22.1/cmd/workspace/workspace/overrides.go),
`--file` decodifica el `content` base64 con Go y escribe los bytes; nunca pasa el
wheel por UTF-8/cp1252. SOURCE se reserva al notebook. No hay fallback a RAW ni
omisión del SHA. Un download fallido nunca se considera verificado.

El límite documentado de `content` es 10 MB; el wheel observado tiene 16.215
bytes. `MAX_NOTEBOOK_SIZE_EXCEEDED`, objeto ausente y permisos insuficientes se
convierten en códigos estáticos útiles sin mostrar stderr/rutas del proveedor.
Si crece hasta superar el límite, detenerse y revisar otro transporte documentado;
no truncar, comparar solo nombre/tamaño ni saltar integridad. El operador debe
poder leer/exportar, y el runner necesita CAN_READ en código/artefactos.

## Revisión integral: controles conservados

- Build en temp limpio, `SOURCE_DATE_EPOCH`, versión derivada de fuentes, un wheel
  seleccionado explícitamente, METADATA coherente y SHA congelado al aprobar.
  DAB sincroniza notebook fuente y publica la ruta exacta seleccionada.
- Antes/después de publicar: fingerprint del bundle, Job existente/identidad,
  ausencia de runs activos/triggers, parámetros, versión, timeout, concurrencia,
  RUN AS, ACL de notebook/FILE, celdas Python y SHA remoto. Se rechazan wheels
  antiguos, nombres similares y bytes distintos. No se altera Terraform.
- El notebook compara la distribución instalada con `expected_package_version`
  **antes** de importar `admm_silver` o acceder a datos. El SHA remoto prueba el
  artefacto; no prueba por sí solo qué distribución instaló el cluster.
- `--deploy-only` reutiliza estado/Job/cómputo existentes, publica artefactos y
  actualiza CAN_READ de sus subdirectorios; nunca hace plan/apply, export SQL,
  upload del receipt, run-now ni arranque de cluster. Es una escritura remota y
  requiere autorización del operador; no se ha ejecutado en esta corrección.
- FULL, MERGE incremental, gates de calidad, auditoría, once métricas y replay
  sin nueva versión Delta siguen cubiertos por Spark/Delta local. No se modifica
  la transformación ni se confunde esta prueba con aceptación Azure.

## committed_evidence: sin causa inventada

La excepción original del run 11937840542482 no está disponible. `fs cat` y
CONTRACT_VALID del operador no prueban acceso POSIX bajo el runner. El notebook
pasa parent/receipt como widgets, abre UTF-8 en el driver, valida objeto/parent y
luego valida estado COMMITTED, las 11 fuentes y hashes antes de publicación.
[Volumes soporta POSIX/Python](https://learn.microsoft.com/en-us/azure/databricks/volumes/volume-files);
DBR 15.4 dedicado es compatible con este uso; las restricciones Scala no justifican
sustituir `open()` Python. Requiere USE_CATALOG, USE_SCHEMA y READ_VOLUME para el
runner, además de acceso de storage de la external location. Los grants reportados
no equivalen a una lectura real dentro del Job.

El manejador actual conserva código, etapa, tipo y ubicación relativa/línea,
mediante allowlist, tanto en salida como en error de tarea. No imprime excepción
cruda, receipt ni error_trace; el colector recupera solo esos campos seguros.
Los tests cubren archivos ausentes/denegados, encoding, JSON, parent, hashes,
fallos de configuración y propagación. **No hay evidencia para atribuir el fallo
histórico a permisos, FUSE o JSON; no se cambia esa lógica especulativamente.**

## Capacidad: NO-GO conservado

La evidencia del operador indica NotEnabledOnSubscription para D4ds_v5 y D4ads_v5,
y ausencia de status para D4ds_v4. No hay SKU elegible demostrada. Se conserva
SCOPED_PROVIDER_STATUS_MISSING; no se infiere disponibilidad de campos omitidos.

Distinguir:

| Evidencia | Qué permite concluir |
| --- | --- |
| NotEnabledOnSubscription / restricciones Resource SKUs | Rechazo de elegibilidad; no identifica por sí solo la causa comercial/política interna |
| NotAvailableInRegion | Restricción regional; no es un contador de cuota |
| Usage + quotas regional y familia | Margen de vCPU; no prueba elegibilidad ni stock |
| Política Databricks y list-node-types | Compatibilidad/autorización del workspace; no garantiza capacidad Azure |
| CLOUD_PROVIDER_RESOURCE_STOCKOUT | Fallo físico de asignación en ese intento; no justifica reintentar pagando indefinidamente |
| Status omitido / consulta vacía | Evidencia insuficiente; nunca disponible por defecto |

Vía sin clusters: revisar la colección de node_info/política/identidad; obtener en
Azure Portal **Subscription → Usage + quotas** las cuotas regionales y de las tres
familias; el Owner puede consultar **Resource SKUs** con `--all` y región East US
(incluye restricciones que el filtro por disponibilidad oculta). No insistir con
consultas completas que agotan el timeout ni interpretar `[]` como habilitación.
Si no hay evidencia inequívoca, solicitar a Azure Support **SKU enablement en la
suscripción/región para esas SKU** adjuntando los estados recogidos; no solicitar
incremento de presupuesto ni asumir que aumentar cuota resuelve el rechazo.
Un ticket o respuesta verbal no convierte el preflight en PASS: se requiere
metadata verificable por los controles existentes antes de ejecutar. Si el API
sigue omitiendo status, permanece bloqueado hasta diseñar/revisar una fuente
autoritativa equivalente; este cambio no introduce una excepción manual.

Fuentes: [SKU no disponible](https://learn.microsoft.com/en-us/azure/azure-resource-manager/troubleshooting/error-sku-not-available?tabs=azure-cli),
[cuota frente a capacidad](https://learn.microsoft.com/en-us/azure/virtual-machines/quotas).
Ninguna lectura garantiza stock físico futuro; no se crean reservas facturables.

## Matriz de validación

PASS local no significa validación en Databricks. PASS operador identifica
explícitamente la evidencia suministrada, no una observación de este entorno.

| Validación | Estado | Evidencia |
| --- | --- | --- |
| FILE/RAW falla | PASS | Error real informado + reproducción con CLI 1.22.1/HTTP loopback |
| AUTO, endpoint/query/auth, bytes binarios | PASS local | CLI oficial fijado por SHA; servidor estricto; 16.215 bytes con NUL/0xff/CRLF |
| Denegado, ausente, límite, base64 inválido | PASS local | CLI real recibe errores y no genera archivo exitoso |
| Bytes alterados / SHA incorrecto / versión antigua | PASS local | Regresiones de notebook_guard; no se debilita SHA |
| Notebook celdas/parámetros/version pre-import | PASS local | Suites guard, notebook y orquestación |
| deploy-only no inicia compute ni Terraform | PASS local | Orquestación con comandos permitidos, flags inválidos y guardas de costo |
| Reproducibilidad / OneDrive PermissionError | PASS local | Builds reales reproducibles + regresión de limpieza denegada |
| Wheel FILE 16.215 bytes y Job lo referencia | PASS operador | Evidencia suministrada; no consulta remota propia |
| Receipt/parent y grants informados | PASS operador | CONTRACT_VALID operador; no prueba lectura del runner |
| AUTO + SHA contra Workspace real | NOT VERIFIED | Sin credenciales/acceso de Workspace en este entorno |
| Wheel instalado/receipt POSIX con runner | NOT VERIFIED | Requiere ejecución autorizada y capacidad |
| Causa histórica committed_evidence | NOT VERIFIED | Excepción original irrecuperable |
| Elegibilidad dos v5 | FAIL operador | NotEnabledOnSubscription; no hay bypass |
| Elegibilidad v4 y stock físico | NOT VERIFIED | Status ausente; SCOPED_PROVIDER_STATUS_MISSING |
| 11 tablas y replay real | NOT VERIFIED | No se ejecutaron Jobs ni clusters |

## Un único procedimiento del operador (Git Bash)

Después de fusionar, conservar `work/silver-deployment` y la sesión OAuth nativa:

```bash
git switch main
git pull --ff-only origin main
bash automation/silver/collect_workspace.sh --profile admm-workspace
```

El wrapper evita conversión MSYS y cp1252. Lee Job/node metadata/policy,
receipt/contrato y diagnóstico seguro de la tarea antigua; verifica el **wheel
local seleccionado originalmente** y notebook contra el Workspace. No imprime
receipt ni tokens. Si falta staging, registra NOT_VERIFIED y sigue recogiendo
metadatos; no construye ni descarga un original para aprobarse a sí mismo.
READ_ONLY_COLLECTION significa colección, no aceptación.

Solo con autorización para **publicar artefactos sin ejecutar**:

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1 PYTHONIOENCODING=utf-8
winpty python automation/silver/launch.py --stage silver --uc-mode workspace --config work/silver-dev.json --deploy --deploy-only
bash automation/silver/collect_workspace.sh --profile admm-workspace
```

Revisar/aprobar el hash mostrado; exige estado DAB y Job existentes. El comando
puede actualizar permisos CAN_READ de artefactos; no ejecutarlo bajo esta tarea.
`release.json=ARTIFACTS_VERIFIED` exige comparación remota completa, no basta que
DAB devuelva éxito. Si falla, detenerse; no quitar validaciones.

**Aceptación final pendiente**, solo tras ELIGIBLE_SNAPSHOT y autorización expresa
para coste de un run (no ejecutar ahora):

```bash
python automation/silver/check_capacity.py --profile admm-workspace --job-id 210884055971910 && winpty python automation/silver/launch.py --stage silver --uc-mode workspace --config work/silver-dev.json --deploy
```

La ruta normal conserva plan UC guardado/aprobación del binario y puede exportar
SQL/upload de evidencia: revisar el plan, no aprobar cambios inesperados. Un
único Job debe reportar SUCCESS, parent confirmado exacto, 11 métricas (las cinco
INCREMENTAL y seis FULL del contrato), cero rechazos, conteos reconciliados,
claves/esquemas válidos y `replay_verified=true` sin nuevas versiones Delta.
Contrastar acceptance.json con run, bundle SHA y release actuales. Un archivo de
aceptación antiguo no prueba esta versión. No ejecutar R2/ADF ni tocar watermarks.
**Silver sigue pendiente de aceptación real.**

## Resultados locales de este PR

- **291 passed**, sin skips: suite completa `automation/silver/tests` con
  `SILVER_DATABRICKS_BIN=/tmp/admm-cli-1221/databricks`,
  `SILVER_WHEEL_BUILD_TESTS=1`, `SILVER_PROVIDER_AUTH_TESTS=1`,
  `SILVER_TERRAFORM_BIN=/workspace/work/infrastructure/bin/terraform` y deps locales.
- Incluye **6 passed** del CLI real v1.22.1 contra HTTP loopback estricto.
  El binario oficial se comprueba en CI con SHA-256 del ZIP Linux amd64
  `2f24260441f211e75df4266fba506983cd2373c622cb34437d752befba8261af`.
  La credencial de esa prueba es un literal sintético aceptado solo por localhost;
  no usa ni exporta la sesión OAuth real. El servidor contempla la consulta previa
  `/.well-known/databricks-config` observada en v1.22.1, sin relajar la solicitud
  exacta a workspace/export. Esto prueba el cliente/contrato, no el servicio vivo.
- **50 passed**, sin skips, en **193,49 s**: `databricks/silver/tests` con Spark
  local y Delta 3.2.1; jars cacheados y sin servicios Azure.
- `terraform fmt -check`, `validate` en cuatro roots y **12 pruebas plan/mock**
  existentes: **PASS** (2 plataforma, 2 módulo, 4 UC account, 4 UC workspace).
  El primer intento restringido no pudo abrir el socket local de los plugins;
  se repitió con permiso de loopback y todos pasaron. No backend real ni apply.
- `compileall`, sintaxis Bash, YAML y `git diff --check`: **PASS**.
- Fallos pendientes de pruebas locales: **ninguno**. Validaciones remotas: las
  marcadas NOT VERIFIED/FAIL en la matriz; no se ejecutó ningún comando de deploy.

La primera versión del fixture CLI esperaba una sola petición y falló por la
consulta `.well-known`; fue corregido a partir del tráfico observado, no mediante
un cambio al launcher. CI instala el CLI fijado y ejecuta estas regresiones.
