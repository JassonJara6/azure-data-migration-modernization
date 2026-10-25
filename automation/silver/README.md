# Puesta en marcha Silver — infraestructura y automatización

**Workspace y metastore ya desplegados:** usar la [ruta UC con perfil workspace](UNITY-CATALOG-WORKSPACE.md), sin Account ID/Account Admin para preparar UC. Sus comandos `--stage uc` y `--stage silver` prevalecen sobre la ruta histórica de cuenta descrita más abajo. El Job conserva la validación de su principal y RUN AS.

Silver y la infraestructura están fusionados en `main`. La **[guía única de bootstrap y diagnóstico](BOOTSTRAP.md)** gobierna el orden de puesta en marcha y los bloqueos de identidad. Este documento conserva el detalle de infraestructura, seguridad, costos y ejecución del Job. No se realizan merges, cherry-picks ni checkout automático de código remoto.

Contexto fijado: RG `rg-admm-dev-platform`, East US, ADLS `stadmmdevdlf6d99ba4`, filesystem Bronze existente, parent `20d65511-8190-41a0-8ad2-23d7568363d2`. Su COMMITTED de once cargas es evidencia comunicada por el operador; esta entrega no lo volvió a consultar. R2/ADF no se ejecutan.

## Infraestructura y estados

| Etapa | Root / estado | Recursos nuevos |
| --- | --- | --- |
| Azure | `terraform/environments/dev`, **misma** clave `dev/platform.tfstate` | Módulo opt-in: workspace Premium compatible con UC, Access Connector con MI de sistema, RBAC por contenedor. Solo si falta, registro explícito del RP Microsoft.Databricks. |
| UC | `terraform/databricks/dev`, **nueva** clave `dev/databricks-uc.tfstate` | Principal Databricks del Job, asignaciones del workspace, credenciales y locations por filesystem, catálogo/esquemas/volúmenes, grants y política de cómputo. |
| Job | Copia de #18 en `work/silver-deployment/bundle` | Un Job sin schedule, RUN AS el principal nuevo y cluster efímero de un nodo por ejecución. |

Ambos estados Terraform utilizan el backend Azure AD existente `admmdevtfstatef6d99ba4/tfstate`. **No se modifica el backend existente, migra estado ni recrea la plataforma.** La segunda clave permite inicializar el proveedor workspace después de conocer su URL; no se usa `-target` ni un único apply circular Azure/UC. No hacer `init -migrate-state`, `state rm`, `import` improvisado ni cambiar subscription/location para sortear un error.

`enable_databricks=false` por defecto conserva el root habitual sin workspace. El launcher lo activa solo en un plan guardado. Tras crear el módulo debe mantenerse activo en futuros planes; `prevent_destroy` bloquea su retirada accidental. La versión AzureRM existente **4.81.0 no se actualiza**; se añade checksum local de plataforma. UC fija proveedor Databricks 1.95.0 con locks Linux/Windows.

El workspace usa networking administrado de Azure Databricks con acceso público autenticado; no crea VNet propia, endpoints privados, pools, SQL warehouses ni clusters interactivos. **Decisión de costo del MVP:** `no_public_ip=false`, nodos efímeros con IP pública y NSGs administrados por Databricks, sin solicitar el NAT persistente de Secure Cluster Connectivity. No abrir SSH/RDP ni reglas adicionales. Esto no es un diseño privado/SCC de producción: una organización que exija SCC debe revisar esa opción y sus costos de red antes de aplicar. Los recursos de red que Azure Databricks cree en su managed resource group son administrados por el servicio, no se promete coste cero en reposo.

## Seguridad efectiva

- MI del Access Connector: **Storage Blob Data Reader solo en `bronze`**; Data Contributor solo en los contenedores existentes `silver` y `audit`. No hay rol a nivel cuenta/subscripción ni modificación de grants de ADF.
- Tres storage credentials con esa MI; la de Bronze y su external location son `read_only=true`. UC no concede al Job uso directo de credenciales/locations ni creación de volúmenes.
- Catálogo aislado `admm_dev` con managed storage en `silver/admm-silver/managed`; esquema `silver` para destinos/control/cuarentena, esquema `inputs` para los volúmenes externos.
- Volumen `inputs.bronze`: raíz exacta del filesystem Bronze. Volumen `inputs.receipts`: `audit/admm-silver-receipts`, separado de otros registros existentes. El operador es propietario; el Job recibe **READ_VOLUME** en ambos, sin WRITE_VOLUME. Solo ese operador publica recibos mediante UC; administradores UC/Azure de confianza pueden sobrepasar esta frontera, como en cualquier plataforma administrada. Revisar sus roles y proteger sus credenciales.
- Principal Databricks nuevo y sin secreto/token emitido por Terraform, asignado USER al workspace. Tiene USE_CATALOG, USE_SCHEMA y CREATE_TABLE/SELECT/MODIFY únicamente en Silver; sin CREATE_SCHEMA ni privilegios metastore. El operador puede usarlo como RUN AS mediante un rule set creado exclusivamente sobre este principal nuevo.
- Las tablas creadas por el Job son de su propiedad; no compartir su identidad ni otorgar credenciales OAuth a consumidores. Restringir acceso a cuarentena/PII antes de ampliar lectores.
- Los `databricks_grant` se limitan a principal/objeto propios. El rule set del SP y permisos de política son autoritativos **solo sobre objetos nuevos de este proyecto**. No se adopta ni reemplaza un catálogo, location, credencial o volumen existente que colisione: el plan/API se detiene; un administrador debe resolver el conflicto explícitamente.

## Dependencias administrativas (no se presuponen)

1. **Azure:** sesión Azure CLI del operador en la suscripción correcta. Permisos para crear workspace/managed RG y Access Connector; normalmente Contributor con alcance apropiado y capacidad de crear RG administrado. Además Owner o RBAC Administrator/User Access Administrator con capacidad `roleAssignments/write` en los tres contenedores. Blob Data Contributor sobre el contenedor del backend para locks/estado. No se usan account keys.
2. **Cuenta Databricks:** cuenta Azure Databricks del tenant correcto; perfil CLI OAuth U2M, por defecto `admm-account`, host `https://accounts.azuredatabricks.net`, Account ID (no tenant/subscription). El administrador de cuenta debe permitir listar/crear SP, asignar usuarios/SP al workspace, leer/asignar el metastore y gestionar el rol servicePrincipal.user. Azure Owner **no equivale** a administrador de cuenta Databricks.
3. **Unity Catalog:** el operador necesita autoridad para crear storage credentials, external locations y catálogo en el metastore seleccionado (metastore admin o privilegios delegados suficientes). Ser account admin tampoco demuestra automáticamente todos esos permisos de UC. El código no concede privilegios amplios sobre un metastore compartido para esquivar esta dependencia.
4. **Regional:** se enumeran metastores de cuenta, se elige East US y se comprueba la asignación del workspace. Si hay uno regional se reutiliza incluso cuando se permite creación. Si hay varios/ambigüedad se exige ID explícito. Sin ninguno, se bloquea salvo autorización de proponer creación; el plan UC todavía necesita aprobación. Se vuelve a descubrir tras crear el workspace, por si Azure lo habilitó automáticamente. Una asignación automática existente no se toma en gestión ni se sustituye. Un metastore creado por este proyecto sigue protegido como objeto regional compartible.
5. **Microsoft.Databricks RP:** se comprueba su registro. No hay registro implícito durante `plan` (`resource_provider_registrations=none`). Si falta, aparece como recurso explícito en el plan y depende de su aprobación; no se desregistra si posteriormente lo reutilizan otros workspaces.
6. **Capacidad:** disponibilidad de DBR `15.4.x-scala2.12`, VM `Standard_D4ds_v5` y cuota Azure de vCPU regional/familia. Se verifican tipos/runtime por API tras crear el workspace; que el tipo exista no garantiza cuota/capacidad instantánea. Un fallo debe investigarse sin iniciar ADF ni cambiar Bronze. Los grants Azure RBAC pueden tardar minutos en propagarse al data plane; una validación de external location puede fallar transitoriamente tras el primer apply. Conservar estado y reintentar el procedimiento tras comprobar propagación, sin recrear recursos ni omitir validaciones.

Si faltan cuenta, OAuth o privilegios administrativos, las etapas UC/Job se detienen; el bootstrap Azure independiente puede planificarse con una sesión ARM autorizada. No existe un aprovisionamiento UC universal desde una cuenta Azure Contributor. Los administradores deben preparar ese acceso una sola vez; esta entrega no lo pudo verificar en vivo.

## Un único procedimiento del operador

Preparación única: integrar los PR revisados #18 y el de infraestructura; Python **3.11+**, Terraform **1.9+**, Azure CLI, Databricks CLI **0.285.0+** (necesario para driver flexible node types), `winpty` de Git for Windows y herramientas de construcción wheel. Instalar `automation/silver/requirements.txt` y las dependencias existentes de `adf/tools/requirements-commit.txt`; ODBC Driver 18 y el SQL Server local deben estar disponibles. El cliente local no necesita ejecutar Spark ni disponer de un cluster existente.

Preparar Azure CLI para la etapa workspace; OAuth de cuenta solo es requisito para `--stage all`; el perfil de cuenta debe corresponder al administrador autorizado. No pasar PATs ni contraseñas a flags/variables Bash. El launcher rechaza DATABRICKS_TOKEN en el entorno. El acceso workspace usa Azure CLI con ARM resource ID, sin escribir tokens en Terraform ni construir un `.databrickscfg` con secretos.

Desde la raíz del repositorio en **Windows/Git Bash**:

```bash
winpty python automation/silver/launch.py --stage all --config work/silver-dev.json --deploy
```

En `--stage all`, si faltan, solicita juntos Account ID, nombre del perfil OAuth, UPN del operador y si se permite **proponer** un metastore nuevo cuando no exista. Guarda solo valores no secretos en `work/silver-dev.json`, ignorado por Git. También puede prepararse a partir de `config.example.json`. Opciones como `metastore_id`/`node_type_id` se revisan en ese archivo; no se inventan IDs. Las llamadas nativas se hacen desde Python con argv, sin interpolación de shell ni conversión MSYS de los paths `/api` y `/Volumes`.

El mismo proceso:

1. Comprueba tenant/suscripción y RP, sin exigir acceso de cuenta para la etapa Azure; valida que el estado original exista y apunte al RG/ADLS indicados. Nunca bootstrappea una plataforma nueva si falta el estado.
2. Inicializa/valida el root existente y guarda su plan binario. Genera un archivo legible y resumen de cambios. Rechaza **todo destroy/replace**, importación implícita, cambios de recursos protegidos y privilegios no permitidos. Solicita `APPLY-PLATFORM <sha256>` después de indicar el archivo revisable. Aplica **ese mismo plan**, comprobando nuevamente su SHA256; sin aprobación no aplica.
3. Con el workspace disponible descubre nuevamente metastore/asignación, valida identidad del operador, planifica UC en su clave separada y solicita `APPLY-UC <sha256>`. Mantiene las decisiones leyendo los recursos del estado en reintentos (también tras un apply parcial sin outputs calculados), para no proponer borrar una asignación/registro/metastore que creó previamente. Bloquea un SP homónimo no perteneciente a ese estado en vez de duplicarlo.
4. Verifica metastore, tipos de nodo/runtime y grants de entrada. El propio Job comprobará paths/contenido/UC bajo RUN AS, no solo bajo el operador.
5. Copia el bundle #18 a un staging ignorado, cambia su cómputo existente por `job_clusters` efímeros con política y RUN AS. No edita el bundle fuente ni crea un cluster permanente para satisfacer `existing_cluster_id`. No desplegar en paralelo el bundle fuente por otra vía: debe haber un único Job escritor por namespace. Se bloquea un Job homónimo que no coincida con el ID de despliegue conservado; si se perdió `work/`, recuperar/revisar el estado antes de crear otro, sin adopción silenciosa. Si ese Job tiene un run activo, se bloquea otro lanzamiento para evitar ejecuciones en cola/costo adicional; cancelar o esperar, sin reenviar ADF. Construye una wheel con versión derivada del contenido y SOURCE_DATE_EPOCH del commit, valida el bundle real y congela/verifica su artefacto. La aprobación cubre también el notebook; no se recompila otro payload detrás de la aprobación. Solicita `DEPLOY-RUN-SILVER <sha256>` sobre la configuración/artefacto revisables.
6. Invoca el exportador SQL real de #18 con **entrada oculta** y solo SELECT sobre los recibos COMMITTED; no ejecuta procedimientos de commit. Valida el contrato/hash del JSON antes de subirlo. No hay contraseñas en argv, shell ni Git. Para el escenario local usa la excepción explícita de certificado solo en loopback, igual que #18. Reexporta en cada intento; nunca inventa manifiestos. SQL necesita SELECT de table_config, bronze_run_audit y bronze_parent_audit; puede configurarse una identidad SQL solo lectura.
7. Publica el JSON en el volumen protegido sin sobrescribir. Si ya existe, descarga y exige bytes idénticos antes de reutilizarlo; una diferencia bloquea para investigación. Despliega el único Job; concede CAN_READ al runner solamente en su carpeta propia de notebook/wheel (no en el home del operador), y lo ejecuta una vez, sin schedule/retries automáticos. Timeout 2 horas, una ejecución simultánea, sin workers adicionales. El cómputo se termina al finalizar/cancelar.
8. Obtiene el resultado terminal y salida del task. Solo acepta SUCCESS + once fuentes exactas + cero rechazos + `replay_verified=true` y métricas completas. El replay lo ejecuta el notebook de #18 sin nuevas escrituras Delta. Genera `work/silver-deployment/acceptance.json` con RunId, métricas/versiones/digest; **solo después de evidencia real**. No simula aceptación si falla una API, tarea o permiso.

Omitir `--deploy` genera planes y revisiones, **sin applies ni deploy/run**. La etapa predeterminada es `workspace`: no requiere cuenta/CLI Databricks ni ejecuta UC/SQL/Job. Para continuar todo el flujo se debe elegir explícitamente `--stage all`. Si el workspace no existe, el plan UC queda explícitamente diferido: no puede verificarse contra un proveedor cuyo workspace todavía no existe. Incluso en modo plan se requieren acceso real y estado; esta entrega solo ejecutó validación/mocks sin backend.

No se graban tokens/contraseñas. Los archivos de plan/estado/metadatos y recibos son privados en `work/`, fuera de Git y de artifacts CI. Proteger la carpeta con ACL del usuario en Windows (chmod no sustituye ACL NTFS). Los errores de comandos se resumen sin volcar mensajes crudos que puedan contener secretos; usar la etapa, códigos y las revisiones privadas para investigar. No publicar salidas completas de providers/SDK sin revisar.

## Costos y parada

- **Ningún cluster siempre encendido:** solo Job compute Dedicated/SINGLE_USER asignado al SP, DBR 15.4 LTS, driver único `Standard_D4ds_v5` de 4 vCPU/16 GiB, cero workers, On Demand. El SKU alternativo permitido es Standard_D4as_v5 (también cuatro vCPU); ampliar tamaños requiere revisar la política. No se usan Spot para evitar fallos de preemption durante la aceptación.
- Se factura VM + DBU Premium Jobs por tiempo activo, incluido arranque; timeout 2h limita una ejecución, **no es un presupuesto monetario**. El doble chequeo de replay retorna el recibo; no inicia otro cluster. Consultar precios actuales de East US en [Azure Databricks](https://azure.microsoft.com/pricing/details/databricks/) y presupuesto/alertas del tenant antes de aprobar. No se incluyen cifras inventadas ni recursos de presupuesto adicionales.
- ADLS Silver/Delta, cuarentena y auditoría generan almacenamiento/transacciones. Managed RG puede mantener discos, IP/NAT/red según configuración predeterminada de Azure Databricks: revisar Cost Analysis después del apply y tras cada ejecución, incluso sin DBU activo. Workspace Premium por sí solo no garantiza costo cero en reposo de sus recursos asociados.
- Cancelar solamente el último run registrado, desde el mismo procedimiento: `winpty python automation/silver/launch.py --config work/silver-dev.json --stop`. Usa su RunId guardado, sin detener clusters ajenos. Ctrl+C sobre el launcher no demuestra que el Job se haya cancelado; usar --stop y comprobar TERMINATED en UI/API. Los Jobs suelen terminar su cluster automáticamente al concluir; verificarlo tras errores.
- No hay limpieza destructiva automática. No ejecutar `terraform destroy` del root de plataforma ni borrar el managed RG, storage o backend. Los objetos compartidos/regionales y destinos están protegidos con prevent_destroy. Para retirar definitivamente este MVP: aprobar una limpieza específica, cancelar ejecuciones, revisar/retener evidencia/Delta, retirar solo el Job y recursos propios en orden UC→Azure, revisando cada plan. **Conservar siempre los recursos existentes, el metastore regional reutilizado, ADF, Bronze y ambos estados**; retirar un prevent_destroy exige revisión explícita, nunca state rm para ocultar dependencias.

## Pruebas y alcance de evidencia

CI valida sintaxis/provider schemas con `init -backend=false`, `validate`, `fmt` de los roots afectados y **planes mock** `terraform test` (command=plan). No inyecta credenciales, aplica, conecta a estados reales o despliega Jobs. Las pruebas Python verifican guardas de planes, RBAC/UC, descubrimiento, aprobación por hash, no-apply por defecto, configuración y aceptación. Fixture `tests/fixtures/pr18-bundle.yml` copia únicamente la configuración pública de #18, commit e66d923, para comprobar compatibilidad; no incluye transformaciones ni recibos reales.

Localmente el overlay también se comprobó contra el JSON Schema oficial Databricks CLI 0.275.0. Eso no demuestra que el runtime, cuota, IAM/UC, exportación SQL o Jobs funcionen en Azure.

**Limitación de entrega:** no hay credenciales Azure/Databricks ni acceso al backend real en este entorno. No se generó un plan real contra `dev/platform.tfstate`; no puede certificarse ausencia de drift real. Sí se comprobó que el backend/direcciones existentes permanecen intactos y que el launcher rechaza planes destructivos/protegidos antes de aplicar. La comprobación final de cero destrucciones/reemplazos ocurre en los planes reales del operador. No se ejecutaron apply, ADF, R2, despliegue ni aceptación Azure.

Resultado de la entrega de infraestructura original: **38 pruebas Python y 8 casos Terraform con proveedores mock aprobados**; además 83 regresiones offline Bronze (63 pytest y 20 ADF). Las 18 integraciones SQL se omitieron en el run normal. Ninguna prueba representa una ejecución real de Azure/Databricks.


## Windows/OneDrive: preparar el wheel sin borrar `dist`

El launcher ya no ejecuta `rmtree(bundle/dist)` ni renombra esa carpeta. Cada intento construye desde una copia limpia de `databricks/silver` en el temporal local del sistema, excluyendo `build`, `dist`, egg-info y cachés. El build utiliza el mismo intérprete Python del launcher, una versión derivada del contenido de `src` y `pyproject.toml`, y `SOURCE_DATE_EPOCH` del commit. La reproducibilidad se comprueba con dos builds locales reales sobre los mismos inputs, epoch y herramientas resueltas; no implica igualdad entre distintas versiones de setuptools/wheel.

Solo se acepta exactamente un wheel en el directorio de salida nuevo. Si el build falla, no produce wheel o produce varios, el flujo se bloquea: nunca recurre a un wheel anterior. Se copia el resultado a `work/silver-deployment/bundle/dist/wheel-<id>/<nombre>.whl`, con creación exclusiva y comprobación SHA-256 de la copia. No se borra ni se sobrescribe el contenido anterior de `dist`; tampoco se modifica la identidad persistente `.databricks` del bundle.

En el bundle generado, tanto `artifacts.silver_wheel.files[].source` como `tasks[].libraries[].whl` apuntan al **mismo archivo exacto**, sin `dist/*.whl`. El fingerprint de aprobación incluye ese wheel, el YAML, el verificador y los notebooks. Los wheels antiguos quedan fuera de la selección y del fingerprint. Dos builds con iguales inputs/epoch/herramientas producen el mismo SHA del wheel; la ruta nueva por intento puede cambiar el fingerprint del bundle y exige su aprobación actual. `dynamic_version=false` impide que DAB cambie el artefacto aprobado. El build de DAB solo ejecuta `verify_artifact.py`: comprueba el SHA fijado incluso bajo Python `-O`; no reconstruye ni escoge otro wheel.

La limpieza del temporal de compilación es opcional. Si Windows/antivirus impide eliminarlo se informa `BUILD_TEMP_CLEANUP_DEFERRED` y se conserva el wheel ya copiado/verificado. Este manejo no ignora fallos del build, copia o verificación y no cambia atributos, permisos ni configuración de OneDrive. El temporal usa la ubicación del sistema (`tempfile`); si se personalizó TEMP/TMP, debe ser una ubicación local apropiada para compilaciones.

Después de integrar la corrección y **solo con autorización para aplicar/desplegar/ejecutar**, desde Git Bash:

```bash
winpty python automation/silver/launch.py --stage silver --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json --deploy
```

No es necesario borrar ni renombrar `dist` antes de repetirlo. Se mantienen los controles Terraform, la delegación RUN AS, la exportación protegida del recibo real y la aprobación separada `DEPLOY-RUN-SILVER`. Sin `--deploy` solo se planifica; no se construye ni ejecuta Silver. La validación local usa mocks de Windows/Job y builds reales de wheel sin recursos Azure; no representa un despliegue real ni certifica la ejecución de las once fuentes.

Validación de esta corrección: **147 pruebas locales aprobadas**, incluyendo WinError 5 simulado, selección frente a wheels obsoletos, fallos de build, verificación SHA con Python `-O` y dos compilaciones reales reproducibles. Bundle generado contrastado con JSON Schema oficial de Databricks CLI 0.275.0. `compileall` y `git diff --check` aprobados; sin prueba de despliegue real en Windows/Databricks.

### Notebook synchronization and RUN AS preflight

The launcher stages the bundle under Git-ignored `work/`. DAB honors Git ignore
rules for normal synchronization even though bundle validation resolves a local
notebook path into a remote Job path. The source bundle and staging overlay now
explicitly include `notebooks/run_silver.py` in `sync.include` (DAB includes are
added independently of Git ignore rules). Keep the task path relative:
`./notebooks/run_silver.py`, source `WORKSPACE`. DAB uploads the source notebook,
recognizes its `# Databricks notebook source` header, and resolves the Job path to
`${workspace.file_path}/notebooks/run_silver` without the `.py` extension. Do not
hardcode a remote notebook path or manually import the notebook.

After an approved deployment, before `run-now`, the launcher reads the actual
Job settings, checks the resolved notebook and wheel paths under this bundle,
and verifies their Workspace object types and effective runner ACLs. Missing
objects, unexpected paths or absent read permission block execution. The runner
receives only `CAN_READ` on the `files` and `artifacts` directories, inherited by
the notebook/wheel; deployment state is not granted. Existing inherited grants
are not removed. UC data permissions and RUN AS delegation checks remain intact.

These are local regression checks, not evidence of an actual Databricks upload
or successful Silver execution. Merge the fix and repeat the existing launcher
procedure with its hash approvals; a real deployment/Job still requires operator
authorization. No manual notebook upload or new infrastructure is needed.

### Capacidad del clúster Silver

El launcher usa ahora alternativas compatibles para el único nodo driver, con
verificaciones de catálogo del workspace, restricciones East US, cuotas y política
existente. Véase [procedimiento y límites de capacidad](COMPUTE-CAPACITY.md).
CLI 0.275.0 ya no basta para esta ruta; se requiere 0.285.0+. Las comprobaciones
no garantizan disponibilidad física y no se realizan reintentos automáticos del Job.

Para investigar el bloqueo de capacidad **sin desplegar ni ejecutar compute**, use
el [procedimiento único de solo lectura del issue #30](ISSUE-30.md). El informe
comparte la selección real del launcher y explica cada candidato y cuota.
