# Unity Catalog existente y Silver con el perfil de workspace

Esta es la guía vigente para continuar **después de fusionar este PR**. No se necesita habilitar Account Console para planificar/aprovisionar los objetos UC con los permisos ya delegados al workspace. La configuración y ejecución del Job conserva requisitos de identidad independientes; pertenecer a `admins` no los demuestra.

Estado comunicado por el operador: workspace `dbw-admm-dev-f6d99ba4`, ID `7405617270738925`; metastore asignado `b67792ab-c53b-48b4-ae57-fc45b869ed7a`; perfil autenticado `admm-workspace`. El grupo `_workspace_admins_dbw_admm_dev_f6d99ba4_7405617270738925` tiene CREATE_CATALOG, CREATE_STORAGE_CREDENTIAL y CREATE_EXTERNAL_LOCATION. Esta entrega no consultó Azure/Databricks ni confirmó esos permisos en vivo.

## Qué requiere cada operación

| Operación | Permisos/contexto | Ruta nueva |
| --- | --- | --- |
| Leer asignación actual y usuario del workspace | Sesión válida del workspace | GET de `current-metastore-assignment` y SCIM `/Me`; compara Workspace ID, metastore y operador configurado. |
| Crear catálogo con managed storage, credenciales y external locations | Privilegios UC delegados indicados; capacidad de usar el Access Connector y permisos Azure de su MI ya configurados | Provider **solo workspace**, perfil OAuth `admm-workspace`. Valida acceso al almacenamiento; no omite `skip_validation`. |
| Crear esquemas/volúmenes/grants propios | Propiedad/permisos UC correspondientes sobre los objetos del proyecto | El operador crea y posee objetos; el runner recibe únicamente lectura en entradas. No se conceden privilegios globales. |
| Crear o reasignar un metastore, descubrir todos los metastores de la cuenta | Administración de cuenta y permisos sobre el metastore según la operación | Fuera de la ruta nueva. Si la asignación no coincide se bloquea; el administrador autorizado debe revisarla. |
| Crear identidades de cuenta o asignarlas mediante las APIs MWS de cuenta | Privilegios administrativos/delegados sobre identidades/asignaciones; no se deducen de `admins` | Se omiten completamente: se reutiliza un principal ya existente y asignado al workspace. Crear un SP no exige universalmente Account Admin: puede admitirse desde el workspace según la federación/políticas; esta ruta no lo crea ni presupone esa capacidad. |
| Dar al operador `roles/servicePrincipal.user` sobre un SP | SP Manager o administrador autorizado para delegar ese rol | **No exige universalmente Account Admin.** No lo concede automáticamente el launcher. Manager, Owner Azure o workspace admin no sustituyen el rol User. |
| Leer/probar la delegación RUN AS | Acceso de lectura al rule set del SP a través del workspace | Usa Account Access Control **Proxy** en el host del workspace, con el mismo perfil. El nombre del rule set necesita un **Account ID no secreto**, pero no login ni rol Account Admin. |
| Crear política y Job efímero y ejecutarlo como SP | Permisos de workspace/cómputo, política, RUN AS y UC del SP | Solo después de validar el principal y la delegación. No hay fallback a un usuario personal. |

Los tres privilegios CREATE_* permiten preparar UC, pero no demuestran acceso de la MI a ADLS, disponibilidad de cómputo ni RUN AS. Los fallos de validación de locations se investigan conservando estado y protecciones; no se arreglan deshabilitando validaciones. La lectura del backend Terraform sigue requiriendo la sesión Azure CLI correcta y permisos de datos al backend existente; el perfil Databricks no sustituye Azure CLI.

## Diseño y estado Terraform

`terraform/databricks/workspace` es una configuración **alternativa** de workspace, sin proveedor account, recursos Azure, metastore, asignación, creación de usuarios/SP ni cambios de entitlements. Reutiliza las definiciones/contrato UC de la configuración histórica, conservando las direcciones de catálogo, esquemas, volúmenes, credenciales y locations. La separación de root evita inicializar o autenticar accidentalmente el provider de cuenta.

Usa exactamente el backend y clave UC existentes, **`dev/databricks-uc.tfstate`**. No crea otro estado para duplicar objetos ni modifica `dev/platform.tfstate`. Las dos configuraciones `dev` y `workspace` son mutuamente excluyentes: no ejecutarlas concurrentemente ni alternarlas para saltarse un fallo. El launcher bloquea la ruta workspace si el estado contiene identidades/asignaciones/metastore o recursos de runner administrados por la ruta anterior. En ese caso hace falta una migración explícita revisada; **no** borrar estado, cambiar backend, hacer `state rm` o imports improvisados. También se bloquea una transferencia implícita si los objetos del estado tienen otro propietario. Un catálogo/location homónimo fuera del estado no se adopta: la operación debe fallar para investigación.

Sin `runner_application_id`, se planifican solo los once objetos UC base (tres credenciales, tres locations, catálogo, dos esquemas, dos volúmenes). No se crea política, cluster, identidad ni permisos del runner. Con un SP existente se añaden cinco grants, política y permisos de política sobre recursos propios. Nunca se modifica la identidad ni sus entitlements. Las políticas y grants opcionales usan direcciones indexadas, protegidas por la revisión del plan; quitar el runner después de gestionarlos propone eliminaciones y queda bloqueado.

El modo predeterminado para UC es `workspace`. La ruta histórica sigue disponible **solo por elección explícita** `--uc-mode account`; mantiene sus dependencias administrativas originales. No se obtiene Account ID ni se solicita UPN para planificar UC en modo workspace: se lee `/Me`; si ya hay un UPN en la config debe coincidir. Las opciones de entorno existentes se conservan; no se sobrescribe el archivo con una plantilla. El perfil se propaga a CLI, Terraform, bundle y cancelación del run; se eliminan overrides ambientales Databricks que pudieran seleccionar otra cuenta. PATs siguen rechazados.

Los planes permanecen guardados en `work/silver-deployment`; la etapa nueva se llama `uc-workspace`. Se conservan revisión legible, SHA256, aprobación exacta `APPLY-UC-WORKSPACE <sha256>` y nueva comprobación del plan antes de aplicar. El allowlist específico rechaza recursos de cuenta/Azure, destrucciones, reemplazos, imports incluso no-op y escritura en entradas. Bronze mantiene credential/location read-only y los grants del SP son READ_VOLUME; Azure RBAC no se modifica.

## Comandos exactos tras fusionar

Desde la raíz del repositorio, Windows/Git Bash, con las dependencias existentes instaladas y las sesiones Azure CLI/perfil `admm-workspace` válidas. No hace falta reloguear ni configurar Account Console si la sesión ya funciona. No usar PAT, contraseñas en flags ni `--debug`.

**1. Plan UC reutilizando el workspace y metastore desplegados; sin apply ni Jobs:**

```bash
python automation/silver/launch.py --stage uc --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json
```

Este comando sustituye el intento inicial con `--stage all` para aislar UC. `uc` y `silver` solo leen los outputs/estado de plataforma; **no planifican ni aplican Azure**. Workspace URL, Access Connector y cuenta de almacenamiento se obtienen de los outputs existentes, sin inventar IDs. Se verifica que el perfil atienda al mismo Workspace ID y metastore. Si falta el output `silver_workspace`, se bloquea; revisar el estado/despliegue original, sin crear otro workspace.

Si `allow_create_metastore=true` quedó de una configuración anterior, establecerlo en `false` en el JSON privado: workspace mode nunca crea ni reasigna. Si el UPN guardado corresponde a otra sesión, el launcher bloquea la discrepancia; revisar la identidad autorizada y corregir explícitamente el UPN (o dejarlo `null` para descubrir el usuario actual). Eso no cambia la identidad del Job.

**2. Solo con autorización para aplicar los objetos UC revisados:**

```bash
winpty python automation/silver/launch.py --stage uc --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json --deploy
```

Revisar el plan legible indicado y aprobar el hash exacto. Sin runner configurado termina con `UC_FOUNDATION_CONFIGURED` y **Silver bloqueado**, sin exportar recibos, desplegar ni ejecutar Jobs. Con runner, verifica grants/runtime/nodo y termina sin ejecutar el Job; no declara comprobada la delegación RUN AS. Este comando no se ejecutó durante la implementación del PR.

**3. Preparar una sola vez la identidad existente del Job y verificar su plan:**

El operador/SP Manager autorizado debe proporcionar en `work/silver-dev.json`:

```json
{
  "runner_application_id": "UUID_REAL_DEL_SP_EXISTENTE_ASIGNADO_AL_WORKSPACE",
  "account_id": "UUID_REAL_DE_LA_CUENTA_DATABRICKS"
}
```

Añadir estas claves conservando el resto del archivo; los placeholders no son valores ejecutables. El SP debe estar activo, tener `workspace-access`, y el operador debe tener una concesión **directa** `roles/servicePrincipal.user` sobre ese SP. No se aceptan un UPN como runner, un grupo `admins` ni el rol Manager como sustitutos. La prueba automatizada es conservadora: no expande grants de grupos de cuenta; si solo existe delegación por grupo, exige revisión/delegación directa por su administrador antes de ejecutar. No se declara ausente un permiso global: simplemente no queda probado por esta ruta.

El Account ID se necesita aquí únicamente para nombrar y leer el rule set vía proxy; no desbloquea Account Console ni convierte al usuario en Account Admin. Si no se conoce o el GET está denegado, **la ejecución Silver se bloquea con un mensaje específico**. Pedir al SP Manager/administrador el dato y la delegación/lectura necesaria; no elevar al operador a Account Admin como solución automática. Si no existe un SP asignado, resolver esa provisión/asignación mediante la administración autorizada correspondiente. La ruta nueva no inventa ni crea uno.

```bash
python automation/silver/launch.py --stage silver --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json
```

Sin `--deploy`, este paso planifica la configuración UC/política para el SP existente; no ejecuta Job ni afirma que RUN AS esté probado. La comprobación de roles del SP se exige antes de cualquier despliegue/ejecución Silver.

**4. Solo después de revisar el plan y autorizar despliegue/cómputo:**

```bash
winpty python automation/silver/launch.py --stage silver --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json --deploy
```

Antes de aplicar verifica el SP y el rule set por el perfil workspace. Conserva las aprobaciones por hash de UC y del bundle, la exportación SQL real protegida del recibo COMMITTED, upload sin sobrescritura, Job único DBR 15.4 efímero, once métricas y replay idempotente. La contraseña SQL sigue usando el mecanismo oculto del exportador existente; no compartirla ni escribirla en config. Solo evidencia real satisfactoria produce `acceptance.json`. No se ejecutan ADF ni R2, ni se actualizan watermarks Bronze. Costos/cancelación y detalles del flujo siguen en [README](README.md); la selección de ruta/etapas de este documento prevalece sobre instrucciones históricas que exigen OAuth de cuenta desde el principio.

## Bloqueos que no se eliminan

- Asignación ausente/incorrecta: debe revisarla quien tenga autoridad para administrar la asignación del metastore; no se crea/reasigna automáticamente.
- Falta de permisos CREATE_*, uso del Access Connector, RBAC de su MI, conflicto de nombres/rutas o backend: corregir el permiso/objeto exacto por la administración autorizada, sin duplicar recursos o omitir validación.
- Estado con objetos de la ruta administrativa: migración revisada antes de cambiar configuración; el plan no puede eliminarlos/reemplazarlos.
- SP inexistente/inactivo/no asignado o RUN AS no probado: UC puede prepararse sin él, **Silver no se ejecuta**. Workspace admin no equivale a SP User.
- Account ID desconocido o lectura de rule set denegada: bloquea la prueba del Job, no el plan/aprovisionamiento UC base. Puede resolverlo un SP Manager/delegado; Account Admin solo si la operación/política concreta lo requiere.

## Evidencia y referencias

Validación local agregada: **230 pruebas Python aprobadas** (115 de automatización, 83 Bronze y 32 Silver/Spark/Delta), con **18 integraciones SQL omitidas**; `fmt`/`validate` en los cuatro roots y **12 casos Terraform de planes mock** aprobados. `compileall` y `git diff --check` aprobados. Las pruebas son locales y los planes Terraform usan proveedores simulados. No se accedió al backend ni al workspace real; no hay credenciales Azure/Databricks configuradas en este entorno. No se hizo apply, creación Azure, exportación SQL, Job, modificación Bronze ni R2. Los resultados locales/CI constan en el PR; no representan aceptación real UC/Silver.

Referencias revisadas: [provider 1.95: data service principal en workspace](https://github.com/databricks/terraform-provider-databricks/blob/v1.95.0/docs/data-sources/service_principal.md), [roles de service principal](https://learn.microsoft.com/azure/databricks/admin/users-groups/service-principal-acl), [Account Access Control Proxy — SDK oficial](https://github.com/databricks/databricks-sdk-go/blob/v0.111.0/service/iam/impl.go), [formato de rule sets y principales](https://github.com/databricks/terraform-provider-databricks/blob/v1.95.0/docs/resources/access_control_rule_set.md), [privilegios UC](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/manage-privileges/privileges).
