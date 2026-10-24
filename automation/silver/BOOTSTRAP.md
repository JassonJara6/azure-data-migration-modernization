# Desbloqueo de identidad y primera ejecución Silver

**Actualización:** para el workspace/metastore ya existentes, continuar con [Unity Catalog mediante `admm-workspace`](UNITY-CATALOG-WORKSPACE.md). No es necesario resolver Account Console para aprovisionar los objetos UC con los permisos delegados. La ruta de cuenta de esta guía es histórica y ahora requiere `--uc-mode account` explícito; RUN AS del Job sigue teniendo controles independientes.

Guía de ejecución vigente. Silver y su infraestructura ya están fusionados en `main`. Este cambio separa diagnóstico, bootstrap Azure y configuración Databricks; no ejecuta despliegues. El parent Bronze comunicado por el operador sigue siendo `20d65511-8190-41a0-8ad2-23d7568363d2`, COMMITTED con once cargas. R2 permanece pendiente y **no se ejecuta como parte de Silver**.

## Diagnóstico técnico y límites

Microsoft define **AADSTS530035 = BlockedBySecurityDefaults**: una solicitud bloqueada por Security Defaults, por autenticación heredada o por considerarse insegura. No es el código genérico de Conditional Access (`53003`) ni un error de RBAC de suscripción. El código por sí solo no identifica el factor concreto del usuario: revisar el evento fallido con hora UTC, correlation ID y error code. Que Azure CLI sea moderno no prueba que el intento concreto haya satisfecho las exigencias de autenticación/MFA. No desactivar Security Defaults, omitir MFA, cambiar políticas o usar contraseñas de aplicación para sortearlo.

| Síntoma | Comprobación y conclusión permitida |
| --- | --- |
| Org user no ve la suscripción | Comparar tenant de la suscripción y sesión; directorio/filtro de suscripciones del Portal; objeto exacto al que se asignó Owner, asignación activa/PIM y alcance. Un UPN similar, una asignación en otro tenant o un rol elegible no demuestran acceso efectivo. Reautenticar después de cambios ya autorizados y considerar propagación. |
| CLI falla con 530035 | Un administrador Entra revisa Sign-in logs del **intento** (usuario, aplicación Azure CLI, recurso, cliente, dispositivo, resultado MFA, riesgo y Security Defaults). Revisar registro de métodos MFA y completar el flujo interactivo admitido. No atribuir una causa específica sin ese evento. |
| Cuenta personal funciona en ARM y falla en Graph | Usar Azure CLI/ARM sin `az ad signed-in-user show`; el diagnóstico no depende de `/me`. La consulta opcional de Graph puede fallar sin impedir el bootstrap Azure. |
| Account Console falla | Verificar acceso de la identidad organizacional al tenant, cuenta Azure Databricks correcta y rol de cuenta. Distinguir error Entra, 401/403 y fallo de DNS/TLS/proxy/navegador. Owner Azure no demuestra account admin Databricks ni metastore admin. Un error de conexión no prueba ausencia de cuenta/metastore. |
| Workspace aún no existe | Sí se puede provisionar primero mediante AzureRM/ARM, sin acceder a Account Console ni inicializar el provider Databricks. **Eso no corrige 530035** ni garantiza acceso posterior a UC. |

Un usuario Azure Owner no es por ello administrador de Microsoft Entra. La primera habilitación/administración de la cuenta Azure Databricks puede requerir al Global Administrator del tenant para establecer el primer account admin organizacional. Confirmar la situación real con ese administrador; el launcher no concede roles de directorio ni presume ese acceso. La habilitación automática de UC depende de la cuenta, región y permisos; tras crear el workspace se vuelve a descubrir el metastore y su asignación. Nunca se crea uno regional para sortear un error de lectura.

## Un procedimiento, en tres etapas con puntos de autorización

Prerrequisitos locales: Python 3.11+, Azure CLI actualizado, Terraform 1.9+; para UC/Job, Databricks CLI moderno, dependencias `automation/silver/requirements.txt`, dependencias del exportador Bronze existente y ODBC Driver 18. PowerShell/Windows y Git Bash usan el mismo launcher Python; `winpty` solo es necesario para los prompts interactivos en Git Bash. No hace falta Spark local. No introducir contraseñas/tokens en config, argumentos ni variables Bash; no compartir caches de autenticación ni salidas `--debug`.

**1. Autenticación admitida y diagnóstico de solo lectura.** Usar una sesión Azure CLI ya válida para el tenant y la suscripción indicados. Si falta, el operador inicia una autenticación interactiva de navegador, sin `-u/-p` ni secretos. En Git Bash:

```bash
az login --tenant 2c60929f-5f02-4969-8e91-62fdac1de9d4
az account set --subscription b3d627f1-966e-4bd4-861a-e606b963875d
python automation/silver/launch.py --diagnose
```

`account set` solo selecciona contexto CLI local. No es un cambio de permisos Azure. No usar `--allow-no-subscriptions` como evidencia de acceso. El login puede funcionar con la cuenta personal ya autorizada para el bootstrap ARM; esto usa su acceso legítimo y **no arregla ni sustituye** la autenticación organizacional requerida por Databricks. No automatizar selección de otra identidad ante un bloqueo de seguridad.

El diagnóstico no inicia login, no pide credenciales, no escribe archivos, no ejecuta Terraform ni Databricks, y hace exclusivamente lecturas. Comprueba sesión, coincidencia de IDs, suscripción Enabled por ARM, RG/región, permisos Allow/NotActions en suscripción/RG/los tres contenedores, registro de Microsoft.Databricks y workspaces existentes. Muestra solo campos permitidos, nunca texto crudo de CLI. `AZURE_ACCESS_OBSERVED` demuestra la lectura ARM de la suscripción y contexto, **no que el despliegue esté autorizado o completo**. `ALLOW_ACTIONS_OBSERVED` no descarta deny assignments, condiciones RBAC, Azure Policy, locks, cuotas ni permisos data-plane sobre el backend. Un check `UNAVAILABLE` es ausencia de evidencia, no prueba de inexistencia del recurso o rol. No afirma que el usuario sea Owner a partir de permisos efectivos.

Opcionalmente, **solo para un administrador con sesión y permisos de Graph ya disponibles**, añadir `--graph --correlation-id UUID_DEL_INTENTO`. Consulta `/me`, Security Defaults y hasta cinco eventos del correlation ID, sin mensajes ni datos de autenticación. `/me` es auxiliar; `Policy.Read.All` permite leer Security Defaults y `AuditLog.Read.All` los eventos, con el consentimiento/rol/licenciamiento que exija el tenant (por ejemplo Reports Reader y Entra P1/P2 para acceso programático a sign-ins). La herramienta no solicita consentimiento ni eleva permisos. Un 403 se registra y no invalida ARM. Si hay paginación o cero eventos, no se declara causa confirmada; revisar logs con el administrador. El evento debe corresponder al tenant, usuario y hora correctos. Si ni siquiera hay sesión válida, el operador/administrador debe revisar el evento desde su sesión autorizada en Entra; no hay una llamada de CLI que quite ese requisito.

**2. Bootstrap del workspace sin Account Console.** Con diagnóstico revisado y backend existente accesible:

```bash
python automation/silver/launch.py --stage workspace --config work/silver-dev.json
```

Este es también el comportamiento por defecto sin `--stage`: solo plan Azure. Genera config no secreta sin preguntar Account ID/UPN, valida estado original y RG/ADLS, prepara el plan del módulo opt-in. No llama APIs de cuenta, UC, exportador SQL ni Job, aunque ya exista el workspace. No hace `apply`. No usa `-target`, no migra backend ni cambia direcciones de recursos Bronze. La revisión rechaza destrucciones/reemplazos, importaciones y cambios protegidos. Si falta registro de Microsoft.Databricks, solo lo **propone explícitamente** en el plan.

**No ejecutar durante esta revisión.** Cuando el responsable autorice crear recursos facturables, el mismo comando con `--deploy` permite aprobar `APPLY-PLATFORM <sha256>` sobre el plan guardado y revisado; nunca aplica por defecto. Tras ello termina con UC `NOT_CONFIGURED_OR_VERIFIED` y Silver `NOT_DEPLOYED`. No necesita Account ID ni Databricks CLI en esta etapa. Si ya se creó el workspace y falla UC más adelante, se conserva y se reanuda; no borrar recursos ni resetear estado.

Permisos para esta etapa: creación de workspace Premium y Access Connector en el RG, capacidad de crear el managed RG a nivel suscripción, y asignar RBAC en `bronze/silver/audit`; además Blob Data Contributor en el backend para leer/lockear/escribir estado cuando corresponda. Owner de administración no implica acceso de datos al backend. El runner Silver mantiene solo lectura sobre Bronze y no se cambian grants de ADF. Los mensajes de fallo incluyen etapa Terraform y códigos permitidos, sin imprimir stderr sensible. Un error genérico de transporte sigue necesitando análisis local revisado; no se considera resuelto.

**3. Cuenta/UC y primera ejecución Silver, después del bootstrap autorizado.** El administrador habilita/confirma acceso organizacional a la cuenta Databricks del mismo tenant y permisos UC adecuados. Configurar una vez OAuth U2M mediante el flujo oficial de navegador (sin PAT):

```bash
databricks auth login --host https://accounts.azuredatabricks.net --account-id ID_REAL_DE_CUENTA --profile admm-account
```

Account ID es el de Databricks, no el tenant ni la suscripción. La CLI administra su propia caché local; no copiar tokens al repositorio/launcher. El operador de workspace debe ser el UPN autorizado de la config; si cambió la sesión Azure personal→organizacional, volver a comprobar tenant/suscripción con `--diagnose`. El launcher comprueba también la identidad `/Me` del workspace antes del plan UC y bloquea discrepancias; tener acceso con la cuenta personal no prueba acceso del usuario organizacional.

Con autorización explícita para las etapas de despliegue restantes:

```bash
winpty python automation/silver/launch.py --stage all --config work/silver-dev.json --deploy
```

Reanuda el estado Azure, pide juntos solo Account ID/perfil/UPN/decisión de metastore si faltan y conserva las demás opciones. Descubre el metastore East US real y asignación; reutiliza el existente/autoasignado, bloquea ambigüedades o permisos faltantes y mantiene creación deshabilitada por defecto. Solicita aprobación separada del plan UC y del bundle por SHA. El [procedimiento de infraestructura](README.md) detalla los permisos, costos, parada y verificaciones de cada etapa.

El flujo Silver existente exporta el recibo SQL real COMMITTED con el mecanismo local de entrada oculta del exportador: no añadir credenciales a esta guía/config, no enviarlas al asistente ni automatizar su captura. La exportación solo lee auditoría y no modifica watermarks. Sube evidencia sin sobrescribir, construye wheel, despliega un Job efímero DBR 15.4 LTS, valida las once métricas y el replay idempotente. Solo genera `acceptance.json` tras resultado real satisfactorio; un workspace creado o un bundle validado no demuestra aceptación Silver. No se vuelve a ejecutar ADF ni R2. Sin autorización de despliegue, omitir `--deploy`: solo genera planes, sin crear recursos ni ejecutar el exportador/Job.

Intervenciones inevitables: autenticación/MFA en navegador; revisión administrativa del evento 530035 y acceso de cuenta si siguen bloqueados; aprobación de planes/costo; exportación SQL local mediante el mecanismo protegido ya existente. No se requieren comandos manuales por tabla ni modificar políticas para que pase la automatización.

## Evidencia de esta entrega

- Base real GitHub `main`: `58babf868c2ad48ac1a805d4b0d1a8030ffda4b8` (PR #19 fusionado). Rama independiente `feature/databricks-identity-bootstrap`.
- No hay Azure CLI ni credenciales Azure/Databricks configuradas en este entorno. Ejecución local de `--diagnose`: `BLOCKED`, `TOOL_MISSING: az`. No se pudo consultar suscripción, sign-ins, RBAC, backend ni cuenta Databricks reales.
- Resultado local: **183 pruebas Python aprobadas** (68 de automatización, 83 Bronze y 32 Silver/PySpark/Delta); **18 integraciones SQL omitidas**. `fmt`, `validate` de los tres roots y **8 casos Terraform con proveedores mock** aprobados; `compileall` y `git diff --check` aprobados. La regresión Silver se repitió satisfactoriamente con los JAR Delta fijados y ya descargados tras fallar su resolución inicial de dependencias en este sandbox. Las pruebas de identidad y Terraform usan respuestas/proveedores sintéticos y no representan evidencia cloud. No se ejecutaron `apply`, cambios de usuarios/políticas, creación de recursos, SQL, Job, ADF ni R2. No se declara resuelto 530035 ni completada Silver.

Referencias oficiales: [códigos Entra — 530035](https://learn.microsoft.com/entra/identity-platform/reference-error-codes), [Security Defaults](https://learn.microsoft.com/entra/fundamentals/security-defaults), [Graph signIns y permisos](https://learn.microsoft.com/graph/api/signin-list), [roles de cuenta Databricks](https://learn.microsoft.com/azure/databricks/admin/users-groups/roles), [crear workspace Azure](https://learn.microsoft.com/azure/databricks/admin/workspace/), [habilitar Unity Catalog](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/enable-workspaces). La definición de 530035 y las exigencias de Security Defaults se contrastaron con las fuentes públicas MicrosoftDocs/entra-docs; las causas concretas del tenant siguen sin evidencia directa.
