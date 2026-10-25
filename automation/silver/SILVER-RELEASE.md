# Cierre funcional Silver: publicar, verificar y ejecutar por separado

## Hallazgos confirmados y límites

Evidencia histórica previa a PR #36: el operador informó que el Job `210884055971910` sigue referenciando
`admm_silver-0.1.0+e86a2e0b709b-py3-none-any.whl`. El código actual de main con
PR #35 genera `0.1.0+cb70e1c8cdcb` en este checkout Linux. El identificador se
calcula sobre los bytes reales del proyecto; otro checkout con finales de línea
diferentes puede generar otro hash. No se hardcodea esa versión en el launcher.

Defectos reproducidos:

- El guard anterior admitía cualquier wheel bajo `artifacts/` con permiso de
  lectura, incluido uno antiguo, sin comparar versión ni contenido.
- Comprobaba existencia del notebook, pero no código ni parámetros desplegados.
- Actualizar artefactos estaba acoplado a validar capacidad, exportar SQL y
  arrancar un Job. Por tanto un estado de capacidad desconocido impedía incluso
  reemplazar el código antiguo sin ejecutar cómputo.
- El launcher terminaba ante un run fallido antes de recuperar la salida segura
  de la tarea, ocultando también los nuevos diagnósticos del notebook.

Correcciones:

- Build limpio y reproducible en temp, selección exacta del wheel, SHA-256,
  estado DAB persistente y protección OneDrive se conservan.
- Tras publicar, se exporta el FILE wheel como AUTO y se exige su SHA-256 local.
  Se exporta el notebook como SOURCE y se comparan AST y fronteras de celdas:
  cambios de código, celdas o MAGIC bloquean. La normalización inocua de espacios
  o cabecera del export no fuerza un falso fallo.
- El notebook exige que la distribución instalada coincida con
  `expected_package_version` antes de importar el paquete o leer datos; esa
  expectativa se toma del METADATA del wheel construido, no de archivos editables
  después del build ni del operador. Así un entorno que cargue
  otra versión falla antes de ingesta, aunque el archivo remoto sea correcto.
- Se comparan run-as, parámetros reales del notebook, defaults del Job,
  concurrencia, reintentos, timeout y ausencia de triggers contra el bundle.
- `--deploy --deploy-only` publica/verifica artefactos sin Terraform plan/apply,
  exportación SQL, escritura del receipt o `jobs run-now`. Lee outputs/estado UC
  existentes, valida identidad/configuración y conserva el cómputo del Job
  existente; no adopta Jobs ni crea uno si falta el registro local de despliegue.
  Requiere cluster DBR 15.4 SINGLE_USER, driver de 4 vCPU/16 GiB, cero workers,
  On Demand, política/runner esperados, sin autoscale/pools ni alternativas nuevas.
- Publicar sigue siendo un cambio remoto de archivos/definición del Job y ACL
  mínimas de artefactos: requiere autorización y aprobación del hash
  `DEPLOY-ARTIFACTS-SILVER`. No se publica nada automáticamente al leer esta guía.
- Un fallo de ejecución recupera metadata segura de la tarea y escribe
  `last-failure.json`; nunca imprime `error`, `error_trace`, filas o receipt.
  Fallar esa lectura secundaria no sustituye el fallo primario ni crea éxito.

No está demostrada la causa del run histórico `11937840542482` / task
`831744328908969`. Que el operador lea el receipt con `fs cat` y que el runner
posea USE_CATALOG/USE_SCHEMA/READ_VOLUME no demuestra el acceso FUSE dentro de
aquel proceso. No se cambiaron FUSE, permisos UC, Bronze ni datos para adivinar
una solución. Se conserva el lector y los códigos concretos de PR #35.

## Capacidad: UNKNOWN sigue bloqueando ejecuciones

`SCOPED_PROVIDER_STATUS_MISSING` no es evidencia de una SKU disponible.
No se sustituye por `status=[]`, no se inventan cuotas y no se vuelve a llamar
al catálogo completo que agotó su timeout. La ruta normal de ejecución conserva
la validación estricta. El modo de publicación omite **solo** esa autorización de
cómputo porque no ejecuta ninguno: su informe dice `capacity=NOT_VERIFIED`,
`execution=NOT_STARTED`. No usar la UI para saltar ese bloqueo.

No se encontró un GET documentado por nombre de VM que sustituya toda la
evidencia de restricciones ARM ausente. El listado genérico de tamaños o precios
no acredita elegibilidad de la suscripción. Si el proveedor omite la información,
hace falta evidencia autorizada del workspace/proveedor; este PR no declara
resuelta la capacidad real mediante pruebas locales.

## Un único script de recopilación de solo lectura

Con el perfil OAuth y login Azure existentes, sin tokens ni contraseñas en argv:

```bash
bash automation/silver/collect_workspace.sh --profile admm-workspace
```

El wrapper configura `MSYS_NO_PATHCONV=1`, `MSYS2_ARG_CONV_EXCL=*` y UTF-8,
resolviendo las rutas de Git Bash. El colector cachea lecturas repetidas en RAM;
no guarda payloads crudos, no lee archivos de credenciales ni solicita secretos.
Solo muestra versión esperada/observada, forma de metadata del proveedor,
resultado de capacidad, validez completa del receipt leído **como operador** y
metadata segura de la tarea histórica cuando exista. No imprime el receipt,
mensajes originales, trazas, URLs de paginación ni cuerpos de errores.
Puede leer Jobs existentes, pero nunca ejecutarlos; no llama a Terraform, SQL,
ADF, creación de clusters ni al catálogo ARM completo.

## Comandos posteriores a la fusión — no ejecutados en esta entrega

En Git Bash, actualizar sin descartar modificaciones locales:

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1 PYTHONIOENCODING=utf-8
git switch main
git pull --ff-only origin main
silver_args=(--stage silver --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --runner-application-id 7d5adada-f25c-47f2-ab76-9880b52d5490 --config work/silver-dev.json)
```

Si Git indica cambios locales/conflictos, detenerse y revisarlos; no ejecutar
reset/clean ni sobrescribir configuración/estado para forzar la actualización.

**Solo con autorización para actualizar archivos y el Job, sin arrancarlo:**

```bash
winpty python automation/silver/launch.py "${silver_args[@]}" --deploy --deploy-only
```

Revisar el hash y YAML generado antes de aprobar. Se exige el registro existente
`work/silver-deployment/job-id.json` y el estado `.databricks` del bundle. No
borrarlos ni adoptar manualmente otro Job. `release.json` solo aparece como
`ARTIFACTS_VERIFIED` después de verificar bytes/código/parámetros remotos; incluye
nombre y SHA del wheel. **No acredita instalación dentro de un cluster, ejecución
Silver, capacidad ni reconciliación de datos.** Un acceptance.json antiguo tampoco
acredita el nuevo release: comparar su bundle hash y el run correspondiente.

**Solo cuando capacidad sea ELIGIBLE_SNAPSHOT y exista autorización expresa para
un run facturable**, el AND evita entrar al launcher si el check falla:

```bash
python automation/silver/check_capacity.py --profile admm-workspace --job-id 210884055971910 && winpty python automation/silver/launch.py "${silver_args[@]}" --deploy
```

La ruta normal conserva planes UC/aprobaciones por hash y exportación SQL real
con contraseña oculta. No aprobar cambios UC inesperados. Recompila y verifica
el release antes de ejecutar; no usa a ciegas el wheel de una publicación previa.
Un snapshot no garantiza capacidad física: no repetir automáticamente runs con
stockout o cuotas denegadas. El éxito debe generar `acceptance.json` con
`status=ACCEPTED`, once métricas exactas, cero rechazos y `replay_verified=true`.
R2 Bronze/ADF no se ejecuta. Gold queda fuera de este PR por instrucción del
operador («Cerrar Silver primero»); no se declara un pipeline Gold existente.

## Fuentes del contrato de artefactos

- [Workspace export AUTO/SOURCE y workspace files](https://docs.databricks.com/api/workspace-files/v1/export)
- [Workspace files incluyen wheels](https://docs.databricks.com/aws/en/files/workspace)
- [Contrato de capacidad y límites](ISSUE-33.md)

## Evidencia histórica de PR #36 (no resultados del cambio FILE/AUTO)

- Automatización: `SILVER_WHEEL_BUILD_TESTS=1 SILVER_PROVIDER_AUTH_TESTS=1 SILVER_TERRAFORM_BIN=/workspace/work/infrastructure/bin/terraform PYTHONPATH=/workspace/work/silver/deps python -m pytest -q automation/silver/tests --tb=short` — **274 aprobadas**, sin omisiones.
- Silver: `SILVER_DELTA_JARS=/workspace/work/silver/jars/delta-spark_2.12-3.2.1.jar,/workspace/work/silver/jars/delta-storage-3.2.1.jar PYTHONPATH=/workspace/work/silver/deps:/workspace/work/silver/bronze-deps python -m pytest -q databricks/silver/tests --tb=short` — **50 aprobadas**, sin omisiones, con OSS Spark/Delta local.
- Terraform fmt/validate en cuatro roots y **12 pruebas de planes con providers mock** aprobadas, sin backend real ni apply.
- Sintaxis Bash, compileall y git diff --check aprobados.

Las regresiones cubren wheel antiguo/bytes cambiados, notebook antiguo o MAGIC,
fronteras de celdas, parámetros/runner/concurrencia alterados, publicación sin
plan/apply/SQL/fs/run-now, conservación del cluster y límites de costo,
configuración UC divergente, falla de tarea/lectura secundaria y redacción del
colector. Las compilaciones reales verifican reproducibilidad y correspondencia
del nombre del wheel con la versión calculada por el colector.
No son evidencia de instalación/ejecución Databricks real ni de FUSE/stock Azure.

### Verificar la corrección FILE/AUTO sin publicar ni ejecutar el Job

`RAW` fue rechazado por el servicio para assets `FILE`. Se usa `AUTO` en
`GET /api/2.0/workspace/export`, que incluye Workspace files. El CLI decodifica
base64 y escribe bytes con `os.WriteFile`; no se convierten a texto. SHA-256,
entradas aprobadas congeladas y exportación SOURCE del notebook se conservan.
Véase el [contrato AUTO](https://docs.databricks.com/api/workspace-files/v1/export)
y la [implementación CLI v1.22.1](https://github.com/databricks/cli/blob/v1.22.1/cmd/workspace/workspace/overrides.go).

Git Bash, después de fusionar el PR, conservando el bundle local que se publicó:

```bash
git switch main
git pull --ff-only origin main
bash automation/silver/collect_workspace.sh --profile admm-workspace
```

El único colector comprueba metadatos, receipt y artefactos seleccionados locales
contra los bytes remotos y las celdas del notebook. Solo GET/export/fs cat y
escrituras temporales locales: nunca publica ni ejecuta. `READ_ONLY_COLLECTION`
no equivale a aprobación: revisar cada subresultado. Si falta el original local,
`artifacts.status=NOT_VERIFIED`; no reconstruirlo ni adoptar el remoto para forzar
igualdad. Permisos denegados, ausencia, tamaño excesivo o SHA distinto bloquean.
Véase la [matriz y aceptación condicionada](WORKSPACE-FILE-VERIFICATION.md).
