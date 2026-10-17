# Diagnóstico y cierre desde Windows / Git Bash

## Estado de la ejecución real

- Parent comunicado por el operador: `20d65511-8190-41a0-8ad2-23d7568363d2`.
- ADLS: `stadmmdevdlf6d99ba4`, contenedor `bronze`; resource group: `rg-admm-dev-platform`.
- El operador informa once cargas ADF realizadas, conexión pyodbc a `127.0.0.1,1433` verificada y permisos de `bronze_reconciler` configurados.
- **R1: reconciliación y confirmación reales pendientes de evidencia. R2 en Azure: PENDIENTE.** No se ha ejecutado el coordinador contra estos recursos durante esta revisión. No se afirma que los watermarks estén confirmados. Se conserva `WATERMARK_CONFIRMATION_BLOCKED`.

El mensaje anterior `OperationalError` no contiene evidencia suficiente para identificar su causa. Un `SELECT 1` exitoso tampoco demuestra que la conexión del coordinador seleccione AdventureWorks, use la misma contraseña/TLS ni pueda ejecutar los procedimientos. No se atribuye el fallo real a una causa sin obtener el diagnóstico de esa ejecución.

## Correcciones verificadas

- Conexión SQL interactiva con entrada oculta y valores ODBC delimitados y escapados (incluidos `;`, `}`, comillas, espacios, `$`, `!`, backticks y barras). La contraseña no pasa por Bash, argumentos, historial ni archivos. En modo interactivo se ignora una cadena `BRONZE_SQL_CONNECTION_STRING` antigua. La base es explícitamente AdventureWorks.
- Se valida la base antes de leer auditoría. Conexión cifrada; la excepción de certificado explícita se restringe a SQL de loopback y no cambia TLS de Azure.
- Se drenan todos los resultados ODBC de los procedimientos: un error diferido después del primer resultado ya no se acepta como éxito. El cierre de un cliente/cursor no oculta el diagnóstico original.
- Se comprueba el recibo durable del parent (identidad, once hijos y hash) antes de informar COMMITTED, también en reintentos históricos. Un corte durante el commit o su consulta posterior se informa como `parent_commit_outcome=UNKNOWN`: repetir **el mismo parent**, sin recopy ni reset, resuelve el resultado consultando SQL.
- Se detecta paginación ADF con tokens repetidos. Se mantienen los controles de estado terminal, once hijos, Parquet, tuplas completas, conteos, ETags e idempotencia y la transacción SQL del parent.

## Única ejecución del operador

Desde Git Bash, en el repositorio local, con su entorno Python ya preparado (dependencias de `adf/tools/requirements-commit.txt`, ODBC Driver 18 y `winpty` de Git for Windows), use el comando de abajo. Requiere la identidad Azure existente con lectura de ADF/ADLS y la contraseña SQL de `bronze_reconciler`; no requiere ni concede permisos de despliegue. El comando actualiza únicamente la rama existente y ejecuta el coordinador. Si hay cambios locales incompatibles, Git se detiene antes de ejecutarlo.

```bash
git switch feature/bronze-audit-reconciliation-commit && \
  git pull --ff-only origin feature/bronze-audit-reconciliation-commit && \
  winpty python adf/tools/commit_bronze_parent.py --interactive \
    --sql-server '127.0.0.1,1433' --sql-user bronze_reconciler \
    --trust-local-sql-certificate \
    --account stadmmdevdlf6d99ba4 --resource-group rg-admm-dev-platform \
    --parent-run-id 20d65511-8190-41a0-8ad2-23d7568363d2
```

Introduzca la contraseña **solo en el prompt oculto**. No necesita escribir `export`, entrecomillarla ni escapar caracteres en Bash. Si no se puede ocultar la entrada, el programa se detiene. No active trazas Bash, ODBC ni HTTP.

La suscripción se obtiene de `AZURE_SUBSCRIPTION_ID` si está configurada o de la sesión `az account show`; si no existe contexto, se solicita el UUID no secreto. La fábrica se obtiene de `ADF_FACTORY`/`BRONZE_ADF_FACTORY` si están configuradas o localizando el parent existente mediante lecturas en el resource group; si el permiso de listado es insuficiente o hay ambigüedad, se solicita el nombre. No se crea ni ejecuta ningún pipeline. La identidad Azure usa las credenciales disponibles y permite autenticación de navegador en este modo interactivo. Los valores de contexto preconfigurados deben corresponder a los recursos indicados.

**Efectos:** lee estado ADF y archivos ADLS; escribe atestaciones y, solo si pasan todos los controles, confirma las once cargas en una transacción SQL. No modifica Azure. Un fallo puede dejar atestaciones RECONCILED sin confirmar; no equivalen a avance de cursor. Una repetición de un parent confirmado solo lee su recibo durable.

## Interpretación del resultado

La salida JSON de error incluye `stage`, `code`, `hint` y, cuando están disponibles, SQLSTATE y un código nativo SQL de una lista permitida; puede incluir RunIds. No imprime texto bruto de excepciones, consultas, filas, URLs, contraseñas ni cadenas de conexión. Los errores conocidos de validación tienen motivos estáticos. No se intenta limpiar una traza potencialmente secreta mediante reemplazos parciales.

| Etapa/código | Interpretación |
| --- | --- |
| `sql_connect` / `SQL_LOGIN_FAILED` | Login rechazado; revisar identidad/contraseña introducida. |
| `sql_connect` / `SQL_TLS_FAILED` | Validación de certificado fallida; excepción permitida solamente para loopback local. |
| `sql_validate_session` / `SQL_WRONG_DATABASE` | La conexión heredada no selecciona AdventureWorks. |
| `SQL_PERMISSION_DENIED` | Falta un grant de objeto; no conceder escritura directa ni db_owner. |
| `SQL_CONTRACT_MISMATCH` | Objetos/firma SQL o base no coinciden con el código versionado. |
| `SQL_GUARD_REJECTED` | Una guarda SQL rechazó evidencia/estado; buscar el número nativo en `adf/sql/`. No forzar el commit. |
| `azure_verify_parent` | El estado/RunIds de ADF deben verificarse; no repetir el pipeline. |
| `adls_reconcile_child` / `reconcile_child` | Lectura o validación del Parquet; revisar el child indicado sin editar su carpeta. |
| `sql_commit_parent` / `sql_verify_parent_receipt` con `UNKNOWN` | La respuesta no demuestra rollback ni commit. Repetir el mismo comando consulta el recibo durable. |

Solo una salida COMMITTED respaldada por el recibo durable acredita confirmación. Conservar la salida y los manifiestos. La prueba **R2 real**, el contraste independiente de Parquet/SQL y la aceptación final siguen el [runbook](bronze-end-to-end-rollout.md#two-consecutive-approved-acceptance-runs) y requieren una ejecución futura expresamente aprobada; no forman parte de este comando ni de esta revisión.
