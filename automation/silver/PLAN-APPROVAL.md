# Hash, revisión y aplicación de planes Terraform

El SHA de aprobación identifica **un archivo binario guardado concreto**, no una configuración ni un conjunto de recursos. Repetir un comando genera otro plan y puede producir otro SHA aunque Terraform siga mostrando `7 to add, 0 to change, 0 to destroy`. No se debe sustituir la aprobación por una comparación de contadores ni por un hash de acciones resumidas.

## Qué se calcula y por qué cambia

`plan_guard.sha(path)` abre el archivo en modo binario y calcula `hashlib.file_digest(stream, 'sha256').hexdigest()`. En `Launcher.plan`, el argumento es el archivo producido por `terraform plan -out=...`: **todos los bytes del `.tfplan`**, sin normalización. No se calcula sobre `terraform show -json` ni sobre `terraform show -no-color`.

El archivo de Terraform 1.9.8 es un contenedor con `tfplan`, snapshots `tfstate`/`tfstate-prev`, configuración y lock de proveedores. El contenido `tfplan` incorpora el timestamp del plan. También pueden variar configuración, valores de data sources, estado refrescado y otros metadatos sin cambiar el contador de recursos. Dos listas de siete creaciones no demuestran igualdad de atributos, principal RUN AS, grants o política.

Se reprodujo **sin apply ni recursos cloud** con siete instancias del recurso integrado `terraform_data`, sin cambiar su configuración:

- Dos planes guardados separados por más de un segundo: SHA binarios distintos.
- `terraform show -json`: la única clave diferente fue `timestamp`.
- Dentro del contenedor ZIP solo cambió el payload `tfplan`; no cambiaron los timestamps de las entradas ZIP. La diferencia observada no fue solo una fecha del archivo en el sistema operativo.

Esta reproducción forma parte de las pruebas automatizadas con Terraform real. No se recibieron los tres binarios originales del operador: no se atribuye cada diferencia de esos archivos a un campo específico sin inspeccionarlos. El comportamiento comunicado es compatible con la generación normal de planes nuevos.

## Defecto encontrado y corrección

La implementación anterior sobrescribía `uc-workspace.tfplan` y su revisión en cada invocación. Además, calculaba el hash después de renderizar la revisión, y la comprobación posterior a la aprobación quedaba antes de otro `terraform show`. Ese segundo control comparaba únicamente direcciones/acciones devueltas por el allowlist, no todo el contenido del JSON. Una sustitución accidental/concurrente del archivo podía desasociar revisión, hash y aplicación. **La variación normal del hash no es el defecto; lo es esta ventana de integridad y la reutilización de rutas.**

Ahora cada invocación tiene su propio directorio privado:

```text
work/silver-deployment/plans/uc-workspace-<identificador>/
  variables.tfvars.json
  plan.tfplan
  plan.txt
  review.json
```

El launcher imprime las rutas reales; no se deben adivinar sufijos ni buscar un supuesto archivo global «más reciente». No sobrescribe un plan pendiente de otra invocación y conserva evidencias para revisión. `work/` continúa fuera de Git. Los planes y revisiones pueden contener información sensible: mantener los permisos privados de la carpeta y ACL del usuario Windows.

La secuencia es:

1. Generar el binario una sola vez y calcular su SHA inmediatamente.
2. Leer JSON, pasar todas las guardas y generar texto del **mismo binario**; comprobar el SHA después de cada lectura.
3. Guardar el texto completo y metadata que identifica el binario y la revisión.
4. Si hay cambios y `--deploy`, pedir la aprobación exacta sobre ese SHA.
5. Volver a verificar binario y texto, releer el JSON, comprobar hash, allowlist y **todo el JSON original**, no solo las direcciones/acciones.
6. En Silver/workspace, repetir los controles de autorización RUN AS después de la espera humana.
7. Comprobar otra vez texto y binario inmediatamente antes de invocar `terraform apply -input=false <ese-mismo-plan.tfplan>`. No ejecutar otro `plan` ni aplicar el directorio con variables actuales.

Terraform conserva sus propias validaciones de bloqueo del backend y plan obsoleto. Un fallo no provoca replanning ni reintento automático de apply. Las guardas de destrucción/reemplazo, importación, alcance y permisos READ_VOLUME permanecen activas. El hash no es una firma ni un mecanismo de aislamiento frente a otro proceso malicioso con los mismos permisos locales; la carpeta privada sigue siendo parte de la frontera de confianza. No ejecutar despliegues simultáneos del proyecto.

## Campos de revisión

| Campo | Contenido / uso |
| --- | --- |
| `plan_sha256` (`sha256` en metadata, conservado como alias) | SHA-256 del binario completo. **Único hash aceptado para aprobar apply.** |
| `plan_file` | Ruta exacta del binario que se aplicaría. |
| `review_file` / `review_text_sha256` | Texto completo del plan y su integridad. |
| `comparison_json_sha256` | JSON completo de `terraform show -json`, con claves ordenadas y excluyendo **solo** el `timestamp` de nivel superior. Solo ayuda a comparar invocaciones. |
| `terraform_timestamp` | Timestamp observado del plan, sin eliminarlo del binario. |

El hash comparativo incluye variables, estado/valores, configuración, atributos, unknowns, outputs y timestamps anidados. No normaliza un timestamp de negocio ni elimina valores desconocidos. Puede cambiar aunque coincidan los contadores. **Nunca autoriza apply**: la confirmación sigue exigiendo el hash binario de 64 caracteres de la ejecución activa. La metadata solo informa; el launcher conserva en memoria la identidad y contenido originales para verificar la aprobación.

## Delegación RUN AS

En `--stage silver --uc-mode workspace --deploy` se valida antes de planificar:

- Que el perfil atiende al workspace/metastore esperado y que `/Me` es el operador autorizado.
- Que el SP seleccionado por `applicationId` es único, activo y tiene `workspace-access`.
- Que el rule set devuelto corresponde exactamente a `accounts/<account-id>/servicePrincipals/<application-id>/ruleSets/default`.
- Que existe un grant **directo** `roles/servicePrincipal.user` a `users/<UPN del operador>`.

Se usa el proxy de control de acceso en el host del workspace y el perfil seleccionado. Account ID es metadata necesaria para nombrar el rule set; no se requiere login Account Admin. Manager, workspace `admins`, Azure Owner o grants de grupo cuya pertenencia no se ha expandido no se aceptan como prueba del rol User. No se modifican grants para superar el control.

La validación ya existía antes del plan. Esta corrección la repite **después de aprobar el plan, antes de aplicar**, y otra vez **antes de desplegar el bundle**, tras la espera de aprobación del Job y la preparación del recibo. Una revocación detectada bloquea el flujo. No convierte la comprobación en una autorización permanente: Databricks sigue comprobando permisos al desplegar/ejecutar. En modo plan sin `--deploy` se verifica la identidad/SP cuando corresponda, pero no se afirma haber probado el permiso RUN AS; `--stage uc` tampoco lo certifica.

## Procedimiento seguro para continuar

Tras revisar/integrar el cambio, obtener primero un plan sin efectos cloud, desde la raíz:

```bash
python automation/silver/launch.py --stage silver --uc-mode workspace --workspace-profile admm-workspace --metastore-id b67792ab-c53b-48b4-ae57-fc45b869ed7a --config work/silver-dev.json
```

No añadir `--deploy` durante esta revisión. Abrir el `review_file` indicado y examinar los atributos/grants/política completos, no solo el `7/0/0`. Comparar `comparison_json_sha256` entre planes como ayuda, nunca como autorización. Los planes antiguos no se reutilizan mediante una opción oculta.

**Solo cuando exista autorización posterior para aplicar:** ejecutar una única invocación interactiva con `--deploy`. Mantenerla abierta mientras se revisa el archivo de esa invocación y escribir exactamente `APPLY-UC-WORKSPACE <plan_sha256 actual completo>` en ese mismo proceso. No cerrar/reiniciar para intentar conservar el SHA anterior: eso genera otro plan que necesita otra revisión/aprobación. `winpty` puede ser necesario para prompts en Git Bash. Tras aplicar, `--stage silver --deploy` puede continuar hacia el Job y solicitar una aprobación separada `DEPLOY-RUN-SILVER`; no usarlo si solo se autorizó UC. Para aplicar solo UC existe `--stage uc --deploy`, que no despliega Jobs ni certifica RUN AS. No se aplicó ninguna de estas opciones durante esta investigación.

Esta entrega solo ejecutó análisis, pruebas unitarias con apply/Jobs **simulados** y planes reales locales con fixtures/built-in provider. No ejecutó `terraform apply`, Jobs, Silver, R2 ni operaciones Azure/Databricks. La delegación del SP real del operador y su estado remoto no se consultaron.

Validación de esta corrección: **134 pruebas locales aprobadas**, incluidas la reproducción con Terraform real, el transporte OAuth de loopback y los casos de apply/Job simulados; `compileall` y `git diff --check` aprobados. No se validó el estado remoto ni la delegación real del operador.
