# Glue Jobs Local Runner (Docker + Bash)

> Objetivo: ejecutar **AWS Glue Jobs (Glue 4.0 / Spark 3.3 / Python 3.x)** en local usando **Docker**, reutilizando el código y `template.yaml` de tu repositorio **SDLF** (Serverless Data Lake Framework), sin acoplar este “runner” al repo corporativo.

## 1) ¿Qué problema resuelve?

* Permite correr Jobs de Glue **desde VS Code / terminal** sin desplegar infraestructura.
* Lee `template.yaml` del Job para autocompletar key-values (por ejemplo `--domain`, `--sheet_id`, etc.).
* Usa autenticación **AWS SSO** en tu máquina y exporta credenciales temporales al contenedor.
* Mantiene el runner como repo independiente (GitHub personal), apuntando a un repo SDLF local.

## 2) Requisitos previos

### 2.1 Docker

Verifica instalación:

```bash
docker --version
# opcional
docker info
```

### 2.2 AWS CLI v2

Verifica instalación:

```bash
aws --version
```

**Requisito:** tener configurado AWS SSO (perfiles) para los ambientes que uses.

Pruebas rápidas:

```bash
aws sts get-caller-identity --profile local-dev
aws sso login --profile local-dev
```

### 2.3 yq

El script puede usar `yq` local o, si no existe, ejecuta `mikefarah/yq` en Docker.

```bash
yq --version
```

## 3) Estructura recomendada

* **Repo A (corporativo, local):** `sdlf-main-datalake-engineering` (NO se publica)
* **Repo B (personal):** `glue-local-runner`

Ejemplo:

```text
/home/user/projects/
  sdlf-main-datalake-engineering/        # repo corporativo (local)
  glue-local-runner/                     # repo runner (personal)
```

## 4) Docker image (Glue libs)

Ejemplo de `Dockerfile` (Glue 4.0):

```dockerfile
FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

RUN python3 -m pip install --no-cache-dir \
    google-api-python-client \
    google-auth-oauthlib \
    google-auth-httplib2 \
    fastparquet \
    workalendar

# Workdir neutro (se sobreescribe al hacer docker run)
WORKDIR /workspace
```

Build:
* Comando para construir la imagen
```bash
docker build --no-cache -t mi-glue-job:local .
```

## 5) Configuración del runner

### 5.1 Variable SDLF_DIR

Este runner necesita saber **dónde está el repo SDLF en la máquina**.

Ejemplos:

```.env
SDLF_DIR="/home/user/projects/sdlf-main-datalake-engineering"
```

## 6) Ejecución

Desde el repo **runner**:

### Existe dos métodos de ejecución:
**1. Definiendo los parametros en el comando**
* Los parametros "AWS_PROFILE" - "LAYER" y "PELINE_NAME" se deben definir de acuerdo a la capa y Job a ejecutar:
```bash
AWS_PROFILE=[profile_name] LAYER=[layer_name] PIPELINE_NAME=[job_name] ./local_run.sh
```

**2. Configurando los parámetros a nivel interno**
```bash
./local_run.sh
```

El script construye la ruta del Job:

```text
<SDLF_DIR>/transforms/sdlf-engineering-${PIPELINE_NAME}-${LAYER}-job/
  sdlf-engineering-${PIPELINE_NAME}-${LAYER}-job.py
  template.yaml
```

## 7) Cómo funciona (alto nivel)

1. **Valida** binarios (`aws`, `docker`) y que SSO esté activo.
2. Resuelve `AWS_REGION`, `ACCOUNT_ID` y `ENVIRONMENT` desde el `AWS_PROFILE`.
3. Ubica el `JOB_PATH` en tu repo SDLF.
4. Lee `template.yaml` (con `yq`) y extrae defaults.
5. Exporta credenciales temporales a un `env-file` seguro.
6. Ejecuta `spark-submit` dentro del contenedor Glue libs, montando:

   * tu repo SDLF en `/workspace`
   * un directorio temporal del runner en `/runner_tmp` (bootstrap + log4j)

## 8) Troubleshooting

* **SSO inválido:**

  ```bash
  aws sso login --profile local-dev
  ```

* **No encuentra el Job** (`No existe JOB_PATH`):

  * verifica `SDLF_DIR`
  * verifica `PIPELINE_NAME` y `LAYER`
  * confirma que el job existe en `transforms/`.

* **Permisos S3/Glue Catalog:**

  * revisa que tu rol SSO tenga acceso al bucket y Glue Catalog que el Job usa.

* **Dependencias Python faltantes:**

  * agrega al `Dockerfile` y reconstruye la imagen.

## 9) Seguridad

* No hardcodear llaves. El runner usa credenciales temporales de SSO.
* Evitar imprimir secretos; el script no debe loguear tokens/secret values.
* Si usas `.env` local, **no** lo subas a repos.

---
