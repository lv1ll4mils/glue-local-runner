#!/usr/bin/env bash
set -euo pipefail

############################################
# Configuración editable (por defecto)
############################################
if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi
############################################
# Convención de perfiles locales: local-<env> (local-dev|local-test|local-prod)
AWS_PROFILE="${AWS_PROFILE:-engineer-dev}"
# Identidad del job a correr (usado para resolver JOB_PATH)
PIPELINE_NAME="${PIPELINE_NAME:-presupuesto-s4h}"

# Imagen local (ya construida desde amazon/aws-glue-libs:5.0.6)
IMAGE="${IMAGE:-mi-glue-job:local}"

# Definir LAYER si no existe
LAYER="${LAYER:-raw}"

############################################
# Utilidades de logging
############################################
die() { echo "ERROR: $*" >&2; exit 1; }
warn() { echo "WARN:  $*" >&2; }

require_bin() { command -v "$1" >/dev/null 2>&1 || die "No se encontró '$1' en PATH"; }

ensure_sso() {
  # Evita re-login si el token SSO aún es válido
  if aws sts get-caller-identity --profile "$AWS_PROFILE" >/dev/null 2>&1; then
    echo "SSO OK para perfil '$AWS_PROFILE'."
  else
    echo "SSO inválido/ausente. Ejecutando 'aws sso login --profile $AWS_PROFILE'..."
    aws sso login --profile "$AWS_PROFILE"
  fi
}

# Wrapper de yq: usa yq local o el contenedor mikefarah/yq
# Nota: aquí montamos SDLF_DIR para que yq lea template.yaml desde ese repo.
yq_cmd() {
  if command -v yq >/dev/null 2>&1; then
    yq "$@"
  else
    docker run --rm -i \
      -v "$SDLF_DIR":/workspace:ro \
      -w /workspace \
      --pull=missing \
      mikefarah/yq:4 "$@"
  fi
}

# Exporta credenciales temporales (SSO) a un env-file para el contenedor
export_credentials_envfile() {
    # Exporta credenciales temporales y las guarda en un env-file seguro
    local envfile="$1"
    rm -f "$envfile"
    # Añade REGION siempre
    {
        echo "AWS_REGION=${AWS_REGION}"
        echo "AWS_DEFAULT_REGION=${AWS_REGION}"
    } >>"$envfile"

    # Exporta AK/SK/SESSION usando el perfil SSO
    # shellcheck disable=SC2046
    eval $(aws configure export-credentials --profile "$AWS_PROFILE" --format env)

    # Persistimos solo las tres vars necesarias
    {
        echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
        echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
        echo "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}"
    } >>"$envfile"

    chmod 600 "$envfile"
    echo "Credenciales temporales exportadas a ${envfile}."
}

# Bootstrap Python: inyecta nivel de log justo después de crear SparkContext
make_bootstrap(){
    # Crea el bootstrap DENTRO del repo para que Docker lo vea.
    local dst="$1" ; local target_py="$2"
    mkdir -p "$(dirname "$dst")"
    cat >"$dst" <<'PYEOF'
import os, runpy, sys
# Parche: al inicializar SparkContext en el script, se baja log level.
try:
    import pyspark
    _orig_init = pyspark.context.SparkContext.__init__
    def _init_with_log_level(self, *args, **kwargs):
        _orig_init(self, *args, **kwargs)
        self.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    pyspark.context.SparkContext.__init__ = _init_with_log_level
except Exception as e:
    # Si por alguna razón no está pyspark aún, seguimos sin romper.
    pass
# Ejecuta tu script original con los mismos argumentos
target = os.environ["TARGET_PY"]
sys.argv = [target] + sys.argv[1:]
runpy.run_path(target, run_name="__main__")
PYEOF
    export TARGET_PY="$target_py"
}

############################################
# Prechequeos de binarios y SSO
############################################
require_bin aws
require_bin docker

# SDLF_DIR: ruta local del repo corporativo SDLF (obligatoria)
SDLF_DIR="${SDLF_DIR:-}"
[ -n "${SDLF_DIR}" ] || die "Debes definir SDLF_DIR apuntando a tu repo SDLF local. Ej: export SDLF_DIR=/home/user/projects/sdlf-main-datalake-engineering"
[ -d "${SDLF_DIR}" ] || die "SDLF_DIR no existe o no es directorio: '${SDLF_DIR}'"

# Asegura SSO válido (no re-logea si no hace falta)
ensure_sso


############################################
# Contexto perfil SSO (REGION, ACCOUNT, ENV)
############################################
# REGION: si no viene por env, la tomamos del bloque del perfil SSO
AWS_REGION="${AWS_REGION:-$(aws configure get region --profile "$AWS_PROFILE" || true)}"
[ -n "${AWS_REGION:-}" ] || die "No se pudo resolver AWS_REGION desde el perfil '$AWS_PROFILE'. Define region en el perfil o exporta AWS_REGION."

# ACCOUNT_ID: vía STS con el perfil SSO
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text --profile "$AWS_PROFILE")"
[ -n "${ACCOUNT_ID:-}" ] || die "No se pudo resolver ACCOUNT_ID vía STS."

# ENVIRONMENT: de la convención local-<env>
ENVIRONMENT="$(echo "$AWS_PROFILE" | cut -d'-' -f2)"
[ -n "${ENVIRONMENT:-}" ] || die "No se pudo derivar ENVIRONMENT desde AWS_PROFILE='$AWS_PROFILE'. Esperado: local-<env>."

############################################
# Rutas del Job en SDLF
############################################
JOB_DIR="transforms/sdlf-engineering-${PIPELINE_NAME}-${LAYER}-job"
JOB_NAME="sdlf-engineering-${PIPELINE_NAME}-${LAYER}-job.py"
JOB_PATH="${SDLF_DIR}/${JOB_DIR}/${JOB_NAME}"
[ -f "$JOB_PATH" ] || die "No existe JOB_PATH='$JOB_PATH'."

TEMPLATE_PATH="${SDLF_DIR}/${JOB_DIR}/template.yaml"

# Paths dentro del contenedor
JOB_PATH_CONT="/workspace/${JOB_DIR}/${JOB_NAME}"
TEMPLATE_PATH_CONT="/workspace/${JOB_DIR}/template.yaml"

############################################
# Defaults / overrides (key-values desde template.yaml)
############################################
DOMAIN_AUTO=""
FOLDER_AUTO=""
SHEET_AUTO=""
WORKSHEET_AUTO=""
RANGE_AUTO=""
SECRET_AUTO=""
YEAR_AUTO=""
TOKEN_AUTO=""

#==============================================================
# EXTRACCIÓN DE TODAS LAS KEY-VALUE VIGENTES EN LOS JOBS SDLF
#==============================================================

if [ -f "$TEMPLATE_PATH" ]; then
  echo "Usando template: $TEMPLATE_PATH"

  # ---------------- DOMAIN ----------------
  # 1) Parameters: pDomain o Domain
  DOMAIN_PARAM="$( yq_cmd -r '.Parameters.pDomain.Default // .Parameters.Domain.Default // ""' "$TEMPLATE_PATH" )"
  [ "$DOMAIN_PARAM" = "null" ] && DOMAIN_PARAM=""  # normaliza null→vacío

  if [ -n "${DOMAIN_PARAM//[[:space:]]/}" ]; then
    DOMAIN_AUTO="$DOMAIN_PARAM"
  else
    # 2) DefaultArguments["--domain"] en cualquier recurso/profundidad
    # 2a) ¿Referencia a parámetro (Ref)?
    DOMAIN_REF="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--domain"].Ref // ""' "$TEMPLATE_PATH" | head -n1 )"
    [ "$DOMAIN_REF" = "null" ] && DOMAIN_REF=""

    if [ -n "${DOMAIN_REF//[[:space:]]/}" ]; then
      DOMAIN_AUTO="$( yq_cmd -r ".Parameters[\"$DOMAIN_REF\"].Default // \"\"" "$TEMPLATE_PATH" )"
      [ "$DOMAIN_AUTO" = "null" ] && DOMAIN_AUTO=""
    else
      # 2b) ¿String literal?
      DOMAIN_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--domain"] // ""' "$TEMPLATE_PATH" | head -n1 )"
      [ "$DOMAIN_STR" = "null" ] && DOMAIN_STR=""
      DOMAIN_AUTO="$DOMAIN_STR"
    fi
  fi

  # Trim espacios
  DOMAIN_AUTO="$( printf '%s' "$DOMAIN_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"


  # ---------------- FOLDER_ID ----------------
  FOLDER_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--drive_folder_id"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$FOLDER_STR" = "null" ] && FOLDER_STR=""
  FOLDER_AUTO="$FOLDER_STR"

  FOLDER_AUTO="$( printf '%s' "$FOLDER_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

  # ---------------- SHEET_NAME ----------------
  SHEET_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--sheet_id"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$SHEET_STR" = "null" ] && SHEET_STR=""
  SHEET_AUTO="$SHEET_STR"

  SHEET_AUTO="$( printf '%s' "$SHEET_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

  # ---------------- WORKSHEET_NAME ----------------
  WORKSHEET_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--worksheet_name"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$WORKSHEET_STR" = "null" ] && WORKSHEET_STR=""
  WORKSHEET_AUTO="$WORKSHEET_STR"

  WORKSHEET_AUTO="$( printf '%s' "$WORKSHEET_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

# ---------------- RANGE_NAME ----------------
  RANGE_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--gsheet_range"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$RANGE_STR" = "null" ] && RANGE_STR=""
  RANGE_AUTO="$RANGE_STR"

  RANGE_AUTO="$( printf '%s' "$RANGE_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

  # ---------------- SECRET_NAME ----------------
  SECRET_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--secret_name"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$SECRET_STR" = "null" ] && SECRET_STR=""
  SECRET_AUTO="$SECRET_STR"

  SECRET_AUTO="$( printf '%s' "$SECRET_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

  # ---------------- YEAR_REF ----------------
  YEAR_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--base_year"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$YEAR_STR" = "null" ] && YEAR_STR=""
  YEAR_AUTO="$YEAR_STR"

  YEAR_AUTO="$( printf '%s' "$YEAR_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

  # ---------------- TOKEN_REF ----------------
  TOKEN_STR="$( yq_cmd -r '.. | select(has("DefaultArguments")) | .DefaultArguments["--base_year"] // ""' "$TEMPLATE_PATH" | head -n1 )"
  [ "$TOKEN_STR" = "null" ] && TOKEN_STR=""
  TOKEN_AUTO="$TOKEN_STR"

  TOKEN_AUTO="$( printf '%s' "$TOKEN_AUTO" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//' )"

else
  warn "No se encontró template.yaml en $(dirname "$JOB_PATH"). Se usarán defaults/overrides."
fi

############################################
# Parámetros del job (dinámicos con override)
############################################
BUCKET_PREFIX="${BUCKET_PREFIX:-pragma-datalake}"
DB_PREFIX="${DB_PREFIX:-${BUCKET_PREFIX//-/_}}"


RAW_BUCKET="${RAW_BUCKET:-${BUCKET_PREFIX}-${ENVIRONMENT}-${AWS_REGION}-${ACCOUNT_ID}-raw}"
STAGE_BUCKET="${STAGE_BUCKET:-${BUCKET_PREFIX}-${ENVIRONMENT}-${AWS_REGION}-${ACCOUNT_ID}-stage}"
ANALYTICS_BUCKET="${ANALYTICS_BUCKET:-${BUCKET_PREFIX}-${ENVIRONMENT}-${AWS_REGION}-${ACCOUNT_ID}-analytics}"

DELIVERY_CATALOG_SG="${DELIVERY_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_delivery_stage}"
FINANCIAL_CATALOG_SG="${FINANCIAL_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_financial_stage}"
TERRITORIES_CATALOG_SG="${TERRITORIES_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_territories_stage}"
TALENT_CATALOG_SG="${TALENT_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_talent_stage}"

DELIVERY_CATALOG_AN="${DELIVERY_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_delivery_analytics}"
FINANCIAL_CATALOG_AN="${FINANCIAL_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_financial_analytics}"
TERRITORIES_CATALOG_AN="${TERRITORIES_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_territories_analytics}"
TALENT_CATALOG_AN="${TALENT_CATALOG:-${DB_PREFIX}_${ENVIRONMENT}_talent_analytics}"

# Selector de bucket según LAYER
resolve_layer_bucket() {
  # Normaliza a minúsculas por si viene en MAYÚSCULAS
  local layer="${LAYER,,}"
  case "${layer}" in
    raw)        echo "${RAW_BUCKET}" ;;
    stage)      echo "${STAGE_BUCKET}" ;;
    analytics)  echo "${ANALYTICS_BUCKET}" ;;
    *)          die "LAYER inválida: '${LAYER}'. Esperado: raw|stage|analytics" ;;
  esac
}

# Selección en runtime
LAYER_BUCKET="$(resolve_layer_bucket)"

# key-value: del template si existe; si no, de variables definidas
PIPELINE="${PIPELINE:-${PIPELINE_NAME}}"
DOMAIN="${DOMAIN:-${DOMAIN_AUTO}}"
FOLDER="${FOLDER:-${FOLDER_AUTO}}"
SHEET="${SHEET:-${SHEET_AUTO}}"
WORKSHEET="${WORKSHEET:-${WORKSHEET_AUTO}}"
RANGE="${RANGE:-${RANGE_AUTO}}"
SECRET="${SECRET:-${SECRET_AUTO}}"
YEAR="${YEAR:-${YEAR_AUTO}}"
TOKEN="${TOKEN:-${TOKEN_AUTO}}"
JOB_NAME="${JOB_NAME:-${JOB_NAME}}"
REGION="${REGION:-${AWS_REGION}}"

# f) DATABASE: [fijo]_[env]_[dominio]_[layer]
DB_SUFFIX="${DB_SUFFIX:-${LAYER}}"
DATABASE="${DATABASE:-${DB_PREFIX}_${ENVIRONMENT}_${DOMAIN}_${DB_SUFFIX}}"
# Warehouse de Glue/Iceberg
GLUE_WAREHOUSE_URI="${GLUE_WAREHOUSE_URI:-s3://${LAYER_BUCKET}/${DOMAIN}/${PIPELINE}}"


############################################
# Temp runner: log4j2 + bootstrap + envfile
############################################
RUN_TMP="$(mktemp -d -t glue_local_runner.XXXX)"
ENVFILE="${RUN_TMP}/env.creds"
LOG4J2_PATH_HOST="${RUN_TMP}/log4j2.properties"
BOOTSTRAP_HOST="${RUN_TMP}/_bootstrap.py"

cleanup() { rm -rf "$RUN_TMP"; }
trap cleanup EXIT

cat >"$LOG4J2_PATH_HOST" <<'EOF'
status = warn
name = SparkLog4j2
filters = threshold
filter.threshold.type = ThresholdFilter
filter.threshold.level = WARN

logger.ivy.name = org.apache.ivy
logger.ivy.level = ERROR
logger.slf4j.name = org.slf4j
logger.slf4j.level = ERROR

appender.console.type = Console
appender.console.name = Console
appender.console.target = SYSTEM_OUT
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1} - %m%n

rootLogger.level = WARN
rootLogger.appenderRefs = console
rootLogger.appenderRef.console.ref = Console

logger.spark.name = org.apache.spark
logger.spark.level = ERROR
logger.hadoop.name = org.apache.hadoop
logger.hadoop.level = ERROR
logger.aws.name = com.amazonaws
logger.aws.level = ERROR
logger.iceberg.name = org.apache.iceberg
logger.iceberg.level = WARN
EOF

export_credentials_envfile "$ENVFILE"
make_bootstrap "$BOOTSTRAP_HOST"

############################################
# Spark args (Glue 4.0 / Spark 3.3)
############################################
# Silenciar logs JVM y Spark UI
SPARK_LOG_LEVEL="${SPARK_LOG_LEVEL:-WARN}"
SPARK_CONSOLE_PROGRESS="${SPARK_CONSOLE_PROGRESS:-false}"
SPARK_EVENTLOG_ENABLED="${SPARK_EVENTLOG_ENABLED:-false}"

# Versiones Iceberg/Spark (alineado con Glue 4.0: Spark 3.3)
ICEBERG_VERSION="${ICEBERG_VERSION:-1.4.3}"
SPARK_RUNTIME_COORD="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:${ICEBERG_VERSION}"
ICEBERG_AWS_BUNDLE="org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"

# Args Spark-Glue
SPARK_ARGS=(
    --packages "${SPARK_RUNTIME_COORD},${ICEBERG_AWS_BUNDLE}" \
    --conf spark.ui.showConsoleProgress="${SPARK_CONSOLE_PROGRESS}" \
    --conf spark.eventLog.enabled="${SPARK_EVENTLOG_ENABLED}" \
    --conf spark.driver.extraJavaOptions="-Dlog4j2.configurationFile=/usr/lib/spark/conf/log4j2.properties" \
    --conf spark.executor.extraJavaOptions="-Dlog4j2.configurationFile=/usr/lib/spark/conf/log4j2.properties" \
    --conf spark.ui.enabled=false \
    --conf spark.driver.memory="8g" \
    --conf spark.executor.memory="8g" \
    --conf spark.executor.cores="2" \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.glue_catalog.warehouse="${GLUE_WAREHOUSE_URI}" \
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true
)

cat <<EOVARS
=========================== Variables resueltas ============================
AWS_PROFILE=${AWS_PROFILE}
JOB_NAME=${JOB_NAME}
DOMAIN=${DOMAIN}
===========================================================================
EOVARS

############################################
# Ejecución: spark-submit en el contenedor
############################################
docker run --rm -it \
  --env-file "$ENVFILE" \
  -e SPARK_LOG_LEVEL="$SPARK_LOG_LEVEL" \
  -e TARGET_PY="$JOB_PATH_CONT" \
  -v "$SDLF_DIR":/workspace:ro \
  -v "$RUN_TMP":/runner_tmp \
  -v "/runner_tmp/log4j2.properties":/usr/lib/spark/conf/log4j2.properties:ro \
  -w /workspace \
  "$IMAGE" \
  spark-submit \
    ${SPARK_ARGS[@]} \
    /runner_tmp/_bootstrap.py \
      --JOB_NAME "${JOB_NAME}" \
      --raw_bucket "${RAW_BUCKET}" \
      --stage_bucket "${STAGE_BUCKET}" \
      --analytics_bucket "${ANALYTICS_BUCKET}" \
      --pipeline "${PIPELINE}" \
      --domain "${DOMAIN}" \
      --database "${DATABASE}" \
      --delivery_stage_database "${DELIVERY_CATALOG_SG}" \
      --financial_stage_database "${FINANCIAL_CATALOG_SG}" \
      --territories_stage_database "${TERRITORIES_CATALOG_SG}" \
      --talent_stage_database "${TALENT_CATALOG_SG}" \
      --delivery_analytics_database "${DELIVERY_CATALOG_AN}" \
      --financial_analytics_database "${FINANCIAL_CATALOG_AN}" \
      --territories_analytics_database "${TERRITORIES_CATALOG_AN}" \
      --talent_analytics_database "${TALENT_CATALOG_AN}" \
      --drive_folder_id "${FOLDER}" \
      --sheet_id "${SHEET}" \
      --worksheet_name "${WORKSHEET}" \
      --gsheet_range "${RANGE}" \
      --secret_name "${SECRET}" \
      --base_year "${YEAR}" \
      --region_name "${REGION}" \
      --token_key "${TOKEN}"