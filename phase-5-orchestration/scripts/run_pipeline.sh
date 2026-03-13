#!/bin/bash
# ─────────────────────────────────────────────────────────────
# E-Commerce Data Platform — One-Command Pipeline Runner
#
# Starts the entire platform, waits for services to be healthy,
# runs dbt models, and validates data quality.
#
# Usage:
#   chmod +x run_pipeline.sh
#   ./run_pipeline.sh              # full run
#   ./run_pipeline.sh --dbt-only   # skip Docker, just run dbt
#   ./run_pipeline.sh --health     # just run health check
# ─────────────────────────────────────────────────────────────

set -e  # exit immediately on any error

# ── Colors ───────────────────────────────────────────
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # no color

# ── Helpers ──────────────────────────────────────────
log()     { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; exit 1; }

# ── Argument handling ─────────────────────────────────
DBT_ONLY=false
HEALTH_ONLY=false

for arg in "$@"; do
  case $arg in
    --dbt-only)   DBT_ONLY=true ;;
    --health)     HEALTH_ONLY=true ;;
  esac
done

# ─────────────────────────────────────────────────────
# Health check only mode
# ─────────────────────────────────────────────────────
if [ "$HEALTH_ONLY" = true ]; then
  log "Running health check only..."
  python phase-5-orchestration/monitoring/pipeline_health.py
  exit $?
fi

# ─────────────────────────────────────────────────────
# STEP 1 — Start Docker services
# ─────────────────────────────────────────────────────
if [ "$DBT_ONLY" = false ]; then
  log "Starting Docker services..."
  docker-compose up -d

  # Wait for PostgreSQL to be healthy
  log "Waiting for PostgreSQL to be ready..."
  RETRIES=30
  until docker exec ecommerce_oltp pg_isready -U deuser -d ecommerce_db > /dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ $RETRIES -eq 0 ]; then
      error "PostgreSQL did not become healthy in time"
    fi
    echo -n "."
    sleep 2
  done
  echo ""
  success "PostgreSQL is ready"

  # Wait for Airflow webserver
  log "Waiting for Airflow to be ready..."
  RETRIES=30
  until curl -s http://localhost:8080/health > /dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ $RETRIES -eq 0 ]; then
      warn "Airflow not ready yet — continuing anyway"
      break
    fi
    echo -n "."
    sleep 3
  done
  echo ""
  success "All services started"
fi

# ─────────────────────────────────────────────────────
# STEP 2 — Run dbt models
# ─────────────────────────────────────────────────────
log "Running dbt transformations..."

docker exec ecommerce_dbt dbt run
if [ $? -ne 0 ]; then
  error "dbt run failed — check model errors above"
fi
success "dbt models completed"

# ─────────────────────────────────────────────────────
# STEP 3 — Run dbt tests
# ─────────────────────────────────────────────────────
log "Running dbt data quality tests..."

docker exec ecommerce_dbt dbt test
if [ $? -ne 0 ]; then
  warn "Some dbt tests failed — review above"
else
  success "All dbt tests passed"
fi

# ─────────────────────────────────────────────────────
# STEP 4 — Run Great Expectations checks
# ─────────────────────────────────────────────────────
log "Running Great Expectations data quality checks..."

python phase-5-orchestration/great_expectations/expectations.py
if [ $? -ne 0 ]; then
  warn "Some data quality checks failed — review above"
else
  success "All data quality checks passed"
fi

# ─────────────────────────────────────────────────────
# STEP 5 — Run pipeline health check
# ─────────────────────────────────────────────────────
log "Running pipeline health check..."
python phase-5-orchestration/monitoring/pipeline_health.py

# ─────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo -e "${GREEN}  🎉 Pipeline run complete!${NC}"
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo ""
echo "  Airflow UI  → http://localhost:8080  (admin / admin123)"
echo "  pgAdmin     → http://localhost:5050  (admin@admin.com / admin123)"
echo "  Kafka UI    → http://localhost:8090"
echo ""