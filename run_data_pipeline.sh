#!/bin/bash
# =============================================================================
# run_data_pipeline.sh — Trigger Airflow DAG and auto-commit DVC tracking files
# =============================================================================
# Location: ENTITY-RESOLUTION/run_data_pipeline.sh
# Usage:    ./run_data_pipeline.sh
# =============================================================================

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PIPELINE_DIR="${REPO_ROOT}/Data-Pipeline"
COMPOSE="docker compose -f ${PIPELINE_DIR}/docker-compose.yml"
DAG_ID="er_data_pipeline"
POLL_INTERVAL=30

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[Pipeline]${NC} $1"; }
warn() { echo -e "${YELLOW}[Pipeline]${NC} $1"; }
err()  { echo -e "${RED}[Pipeline]${NC} $1" >&2; }

# --- Preflight checks ---
if ! ${COMPOSE} ps --status running 2>/dev/null | grep -q "airflow"; then
    err "Airflow is not running."
    err "Start with: cd Data-Pipeline && docker compose up -d"
    exit 1
fi

# --- Step 1: Trigger the DAG ---
RUN_ID="manual__$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"
log "Triggering DAG: ${DAG_ID}"
log "Run ID: ${RUN_ID}"

if ! ${COMPOSE} exec -T airflow-scheduler \
    airflow dags trigger "${DAG_ID}" --run-id "${RUN_ID}"; then
    err "Failed to trigger DAG."
    exit 1
fi

# --- Step 2: Wait for completion ---
log "Waiting for pipeline to complete (polling every ${POLL_INTERVAL}s)..."

while true; do
    STATE=$(${COMPOSE} exec -T airflow-scheduler \
        airflow dags list-runs -d "${DAG_ID}" --no-backfill -o json 2>/dev/null \
        | python3 -c "
import sys, json
try:
    runs = json.load(sys.stdin)
    for r in runs:
        if r.get('run_id') == '${RUN_ID}':
            print(r.get('state', 'unknown'))
            sys.exit(0)
    print('not_found')
except:
    print('error')
" 2>/dev/null || echo "error")

    case "${STATE}" in
        success)
            log "Pipeline completed successfully!"
            break
            ;;
        failed)
            err "Pipeline FAILED. Check Airflow UI: http://localhost:8080"
            exit 1
            ;;
        running|queued)
            echo -n "."
            sleep "${POLL_INTERVAL}"
            ;;
        not_found)
            warn "Run not found yet, retrying..."
            sleep "${POLL_INTERVAL}"
            ;;
        *)
            warn "State: ${STATE}. Retrying..."
            sleep "${POLL_INTERVAL}"
            ;;
    esac
done

# --- Step 3: Commit DVC tracking files ---
log "Checking for DVC changes..."
cd "${REPO_ROOT}"

git add \
    Data-Pipeline/data/*.dvc \
    Data-Pipeline/data/.gitignore \
    2>/dev/null || true

if git diff --cached --quiet 2>/dev/null; then
    log "No DVC changes to commit."
else
    COMMIT_MSG="DVC: version pipeline run ${RUN_ID}"
    git commit -m "${COMMIT_MSG}"
    log "Committed DVC changes."

    # Uncomment to auto-push:
    # git push origin HEAD
fi

# --- Summary ---
echo ""
echo "=========================================="
log "PIPELINE RUN COMPLETE"
echo "=========================================="
log "  Run ID: ${RUN_ID}"
log "  Git:    $(git rev-parse --short HEAD)"
echo "=========================================="