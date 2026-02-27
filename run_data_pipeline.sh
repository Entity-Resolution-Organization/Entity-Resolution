#!/bin/bash
# =============================================================================
# run_data_pipeline.sh — Trigger Airflow DAG and auto-commit DVC tracking files
# =============================================================================
# Location: ENTITY-RESOLUTION/run_data_pipeline.sh
# Usage:    ./run_data_pipeline.sh
#
# What this does:
#   1. Triggers the Airflow DAG (airflow_data_pipeline)
#   2. Waits for it to complete
#   3. Commits any new/changed .dvc files to Git
#   4. Optionally pushes to remote Git repo
# =============================================================================

set -euo pipefail

# --- Config ---
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PIPELINE_DIR="${REPO_ROOT}/Data-Pipeline"
DAG_ID="er_data_pipeline"
POLL_INTERVAL=30  # seconds between status checks

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()  { echo -e "${GREEN}[Pipeline]${NC} $1"; }
warn() { echo -e "${YELLOW}[Pipeline]${NC} $1"; }
err()  { echo -e "${RED}[Pipeline]${NC} $1"; }

# --- Step 1: Trigger the DAG ---
log "Triggering Airflow DAG: ${DAG_ID}..."

RUN_ID="manual__$(date +%Y-%m-%dT%H-%M-%S)"

docker compose -f "${PIPELINE_DIR}/docker-compose.yml" exec -T \
    airflow-webserver airflow dags trigger "${DAG_ID}" \
    --run-id "${RUN_ID}" 2>/dev/null

if [ $? -ne 0 ]; then
    err "Failed to trigger DAG. Is Airflow running?"
    err "  Start with: cd Data-Pipeline && docker compose up -d"
    exit 1
fi

log "DAG triggered with run_id: ${RUN_ID}"

# --- Step 2: Wait for DAG to complete ---
log "Waiting for pipeline to complete (polling every ${POLL_INTERVAL}s)..."

while true; do
    STATE=$(docker compose -f "${PIPELINE_DIR}/docker-compose.yml" exec -T \
        airflow-webserver airflow dags list-runs \
        -d "${DAG_ID}" --no-backfill -o json 2>/dev/null \
        | python3 -c "
import sys, json
runs = json.load(sys.stdin)
for r in runs:
    if r.get('run_id') == '${RUN_ID}':
        print(r.get('state', 'unknown'))
        break
" 2>/dev/null || echo "unknown")

    case "${STATE}" in
        success)
            log "Pipeline completed successfully!"
            break
            ;;
        failed)
            err "Pipeline FAILED. Check Airflow UI at http://localhost:8080"
            exit 1
            ;;
        running|queued)
            echo -n "."
            sleep "${POLL_INTERVAL}"
            ;;
        *)
            warn "Unknown state: ${STATE}. Retrying..."
            sleep "${POLL_INTERVAL}"
            ;;
    esac
done

# --- Step 3: Commit DVC tracking files ---
log "Checking for DVC changes to commit..."

cd "${REPO_ROOT}"

# Stage any new or changed .dvc files and .gitignore updates
git add \
    Data-Pipeline/data/*.dvc \
    Data-Pipeline/data/.gitignore \
    Data-Pipeline/data/**/*.dvc \
    2>/dev/null || true

# Check if there's anything to commit
if git diff --cached --quiet 2>/dev/null; then
    log "No DVC changes to commit (data unchanged from last run)"
else
    TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
    
    # Get total data size from version info if available
    SIZE_INFO=""
    VERSION_FILE="${PIPELINE_DIR}/data/metrics/dvc_version_info.json"
    if [ -f "${VERSION_FILE}" ]; then
        SIZE_INFO=$(python3 -c "
import json
with open('${VERSION_FILE}') as f:
    info = json.load(f)
print(f\"Total Size: {info.get('total_size_mb', '?')} MB\")
print(f\"Push Status: {info.get('dvc_push_status', '?')}\")
print(f\"Items: {len(info.get('tracked_items', []))}\")
" 2>/dev/null || echo "")
    fi

    COMMIT_MSG="DVC: auto-version pipeline outputs

Run ID: ${RUN_ID}
Timestamp: ${TIMESTAMP}
${SIZE_INFO}

Automated by run_pipeline.sh"

    git commit -m "${COMMIT_MSG}"
    log "Git commit created"

    # --- Step 4: Push to remote (optional) ---
    # Uncomment the following lines to auto-push to GitHub/GitLab
    # log "Pushing to remote..."
    # git push origin HEAD
    # log "Pushed to remote"
fi

# --- Summary ---
echo ""
echo "=========================================="
log "PIPELINE RUN COMPLETE"
echo "=========================================="
log "  Run ID:    ${RUN_ID}"
log "  DAG:       ${DAG_ID}"
log "  DVC Data:  gs://entity-resolution-dvc-bucket"
log "  Git:       $(git rev-parse --short HEAD)"
echo "=========================================="