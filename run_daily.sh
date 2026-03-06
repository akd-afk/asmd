#!/bin/bash
# =============================================================================
# run_daily.sh — ASMDU Daily Collection & Report Cron Wrapper
# =============================================================================
# Schedule via cron, e.g. run at 06:00 every morning:
#   0 6 * * * /opt/oracle/asmdu/run_daily.sh >> /var/log/asmdu.log 2>&1
#
# Exit codes:
#   0 — All steps completed (even if some hosts had partial failures).
#   1 — Fatal error (playbook not found, NAS not mounted, etc.).
# =============================================================================

set -uo pipefail

# =============================================================================
# CONFIGURATION — adjust these to match your environment
# =============================================================================
BASE_DIR="/opt/oracle/asmdu"
NAS_RUNS_ROOT="/mnt/asm-capacity/runs"
INVENTORY="${BASE_DIR}/inventory.ini"
PLAYBOOK="${BASE_DIR}/collect.yml"
REPORT_SCRIPT="${BASE_DIR}/build_html_report.py"
PYTHON3="/usr/bin/python3"
ANSIBLE_PLAYBOOK="/usr/bin/ansible-playbook"
LOOKBACK_DAYS=30   # days of history used for growth trend analysis

# Optional: send a summary email (requires mailx/sendmail on controller)
SEND_EMAIL=false
EMAIL_TO="dba-team@yourcompany.com"
EMAIL_FROM="asmdu-noreply@yourcompany.com"

# =============================================================================
# SETUP
# =============================================================================
RUN_DATE=$(date +%F)
LOG_PREFIX="[ASMDU ${RUN_DATE}]"
REPORT_PATH="${NAS_RUNS_ROOT}/${RUN_DATE}/report.html"
CSV_PATH="${NAS_RUNS_ROOT}/${RUN_DATE}/report.csv"

log() { echo "${LOG_PREFIX} $*"; }
die() { echo "${LOG_PREFIX} FATAL: $*" >&2; exit 1; }

# =============================================================================
# PRE-FLIGHT CHECKS
# =============================================================================
log "Starting ASMDU daily collection run."

cd "${BASE_DIR}" || die "Cannot cd to BASE_DIR: ${BASE_DIR}"

[[ -f "${PLAYBOOK}" ]]       || die "Playbook not found: ${PLAYBOOK}"
[[ -f "${INVENTORY}" ]]      || die "Inventory not found: ${INVENTORY}"
[[ -f "${REPORT_SCRIPT}" ]]  || die "Report script not found: ${REPORT_SCRIPT}"
[[ -x "${ANSIBLE_PLAYBOOK}" ]] || die "ansible-playbook not executable at: ${ANSIBLE_PLAYBOOK}"
[[ -x "${PYTHON3}" ]]          || die "python3 not executable at: ${PYTHON3}"

# Verify NAS is mounted and writable
if ! mkdir -p "${NAS_RUNS_ROOT}" 2>/dev/null; then
    die "NAS root is not writable or not mounted: ${NAS_RUNS_ROOT}"
fi

# =============================================================================
# STEP 1 — ANSIBLE COLLECTION
# =============================================================================
log "Step 1/2: Running Ansible playbook..."

# Capture exit code but don't abort — partial host failures are expected.
# The playbook uses any_errors_fatal: false; Ansible returns 0 on partial failure.
ANSIBLE_FORCE_COLOR=0 \
  "${ANSIBLE_PLAYBOOK}" \
  -i "${INVENTORY}" \
  "${PLAYBOOK}" \
  --forks 30 \
  2>&1
PLAYBOOK_RC=$?

if [[ ${PLAYBOOK_RC} -ne 0 ]]; then
    log "WARNING: Ansible playbook exited with RC=${PLAYBOOK_RC}. Some hosts may have failed. Continuing to report."
fi

# =============================================================================
# STEP 2 — BUILD REPORT
# =============================================================================
log "Step 2/2: Building HTML report (lookback=${LOOKBACK_DAYS}d)..."

"${PYTHON3}" "${REPORT_SCRIPT}" \
    "${NAS_RUNS_ROOT}" \
    "${RUN_DATE}" \
    --lookback "${LOOKBACK_DAYS}" \
    2>&1
REPORT_RC=$?

if [[ ${REPORT_RC} -ne 0 ]]; then
    log "ERROR: Report builder failed with RC=${REPORT_RC}."
else
    log "Report available at: ${REPORT_PATH}"
    log "CSV export at:       ${CSV_PATH}"
fi

# =============================================================================
# STEP 3 — OPTIONAL EMAIL NOTIFICATION
# =============================================================================
if [[ "${SEND_EMAIL}" == "true" ]] && [[ ${REPORT_RC} -eq 0 ]]; then
    log "Sending summary email to ${EMAIL_TO}..."

    # Count alerts by grepping the status files
    CRIT_COUNT=$(grep -rl "overall_ok=False" "${NAS_RUNS_ROOT}/${RUN_DATE}" 2>/dev/null | wc -l)

    SUBJECT="[ASMDU] ASM Capacity Report — ${RUN_DATE}"
    if [[ ${CRIT_COUNT} -gt 0 ]]; then
        SUBJECT="[ASMDU][ACTION REQUIRED] ASM Capacity — ${CRIT_COUNT} host(s) need attention — ${RUN_DATE}"
    fi

    BODY="ASMDU Daily Capacity Report
Date:   ${RUN_DATE}
Report: ${REPORT_PATH}
CSV:    ${CSV_PATH}

Playbook RC: ${PLAYBOOK_RC}
Report RC:   ${REPORT_RC}
"
    echo "${BODY}" | mailx -s "${SUBJECT}" \
        -r "${EMAIL_FROM}" \
        "${EMAIL_TO}" 2>/dev/null || log "WARNING: Email delivery failed (non-fatal)."
fi

# =============================================================================
# DONE
# =============================================================================
log "DONE. Playbook RC=${PLAYBOOK_RC}, Report RC=${REPORT_RC}."
exit 0
