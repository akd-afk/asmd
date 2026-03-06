#!/usr/bin/env python3
# =============================================================================
# build_html_report.py — ASMDU Capacity Report Builder with Growth Tracking
# =============================================================================
# Reads daily ASMDU collection output from NAS, parses raw text into structured
# data, computes day-over-day and week-over-week growth, and produces a
# self-contained HTML dashboard with capacity planning projections.
#
# Usage:
#   python3 build_html_report.py <nas_runs_root> <YYYY-MM-DD> [--lookback N]
#
# Arguments:
#   nas_runs_root  — NAS root containing dated run folders
#   YYYY-MM-DD     — The target run date to report on
#   --lookback N   — Number of past days to include in trend analysis (default 30)
#
# Output:
#   <nas_runs_root>/<YYYY-MM-DD>/report.html   — main dashboard
#   <nas_runs_root>/<YYYY-MM-DD>/report.csv    — machine-readable summary
# =============================================================================

import os
import sys
import re
import glob
import json
import html
import datetime
import argparse
import csv
from collections import defaultdict
from typing import Optional

# =============================================================================
# CONSTANTS
# =============================================================================
CAPACITY_WARN_PCT  = 75.0   # diskgroup % used → yellow warning
CAPACITY_CRIT_PCT  = 85.0   # diskgroup % used → red critical
DAYS_TO_FULL_WARN  = 90     # days-to-full below this → flag as concern
DAYS_TO_FULL_CRIT  = 30     # days-to-full below this → flag as critical

# =============================================================================
# PARSING
# =============================================================================

def read_file(path: str) -> str:
    """Read a file safely, returning an empty string on any error."""
    try:
        with open(path, "r", errors="replace") as f:
            return f.read()
    except Exception:
        return ""


def parse_dg_summary(text: str) -> list[dict]:
    """
    Parse asmdu TB summary output into a list of diskgroup dicts.

    asmdu output lines look like (columns may vary slightly):
      DG_NAME   TOTAL_TB   FREE_TB   USED_TB   USABLE_TB   %USED
      --------  ---------  --------  --------  ----------  ------
      +DATA         10.00     3.50      6.50       1.75     65.00
      +RECO          5.00     4.20      0.80       2.10     16.00

    The parser is regex-driven and tolerant of whitespace/header variations.
    """
    results = []
    # Match lines that start with an optional + followed by a DG name,
    # then 4-6 numeric columns.
    pattern = re.compile(
        r'^\s*'
        r'(\+?\w[\w\-_]*)'          # DG name (group 1)
        r'\s+([\d,]+\.?\d*)'        # TOTAL (group 2)
        r'\s+([\d,]+\.?\d*)'        # FREE  (group 3)
        r'\s+([\d,]+\.?\d*)'        # USED  (group 4)
        r'(?:\s+([\d,]+\.?\d*))?'   # USABLE (optional, group 5)
        r'\s+([\d,]+\.?\d*)',       # %USED (group 6)
        re.MULTILINE
    )
    for m in pattern.finditer(text):
        name      = m.group(1).lstrip('+').upper()
        total_tb  = _float(m.group(2))
        free_tb   = _float(m.group(3))
        used_tb   = _float(m.group(4))
        usable_tb = _float(m.group(5)) if m.group(5) else None
        pct_used  = _float(m.group(6))

        # Sanity-check: skip header/separator rows that accidentally match
        if total_tb == 0.0 and free_tb == 0.0:
            continue

        # Recompute pct_used from raw numbers if parser grabbed a bad column
        if total_tb > 0 and not (0.0 <= pct_used <= 100.0):
            pct_used = round((used_tb / total_tb) * 100, 2)

        results.append({
            "dg":        name,
            "total_tb":  total_tb,
            "free_tb":   free_tb,
            "used_tb":   used_tb,
            "usable_tb": usable_tb,
            "pct_used":  pct_used,
        })
    return results


def _float(val: Optional[str]) -> float:
    """Convert a string (possibly with commas) to float, return 0.0 on failure."""
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(",", ""))
    except ValueError:
        return 0.0


# =============================================================================
# HISTORICAL DATA & GROWTH ANALYSIS
# =============================================================================

def load_history(nas_root: str, host: str, run_date: str, lookback: int) -> list[dict]:
    """
    Walk the past `lookback` days of NAS run folders and collect parsed
    DG summary data for a given host. Returns a list of dicts:
      { date, dg, total_tb, used_tb, free_tb, pct_used }
    sorted by date ascending.
    """
    today      = datetime.date.fromisoformat(run_date)
    history    = []

    for delta in range(lookback, 0, -1):
        past_date = (today - datetime.timedelta(days=delta)).isoformat()
        summary_path = os.path.join(nas_root, past_date, host, "dg_summary.txt")
        if not os.path.isfile(summary_path):
            continue
        text = read_file(summary_path)
        parsed = parse_dg_summary(text)
        for row in parsed:
            history.append({**row, "date": past_date})

    return history


def compute_growth(history: list[dict]) -> dict[str, dict]:
    """
    For each diskgroup found in history, compute:
      - DoD growth (TB/day) using linear regression over the window
      - WoW growth  (7-day delta)
      - MoM growth  (30-day delta, approximate)
      - days_to_full projection
      - growth_rate_tb_per_day (regression slope)

    Returns { dg_name: { ... stats ... } }
    """
    # Group by DG
    by_dg: dict[str, list] = defaultdict(list)
    for row in history:
        by_dg[row["dg"]].append(row)

    results = {}
    for dg, rows in by_dg.items():
        rows_sorted = sorted(rows, key=lambda r: r["date"])
        if len(rows_sorted) < 2:
            results[dg] = _empty_growth()
            continue

        dates_ord = [
            datetime.date.fromisoformat(r["date"]).toordinal()
            for r in rows_sorted
        ]
        used_vals = [r["used_tb"] for r in rows_sorted]
        free_vals = [r["free_tb"] for r in rows_sorted]

        slope = _linreg_slope(dates_ord, used_vals)

        # WoW: compare latest vs 7 days ago (or closest available)
        wow = _delta_over_days(rows_sorted, 7, "used_tb")
        mom = _delta_over_days(rows_sorted, 30, "used_tb")

        # Days to full: free_now / daily_growth_rate
        latest = rows_sorted[-1]
        if slope > 0:
            days_to_full = round(latest["free_tb"] / slope)
        else:
            days_to_full = None  # not growing or shrinking

        projected_date = None
        if days_to_full is not None:
            proj_ord = datetime.date.fromisoformat(latest["date"]).toordinal() + days_to_full
            try:
                projected_date = datetime.date.fromordinal(proj_ord).isoformat()
            except (OverflowError, ValueError):
                projected_date = "beyond range"

        results[dg] = {
            "growth_rate_tb_per_day": round(slope, 4),
            "growth_rate_tb_per_week": round(slope * 7, 3),
            "growth_rate_tb_per_month": round(slope * 30, 2),
            "wow_delta_tb": wow,
            "mom_delta_tb": mom,
            "days_to_full": days_to_full,
            "projected_full_date": projected_date,
            "data_points": len(rows_sorted),
            "history": rows_sorted,
        }
    return results


def _empty_growth() -> dict:
    return {
        "growth_rate_tb_per_day": None,
        "growth_rate_tb_per_week": None,
        "growth_rate_tb_per_month": None,
        "wow_delta_tb": None,
        "mom_delta_tb": None,
        "days_to_full": None,
        "projected_full_date": None,
        "data_points": 0,
        "history": [],
    }


def _linreg_slope(x: list[float], y: list[float]) -> float:
    """Ordinary least-squares slope (dy/dx)."""
    n = len(x)
    if n < 2:
        return 0.0
    sx  = sum(x)
    sy  = sum(y)
    sxy = sum(xi * yi for xi, yi in zip(x, y))
    sxx = sum(xi * xi for xi in x)
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0
    return (n * sxy - sx * sy) / denom


def _delta_over_days(rows: list[dict], days: int, field: str) -> Optional[float]:
    """Return (latest - N-days-ago) delta for `field`, or None if not enough history."""
    if len(rows) < 2:
        return None
    latest_date = datetime.date.fromisoformat(rows[-1]["date"])
    target_date = latest_date - datetime.timedelta(days=days)
    candidates  = [r for r in rows if datetime.date.fromisoformat(r["date"]) <= target_date]
    if not candidates:
        return None
    ref = candidates[-1]
    return round(rows[-1][field] - ref[field], 3)


# =============================================================================
# SEVERITY HELPERS
# =============================================================================

def pct_severity(pct: float) -> str:
    if pct >= CAPACITY_CRIT_PCT:
        return "crit"
    elif pct >= CAPACITY_WARN_PCT:
        return "warn"
    return "ok"


def dtf_severity(days: Optional[int]) -> str:
    if days is None:
        return "ok"
    if days <= DAYS_TO_FULL_CRIT:
        return "crit"
    elif days <= DAYS_TO_FULL_WARN:
        return "warn"
    return "ok"


# =============================================================================
# DISCOVERY
# =============================================================================

def discover_hosts(nas_root: str, run_date: str) -> tuple[str, list[str]]:
    base = os.path.join(nas_root, run_date)
    if not os.path.isdir(base):
        raise SystemExit(f"[ERROR] Run folder not found: {base}")
    hosts = sorted(
        d for d in os.listdir(base)
        if os.path.isdir(os.path.join(base, d))
    )
    return base, hosts


def load_meta(host_dir: str) -> dict:
    meta_path = os.path.join(host_dir, "meta.json")
    try:
        with open(meta_path) as f:
            return json.load(f)
    except Exception:
        return {}


# =============================================================================
# CSV EXPORT
# =============================================================================

def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    fieldnames = [
        "date", "host", "platform", "dg",
        "total_tb", "used_tb", "free_tb", "pct_used",
        "growth_tb_per_day", "growth_tb_per_week", "growth_tb_per_month",
        "wow_delta_tb", "mom_delta_tb",
        "days_to_full", "projected_full_date",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# =============================================================================
# HTML REPORT BUILDER
# =============================================================================

def build_sparkline_data(history: list[dict], field: str = "pct_used") -> str:
    """Return comma-separated values for a mini-sparkline."""
    vals = [str(round(r[field], 1)) for r in history[-30:]]
    return ",".join(vals)


def fmt_tb(val: Optional[float]) -> str:
    if val is None:
        return "—"
    return f"{val:,.2f} TB"


def fmt_delta(val: Optional[float]) -> str:
    if val is None:
        return "—"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:,.3f} TB"


def fmt_days(val: Optional[int]) -> str:
    if val is None:
        return "∞ (stable)"
    if val < 0:
        return "∞ (shrinking)"
    return f"{val:,} days"


def build_report(
    nas_root:  str,
    run_date:  str,
    lookback:  int,
) -> tuple[str, list[dict]]:
    """
    Main builder. Returns (html_string, csv_rows).
    """
    base, hosts = discover_hosts(nas_root, run_date)
    now_str     = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    title       = f"ASM Capacity Report — {run_date}"

    # -------------------------------------------------------------------------
    # Collect all data
    # -------------------------------------------------------------------------
    host_data  = []
    csv_rows   = []
    all_alerts = []   # (severity, host, dg, message)

    for host in hosts:
        hdir    = os.path.join(base, host)
        meta    = load_meta(hdir)
        platform = meta.get("platform", "unknown")
        summary_text = read_file(os.path.join(hdir, "dg_summary.txt"))
        summary_err  = read_file(os.path.join(hdir, "dg_summary.err"))
        dg_parsed    = parse_dg_summary(summary_text)

        # Per-DG root subdir files
        subdir_files = sorted(glob.glob(os.path.join(hdir, "*_root_subdirs.txt")))
        subdir_data  = [(os.path.basename(p), read_file(p)) for p in subdir_files]

        # Historical growth
        history     = load_history(nas_root, host, run_date, lookback)
        growth_map  = compute_growth(history)

        enriched_dgs = []
        for dg in dg_parsed:
            g    = growth_map.get(dg["dg"], _empty_growth())
            sev  = pct_severity(dg["pct_used"])
            dsev = dtf_severity(g["days_to_full"])

            if sev == "crit" or dsev == "crit":
                all_alerts.append(("crit", host, dg["dg"],
                    f"CRITICAL: {dg['dg']} is {dg['pct_used']:.1f}% full"
                    + (f" — full in {g['days_to_full']} days" if g['days_to_full'] else "")))
            elif sev == "warn" or dsev == "warn":
                all_alerts.append(("warn", host, dg["dg"],
                    f"WARNING: {dg['dg']} is {dg['pct_used']:.1f}% full"
                    + (f" — full in {g['days_to_full']} days" if g['days_to_full'] else "")))

            enriched_dgs.append({**dg, "growth": g, "sev": sev, "dsev": dsev})

            # CSV row
            csv_rows.append({
                "date":                 run_date,
                "host":                 host,
                "platform":             platform,
                "dg":                   dg["dg"],
                "total_tb":             dg["total_tb"],
                "used_tb":              dg["used_tb"],
                "free_tb":              dg["free_tb"],
                "pct_used":             dg["pct_used"],
                "growth_tb_per_day":    g["growth_rate_tb_per_day"],
                "growth_tb_per_week":   g["growth_rate_tb_per_week"],
                "growth_tb_per_month":  g["growth_rate_tb_per_month"],
                "wow_delta_tb":         g["wow_delta_tb"],
                "mom_delta_tb":         g["mom_delta_tb"],
                "days_to_full":         g["days_to_full"],
                "projected_full_date":  g["projected_full_date"],
            })

        host_data.append({
            "host":        host,
            "platform":    platform,
            "meta":        meta,
            "dg_parsed":   enriched_dgs,
            "summary_raw": summary_text,
            "summary_err": summary_err,
            "subdir_data": subdir_data,
            "status_ok":   meta.get("ansible_facts_ok", False),
        })

    # -------------------------------------------------------------------------
    # Compute fleet-level totals
    # -------------------------------------------------------------------------
    fleet_total = sum(
        dg["total_tb"]
        for h in host_data for dg in h["dg_parsed"]
    )
    fleet_used = sum(
        dg["used_tb"]
        for h in host_data for dg in h["dg_parsed"]
    )
    fleet_free  = fleet_total - fleet_used
    fleet_pct   = round((fleet_used / fleet_total) * 100, 1) if fleet_total else 0

    # =========================================================================
    # HTML OUTPUT
    # =========================================================================
    H = []   # html accumulator

    def w(*parts):
        H.extend(parts)

    # -------------------------------------------------------------------------
    # HEAD + STYLES
    # -------------------------------------------------------------------------
    w(f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title>
<style>
/* ============================================================
   DESIGN: Industrial/Utilitarian data dashboard.
   Deep navy base, amber accents, monospace data type,
   clean grid layout with hard borders.
   ============================================================ */

@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@400;600;700;900&display=swap');

:root {{
  --navy:    #0d1b2a;
  --navy2:   #132233;
  --navy3:   #1a2e42;
  --amber:   #f0a500;
  --amber2:  #ffbe33;
  --teal:    #00b4d8;
  --ok:      #2ecc71;
  --warn:    #f39c12;
  --crit:    #e74c3c;
  --text:    #d4e0ec;
  --muted:   #6b8299;
  --border:  #1f3349;
  --font-mono: 'Share Tech Mono', 'Courier New', monospace;
  --font-ui:   'Barlow', Arial, sans-serif;
}}

*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

body {{
  background: var(--navy);
  color: var(--text);
  font-family: var(--font-ui);
  font-size: 14px;
  line-height: 1.6;
  padding: 0;
}}

/* ---- Page header ---- */
.page-header {{
  background: var(--navy2);
  border-bottom: 2px solid var(--amber);
  padding: 20px 28px 16px;
  display: flex;
  align-items: baseline;
  gap: 20px;
  flex-wrap: wrap;
}}
.page-header h1 {{
  font-family: var(--font-ui);
  font-weight: 900;
  font-size: 22px;
  letter-spacing: 0.04em;
  color: var(--amber);
  text-transform: uppercase;
}}
.page-header .meta {{
  font-size: 12px;
  color: var(--muted);
  font-family: var(--font-mono);
  margin-left: auto;
}}

/* ---- Alert banner ---- */
.alert-bar {{
  background: #1a1000;
  border-bottom: 1px solid var(--warn);
  padding: 10px 28px;
}}
.alert-bar.crit {{ border-color: var(--crit); background: #1a0808; }}
.alert-item {{
  font-size: 12px;
  font-family: var(--font-mono);
  padding: 3px 0;
}}
.alert-item.crit {{ color: var(--crit); }}
.alert-item.warn {{ color: var(--warn); }}

/* ---- Fleet summary strip ---- */
.fleet-strip {{
  background: var(--navy3);
  border-bottom: 1px solid var(--border);
  padding: 12px 28px;
  display: flex;
  gap: 40px;
  flex-wrap: wrap;
  align-items: center;
}}
.fleet-kpi {{
  display: flex;
  flex-direction: column;
  gap: 2px;
}}
.fleet-kpi .label {{
  font-size: 10px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--muted);
}}
.fleet-kpi .value {{
  font-family: var(--font-mono);
  font-size: 20px;
  color: var(--amber2);
}}
.fleet-kpi .value.ok   {{ color: var(--ok); }}
.fleet-kpi .value.warn {{ color: var(--warn); }}
.fleet-kpi .value.crit {{ color: var(--crit); }}

/* ---- Fleet progress bar ---- */
.fleet-bar-wrap {{
  flex: 1;
  min-width: 200px;
}}
.bar-outer {{
  height: 10px;
  background: var(--navy);
  border: 1px solid var(--border);
  border-radius: 3px;
  overflow: hidden;
  margin-top: 4px;
}}
.bar-inner {{
  height: 100%;
  background: var(--ok);
  transition: width 0.5s;
  border-radius: 2px;
}}
.bar-inner.warn {{ background: var(--warn); }}
.bar-inner.crit {{ background: var(--crit); }}

/* ---- Main content ---- */
.content {{
  padding: 20px 28px;
}}

/* ---- Host card ---- */
.host-card {{
  border: 1px solid var(--border);
  border-radius: 4px;
  margin-bottom: 20px;
  background: var(--navy2);
  overflow: hidden;
}}
.host-header {{
  background: var(--navy3);
  border-bottom: 1px solid var(--border);
  padding: 12px 16px;
  display: flex;
  align-items: center;
  gap: 12px;
  cursor: pointer;
  user-select: none;
}}
.host-header:hover {{ background: #1e3550; }}
.host-name {{
  font-weight: 700;
  font-size: 16px;
  color: var(--teal);
  font-family: var(--font-mono);
}}
.host-platform {{
  font-size: 11px;
  background: var(--navy);
  border: 1px solid var(--border);
  border-radius: 2px;
  padding: 2px 7px;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}}
.host-toggle {{
  margin-left: auto;
  color: var(--muted);
  font-size: 18px;
  transition: transform 0.25s;
}}
.host-body {{ display: block; padding: 16px; }}
.host-body.collapsed {{ display: none; }}

/* ---- DG Table ---- */
.section-label {{
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--muted);
  margin-bottom: 8px;
  margin-top: 16px;
}}
.section-label:first-child {{ margin-top: 0; }}

table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  font-family: var(--font-mono);
}}
thead tr {{
  background: var(--navy);
  border-bottom: 2px solid var(--border);
}}
thead th {{
  padding: 7px 10px;
  text-align: right;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--muted);
  white-space: nowrap;
}}
thead th:first-child {{ text-align: left; }}

tbody tr {{
  border-bottom: 1px solid var(--border);
  transition: background 0.15s;
}}
tbody tr:hover {{ background: var(--navy3); }}
td {{
  padding: 8px 10px;
  text-align: right;
  vertical-align: middle;
  white-space: nowrap;
}}
td:first-child {{ text-align: left; }}

/* Severity colouring */
.sev-ok   {{ color: var(--ok); }}
.sev-warn {{ color: var(--warn); }}
.sev-crit {{ color: var(--crit); font-weight: 700; }}

/* DG name cell */
.dg-name {{
  font-weight: 700;
  color: var(--text);
}}

/* Inline usage bar inside table */
.usage-cell {{ min-width: 120px; }}
.usage-wrap {{ display: flex; align-items: center; gap: 6px; }}
.usage-bar {{
  flex: 1;
  height: 6px;
  background: var(--navy);
  border: 1px solid var(--border);
  border-radius: 2px;
  overflow: hidden;
}}
.usage-fill {{
  height: 100%;
  background: var(--ok);
  border-radius: 1px;
}}
.usage-fill.warn {{ background: var(--warn); }}
.usage-fill.crit {{ background: var(--crit); }}
.usage-pct {{
  font-size: 12px;
  width: 42px;
  text-align: right;
}}

/* ---- Raw output collapsible ---- */
.raw-toggle {{
  font-size: 11px;
  color: var(--muted);
  cursor: pointer;
  border: 1px solid var(--border);
  background: none;
  padding: 3px 10px;
  border-radius: 2px;
  margin-top: 4px;
  transition: color 0.15s;
}}
.raw-toggle:hover {{ color: var(--text); }}
pre.raw-output {{
  background: var(--navy);
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 12px;
  overflow-x: auto;
  font-family: var(--font-mono);
  font-size: 12px;
  color: #8fa8bf;
  margin-top: 8px;
  white-space: pre;
  display: none;
}}

/* ---- Sparkline canvas ---- */
canvas.spark {{
  vertical-align: middle;
  margin-left: 4px;
}}

/* ---- Footer ---- */
.footer {{
  text-align: center;
  padding: 24px;
  font-size: 11px;
  color: var(--muted);
  border-top: 1px solid var(--border);
  font-family: var(--font-mono);
}}

/* ---- Responsive ---- */
@media (max-width: 700px) {{
  .fleet-strip {{ gap: 16px; }}
  table {{ font-size: 11px; }}
  td, th {{ padding: 6px 6px; }}
}}
</style>
</head>
<body>
""")

    # -------------------------------------------------------------------------
    # PAGE HEADER
    # -------------------------------------------------------------------------
    w(f"""<header class="page-header">
  <h1>⬡ ASM Capacity Dashboard</h1>
  <div style="color:var(--teal);font-family:var(--font-mono);font-size:14px">{html.escape(run_date)}</div>
  <div class="meta">Generated: {html.escape(now_str)} &nbsp;|&nbsp; Hosts: {len(hosts)} &nbsp;|&nbsp; Lookback: {lookback}d</div>
</header>
""")

    # -------------------------------------------------------------------------
    # ALERT BANNER
    # -------------------------------------------------------------------------
    if all_alerts:
        has_crit = any(a[0] == "crit" for a in all_alerts)
        w(f'<div class="alert-bar{"  crit" if has_crit else ""}">')
        for sev, host, dg, msg in sorted(all_alerts, key=lambda a: (0 if a[0]=="crit" else 1)):
            w(f'<div class="alert-item {sev}">▲ [{html.escape(host)}/{html.escape(dg)}] {html.escape(msg)}</div>')
        w('</div>')

    # -------------------------------------------------------------------------
    # FLEET SUMMARY STRIP
    # -------------------------------------------------------------------------
    fleet_sev = pct_severity(fleet_pct)
    w(f"""<div class="fleet-strip">
  <div class="fleet-kpi">
    <span class="label">Fleet Total</span>
    <span class="value">{fmt_tb(fleet_total)}</span>
  </div>
  <div class="fleet-kpi">
    <span class="label">Fleet Used</span>
    <span class="value {fleet_sev}">{fmt_tb(fleet_used)}</span>
  </div>
  <div class="fleet-kpi">
    <span class="label">Fleet Free</span>
    <span class="value">{fmt_tb(fleet_free)}</span>
  </div>
  <div class="fleet-kpi">
    <span class="label">Fleet % Used</span>
    <span class="value {fleet_sev}">{fleet_pct:.1f}%</span>
  </div>
  <div class="fleet-kpi">
    <span class="label">Hosts</span>
    <span class="value">{len(hosts)}</span>
  </div>
  <div class="fleet-kpi fleet-bar-wrap">
    <span class="label">Overall Utilisation</span>
    <div class="bar-outer">
      <div class="bar-inner {fleet_sev}" style="width:{min(fleet_pct,100):.1f}%"></div>
    </div>
  </div>
</div>
""")

    # -------------------------------------------------------------------------
    # HOST CARDS
    # -------------------------------------------------------------------------
    w('<div class="content">')

    for idx, hd in enumerate(host_data):
        host      = hd["host"]
        platform  = hd["platform"]
        dg_parsed = hd["dg_parsed"]

        # Compute host-level severity
        host_worst = "ok"
        for dg in dg_parsed:
            for s in [dg["sev"], dg["dsev"]]:
                if s == "crit":
                    host_worst = "crit"
                elif s == "warn" and host_worst != "crit":
                    host_worst = "warn"

        status_icon = {"ok": "✔", "warn": "⚠", "crit": "✖"}.get(host_worst, "?")
        status_color = {"ok": "var(--ok)", "warn": "var(--warn)", "crit": "var(--crit)"}.get(host_worst, "var(--muted)")

        card_id = f"host_{idx}"
        w(f"""<div class="host-card">
  <div class="host-header" onclick="toggleHost('{card_id}')">
    <span style="color:{status_color};font-size:16px">{status_icon}</span>
    <span class="host-name">{html.escape(host)}</span>
    <span class="host-platform">{html.escape(platform)}</span>
""")
        # Per-host mini summary
        if dg_parsed:
            host_total = sum(d["total_tb"] for d in dg_parsed)
            host_used  = sum(d["used_tb"]  for d in dg_parsed)
            host_pct   = round((host_used / host_total) * 100, 1) if host_total else 0
            w(f'    <span style="font-family:var(--font-mono);font-size:12px;color:var(--muted);margin-left:10px">'
              f'{fmt_tb(host_used)} / {fmt_tb(host_total)} ({host_pct:.1f}%)</span>')

        w(f"""    <span class="host-toggle" id="{card_id}_arrow">▼</span>
  </div>
  <div class="host-body" id="{card_id}_body">
""")

        # ---- DG Capacity + Growth Table ----
        if dg_parsed:
            w('<p class="section-label">Diskgroup Capacity &amp; Growth Trends</p>')
            w("""<table>
  <thead><tr>
    <th>Diskgroup</th>
    <th>Total</th>
    <th>Used</th>
    <th>Free</th>
    <th>Usable</th>
    <th colspan="2">% Used</th>
    <th>Growth/Day</th>
    <th>Growth/Week</th>
    <th>Growth/Month</th>
    <th>WoW Δ</th>
    <th>MoM Δ</th>
    <th>Days to Full</th>
    <th>Full Date</th>
    <th>Data Pts</th>
  </tr></thead>
  <tbody>
""")
            for dg in dg_parsed:
                g    = dg["growth"]
                sev  = dg["sev"]
                dsev = dg["dsev"]
                worst_sev = "crit" if "crit" in [sev, dsev] else ("warn" if "warn" in [sev, dsev] else "ok")

                pct_fill  = min(dg["pct_used"], 100)
                usable_str = fmt_tb(dg["usable_tb"]) if dg["usable_tb"] is not None else "—"

                w(f"""    <tr>
      <td class="dg-name sev-{worst_sev}">+{html.escape(dg['dg'])}</td>
      <td>{fmt_tb(dg['total_tb'])}</td>
      <td class="sev-{sev}">{fmt_tb(dg['used_tb'])}</td>
      <td>{fmt_tb(dg['free_tb'])}</td>
      <td>{usable_str}</td>
      <td class="sev-{sev}">{dg['pct_used']:.1f}%</td>
      <td class="usage-cell">
        <div class="usage-wrap">
          <div class="usage-bar"><div class="usage-fill {sev}" style="width:{pct_fill:.1f}%"></div></div>
        </div>
      </td>
      <td>{fmt_delta(g['growth_rate_tb_per_day'])}</td>
      <td>{fmt_delta(g['growth_rate_tb_per_week'])}</td>
      <td>{fmt_delta(g['growth_rate_tb_per_month'])}</td>
      <td>{fmt_delta(g['wow_delta_tb'])}</td>
      <td>{fmt_delta(g['mom_delta_tb'])}</td>
      <td class="sev-{dsev}">{fmt_days(g['days_to_full'])}</td>
      <td class="sev-{dsev}">{html.escape(str(g['projected_full_date'] or '—'))}</td>
      <td style="color:var(--muted)">{g['data_points']}</td>
    </tr>
""")
            w("  </tbody>\n</table>")
        else:
            w('<p style="color:var(--muted);font-style:italic">No diskgroup data parsed — check dg_summary.txt and stderr.</p>')

        # ---- DG Root Subdir Sections ----
        if hd["subdir_data"]:
            w('<p class="section-label" style="margin-top:20px">DG Root Subdir Detail</p>')
            for name, txt in hd["subdir_data"]:
                raw_id = f"{card_id}_{html.escape(name)}_raw"
                w(f"""<button class="raw-toggle" onclick="toggleRaw('{raw_id}')">{html.escape(name)}</button>
<pre class="raw-output" id="{raw_id}">{html.escape(txt.strip())}</pre>
""")

        # ---- Raw DG Summary ----
        raw_sum_id = f"{card_id}_sumraw"
        raw_err_id = f"{card_id}_errraw"
        w(f"""<p class="section-label" style="margin-top:20px">Raw Output</p>
<button class="raw-toggle" onclick="toggleRaw('{raw_sum_id}')">dg_summary.txt (stdout)</button>
<pre class="raw-output" id="{raw_sum_id}">{html.escape(hd['summary_raw'].strip() or '(empty)')}</pre>
""")
        if hd["summary_err"].strip():
            w(f"""<button class="raw-toggle" onclick="toggleRaw('{raw_err_id}')">dg_summary.err (stderr)</button>
<pre class="raw-output" id="{raw_err_id}">{html.escape(hd['summary_err'].strip())}</pre>
""")

        w("  </div>\n</div>")   # end host-body, host-card

    w("</div>")  # end .content

    # -------------------------------------------------------------------------
    # FOOTER + JS
    # -------------------------------------------------------------------------
    w(f"""<div class="footer">
  ASM Capacity Dashboard &nbsp;|&nbsp; {html.escape(run_date)} &nbsp;|&nbsp;
  Hosts: {len(hosts)} &nbsp;|&nbsp; Alerts: {len(all_alerts)} &nbsp;|&nbsp;
  Generated: {html.escape(now_str)}
</div>

<script>
function toggleHost(id) {{
  const body  = document.getElementById(id + '_body');
  const arrow = document.getElementById(id + '_arrow');
  if (body.classList.contains('collapsed')) {{
    body.classList.remove('collapsed');
    arrow.textContent = '▼';
  }} else {{
    body.classList.add('collapsed');
    arrow.textContent = '▶';
  }}
}}

function toggleRaw(id) {{
  const el = document.getElementById(id);
  if (el) el.style.display = el.style.display === 'block' ? 'none' : 'block';
}}
</script>
</body></html>""")

    return "\n".join(H), csv_rows


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Build ASMDU HTML capacity report with growth tracking."
    )
    parser.add_argument("nas_runs_root", help="NAS root containing dated run dirs")
    parser.add_argument("run_date",      help="Report date (YYYY-MM-DD)")
    parser.add_argument("--lookback",    type=int, default=30,
                        help="Days of history for growth analysis (default: 30)")
    args = parser.parse_args()

    # Validate date format
    try:
        datetime.date.fromisoformat(args.run_date)
    except ValueError:
        raise SystemExit(f"[ERROR] Invalid date format: {args.run_date}. Use YYYY-MM-DD.")

    html_str, csv_rows = build_report(
        nas_root=args.nas_runs_root,
        run_date=args.run_date,
        lookback=args.lookback,
    )

    base = os.path.join(args.nas_runs_root, args.run_date)

    # Write HTML
    html_path = os.path.join(base, "report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_str)
    print(f"[OK] HTML report: {html_path}")

    # Write CSV
    csv_path = os.path.join(base, "report.csv")
    write_csv(csv_path, csv_rows)
    print(f"[OK] CSV export:  {csv_path}")


if __name__ == "__main__":
    main()
