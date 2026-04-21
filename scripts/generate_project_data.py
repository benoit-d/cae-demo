#!/usr/bin/env python3
"""Generate project and task CSV data for the CAE Flight Simulator demo.

Produces:
    data/cosmosdb/projects.csv   – 8 projects (one per simulator build)
    data/cosmosdb/tasks.csv      – 13 tasks per project, flat table

Constraints enforced:
    - No employee is double-booked (including future planned tasks)
    - Actual dates are never in the future (today = 2026-04-21)
    - Each task's Skill_Requirement matches the assigned employee's skills
    - Finish-to-Start dependencies cascade through modified planned dates
    - Start = COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start)
    - End   = Start + Standard_Duration
"""

import csv
import os
from datetime import date, timedelta

# ── Configuration ────────────────────────────────────────────────────────

TODAY = date(2026, 4, 21)
PM_EMAIL = "daniel.fortin@caedemo.com"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(SCRIPT_DIR, "..", "data", "cosmosdb")

# ── Task templates ───────────────────────────────────────────────────────
#    Each simulator build follows this sequence.
#    dep_indices = which prior template indices must finish first.

TEMPLATES = [
    {"idx": 0,  "type": "Material_Readiness",     "dur": 5,  "skill": "Structures",       "deps": [],        "ms": 0},
    {"idx": 1,  "type": "Platform_Frame_Assembly", "dur": 25, "skill": "Structures",       "deps": [0],       "ms": 0},
    {"idx": 2,  "type": "Hydraulic_Installation",  "dur": 30, "skill": "Hydraulics",       "deps": [1],       "ms": 0},
    {"idx": 3,  "type": "Electrical_Harness",      "dur": 30, "skill": "Electrical",       "deps": [1],       "ms": 0},
    {"idx": 4,  "type": "Cockpit_Integration",     "dur": 25, "skill": "Structures",       "deps": [2, 3],    "ms": 0},
    {"idx": 5,  "type": "Avionics_Installation",   "dur": 22, "skill": "Avionics",         "deps": [4],       "ms": 0},
    {"idx": 6,  "type": "Visual_System_Setup",     "dur": 18, "skill": "Visual Systems",   "deps": [4],       "ms": 0},
    {"idx": 7,  "type": "Control_Loading_Setup",   "dur": 12, "skill": "Motion Systems",   "deps": [4],       "ms": 0},
    {"idx": 8,  "type": "Motion_Calibration",      "dur": 15, "skill": "Motion Systems",   "deps": [5, 6, 7], "ms": 0},
    {"idx": 9,  "type": "Integration_Test",        "dur": 20, "skill": "Test Engineering", "deps": [8],       "ms": 0},
    {"idx": 10, "type": "Burn_In",                 "dur": 5,  "skill": "Test Engineering", "deps": [9],       "ms": 0},
    {"idx": 11, "type": "Qualification_Test",      "dur": 25, "skill": "Test Engineering", "deps": [10],      "ms": 0},
    {"idx": 12, "type": "Customer_Acceptance",     "dur": 10, "skill": "Test Engineering", "deps": [11],      "ms": 1},
]

TASK_NAMES = {t["type"]: {
    "Material_Readiness":     "Material Readiness Check",
    "Platform_Frame_Assembly":"Platform Frame Assembly",
    "Hydraulic_Installation": "Hydraulic System Installation",
    "Electrical_Harness":     "Electrical Harness & Power Distribution",
    "Cockpit_Integration":    "Cockpit Shell Integration",
    "Avionics_Installation":  "Avionics & Flight Deck Installation",
    "Visual_System_Setup":    "Visual System & Projection Setup",
    "Control_Loading_Setup":  "Control Loading System Setup",
    "Motion_Calibration":     "Motion System Calibration",
    "Integration_Test":       "Full System Integration Test",
    "Burn_In":                "72-Hour Burn-In Run",
    "Qualification_Test":     "Qualification Testing",
    "Customer_Acceptance":    "Customer Acceptance Test",
}.get(t["type"], t["type"]) for t in TEMPLATES}

# ── Employee pools by skill ──────────────────────────────────────────────

SKILL_POOL = {
    "Structures":       ["nat.bouchard@caedemo.com"],
    "Hydraulics":       ["mc.dubois@caedemo.com", "ma.pelletier@caedemo.com", "james.taylor@contractco.com"],
    "Electrical":       ["luc.bergeron@caedemo.com", "cat.morin@caedemo.com", "maria.garcia@contractco.com"],
    "Avionics":         ["phil.gagnon@caedemo.com"],
    "Visual Systems":   ["sophie.lavoie@caedemo.com"],
    "Motion Systems":   ["jp.tremblay@caedemo.com", "francois.cote@caedemo.com"],
    "Test Engineering": ["david.chen@caedemo.com"],
}

# ── 8 Projects sorted by start date ─────────────────────────────────────

PROJECTS = [
    {"id": "PRJ-003", "name": "SIM-003 Boeing 777X — Emirates",       "sim": "SIM-003", "start": date(2025, 9,  1)},
    {"id": "PRJ-001", "name": "SIM-001 Boeing 737 MAX — Air Canada",  "sim": "SIM-001", "start": date(2025, 11, 15)},
    {"id": "PRJ-002", "name": "SIM-002 Airbus A320neo — Lufthansa",   "sim": "SIM-002", "start": date(2026, 1, 10)},
    {"id": "PRJ-006", "name": "SIM-006 Boeing 737 MAX — WestJet",     "sim": "SIM-006", "start": date(2026, 2,  1)},
    {"id": "PRJ-007", "name": "SIM-007 Airbus A320neo — Air France",  "sim": "SIM-007", "start": date(2026, 3,  1)},
    {"id": "PRJ-004", "name": "SIM-004 Airbus A350 — Delta Airlines", "sim": "SIM-004", "start": date(2026, 3, 15)},
    {"id": "PRJ-005", "name": "SIM-005 Boeing 787 — United Airlines", "sim": "SIM-005", "start": date(2026, 4,  1)},
    {"id": "PRJ-008", "name": "SIM-008 Boeing 777X — Qatar Airways",  "sim": "SIM-008", "start": date(2026, 5,  1)},
]

# ── Scheduling engine ────────────────────────────────────────────────────

busy: dict[str, list[tuple[date, date]]] = {}
for pool in SKILL_POOL.values():
    for e in pool:
        busy.setdefault(e, [])


def _earliest_free(email: str, want: date, dur: int) -> date:
    """Return the earliest date >= want where email is free for dur days."""
    s = want
    for _ in range(500):
        e = s + timedelta(days=dur)
        conflict = False
        for bs, be in sorted(busy[email]):
            if s < be and e > bs:
                s = be          # push past this booking
                conflict = True
                break
        if not conflict:
            return s
    return s


def assign(skill: str, want: date, dur: int) -> tuple[str, date]:
    """Pick the employee (with skill) who can start earliest.
    Ties are broken round-robin so work is distributed across the pool."""
    candidates = []
    for email in SKILL_POOL[skill]:
        avail = _earliest_free(email, want, dur)
        task_count = len(busy[email])
        candidates.append((avail, task_count, email))
    # Sort by: earliest available, then fewest existing tasks (load balancing)
    candidates.sort(key=lambda c: (c[0], c[1]))
    best_email = candidates[0][2]
    best_start = candidates[0][0]
    # book
    busy[best_email].append((best_start, best_start + timedelta(days=dur)))
    return best_email, best_start


# ── Generate rows ────────────────────────────────────────────────────────

all_projects: list[dict] = []
all_tasks: list[dict] = []

for proj in PROJECTS:
    pid, pstart = proj["id"], proj["start"]
    t: dict[int, dict] = {}   # idx → computed task info

    # Pass 1 — ideal dates (no resource constraints)
    for tmpl in TEMPLATES:
        i = tmpl["idx"]
        if not tmpl["deps"]:
            ideal = pstart
        else:
            ideal = max(t[d]["_ideal_end"] for d in tmpl["deps"])
        t[i] = {"_ideal_start": ideal, "_ideal_end": ideal + timedelta(days=tmpl["dur"])}

    # Pass 2 — modified dates (resource-constrained) + employee assignment
    for tmpl in TEMPLATES:
        i = tmpl["idx"]
        dur = tmpl["dur"]

        # Dependency-based earliest start (using modified chain)
        if not tmpl["deps"]:
            dep_ready = pstart
        else:
            dep_ready = max(t[d]["_mod_end"] for d in tmpl["deps"])

        emp, mod_start = assign(tmpl["skill"], dep_ready, dur)
        mod_end = mod_start + timedelta(days=dur)

        # Status & actual dates
        if mod_end <= TODAY:
            act_s, act_e, pct = mod_start, mod_end, 100
        elif mod_start <= TODAY:
            elapsed = (TODAY - mod_start).days
            act_s, act_e, pct = mod_start, None, min(95, max(5, int(elapsed / dur * 100)))
        else:
            act_s, act_e, pct = None, None, 0

        # FS predecessor (single column — pick the latest finisher)
        if not tmpl["deps"]:
            fs = ""
        else:
            latest = max(tmpl["deps"], key=lambda d: t[d]["_mod_end"])
            fs = f"{pid}-TSK-{latest + 1:03d}"

        tid = f"{pid}-TSK-{i + 1:03d}"

        t[i].update({
            "_mod_start": mod_start,
            "_mod_end":   mod_end,
            "Task_ID":                tid,
            "Task_Name":              TASK_NAMES.get(tmpl["type"], tmpl["type"]),
            "Parent_Project_ID":      pid,
            "FS_Task_ID":             fs,
            "Task_Type":              tmpl["type"],
            "Milestone":              tmpl["ms"],
            "Skill_Requirement":      tmpl["skill"],
            "Initial_Planned_Start":  t[i]["_ideal_start"].isoformat(),
            "Modified_Planned_Start": mod_start.isoformat(),
            "Actual_Start":           act_s.isoformat() if act_s else "",
            "Standard_Duration":      dur,
            "Actual_End":             act_e.isoformat() if act_e else "",
            "Resource_Login":         emp,
            "Complete_Percentage":    pct,
            "Last_Modified_By":       emp if act_s else PM_EMAIL,
            "Last_Modified_On":       (act_e or TODAY).isoformat() if act_s else pstart.isoformat(),
        })
        all_tasks.append(t[i])

    # Project row
    first, last = t[0], t[12]
    p_mod_start = first["_mod_start"]
    p_dur = (last["_mod_end"] - p_mod_start).days
    done = sum(1 for v in t.values() if v.get("Complete_Percentage") == 100)

    all_projects.append({
        "Project_ID":             pid,
        "Project_Name":           proj["name"],
        "Simulator_ID":           proj["sim"],
        "Initial_Planned_Start":  pstart.isoformat(),
        "Modified_Planned_Start": p_mod_start.isoformat(),
        "Standard_Duration":      p_dur,
        "Actual_End":             last["Actual_End"] if last.get("Complete_Percentage") == 100 else "",
        "Resource_Login":         PM_EMAIL,
        "Complete_Percentage":    int(done / len(t) * 100),
        "Last_Modified_By":       PM_EMAIL,
        "Last_Modified_On":       TODAY.isoformat(),
    })

# ── Write CSVs ───────────────────────────────────────────────────────────

PROJ_COLS = [
    "Project_ID", "Project_Name", "Simulator_ID",
    "Initial_Planned_Start", "Modified_Planned_Start", "Standard_Duration",
    "Actual_End", "Resource_Login", "Complete_Percentage",
    "Last_Modified_By", "Last_Modified_On",
]
TASK_COLS = [
    "Task_ID", "Task_Name", "Parent_Project_ID", "FS_Task_ID", "Task_Type",
    "Milestone", "Skill_Requirement",
    "Initial_Planned_Start", "Modified_Planned_Start", "Actual_Start",
    "Standard_Duration", "Actual_End",
    "Resource_Login", "Complete_Percentage",
    "Last_Modified_By", "Last_Modified_On",
]

os.makedirs(OUT_DIR, exist_ok=True)

all_projects.sort(key=lambda p: p["Project_ID"])
proj_path = os.path.join(OUT_DIR, "projects.csv")
with open(proj_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=PROJ_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(all_projects)

task_rows = [{c: t.get(c, "") for c in TASK_COLS} for t in all_tasks]
task_rows.sort(key=lambda r: (r["Parent_Project_ID"], r["Task_ID"]))
task_path = os.path.join(OUT_DIR, "tasks.csv")
with open(task_path, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=TASK_COLS)
    w.writeheader()
    w.writerows(task_rows)

# ── Console summary ──────────────────────────────────────────────────────

print(f"Generated {len(all_projects)} projects, {len(all_tasks)} tasks")
print(f"  -> {proj_path}")
print(f"  -> {task_path}")
print()

for p in all_projects:
    end_dt = date.fromisoformat(p["Modified_Planned_Start"]) + timedelta(days=p["Standard_Duration"])
    print(f"  {p['Project_ID']} | {p['Project_Name'][:42]:42s} | "
          f"{p['Modified_Planned_Start']} → {end_dt} ({p['Standard_Duration']:>3d}d) | "
          f"{p['Complete_Percentage']:>3d}%")

# Conflict check
print("\nEmployee utilisation:")
for skill, emails in SKILL_POOL.items():
    for email in emails:
        periods = sorted(busy[email])
        total_days = sum((e - s).days for s, e in periods)
        name = email.split("@")[0]
        # check overlaps
        overlaps = 0
        for j in range(len(periods) - 1):
            if periods[j][1] > periods[j + 1][0]:
                overlaps += 1
        flag = " *** OVERLAP ***" if overlaps else ""
        print(f"  {name:30s} [{skill:17s}] {total_days:>4d} days across {len(periods):>2d} tasks{flag}")

# Verify no actual dates in the future
future_actual = [
    t["Task_ID"]
    for t in all_tasks
    if (t.get("Actual_Start") and date.fromisoformat(t["Actual_Start"]) > TODAY)
    or (t.get("Actual_End")   and date.fromisoformat(t["Actual_End"])   > TODAY)
]
if future_actual:
    print(f"\n*** ERROR: {len(future_actual)} tasks have actual dates in the future! ***")
    for tid in future_actual:
        print(f"  {tid}")
else:
    print("\n✓ No actual dates in the future")
