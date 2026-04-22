"""
Clock-In / Task Events Generator.

Generates workforce events and sends them to the ClockInEventStream Eventstream:
  - badge_in / badge_out  — employee shift start/end
  - task_start            — employee begins work on a project task
  - task_complete         — employee finishes a project task (triggers Actual End Date update downstream)
  - break_start / break_end

Events carry referential keys (employee email, project_id, task_id, simulator_id)
so the downstream agent/pipeline can update SQL Database Actual dates.

Usage:
  python clockin_events.py                              # interactive demo, stdout
  python clockin_events.py --scenario shift_start       # just badge-in events
  python clockin_events.py --scenario task_complete      # just task completions
  EVENTHUB_CONNECTION_STRING=... python clockin_events.py --scenario full_day
"""

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timezone


# ── Reference data helpers ──────────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_tasks(path: str) -> list[dict]:
    """Load flat tasks CSV. Returns list of task dicts."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_projects_with_tasks(proj_path: str, task_path: str) -> list[dict]:
    """Load projects + tasks CSVs and attach tasks to their parent project."""
    projects = load_csv(proj_path)
    tasks = load_tasks(task_path)
    for p in projects:
        p["tasks"] = [t for t in tasks if t["Parent_Project_ID"] == p["Project_ID"]]
    return projects


# ── Event builders ──────────────────────────────────────────────────────────

def make_event(event_type: str, employee: dict, extra: dict | None = None) -> dict:
    """Build a clock-in event dict with referential integrity keys."""
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "employee_email": employee["email"],
        "employee_name": f"{employee['first_name']} {employee['last_name']}",
        "employee_id": employee["employee_id"],
        "department": employee["department"],
        "badge_number": employee["badge_number"],
        "project_id": None,
        "task_id": None,
        "simulator_id": None,
        "details": "",
    }
    if extra:
        event.update(extra)
    return event


def badge_in_event(employee: dict) -> dict:
    return make_event("badge_in", employee, {"details": "Shift start"})


def badge_out_event(employee: dict) -> dict:
    return make_event("badge_out", employee, {"details": "Shift end"})


def break_start_event(employee: dict) -> dict:
    return make_event("break_start", employee, {"details": "Break"})


def break_end_event(employee: dict) -> dict:
    return make_event("break_end", employee, {"details": "Back from break"})


def task_start_event(employee: dict, project: dict, task: dict) -> dict:
    return make_event("task_start", employee, {
        "project_id": project["Project_ID"],
        "task_id": task["Task_ID"],
        "simulator_id": project.get("Simulator_ID", ""),
        "details": f"Starting: {task['Task_Name']}",
    })


def task_complete_event(employee: dict, project: dict, task: dict) -> dict:
    return make_event("task_complete", employee, {
        "project_id": project["Project_ID"],
        "task_id": task["Task_ID"],
        "simulator_id": project.get("Simulator_ID", ""),
        "details": f"Completed: {task['Task_Name']}",
    })


# ── Scenarios ───────────────────────────────────────────────────────────────

def scenario_shift_start(employees: list[dict]) -> list[dict]:
    """All day-shift employees badge in."""
    day_emps = [e for e in employees if e["shift_preference"] in ("Day", "Flex")
                and e["employee_type"] != "FullTime" or e["shift_preference"] == "Day"]
    return [badge_in_event(e) for e in day_emps[:12]]  # cap at 12 workers


def scenario_shift_end(employees: list[dict]) -> list[dict]:
    """All day-shift employees badge out."""
    day_emps = [e for e in employees if e["shift_preference"] in ("Day", "Flex")]
    return [badge_out_event(e) for e in day_emps[:12]]


def scenario_task_complete(employees: list[dict], projects: list[dict]) -> list[dict]:
    """Generate task_complete events for tasks currently InProgress."""
    events = []
    for project in projects:
        for task in project.get("tasks", []):
            pct = int(task.get("Complete_Percentage", 0))
            if 0 < pct < 100:  # InProgress
                emp = next((e for e in employees if e["email"] == task["Resource_Login"]), None)
                if emp:
                    events.append(task_complete_event(emp, project, task))
    return events


def scenario_full_day(employees: list[dict], projects: list[dict]) -> list[dict]:
    """Simulate a full day: badge in → tasks → break → tasks → badge out."""
    events = []

    # Morning badge-in
    active_emps = [e for e in employees if e["employee_id"] != "EMP-050"]  # exclude PM
    for emp in active_emps:
        events.append(badge_in_event(emp))

    # Task starts for InProgress tasks
    for project in projects:
        for task in project.get("tasks", []):
            pct = int(task.get("Complete_Percentage", 0))
            if 0 < pct < 100:  # InProgress
                emp = next((e for e in employees if e["email"] == task["Resource_Login"]), None)
                if emp:
                    events.append(task_start_event(emp, project, task))

    # Mid-day breaks for a few employees
    for emp in random.sample(active_emps, min(4, len(active_emps))):
        events.append(break_start_event(emp))
        events.append(break_end_event(emp))

    # Task completions for tasks that are nearly done (InProgress)
    events.extend(scenario_task_complete(employees, projects))

    # End-of-day badge-out
    for emp in active_emps:
        events.append(badge_out_event(emp))

    return events


# ── Send ────────────────────────────────────────────────────────────────────

def send_events(events: list[dict], conn_str: str) -> None:
    if conn_str:
        from azure.eventhub import EventData, EventHubProducerClient
        producer = EventHubProducerClient.from_connection_string(conn_str)
        with producer:
            batch = producer.create_batch()
            for event in events:
                batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)
        print(f"Sent {len(events)} events to Eventstream.")
    else:
        for event in events:
            print(json.dumps(event, indent=2))
        print(f"\n({len(events)} total events — set EVENTHUB_CONNECTION_STRING to send to Eventstream)")


# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate clock-in / task events")
    parser.add_argument(
        "--scenario",
        choices=["shift_start", "shift_end", "task_complete", "full_day"],
        default="full_day",
        help="Which scenario to generate (default: full_day)",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(script_dir, "..")

    employees = load_csv(os.path.join(base_dir, "data", "hr", "employees.csv"))
    projects = load_projects_with_tasks(
        os.path.join(base_dir, "data", "plm", "projects.csv"),
        os.path.join(base_dir, "data", "plm", "tasks.csv"),
    )

    conn_str = os.environ.get("EVENTHUB_CONNECTION_STRING", "")

    if args.scenario == "shift_start":
        events = scenario_shift_start(employees)
    elif args.scenario == "shift_end":
        events = scenario_shift_end(employees)
    elif args.scenario == "task_complete":
        events = scenario_task_complete(employees, projects)
    elif args.scenario == "full_day":
        events = scenario_full_day(employees, projects)
    else:
        events = []

    send_events(events, conn_str)
