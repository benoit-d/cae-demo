"""Validate referential integrity and scheduling constraints."""
import csv
from collections import defaultdict
from datetime import date, timedelta

TODAY = date(2026, 4, 21)
BASE = "c:/Repo/cae-demo/data"

def lcsv(p):
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

emps = lcsv(f"{BASE}/hr/employees.csv")
skills = lcsv(f"{BASE}/hr/skills_certifications.csv")
tasks = lcsv(f"{BASE}/cosmosdb/tasks.csv")
projs = lcsv(f"{BASE}/cosmosdb/projects.csv")
machines = lcsv(f"{BASE}/erp/machines.csv")

emp_emails = {e["email"] for e in emps}
email_to_id = {e["email"]: e["employee_id"] for e in emps}
emp_skills = defaultdict(set)
for s in skills:
    emp_skills[s["employee_id"]].add(s["skill_category"])
sim_ids = {m["simulator_id"] for m in machines}

errors = []

for t in tasks:
    rl = t["Resource_Login"]
    tid = t["Task_ID"]
    # 1) Resource exists
    if rl not in emp_emails:
        errors.append(f"{tid}: Resource_Login '{rl}' not in employees")
    # 2) Skill match
    eid = email_to_id.get(rl)
    if eid and t["Skill_Requirement"] not in emp_skills.get(eid, set()):
        errors.append(f"{tid}: {rl} (={eid}) lacks skill '{t['Skill_Requirement']}'")
    # 3) No future actual dates
    if t["Actual_Start"] and date.fromisoformat(t["Actual_Start"]) > TODAY:
        errors.append(f"{tid}: Actual_Start in future")
    if t["Actual_End"] and date.fromisoformat(t["Actual_End"]) > TODAY:
        errors.append(f"{tid}: Actual_End in future")

for p in projs:
    if p["Simulator_ID"] not in sim_ids:
        errors.append(f"{p['Project_ID']}: Simulator '{p['Simulator_ID']}' not in machines")

# 4) No overlapping bookings per employee
bookings = defaultdict(list)
for t in tasks:
    s = t.get("Modified_Planned_Start")
    d = int(t.get("Standard_Duration", 0))
    if s and d:
        start = date.fromisoformat(s)
        end = start + timedelta(days=d)
        bookings[t["Resource_Login"]].append((start, end, t["Task_ID"]))

for email, periods in bookings.items():
    periods.sort()
    for i in range(len(periods) - 1):
        if periods[i][1] > periods[i + 1][0]:
            errors.append(
                f"OVERLAP: {email}: {periods[i][2]} ends {periods[i][1]} "
                f"vs {periods[i+1][2]} starts {periods[i+1][0]}"
            )

print(f"Projects: {len(projs)}, Tasks: {len(tasks)}, Employees: {len(emps)}, Sims: {len(machines)}")
if errors:
    print(f"\nERRORS ({len(errors)}):")
    for e in errors:
        print(f"  {e}")
else:
    print("\nALL CHECKS PASSED")
    print("  - All Resource_Login emails exist in employees")
    print("  - All Skill_Requirements matched by assigned employee")
    print("  - All Simulator_IDs exist in machines")
    print("  - No actual dates in the future")
    print("  - No employee double-bookings")
