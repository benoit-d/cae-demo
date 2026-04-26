# =============================================================================
# CAE Flight Simulator Manufacturing — Solution Installer
# =============================================================================
# Copy-paste these 3 cells into a Fabric notebook and Run All.
# Or import deploy/SolutionInstaller.ipynb directly.
# =============================================================================

# ── Cell 1 — Install packages ──────────────────────────────────────────────
%pip install --upgrade pip
%pip install -q fabric-cicd azure-identity gitpython

# ── Cell 2 — Clone repo and publish Fabric items ───────────────────────────
import os, shutil, tempfile, glob, requests
from git import Repo
from fabric_cicd import FabricWorkspace, publish_all_items
import notebookutils
from azure.core.credentials import AccessToken

clone_dir = os.path.join(tempfile.gettempdir(), "cae-demo-install")
if os.path.exists(clone_dir): shutil.rmtree(clone_dir)
Repo.clone_from("https://github.com/benoit-d/cae-demo.git", clone_dir, branch="master", depth=1)
workspace_dir = os.path.join(clone_dir, "workspace")
data_dir = os.path.join(clone_dir, "data")

WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except: pass

TOKEN = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
class _Cred:
    def get_token(self, *s, **k): return AccessToken(TOKEN, 0)

ws = FabricWorkspace(workspace_id=WORKSPACE_ID, repository_directory=workspace_dir,
    item_type_in_scope=[
        "Notebook", "Lakehouse", "Environment",
        "Eventhouse",
        "KQLDatabase", "KQLDashboard", "KQLQueryset",
        "SQLDatabase",
        "DataPipeline",
    ], token_credential=_Cred())
publish_all_items(ws)

# ── Cell 3 — Upload seed data + create connections.json ────────────────────
headers = {"Authorization": f"Bearer {TOKEN}"}
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=headers)
items = resp.json().get("value", [])
lh = next((i for i in items if i.get("displayName") == "CAEManufacturing_LH"), None)
if lh:
    for folder in ["erp", "hr", "telemetry", "plm", "mes"]:
        src = os.path.join(data_dir, folder)
        if not os.path.isdir(src): continue
        for f in sorted(glob.glob(os.path.join(src, "*"))):
            dest = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh['id']}/Files/data/{folder}/{os.path.basename(f)}"
            notebookutils.fs.cp(f"file://{f}", dest)
    # Upload KQL scripts
    kql_src = os.path.join(clone_dir, "scripts", "kql")
    if os.path.isdir(kql_src):
        for f in sorted(glob.glob(os.path.join(kql_src, "*"))):
            dest = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh['id']}/Files/scripts/kql/{os.path.basename(f)}"
            notebookutils.fs.cp(f"file://{f}", dest)

    # Create connections config file (if it doesn't exist)
    import json
    onelake_file = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{lh['id']}/Files/config/connections.json"
    storage_token = notebookutils.credentials.getToken("https://storage.azure.com")
    storage_headers = {"Authorization": f"Bearer {storage_token}"}

    check_resp = requests.head(onelake_file, headers=storage_headers)
    if check_resp.status_code == 200:
        print("Config file exists — preserving connection strings")
    else:
        config = {
            "SQL_JDBC_CONNECTION_STRING": "",
            "TELEMETRY_EVENTSTREAM_CONNECTION_STRING": "",
            "CLOCKIN_EVENTSTREAM_CONNECTION_STRING": "",
            "FOUNDRY_AGENT_PROJECT_ENDPOINT": "",
            "FOUNDRY_AGENT_ID": "",
            "TEAMS_WEBHOOK_URL": "",
        }
        config_bytes = json.dumps(config, indent=2).encode("utf-8")
        requests.put(f"{onelake_file}?resource=file", headers=storage_headers)
        requests.patch(f"{onelake_file}?action=append&position=0",
                      headers={**storage_headers, "Content-Type": "application/octet-stream"}, data=config_bytes)
        requests.patch(f"{onelake_file}?action=flush&position={len(config_bytes)}", headers=storage_headers)
        print("Created config/connections.json — fill in connection strings before running PostDeploymentConfig")
        print("  Edit in Lakehouse > Files > config > connections.json")

shutil.rmtree(clone_dir, ignore_errors=True)
