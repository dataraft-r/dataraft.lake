"""Resolve component CI from the umbrella's single immutable family manifest."""
import json
import os
import pathlib
import re
import subprocess
import sys

component = sys.argv[1]
mode = os.environ.get("DATARAFT_FAMILY_MODE", "pinned")
if mode not in ("pinned", "head"):
    raise SystemExit("Unknown family mode")

def checkout(name, ref, path):
    path = pathlib.Path(path)
    if not path.exists():
        subprocess.run(["git", "init", str(path)], check=True)
        subprocess.run(["git", "-C", str(path), "remote", "add", "origin",
                        "https://github.com/dataraft-r/" + name + ".git"], check=True)
    subprocess.run(["git", "-C", str(path), "fetch", "--depth=1", "origin", ref], check=True)
    subprocess.run(["git", "-C", str(path), "checkout", "--detach", "FETCH_HEAD"], check=True)
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()

branch = os.environ.get("FAMILY_BRANCH", "")
ref = "refs/heads/main"
if branch:
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", branch) or ".." in branch:
        raise SystemExit("Invalid family branch")
    candidate = "refs/heads/" + branch
    advertised = subprocess.check_output(["git", "ls-remote", "--heads",
        "https://github.com/dataraft-r/dataraft.git", candidate], text=True)
    if advertised.strip():
        ref = candidate
umbrella = checkout("dataraft", ref, "integration")
lock = json.loads(pathlib.Path("integration/family-lock.json").read_text())
resolved = {"dataraft": {**lock["packages"]["dataraft"], "ref": umbrella}}
for name, spec in lock["packages"].items():
    if name == "dataraft":
        continue
    if name == component:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    else:
        target = "refs/heads/main" if mode == "head" else spec["ref"]
        if mode == "pinned" and not re.fullmatch(r"[0-9a-f]{40}", target):
            raise SystemExit("Family refs must be immutable commit SHAs")
        sha = checkout(name, target, pathlib.Path("family") / name)
        if mode == "pinned" and sha != target:
            raise SystemExit("Resolved family SHA mismatch")
    resolved[name] = {**spec, "ref": sha}
pathlib.Path("check").mkdir(exist_ok=True)
pathlib.Path("check/resolved-family.json").write_text(
    json.dumps({"mode": mode, "packages": resolved}, indent=2) + "\n")
