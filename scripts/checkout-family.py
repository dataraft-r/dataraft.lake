"""Check out immutable family sources; only nightly explicitly requests HEAD."""
import json, os, pathlib, re, subprocess, sys
lock = json.loads(pathlib.Path("family-lock.json").read_text())
mode = os.environ.get("DATARAFT_FAMILY_MODE", "pinned")
if mode not in ("pinned", "head"):
    raise SystemExit("Unknown family mode")
resolved_packages = {}
component = sys.argv[1] if len(sys.argv) > 1 else "dataraft"
for name, spec in lock["packages"].items():
    if name == component:
        continue
    ref = "refs/heads/main" if mode == "head" else spec["ref"]
    if mode == "pinned" and not re.fullmatch(r"[0-9a-f]{40}", ref):
        raise SystemExit("Family refs must be full immutable commit SHAs")
    path = pathlib.Path("packages" if component == "dataraft" else "family") / name
    if name == "dataraft":
        path = pathlib.Path("integration")
    if not path.exists():
        subprocess.run(["git", "init", str(path)], check=True)
        subprocess.run(["git", "-C", str(path), "remote", "add", "origin", "https://github.com/" + spec["repository"] + ".git"], check=True)
    subprocess.run(["git", "-C", str(path), "fetch", "--depth=1", "origin", ref], check=True)
    subprocess.run(["git", "-C", str(path), "checkout", "--detach", "FETCH_HEAD"], check=True)
    resolved = subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()
    if mode == "pinned" and resolved != ref:
        raise SystemExit("Resolved family SHA mismatch")
    resolved_packages[name] = {**spec, "ref": resolved}
    print(name + "=" + resolved, flush=True)

pathlib.Path("check").mkdir(exist_ok=True)
pathlib.Path("check/resolved-family.json").write_text(json.dumps({"mode": mode, "packages": resolved_packages}, indent=2) + "\n")
