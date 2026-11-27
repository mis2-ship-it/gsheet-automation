"""The metrics workflow's store step, run in a real git repository with a remote:
first run commits, an unchanged run commits nothing, and a changed file commits again.
Run from the repo root: python tests/test_metrics_commit.py"""
import os, shutil, subprocess, sys, tempfile, yaml
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    PASS, FAIL = (PASS + 1, FAIL) if cond else (PASS, FAIL + 1)
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))


wf = yaml.safe_load(open(os.path.join(ROOT, ".github", "workflows", "metrics.yml"), encoding="utf-8"))
steps = wf["jobs"]["metrics"]["steps"]
store = [s for s in steps if s.get("name") == "Store the metrics in the repository"][0]["run"]
check("the workflow has the schedule and the manual start", "schedule" in wf[True if True in wf else "on"] and "workflow_dispatch" in wf[True if True in wf else "on"])

work = tempfile.mkdtemp()
remote, repo = os.path.join(work, "remote.git"), os.path.join(work, "repo")
def sh(cmd, cwd):
    return subprocess.run(cmd, cwd=cwd, shell=True, capture_output=True, text=True, env=dict(os.environ, GITHUB_REF_NAME="main"))
sh(f"git init -q --bare -b main {remote}", work)
sh(f"git clone -q {remote} {repo}", work)
sh("git config user.name test && git config user.email test@example.com", repo)
open(os.path.join(repo, "README.md"), "w").write("repo\n")
sh("git checkout -q -b main; git add README.md && git commit -q -m init && git push -q origin main", repo)
os.makedirs(os.path.join(repo, "data"), exist_ok=True)
def gz(path, text):
    import gzip
    with open(path, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as g:
        g.write(text.encode())
def commits():
    return int(sh("git rev-list --count origin/main", repo).stdout.strip() or 0)
def run_store():
    script = os.path.join(work, "store.sh")
    open(script, "w").write("set -e\n" + store)
    return sh(f"bash {script}", repo)

gz(os.path.join(repo, "data", "metrics.json.gz"), '{"rows": [1]}')
before = commits()
r = run_store()
sh("git fetch -q origin", repo)
check("the first run commits the metrics file", r.returncode == 0 and commits() == before + 1, r.stdout[-120:] + r.stderr[-200:])
r = run_store()
sh("git fetch -q origin", repo)
check("a run with the same data commits nothing", r.returncode == 0 and "no change" in r.stdout and commits() == before + 1)
gz(os.path.join(repo, "data", "metrics.json.gz"), '{"rows": [1, 2]}')
r = run_store()
sh("git fetch -q origin", repo)
check("a changed file commits again", r.returncode == 0 and commits() == before + 2, r.stderr[-200:])
print(f"{PASS} passed, {FAIL} failed")
shutil.rmtree(work, ignore_errors=True)
sys.exit(1 if FAIL else 0)
