# run_all.py
from __future__ import annotations
import os, sys, time, argparse, subprocess
from pathlib import Path

LOCK = Path("/tmp/bridge_cron.lock")

def _find_jobs_file(explicit: str|None) -> Path:
    if explicit:
        p = Path(explicit).resolve()
        if p.is_file():
            return p
        sys.stderr.write(f"[run_all] jobs file not found at --jobs {p}\n")
        sys.exit(1)
    here = Path(__file__).resolve().parent
    candidates = [here/"jobs.yaml", here/"jobs.yml", Path.cwd()/"jobs.yaml", Path.cwd()/"jobs.yml"]
    for p in candidates:
        if p.is_file():
            return p
    sys.stderr.write("[run_all] Missing jobs file. Looked for jobs.yaml / jobs.yml next to run_all.py and in CWD.\n")
    sys.exit(1)

def _load_jobs(path: Path) -> list[dict]:
    try:
        import yaml  # PyYAML
    except ImportError:
        sys.stderr.write("[run_all] PyYAML not installed. Add `PyYAML` to requirements.txt\n")
        sys.exit(1)
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    jobs = data.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        sys.stderr.write(f"[run_all] no jobs found in {path}\n")
        sys.exit(1)
    # defaults
    for j in jobs:
        if "tag" not in j: j["tag"] = "nightly"
        if "cmd" not in j: j["cmd"] = ""
        if "name" not in j: j["name"] = "<unnamed>"
    return jobs

def _run(label: str, cmd: str) -> tuple[bool, float]:
    print(f"\n=== [{label}] $ {cmd}")
    t0 = time.time()
    try:
        p = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        if p.stdout: print(p.stdout, end="")
        if p.stderr.strip(): print("\n[stderr]\n"+p.stderr)
        return True, time.time()-t0
    except subprocess.CalledProcessError as e:
        if e.stdout: print(e.stdout, end="")
        if e.stderr: print("\n[stderr]\n"+e.stderr)
        return False, time.time()-t0

def main():
    ap = argparse.ArgumentParser(description="Run jobs from jobs.yaml/.yml")
    ap.add_argument("--tag", default="nightly", help="hourly|nightly|weekly|deploy|all")
    ap.add_argument("--name", help="run only the job with this exact name")
    ap.add_argument("--jobs", help="path to jobs.yaml/.yml")
    args = ap.parse_args()

    # simple lock so overlapping crons don't collide
    if LOCK.exists() and (time.time()-LOCK.stat().st_mtime) < 6*3600:
        print("Another run seems active; exiting.")
        sys.exit(0)
    LOCK.write_text(str(os.getpid()))

    try:
        jobs_file = _find_jobs_file(args.jobs)
        jobs = _load_jobs(jobs_file)

        if args.name:
            chosen = [j for j in jobs if j["name"] == args.name]
        elif args.tag == "all":
            chosen = jobs
        else:
            chosen = [j for j in jobs if j["tag"] == args.tag]

        if not chosen:
            print(f"No jobs for filter tag='{args.tag}' name='{args.name or ''}'.")
            sys.exit(0)

        failures = 0
        print(f"BRidge job runner — file={jobs_file.name} tag={args.tag} count={len(chosen)}")
        for j in chosen:
            ok, dur = _run(j["name"], j["cmd"])
            if not ok:
                failures += 1

        if failures:
            print(f"\n❌ Completed with {failures} failure(s).")
            sys.exit(1)
        print("\n✅ All jobs succeeded.")
    finally:
        try: LOCK.unlink()
        except: pass

if __name__ == "__main__":
    main()
