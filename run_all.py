# run_all.py
import os, sys, time, argparse, subprocess
from pathlib import Path

JOBS_YAML = Path(__file__).with_name("jobs.yaml")
LOCK = Path("/tmp/bridge_cron.lock")

def load_jobs(path: Path):
    jobs, cur = [], None
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s or s.startswith("#"): continue
        if s.startswith("jobs:"): continue
        if s.startswith("- name:"):
            if cur: jobs.append(cur)
            cur = {"name": s.split(":",1)[1].strip()}
            continue
        if ":" in s and cur is not None:
            k, v = s.split(":",1)
            cur[k.strip()] = v.strip()
    if cur: jobs.append(cur)
    for j in jobs:
        j.setdefault("tag","nightly")
        j.setdefault("cmd","")
    return jobs

def run(label, cmd):
    print(f"\n=== [{label}] $ {cmd}")
    t0 = time.time()
    try:
        p = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(p.stdout)
        if p.stderr.strip(): print("\n[stderr]\n"+p.stderr)
        return True, time.time()-t0
    except subprocess.CalledProcessError as e:
        if e.stdout: print(e.stdout)
        if e.stderr: print("\n[stderr]\n"+e.stderr)
        return False, time.time()-t0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="nightly", help="hourly|nightly|weekly|all")
    args = ap.parse_args()

    if LOCK.exists() and (time.time()-LOCK.stat().st_mtime) < 6*3600:
        print("Another run seems active; exiting.")
        sys.exit(0)
    LOCK.write_text(str(os.getpid()))

    try:
        if not JOBS_YAML.exists():
            print("Missing jobs.yaml next to run_all.py")
            sys.exit(1)

        jobs = load_jobs(JOBS_YAML)
        chosen = [j for j in jobs if args.tag=="all" or j["tag"]==args.tag]
        if not chosen:
            print(f"No jobs for tag '{args.tag}'.")
            return

        failures = 0
        print(f"BRidge job runner — tag={args.tag}")
        for j in chosen:
            ok, dur = run(j["name"], j["cmd"])
            if not ok: failures += 1

        if failures:
            print(f"\n❌ Completed with {failures} failure(s).")
            sys.exit(1)
        print("\n✅ All jobs succeeded.")
    finally:
        try: LOCK.unlink()
        except: pass

if __name__ == "__main__":
    main()
