#run_vendor_ingest_queue.py
import os
import sys
import glob
import hashlib
import shutil
import subprocess
from pathlib import Path

FAILED_DIR   = Path(r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\_failed")
ARCHIVE_DIR  = Path(r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\archive")
PROCESSED_DIR = Path(r"C:\Users\tbyer\BRidge-html\HTMLs\Vendor Quotes\_processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Use the venv python to run your existing scripts
PY = Path(sys.executable)  # if you run this inside venv, it's correct

DB = os.environ["DATABASE_URL"]

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def run_sql(sql: str):
    import psycopg2
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def db_mark_seen(filename: str, sha: str) -> bool:
    """Return True if already processed/known, False if newly inserted."""
    import psycopg2
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO public.vendor_ingest_files (filename, file_sha256, status)
        VALUES (%s, %s, 'pending')
        ON CONFLICT (file_sha256) DO NOTHING
    """, (filename, sha))
    inserted = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return inserted

def db_set_status(sha: str, status: str, err: str | None = None):
    import psycopg2
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        UPDATE public.vendor_ingest_files
        SET status=%s,
            last_error=%s,
            processed_at=CASE WHEN %s='ok' THEN now() ELSE processed_at END
        WHERE file_sha256=%s
    """, (status, err, status, sha))
    conn.commit()
    cur.close()
    conn.close()

def call(script_name: str):
    # run python script (must exist in current folder)
    subprocess.run([str(PY), script_name], check=True)

def main():
    # Candidates: any csv in _failed + any xlsx in archive (C&Y)
    files = []
    files += [Path(p) for p in glob.glob(str(FAILED_DIR / "*.csv"))]
    files += [Path(p) for p in glob.glob(str(ARCHIVE_DIR / "*.xlsx"))]

    # Process oldest first to be predictable
    files.sort(key=lambda p: p.stat().st_mtime)

    if not files:
        print("No files found.")
        return

    for p in files:
        sha = sha256_file(p)
        new = db_mark_seen(p.name, sha)
        if not new:
            # already known; skip
            continue

        try:
            print("Processing:", p)

            # Dispatch by filename pattern
            name_lower = p.name.lower()

            if "pro metal quotation chicago" in name_lower and p.suffix.lower() == ".xlsx":
                # C&Y Excel ingester already auto-picks newest from archive
                call("ingest_cy_quote_sheet.py")

            elif "lewis salvage vendor quotes" in name_lower and p.suffix.lower() == ".csv":
                call("ingest_lewis_quotes.py")

            elif "prices for vendor quotes" in name_lower and p.suffix.lower() == ".csv":
                call("ingest_vendor_quotes_csv.py")

            else:
                raise RuntimeError(f"Unknown file type/pattern: {p.name}")

            # After any ingest: re-canonicalize everything + refresh views that pricing uses
            run_sql("""
                UPDATE public.vendor_quotes
                SET material_canonical = public.resolve_material_canonical(vendor, material)
                WHERE material IS NOT NULL;
            """)

            run_sql("""
                CREATE OR REPLACE VIEW public.v_vendor_latest_quotes_all_norm AS
                SELECT DISTINCT ON (public.norm_key(vendor), public.norm_key(material_canonical))
                  id, vendor, material, material_canonical,
                  public.norm_key(vendor) AS vendor_key,
                  public.norm_key(material_canonical) AS material_key,
                  price_per_lb, unit_raw, sheet_date, inserted_at, source_file
                FROM public.vendor_quotes
                WHERE material_canonical IS NOT NULL
                ORDER BY
                  public.norm_key(vendor),
                  public.norm_key(material_canonical),
                  sheet_date DESC,
                  inserted_at DESC;
            """)

            run_sql("""
                CREATE OR REPLACE VIEW public.v_vendor_latest_quotes_best_norm AS
                SELECT DISTINCT ON (public.norm_key(material_canonical))
                  id, vendor, material, material_canonical,
                  public.norm_key(material_canonical) AS material_key,
                  price_per_lb, unit_raw, sheet_date, inserted_at, source_file
                FROM public.vendor_quotes
                WHERE material_canonical IS NOT NULL
                ORDER BY
                  public.norm_key(material_canonical),
                  sheet_date DESC,
                  inserted_at DESC;
            """)

            db_set_status(sha, "ok", None)

            # Move file to _processed so it’s visibly “done”
            dest = PROCESSED_DIR / p.name
            shutil.move(str(p), str(dest))
            print("Moved to:", dest)

        except Exception as e:
            db_set_status(sha, "error", str(e))
            print("FAILED:", p.name, "->", e)

if __name__ == "__main__":
    main()
