# apply_migrations.py
import os, glob, hashlib
import psycopg

def norm_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def main() -> int:
    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        print("apply_migrations: DATABASE_URL missing")
        return 2

    db = norm_db_url(db)
    mig_dir = os.getenv("MIGRATIONS_DIR", "migrations")
    files = sorted(glob.glob(os.path.join(mig_dir, "*.sql")))

    if not files:
        print(f"apply_migrations: no migrations found in {mig_dir}/")
        return 0

    with psycopg.connect(db) as conn:
        conn.execute("""
          create table if not exists public.schema_migrations(
            version text primary key,
            applied_at timestamptz not null default now(),
            checksum text not null
          );
        """)
        conn.commit()

        for path in files:
            ver = os.path.basename(path)
            raw = open(path, "r", encoding="utf-8-sig").read()
            sql = raw.lstrip("\ufeff").strip()

            # Skip empty/whitespace-only migration files
            if not sql:
                print(f"skip-empty {ver}")
                continue

            ch = sha256_text(sql)

            row = conn.execute(
                "select checksum from public.schema_migrations where version=%s",
                (ver,)
            ).fetchone()

            if row:
                if row[0] != ch:
                    raise RuntimeError(f"Migration checksum mismatch for {ver} (edited after apply).")
                print(f"skip {ver}")
                continue

            print(f"apply {ver}")
            with conn.transaction():
                conn.execute(sql)
                conn.execute(
                    "insert into public.schema_migrations(version, checksum) values (%s,%s)",
                    (ver, ch),
                )

    print("apply_migrations: OK")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())