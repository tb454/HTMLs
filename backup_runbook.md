# BRidge Backup & Retention Runbook

## Daily Backups
- Command: `pg_dump -Fc $DATABASE_URL > backup_$(date +%F).dump`
- Run every day at 08:00 UTC.
- Retain 30 daily, 12 monthly, 7 yearly backups.

## Retention Policy
- 30 daily backups (last month).
- 12 monthly backups (last year).
- 7 yearly backups (7 years of history).
- Old backups are pruned automatically.

## Restore Steps
1. Provision an empty Postgres database.
2. Run: `pg_restore -d $TARGET_DB backup_YYYY-MM-DD.dump`
3. Verify schema & row counts.

## On-Demand Export
- `/admin/export_all` provides a full CSV/ZIP dump (contracts, BOLs, inventory, audit).
- Use for quick snapshots outside of scheduled dumps.

---
