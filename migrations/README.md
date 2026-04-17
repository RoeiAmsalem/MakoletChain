# Database Migrations

Every schema change lives here as a numbered `.sql` file.

## Naming
`NNN_short_description.sql` — zero-padded 3-digit prefix, snake_case.
Examples: `002_add_employee_aliases.sql`, `003_add_iec_contract_number.sql`

## Rules
1. One logical change per file
2. Migrations are FORWARD-ONLY. No rollback scripts. If you need to undo, write a new migration.
3. Migrations are IDEMPOTENT where possible (`ADD COLUMN IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`).
4. Never edit an already-applied migration file. Write a new one.
5. Never skip numbers.

## How to apply
Locally / on staging / on prod:
```
python3 scripts/migrate.py
```

The script reads `_migrations` table, finds unapplied files by number, applies each in a transaction, and records the filename + applied_at timestamp.

## Promotion flow
1. On dev, add `migrations/NNN_your_change.sql`
2. Run `python3 scripts/migrate.py` locally to test
3. Push dev, deploy to staging, migrations auto-run on deploy
4. When ready for prod: merge dev → main, push, pull on server, migrations auto-run on restart
