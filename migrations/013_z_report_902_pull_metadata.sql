-- Track HOW each z_report_902 row was produced so /z-status can show
-- accurate provenance per pull:
--   trigger_type ∈ {'auto','manual'}    -- cron/scheduler vs CLI/admin trigger
--   auth_source  ∈ {'chain','per_store'} -- chain-account login vs per-branch login
-- Old rows stay NULL; the UI renders "—" for those.

ALTER TABLE z_report_902 ADD COLUMN trigger_type TEXT;
ALTER TABLE z_report_902 ADD COLUMN auth_source TEXT;
