-- Row-level security: the tenant blast door.
-- Applied by init_db() as platoon_owner on every boot (idempotent). The app
-- connects as platoon_app, which owns nothing and has no BYPASSRLS, so these
-- policies bind every statement it runs. current_setting(..., true) is NULL
-- when the variable is unset, and `root_id = NULL` is never true: a connection
-- that has not declared a tenant sees zero rows and can write none. Once the
-- variable HAS been set on a connection, rolling back the transaction that set
-- it reverts it to '' rather than to unset -- and ''::int raises, which would
-- turn default-deny into a 500 on the next pooled request. NULLIF folds both
-- shapes back to NULL.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['units', 'personnel', 'personnel_profile', 'scheduled_events',
                           'duty_roster', 'report_history', 'audit_log', 'settings',
                           'users', 'invites', 'subscriptions']
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant ON %I USING (root_id = NULLIF(current_setting(''app.root_id'', true), '''')::int) '
      'WITH CHECK (root_id = NULLIF(current_setting(''app.root_id'', true), '''')::int)', t);
  END LOOP;
END $$;
