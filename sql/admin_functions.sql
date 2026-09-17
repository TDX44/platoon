-- The platform-operator dashboard's only view of the database.
--
-- Like sql/auth_functions.sql these are SECURITY DEFINER, owned by
-- platoon_owner, so RLS does not apply inside them: that is the whole point,
-- they answer questions about EVERY tenant. SET search_path FROM CURRENT pins
-- the schema (public in production, the test schema under tests), the standard
-- guard against search_path hijacking of definer functions.
--
-- WHAT MAY BE EXPOSED HERE: counts, sizes, timestamps, organisation names and
-- slugs, and the email/name/role of a USER of the app. Nothing else. Not
-- soldier names, not profile rows, not audit `details` text, not invite
-- tokens, not logo bytes, not Clerk ids. Widening this is not a small change:
-- it turns an operator's headcount page into a cross-tenant data export, and
-- the people in `personnel` never agreed to be in one. Add a column only with
-- a reason you would write down.
--
-- THE DATABASE CANNOT TELL AN ADMIN REQUEST FROM ANY OTHER. platoon_app holds
-- EXECUTE on all three functions, because that is the role the application
-- connects as — so any signed-in request could read every tenant if the Python
-- side let it. Every caller of an admin_ function MUST sit behind
-- @platform_admin_required in server.py, and tests/test_platform_admin.py
-- fails the build if one does not.
--
-- p_now is the app's clock (app_stamp()), passed in rather than read here for
-- the same reason auth_invite() takes it: stored stamps are text written in a
-- tenant's own timezone, and the database container's clock is not that. It is
-- not a tenant argument — none of these take one, and none reads app.root_id.

CREATE OR REPLACE FUNCTION admin_totals(p_now text)
RETURNS TABLE (organisations bigint, unit_count bigint, personnel_count bigint,
               user_count bigint, unattached_users bigint, pending_invites bigint,
               database_bytes bigint)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT (SELECT count(*) FROM units u WHERE u.parent_id IS NULL),
         (SELECT count(*) FROM units),
         (SELECT count(*) FROM personnel),
         -- Attached vs signed-in-and-joined-nothing: the second number is how
         -- many people arrived and never got an invite.
         (SELECT count(*) FROM users u WHERE u.unit_id IS NOT NULL),
         (SELECT count(*) FROM users u WHERE u.unit_id IS NULL),
         -- The app's own definition of pending (see get_invites / auth_invite):
         -- neither accepted nor expired.
         (SELECT count(*) FROM invites i WHERE i.accepted_at = '' AND i.expires_at > p_now),
         pg_database_size(current_database());
$$;

CREATE OR REPLACE FUNCTION admin_organisations(p_now text)
RETURNS TABLE (org_id int, org_name text, org_slug text, org_kind text, created_stamp text,
               unit_count bigint, personnel_count bigint, user_count bigint,
               owner_emails text, pending_invites bigint, has_logo boolean,
               last_activity text, audit_7d bigint)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT r.id, r.name, r.slug, r.kind, r.created_at,
         (SELECT count(*) FROM units u WHERE u.root_id = r.id),
         (SELECT count(*) FROM personnel p WHERE p.root_id = r.id),
         (SELECT count(*) FROM users us WHERE us.root_id = r.id),
         (SELECT string_agg(us.email, ', ' ORDER BY us.email) FROM users us
           WHERE us.root_id = r.id AND us.role = 'owner' AND us.email <> ''),
         (SELECT count(*) FROM invites i
           WHERE i.root_id = r.id AND i.accepted_at = '' AND i.expires_at > p_now),
         EXISTS (SELECT 1 FROM settings s WHERE s.root_id = r.id AND s.key = 'logo'),
         (SELECT max(a."timestamp") FROM audit_log a WHERE a.root_id = r.id),
         (SELECT count(*) FROM audit_log a WHERE a.root_id = r.id
            AND a."timestamp" >= to_char(p_now::timestamp - interval '7 days',
                                         'YYYY-MM-DD HH24:MI:SS'))
    FROM units r
   WHERE r.parent_id IS NULL
   ORDER BY r.name;
$$;

CREATE OR REPLACE FUNCTION admin_recent_users(p_limit int)
RETURNS TABLE (user_id int, email text, full_name text, role text,
               org_name text, signed_in boolean)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  -- `users` has no created_at, so the identity sequence is the arrival order.
  SELECT u.id, u.email, u.full_name, u.role,
         (SELECT r.name FROM units r WHERE r.id = u.root_id),
         (u.clerk_user_id <> '')
    FROM users u
   ORDER BY u.id DESC
   LIMIT greatest(0, least(p_limit, 200));
$$;

DO $$
DECLARE f text;
BEGIN
  FOREACH f IN ARRAY ARRAY[
    'admin_totals(text)', 'admin_organisations(text)', 'admin_recent_users(int)']
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO platoon_app', f);
  END LOOP;
END $$;
