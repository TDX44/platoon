-- The platform-operator dashboard's only view of the database.
--
-- Like sql/auth_functions.sql these are SECURITY DEFINER, owned by
-- platoon_owner, so RLS does not apply inside them: that is the whole point,
-- they answer questions about EVERY tenant. SET search_path FROM CURRENT pins
-- the schema (public in production, the test schema under tests), the standard
-- guard against search_path hijacking of definer functions.
--
-- WHAT MAY BE EXPOSED HERE: counts, sizes, timestamps, organization names and
-- slugs, and the email/name/role of a USER of the app. Nothing else. Not
-- soldier names, not profile rows, not audit `details` text, not invite
-- tokens, not logo bytes, not Clerk ids. Widening this is not a small change:
-- it turns an operator's headcount page into a cross-tenant data export, and
-- the people in `personnel` never agreed to be in one. Add a column only with
-- a reason you would write down.
-- Billing adds an account's billing mode, trial stamps and Stripe STATUS
-- word — never a Stripe customer or subscription id.
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

-- CREATE OR REPLACE cannot change a RETURNS TABLE column list ("cannot change
-- return type of existing function"), and adding a column to the dashboard is
-- the likeliest next edit to this file. A failing statement here aborts
-- init_db()'s transaction, which means the app does not boot at all — so drop
-- first. This runs inside init_db()'s single transaction, under the same
-- advisory lock the rest of the boot takes, and Postgres's DDL is
-- transactional: a concurrent request in the older worker keeps seeing the old
-- function until this commits, and then sees the new one. There is no window
-- in which the function is missing.
DROP FUNCTION IF EXISTS admin_totals(text);
DROP FUNCTION IF EXISTS admin_organizations(text);
-- The pre-rename British-spelled name. A deployed instance still has it, granted
-- to platoon_app; drop the orphan so no un-routed SECURITY DEFINER function lingers.
DROP FUNCTION IF EXISTS admin_organisations(text);
DROP FUNCTION IF EXISTS admin_recent_users(int);
DROP FUNCTION IF EXISTS admin_billing_rows();
DROP FUNCTION IF EXISTS admin_org_units(int);

CREATE OR REPLACE FUNCTION admin_totals(p_now text)
RETURNS TABLE (organizations bigint, unit_count bigint, personnel_count bigint,
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

CREATE OR REPLACE FUNCTION admin_organizations(p_now text)
RETURNS TABLE (org_id int, org_name text, org_slug text, org_kind text, created_stamp text,
               unit_count bigint, personnel_count bigint, user_count bigint,
               owner_emails text, pending_invites bigint, has_logo boolean,
               last_activity text, audit_7d bigint, org_timezone text)
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
                                         'YYYY-MM-DD HH24:MI:SS')),
         -- The org's duty day. Scoped (root_id, NULL, 'org_timezone') exactly
         -- as app_today() reads it; NULL here means the row has never been set
         -- and that tenant is still running on PLATOON_TZ.
         (SELECT s.value FROM settings s
           WHERE s.root_id = r.id AND s.unit_id IS NULL AND s.key = 'org_timezone')
    FROM units r
   WHERE r.parent_id IS NULL
   ORDER BY r.name;
$$;

CREATE OR REPLACE FUNCTION admin_recent_users(p_limit int)
RETURNS TABLE (user_id int, email text, full_name text, role text,
               org_name text, root_id int, signed_in boolean)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  -- `users` has no created_at, so the identity sequence is the arrival order.
  SELECT u.id, u.email, u.full_name, u.role,
         (SELECT r.name FROM units r WHERE r.id = u.root_id),
         u.root_id,
         (u.clerk_user_id <> '')
    FROM users u
   ORDER BY u.id DESC
   -- 2000, raised from 200 when the dashboard became a directory rather
   -- than a "25 most recent" list. Still a hard cap: the payload is one
   -- JSON response and an operator page, not an export endpoint.
   LIMIT greatest(0, least(p_limit, 2000));
$$;

-- Every attached account's billing columns, for the overview's Billing
-- column and its five counts. The state itself is computed in Python by
-- billing_rules.billing_state() so the rule lives in one place.
CREATE OR REPLACE FUNCTION admin_billing_rows()
RETURNS TABLE (user_id int, email text, root_id int, billing_mode text,
               trial_ends_at timestamptz, extended_at timestamptz, stripe_status text,
               cancel_at_period_end boolean, current_period_end timestamptz,
               stripe_price_lookup_key text)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT u.id, u.email, u.root_id, COALESCE(s.billing_mode, 'default'),
         s.trial_ends_at, s.extended_at,
         s.stripe_status, COALESCE(s.cancel_at_period_end, false), s.current_period_end,
         -- The PLAN, not the price id: a lookup key is a name we chose
         -- ('platoon_leader_monthly'), not a Stripe object id, so it stays
         -- inside the rule at the top of this file. The amount is never stored
         -- -- the dashboard multiplies by whatever Stripe says the price is
         -- today, the same figure the pricing screen shows.
         s.stripe_price_lookup_key
    FROM users u LEFT JOIN subscriptions s ON s.user_id = u.id
   WHERE u.unit_id IS NOT NULL;
$$;

-- One organization's unit tree, for the drill-down. Structure only: a unit's
-- name, slug, kind, parent and how many personnel rows hang off it. NOT the
-- personnel themselves -- the rule at the top of this file still holds, and a
-- drill-down is exactly where someone would be tempted to break it.
CREATE OR REPLACE FUNCTION admin_org_units(p_org_id int)
RETURNS TABLE (unit_id int, unit_name text, unit_slug text, unit_kind text,
               parent_id int, depth int, personnel_count bigint, user_count bigint)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  WITH RECURSIVE tree AS (
    SELECT u.id, u.name, u.slug, u.kind, u.parent_id, 0 AS depth
      FROM units u WHERE u.id = p_org_id AND u.parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.slug, c.kind, c.parent_id, t.depth + 1
      FROM units c JOIN tree t ON c.parent_id = t.id
  )
  SELECT t.id, t.name, t.slug, t.kind, t.parent_id, t.depth,
         (SELECT count(*) FROM personnel p WHERE p.unit_id = t.id),
         (SELECT count(*) FROM users us WHERE us.unit_id = t.id)
    FROM tree t
   ORDER BY t.depth, t.name;
$$;

DO $$
DECLARE f text;
BEGIN
  FOREACH f IN ARRAY ARRAY[
    'admin_totals(text)', 'admin_organizations(text)', 'admin_recent_users(int)',
    'admin_billing_rows()', 'admin_org_units(int)']
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO platoon_app', f);
  END LOOP;
END $$;
