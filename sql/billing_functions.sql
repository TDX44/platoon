-- Billing's only cross-tenant surface.
--
-- SECURITY DEFINER, owned by platoon_owner, SET search_path FROM CURRENT,
-- EXECUTE revoked from PUBLIC and granted to platoon_app: the pattern of
-- sql/auth_functions.sql and sql/admin_functions.sql. The webhook has no
-- session and no tenant — Stripe is the caller — so it cannot go through RLS
-- and must go through these instead. Nothing else may call them:
-- tests/test_billing.py greps server.py and fails the build if
-- billing_find_by_customer / billing_apply_stripe / billing_record_event are
-- reached from anywhere but the webhook, or billing_set_mode from anywhere
-- but a route behind @platform_admin_required.
--
-- billing_apply_stripe writes ONLY the mirrored Stripe columns. It cannot
-- touch billing_mode or the trial stamps: those are ours, and a payload off
-- the wire must never be able to comp an account or extend a trial.
--
-- Dropped before recreated, like admin_functions.sql: CREATE OR REPLACE
-- cannot change a RETURNS TABLE column list, and a failing statement here
-- would stop the app booting.
DROP FUNCTION IF EXISTS billing_find_by_customer(text);
DROP FUNCTION IF EXISTS billing_apply_stripe(text, text, text, text, timestamptz, boolean);
DROP FUNCTION IF EXISTS billing_record_event(text);
DROP FUNCTION IF EXISTS billing_set_mode(int, text, text);

-- stripe_subscription_id comes back too: the webhook has to know which
-- subscription the account already holds before billing_apply_stripe adopts a
-- newer one, so the superseded one can be cancelled at Stripe instead of
-- quietly billing the same card forever. Reading it here keeps every
-- cross-tenant read inside the enumerated functions.
CREATE OR REPLACE FUNCTION billing_find_by_customer(p_customer text)
RETURNS TABLE (user_id int, root_id int, stripe_subscription_id text)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT s.user_id, s.root_id, s.stripe_subscription_id
    FROM subscriptions s WHERE s.stripe_customer_id = p_customer;
$$;

-- A subscription event for an id other than the stored one is applied only
-- when it opens access (a new subscription supersedes the old) or when
-- nothing is stored yet. A late `deleted` for a subscription the account has
-- since replaced therefore cannot lock it — the shape of Resyrv's
-- cancellation incident. NULL arguments keep the stored value (the
-- invoice.payment_failed path knows only the customer and the status).
CREATE OR REPLACE FUNCTION billing_apply_stripe(p_customer text, p_subscription text, p_status text,
                                                p_lookup_key text, p_period_end timestamptz, p_cancel boolean)
RETURNS int
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  UPDATE subscriptions s
     SET stripe_subscription_id  = COALESCE(p_subscription, s.stripe_subscription_id),
         stripe_status           = p_status,
         stripe_price_lookup_key = COALESCE(p_lookup_key, s.stripe_price_lookup_key),
         current_period_end      = COALESCE(p_period_end, s.current_period_end),
         cancel_at_period_end    = COALESCE(p_cancel, s.cancel_at_period_end),
         updated_at              = now()
   WHERE s.stripe_customer_id = p_customer
     AND (p_subscription IS NULL
          OR s.stripe_subscription_id IS NULL
          OR s.stripe_subscription_id = p_subscription
          OR p_status IN ('active', 'trialing'))
     -- ...and a failed invoice never downgrades a locked status back into an
     -- open one. past_due is OPEN, so without this a late or one-off
     -- invoice.payment_failed would re-open a cancelled account.
     -- COALESCE, not a bare column: s.stripe_status is NULL until the first
     -- subscription event lands, and `NULL IN (...)` is NULL, so `NOT NULL`
     -- is NULL and the row silently fails to match -- a payment failure that
     -- beat customer.subscription.created was dropped without a trace.
     AND NOT (p_status = 'past_due' AND COALESCE(s.stripe_status, '') IN ('canceled', 'unpaid', 'incomplete_expired'))
  RETURNING s.user_id;
$$;

-- true when this delivery is new; NULL (falsy) when it was already recorded.
CREATE OR REPLACE FUNCTION billing_record_event(p_event_id text)
RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  INSERT INTO stripe_events (event_id) VALUES (p_event_id)
  ON CONFLICT (event_id) DO NOTHING
  RETURNING true;
$$;

-- The /admin comp toggle. Creates the row if the account has not signed in
-- attached yet. Returns the account's root_id so the caller can audit it in
-- that tenant; no row when the user does not exist or belongs to no unit.
CREATE OR REPLACE FUNCTION billing_set_mode(p_user_id int, p_mode text, p_by text)
RETURNS int
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  INSERT INTO subscriptions (user_id, root_id, billing_mode, comped_by, updated_at)
  SELECT u.id, u.root_id, p_mode, p_by, now() FROM users u
   WHERE u.id = p_user_id AND u.root_id IS NOT NULL
  ON CONFLICT (user_id) DO UPDATE
     SET billing_mode = EXCLUDED.billing_mode, comped_by = EXCLUDED.comped_by, updated_at = now()
  RETURNING subscriptions.root_id;
$$;

DO $$
DECLARE f text;
BEGIN
  FOREACH f IN ARRAY ARRAY[
    'billing_find_by_customer(text)',
    'billing_apply_stripe(text, text, text, text, timestamptz, boolean)',
    'billing_record_event(text)',
    'billing_set_mode(int, text, text)']
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO platoon_app', f);
  END LOOP;
END $$;
