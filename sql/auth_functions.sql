-- The only code that reads or writes tenant tables without a tenant set.
-- SECURITY DEFINER: runs as the owner (platoon_owner), so RLS does not apply
-- inside. SET search_path FROM CURRENT pins the schema to the one init_db()
-- was connected to (public in production, the test schema under tests), the
-- standard guard against search_path hijacking of definer functions.
-- Everything here is enumerable and small on purpose: if it is not in this
-- file, the application cannot do it across tenants.

CREATE OR REPLACE FUNCTION auth_user_by_clerk_id(p_clerk_user_id text)
RETURNS SETOF users LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT * FROM users WHERE clerk_user_id = p_clerk_user_id AND p_clerk_user_id <> '' LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION auth_user_by_identity(p_email text, p_username text)
RETURNS SETOF users LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  -- Legacy rows only: a local account that has never synced with Clerk.
  SELECT * FROM users
  WHERE clerk_user_id = ''
    AND ((p_email <> '' AND LOWER(email) = LOWER(p_email))
      OR (p_username <> '' AND LOWER(username) = LOWER(p_username)))
  ORDER BY (LOWER(email) = LOWER(p_email)) DESC
  LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION auth_invite(p_token text, p_now text)
RETURNS SETOF invites LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT * FROM invites
  WHERE token = p_token AND accepted_at = '' AND expires_at > p_now
  LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION auth_create_user(
  p_clerk_user_id text, p_username text, p_email text, p_full_name text,
  p_unit_id int, p_role text, p_root_id int)
RETURNS SETOF users LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  INSERT INTO users (username, password_hash, clerk_user_id, email, full_name, unit_id, role, root_id)
  VALUES (p_username, 'clerk', p_clerk_user_id, p_email, p_full_name, p_unit_id, p_role, p_root_id)
  RETURNING *;
$$;

CREATE OR REPLACE FUNCTION auth_claim_legacy_user(
  p_user_id int, p_clerk_user_id text, p_username text, p_email text, p_full_name text,
  p_unit_id int, p_role text, p_root_id int)
RETURNS SETOF users LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  UPDATE users SET clerk_user_id = p_clerk_user_id, username = p_username, email = p_email,
                   full_name = p_full_name, password_hash = 'clerk', pin_hash = '',
                   unit_id = p_unit_id, role = p_role, root_id = p_root_id
  WHERE id = p_user_id AND clerk_user_id = ''
  RETURNING *;
$$;

-- An account that signed up attached to nothing and later redeems an invite.
-- Its row has root_id NULL, which no tenant's RLS can see, so attaching it is
-- pre-tenant work. The invite vouches, in here rather than in the caller: it
-- must already be accepted BY this Clerk account (the app's single-use
-- conditional UPDATE does that under the invite's tenant), and only a row
-- that is still attached nowhere moves.
CREATE OR REPLACE FUNCTION auth_attach_invited_user(p_user_id int, p_token text, p_clerk_user_id text)
RETURNS SETOF users LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  UPDATE users u SET unit_id = i.unit_id, role = i.role, root_id = i.root_id
  FROM invites i
  WHERE i.token = p_token AND i.accepted_by = p_clerk_user_id AND p_clerk_user_id <> ''
    AND u.id = p_user_id AND u.clerk_user_id = p_clerk_user_id AND u.unit_id IS NULL
  RETURNING u.*;
$$;

CREATE OR REPLACE FUNCTION auth_create_root_unit(p_name text, p_kind text, p_slug text, p_user_id int)
RETURNS int LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE new_id int;
BEGIN
  INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (NULL, 0, p_kind, p_name, p_slug)
  RETURNING id INTO new_id;
  UPDATE units SET root_id = new_id WHERE id = new_id;
  UPDATE users SET unit_id = new_id, role = 'owner', root_id = new_id
   WHERE id = p_user_id AND unit_id IS NULL;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'user % is already attached to a unit', p_user_id;
  END IF;
  RETURN new_id;
END $$;

DO $$
DECLARE f text;
BEGIN
  FOREACH f IN ARRAY ARRAY[
    'auth_user_by_clerk_id(text)', 'auth_user_by_identity(text, text)', 'auth_invite(text, text)',
    'auth_create_user(text, text, text, text, int, text, int)',
    'auth_claim_legacy_user(int, text, text, text, text, int, text, int)',
    'auth_attach_invited_user(int, text, text)',
    'auth_create_root_unit(text, text, text, int)']
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO platoon_app', f);
  END LOOP;
END $$;
