"""Self-serve signup and invite attachment through sync_clerk_user().

Run with: python tests/test_auth_flow.py

Drives sync_clerk_user() inside a request context with fake Clerk claims, the
way /api/auth/sync does, so the front-door SQL functions are exercised as the
app role with no tenant set — which is the whole point of them.
"""
import os
import sys
from datetime import timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402
from flask import g  # noqa: E402


def sync(clerk_id, email, invite_token=''):
    with server.app.test_request_context('/api/auth/sync', method='POST'):
        g.auth_claims = {'sub': clerk_id}
        user, err = server.sync_clerk_user({'username': email, 'email': email, 'full_name': 'T',
                                            'invite_token': invite_token})
        if err is None:
            g.db_commit = True
        server._close_db(None)
    return user, err


def stamp_in(days):
    return (server.app_now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')


def make_invite(tree, token, role='leader', days=1, unit=None):
    conn = dbharness.owner_conn()
    conn.execute(
        'INSERT INTO invites (token, label, unit_id, role, root_id, created_by, expires_at, created_at) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        (token, 'PSG', unit or tree['child'], role, tree['root'], 'boss', stamp_in(days), server.app_stamp()))
    conn.commit(); conn.close()


def test_stranger_gets_an_unattached_account():
    user, err = sync('clerk_new_1', 'new1@example.com')
    assert err is None, err
    assert user['unit_id'] is None and user['root_id'] is None, user
    again, err = sync('clerk_new_1', 'new1@example.com')
    assert err is None and again['id'] == user['id'], 'second sign-in finds the same row'


def test_invite_attaches_at_its_unit_with_its_role():
    t = dbharness.make_tree('Hotel Co')
    make_invite(t, 'tok-1')
    user, err = sync('clerk_psg', 'psg@example.com', 'tok-1')
    assert err is None, err
    assert (user['unit_id'], user['role'], user['root_id']) == (t['child'], 'leader', t['root']), user
    conn = dbharness.owner_conn()
    acc = conn.execute("SELECT accepted_by FROM invites WHERE token = 'tok-1'").fetchone()['accepted_by']
    conn.close()
    assert acc == 'clerk_psg'
    user2, err = sync('clerk_late', 'late@example.com', 'tok-1')
    assert err is None and user2['unit_id'] is None, 'a used invite is just a stranger sign-in'


def test_expired_invite_is_ignored():
    t = dbharness.make_tree('Juliet Co')
    make_invite(t, 'tok-old', days=-1)
    user, err = sync('clerk_slow', 'slow@example.com', 'tok-old')
    assert err is None and user['unit_id'] is None


def make_legacy(email, username, unit_id=None, root_id=None):
    """A row that has never synced with Clerk, optionally already attached."""
    conn = dbharness.owner_conn()
    row = conn.execute(
        "INSERT INTO users (username, password_hash, clerk_user_id, email, unit_id, role, root_id) "
        "VALUES (%s, 'x', '', %s, %s, 'leader', %s) RETURNING *",
        (username, email, unit_id, root_id)).fetchone()
    conn.commit(); conn.close()
    return dict(row)


def reread(user_id):
    conn = dbharness.owner_conn()
    row = conn.execute('SELECT * FROM users WHERE id = %s', (user_id,)).fetchone()
    conn.close()
    return dict(row)


def test_an_unattached_legacy_row_is_claimed_by_email():
    legacy = make_legacy('free@example.com', 'free.agent')
    user, err = sync('clerk_free', 'free@example.com')
    assert err is None and user['id'] == legacy['id'] and user['clerk_user_id'] == 'clerk_free', user
    assert user['unit_id'] is None, 'claiming an unattached row attaches you to nothing'


def test_an_attached_legacy_row_is_not_claimed_on_a_bare_email():
    """The takeover: email comes from the request body, so it proves nothing."""
    t = dbharness.make_tree('India Co')
    legacy = make_legacy('old@example.com', 'old.hand', t['child'], t['root'])
    user, err = sync('clerk_thief', 'old@example.com')
    assert err is None and user['id'] != legacy['id'] and user['unit_id'] is None, \
        f'a stranger claiming a tenant email signs in attached to nothing: {user}'
    assert reread(legacy['id'])['clerk_user_id'] == '', 'the tenant row is left untouched'


def test_an_invite_for_the_same_root_vouches_for_the_legacy_row():
    t = dbharness.make_tree('Kilo Co')
    legacy = make_legacy('psg2@example.com', 'psg.two', t['root'], t['root'])
    make_invite(t, 'tok-vouch')
    user, err = sync('clerk_vouched', 'psg2@example.com', 'tok-vouch')
    assert err is None and user['id'] == legacy['id'] and user['clerk_user_id'] == 'clerk_vouched', user
    assert (user['unit_id'], user['role']) == (t['child'], 'leader'), \
        f"the invite's unit and role win over the row's: {user}"


def test_an_invite_for_another_root_does_not_vouch():
    t = dbharness.make_tree('Lima Co')
    other = dbharness.make_tree('Mike Co')
    legacy = make_legacy('cross@example.com', 'cross.hand', t['child'], t['root'])
    make_invite(other, 'tok-elsewhere')
    user, err = sync('clerk_cross', 'cross@example.com', 'tok-elsewhere')
    assert err is None and user['id'] != legacy['id'], f'no other tenant can vouch for this row: {user}'
    assert (user['unit_id'], user['root_id']) == (other['child'], other['root']), \
        f'the signer attaches where their own invite points: {user}'
    assert reread(legacy['id'])['clerk_user_id'] == '', 'the first tenant row is left untouched'


class _Racer:
    """Steals the legacy row from a second connection in the instant between
    sync_clerk_user() finding it and claiming it."""

    def __init__(self, conn, steal, marker='auth_claim_legacy_user'):
        self._conn = conn
        self._steal = steal
        self._marker = marker

    def execute(self, sql, params=None, *a, **kw):
        if self._marker in sql and self._steal:
            self._steal()
            self._steal = None
        return self._conn.execute(sql, params, *a, **kw)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def me_for(user):
    """GET /api/me as `user`, the way the app asks who it is signed in as."""
    dbharness.as_user(user)
    with server.app.test_client() as client:
        res = client.get('/api/me')
    assert res.status_code == 200, res.get_data(as_text=True)
    return res.get_json()


def test_me_names_whoever_invited_you():
    """The home screen says who put you here, so /api/me has to know."""
    t = dbharness.make_tree('November Co')
    make_invite(t, 'tok-who')
    user, err = sync('clerk_invited', 'invited@example.com', 'tok-who')
    assert err is None, err
    # 'boss' has no users row at all, so the invite's own created_by stands.
    assert me_for(user)['invited_by'] == 'boss', me_for(user)

    # And when the inviter IS a local row with a real name, that name wins —
    # nobody wants to be told they were invited by "clerk_a1b2c3".
    conn = dbharness.owner_conn()
    conn.execute(
        "INSERT INTO users (username, password_hash, clerk_user_id, email, full_name, unit_id, role, root_id) "
        "VALUES ('boss', 'x', 'clerk_boss', 'boss@example.com', 'CPT Alvarez', %s, 'owner', %s)",
        (t['root'], t['root']))
    conn.commit(); conn.close()
    assert me_for(user)['invited_by'] == 'CPT Alvarez', me_for(user)


def test_a_founder_was_invited_by_nobody():
    t = dbharness.make_tree('Oscar Co')
    founder = dbharness.make_user(t['root'], role='owner')
    assert me_for(founder)['invited_by'] == '', me_for(founder)
    # Neither was someone who is signed in but attached to nothing: they have
    # no tenant, so there is nowhere for the question to be asked.
    stranger, err = sync('clerk_nobody', 'nobody@example.com')
    assert err is None and me_for(stranger)['invited_by'] == '', me_for(stranger)


def test_the_user_list_is_not_made_n_plus_1_by_this():
    """`invited_by` costs a query, so it must stay out of _user_json()."""
    t = dbharness.make_tree('Papa Co')
    row = dbharness.make_user(t['root'], role='owner')
    conn = dbharness.owner_conn()
    listed = server._user_json(conn, row)
    conn.close()
    assert 'invited_by' not in listed, \
        '_user_json() also builds /api/users rows; one invite query per person is a list killer'


def test_losing_the_race_for_a_legacy_row_is_an_error_not_a_crash():
    legacy = make_legacy('raced@example.com', 'raced.hand')

    def steal():
        conn = dbharness.owner_conn()
        conn.execute("UPDATE users SET clerk_user_id = 'clerk_winner' WHERE id = %s", (legacy['id'],))
        conn.commit(); conn.close()

    real_get_db = server.get_db
    server.get_db = lambda: _Racer(real_get_db(), steal)
    try:
        user, err = sync('clerk_loser', 'raced@example.com')
    finally:
        server.get_db = real_get_db
    assert user is None and err, 'the loser of the race gets an error, not a None the route 500s on'


def test_an_unattached_account_redeems_an_invite_later():
    """Signed up, landed on 'create a unit', then followed an invite link: the
    existing row must attach, not be handed straight back unattached."""
    stranger, err = sync('clerk_later', 'later@example.com')
    assert err is None and stranger['unit_id'] is None, stranger
    t = dbharness.make_tree('Quebec Co')
    make_invite(t, 'tok-later')
    user, err = sync('clerk_later', 'later@example.com', 'tok-later')
    assert err is None, err
    assert user['id'] == stranger['id'], 'the same account, not a second one'
    assert (user['unit_id'], user['role'], user['root_id']) == (t['child'], 'leader', t['root']), user
    conn = dbharness.owner_conn()
    acc = conn.execute("SELECT accepted_by, accepted_at FROM invites WHERE token = 'tok-later'").fetchone()
    conn.close()
    assert acc['accepted_by'] == 'clerk_later' and acc['accepted_at'], acc
    # ...and an attached account presenting another invite is not moved by it.
    make_invite(t, 'tok-again', unit=t['root'], role='owner')
    again, err = sync('clerk_later', 'later@example.com', 'tok-again')
    assert err is None and (again['unit_id'], again['role']) == (t['child'], 'leader'), again


def test_an_invite_is_single_use_under_a_race():
    """Two sign-ins holding the same token: whoever loses the accept lands
    unattached rather than both attaching off one single-use link."""
    t = dbharness.make_tree('Romeo Co')
    make_invite(t, 'tok-race')

    stolen = []

    def steal():
        if stolen:
            return
        stolen.append(True)
        conn = dbharness.owner_conn()
        conn.execute("UPDATE invites SET accepted_at = %s, accepted_by = 'clerk_first' "
                     "WHERE token = 'tok-race'", (server.app_stamp(),))
        conn.commit(); conn.close()

    real_get_db = server.get_db
    server.get_db = lambda: _Racer(real_get_db(), steal, 'UPDATE invites')
    try:
        user, err = sync('clerk_second', 'second@example.com', 'tok-race')
    finally:
        server.get_db = real_get_db
    assert err is None and user['unit_id'] is None, f'the loser must not attach: {user}'
    conn = dbharness.owner_conn()
    acc = conn.execute("SELECT accepted_by FROM invites WHERE token = 'tok-race'").fetchone()['accepted_by']
    conn.close()
    assert acc == 'clerk_first', acc


def test_sync_does_not_undo_a_rename():
    """username is set when the account is made; after that it is the
    owner's to change, and created_by on reports hangs off it."""
    t = dbharness.make_tree('Sierra Co')
    make_invite(t, 'tok-name')
    user, err = sync('clerk_named', 'named@example.com', 'tok-name')
    assert err is None, err
    conn = dbharness.owner_conn()
    conn.execute("UPDATE users SET username = 'SSG Named' WHERE id = %s", (user['id'],))
    conn.commit(); conn.close()
    with server.app.test_request_context('/api/auth/sync', method='POST'):
        g.auth_claims = {'sub': 'clerk_named'}
        again, err = server.sync_clerk_user({'username': 'named-handle', 'email': 'NEW@example.com',
                                             'full_name': 'New Name'})
        g.db_commit = True
        server._close_db(None)
    assert err is None, err
    row = reread(user['id'])
    assert row['username'] == 'SSG Named', f'sync overwrote the rename: {row["username"]!r}'
    assert (row['email'], row['full_name']) == ('new@example.com', 'New Name'), row


def main():
    try:
        test_stranger_gets_an_unattached_account()
        test_invite_attaches_at_its_unit_with_its_role()
        test_expired_invite_is_ignored()
        test_an_unattached_legacy_row_is_claimed_by_email()
        test_an_attached_legacy_row_is_not_claimed_on_a_bare_email()
        test_an_invite_for_the_same_root_vouches_for_the_legacy_row()
        test_an_invite_for_another_root_does_not_vouch()
        test_me_names_whoever_invited_you()
        test_a_founder_was_invited_by_nobody()
        test_the_user_list_is_not_made_n_plus_1_by_this()
        test_losing_the_race_for_a_legacy_row_is_an_error_not_a_crash()
        test_an_unattached_account_redeems_an_invite_later()
        test_an_invite_is_single_use_under_a_race()
        test_sync_does_not_undo_a_rename()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()
