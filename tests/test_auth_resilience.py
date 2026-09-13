"""A Clerk/DNS blip must not look like a bad session.

Run with: python tests/test_auth_resilience.py
"""
import base64
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp()

import server  # noqa: E402  (must follow the DATA_DIR override)
from jwt.exceptions import PyJWKClientConnectionError  # noqa: E402


def fake_token(kid):
    def seg(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b'=').decode()
    return f"{seg({'alg': 'RS256', 'kid': kid})}.{seg({'sub': 'user_1'})}.sig"


class FakeKey:
    def __init__(self, kid):
        self.key_id = kid


class FakeKeySet:
    def __init__(self, kids):
        self.keys = [FakeKey(k) for k in kids]


class FakeClient:
    """Serves a key set until `offline` is set, then fails like a DNS outage."""
    def __init__(self, kids):
        self.key_set = FakeKeySet(kids)
        self.offline = False

    def get_signing_key_from_jwt(self, token):
        if self.offline:
            raise PyJWKClientConnectionError('Fail to fetch data from the url')
        kid = server.jwt.get_unverified_header(token)['kid']
        for key in self.key_set.keys:
            if key.key_id == kid:
                return key
        raise PyJWKClientConnectionError('Fail to fetch data from the url')

    def get_jwk_set(self):
        if self.offline:
            raise PyJWKClientConnectionError('Fail to fetch data from the url')
        return self.key_set


def main():
    server._JWKS_CLIENT = client = FakeClient(['kid-a'])
    server._JWKS_LAST_GOOD = None
    token = fake_token('kid-a')

    # Nothing cached yet: a cold start with Clerk down must still fail loudly.
    client.offline = True
    try:
        server._signing_key_for(token)
        raise AssertionError('with no cached key set, an outage must raise')
    except PyJWKClientConnectionError:
        pass

    # One good fetch primes the fallback.
    client.offline = False
    assert server._signing_key_for(token).key_id == 'kid-a'
    assert server._JWKS_LAST_GOOD is not None, 'a successful fetch must be remembered'

    # Now Clerk goes away: the same session keeps working.
    client.offline = True
    assert server._signing_key_for(token).key_id == 'kid-a', 'an outage must reuse the last key set'

    # But a token signed by a key we have never seen is still rejected.
    try:
        server._signing_key_for(fake_token('kid-unknown'))
        raise AssertionError('an unknown kid must not be served from the fallback')
    except PyJWKClientConnectionError:
        pass

    # An unreachable Clerk is 503 (retry), never 401 (sign out).
    assert server._auth_status_for(server.CLERK_UNREACHABLE) == 503
    assert server._auth_status_for('Unauthorized') == 401
    assert server._auth_status_for('Signature has expired') == 401
    assert server._auth_status_for('Clerk is not configured on the server.') == 500

    print('ok')


if __name__ == '__main__':
    main()
