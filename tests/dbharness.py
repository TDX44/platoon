"""Give each test its own isolated Postgres schema.

Each test used to set DATA_DIR to a fresh temp directory and get a private
SQLite file. The Postgres equivalent is a private *schema* in one shared
database: same isolation, one server. Call setup() before importing server,
teardown() at the end of main().
"""
import os
import secrets

import psycopg

DEFAULT_URL = 'postgresql://platoon_owner:platoon@127.0.0.1:5432/platoon'


def admin_url():
    return os.environ.get('TEST_DATABASE_URL', DEFAULT_URL)


def setup():
    """Create a throwaway schema and point DATABASE_URL at it."""
    schema = 'test_' + secrets.token_hex(6)
    with psycopg.connect(admin_url(), autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
    url = admin_url()
    sep = '&' if '?' in url else '?'
    os.environ['DATABASE_URL'] = f'{url}{sep}options=-csearch_path%3D{schema}'
    os.environ['MIGRATION_DATABASE_URL'] = os.environ['DATABASE_URL']
    return schema


def teardown(schema):
    with psycopg.connect(admin_url(), autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
