"""A stale bulk-present request cannot end a newly booked absence.

Uses the application database role and two real concurrent transactions;
run like the other standalone database tests with TEST_*_DATABASE_URL set.
"""
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness

_schema = dbharness.setup()
import server


def main():
    tree = dbharness.make_tree()
    user = dbharness.make_user(tree['root'], 'owner', 'formation-owner')
    dbharness.as_user(user)
    today = server.app_today()
    tomorrow = (date.fromisoformat(today) + timedelta(days=1)).isoformat()
    with dbharness.owner_conn() as conn:
        pid = conn.execute(
            'INSERT INTO personnel (rank, last, first, unit_id, root_id) '
            'VALUES (%s, %s, %s, %s, %s) RETURNING id',
            ('SGT', 'Fixture', 'Alex', tree['child'], tree['root'])).fetchone()['id']
    url = f'/api/personnel/{pid}'
    guarded = {'status': 'present', 'present_date': today, 'only_if_unaccounted': True}
    client = server.app.test_client()

    # A genuine unaccounted soldier can be marked; retries are harmless.
    response = client.put(url, json=guarded)
    assert response.status_code == 200, response.get_json()
    assert response.get_json()['present_date'] == today
    assert not response.get_json().get('skipped')
    response = client.put(url, json=guarded)
    assert response.get_json()['skip_reason'] == 'already_accounted', response.get_json()

    # An absence booked after the leader loaded their roster survives.
    absence = client.post(f'{url}/schedule', json={
        'status': 'leave', 'from_date': today, 'to_date': tomorrow})
    assert absence.status_code == 201, absence.get_json()
    eid = absence.get_json()['id']
    response = client.put(url, json=guarded)
    assert response.status_code == 200 and response.get_json()['skipped'], response.get_json()
    assert response.get_json()['skip_reason'] == 'away'
    assert response.get_json()['status'] == 'leave'
    with dbharness.owner_conn() as conn:
        event = conn.execute('SELECT state, to_date FROM scheduled_events WHERE id = %s', (eid,)).fetchone()
        assert (event['state'], event['to_date']) == ('active', tomorrow), event

    # Explicitly marking one soldier present remains an intentional override.
    response = client.put(url, json={'status': 'present', 'present_date': today})
    assert response.status_code == 200 and response.get_json()['status'] == 'present'

    # A due event must be detected even before a roster read reconciles it.
    with dbharness.owner_conn() as conn:
        conn.execute('DELETE FROM scheduled_events WHERE person_id = %s', (pid,))
        conn.execute("UPDATE personnel SET present_date = '', status = 'present' WHERE id = %s", (pid,))
        eid = conn.execute(
            'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) '
            "VALUES (%s, %s, %s, 'tdy', %s, %s, 'scheduled') RETURNING id",
            (pid, tree['child'], tree['root'], today, tomorrow)).fetchone()['id']
    response = client.put(url, json=guarded)
    assert response.get_json()['skip_reason'] == 'away', response.get_json()
    assert response.get_json()['status'] == 'tdy'

    # An in-flight booking owns the person first. The guarded write waits,
    # then reads its committed event, so it cannot erase the new booking.
    with dbharness.owner_conn() as conn:
        conn.execute('DELETE FROM scheduled_events WHERE person_id = %s', (pid,))
        conn.execute("UPDATE personnel SET present_date = '', status = 'present' WHERE id = %s", (pid,))
    booking = dbharness.owner_conn()
    booking.execute('SELECT id FROM personnel WHERE id = %s FOR UPDATE', (pid,))
    booking.execute(
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) '
        "VALUES (%s, %s, %s, 'pass', %s, %s, 'scheduled')",
        (pid, tree['child'], tree['root'], today, tomorrow))
    reached_lock = threading.Event()
    original = server._person_or_none

    def observing_person(conn, person_id, **kwargs):
        if kwargs.get('for_update'):
            reached_lock.set()
        return original(conn, person_id, **kwargs)

    server._person_or_none = observing_person
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: server.app.test_client().put(url, json=guarded))
            try:
                assert reached_lock.wait(timeout=5), 'request never reached the person lock'
                assert not future.done(), 'guarded write bypassed the booking transaction'
            finally:
                booking.commit()
            response = future.result(timeout=5)
        assert response.status_code == 200, response.get_json()
        assert response.get_json()['skip_reason'] == 'away', response.get_json()
        assert response.get_json()['status'] == 'pass'
    finally:
        server._person_or_none = original
        booking.close()

    # Guard input cannot be coerced from a string or used for a different day.
    assert client.put(url, json={**guarded, 'only_if_unaccounted': 'false'}).status_code == 400
    assert client.put(url, json={**guarded, 'present_date': tomorrow}).status_code == 400
    assert client.put(url, json={**guarded, 'status': 'leave'}).status_code == 400

    # A unit roster reconciles the tenant in person-id order. While it holds
    # the first person's lock, a booking in a sibling unit can finish without
    # trying to acquire that first person's lock in the opposite order.
    with dbharness.owner_conn() as conn:
        sibling = conn.execute(
            'INSERT INTO units (parent_id, root_id, kind, name, slug) '
            "VALUES (%s, %s, 'platoon', 'Sibling', 'sibling') RETURNING id",
            (tree['root'], tree['root'])).fetchone()['id']
        other_pid = conn.execute(
            'INSERT INTO personnel (rank, last, first, unit_id, root_id) '
            "VALUES ('SPC', 'Sibling', 'Pat', %s, %s) RETURNING id",
            (sibling, tree['root'])).fetchone()['id']
        conn.execute('DELETE FROM scheduled_events WHERE person_id = %s', (pid,))
        conn.execute("UPDATE personnel SET present_date = '', status = 'present' WHERE id = %s", (pid,))
        for target, unit in [(pid, tree['child']), (other_pid, sibling)]:
            conn.execute(
                'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) '
                "VALUES (%s, %s, %s, 'tdy', %s, %s, 'scheduled')",
                (target, unit, tree['root'], today, tomorrow))
    reader_has_first = threading.Event()
    let_reader_continue = threading.Event()
    booking_has_second = threading.Event()
    let_booking_commit = threading.Event()
    reader_wants_second = threading.Event()
    original_sync = server._sync_person_status

    def gated_sync(conn, person_id, duty_day):
        name = threading.current_thread().name
        if name.startswith('booker') and person_id == other_pid:
            booking_has_second.set()
            assert let_booking_commit.wait(timeout=5), 'booking was never released'
        if name.startswith('reader') and person_id == other_pid:
            reader_wants_second.set()
        result = original_sync(conn, person_id, duty_day)
        if name.startswith('reader') and person_id == pid:
            reader_has_first.set()
            assert let_reader_continue.wait(timeout=5), 'reader was never released'
        return result

    server._sync_person_status = gated_sync
    try:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix='reader') as reader_pool, \
                ThreadPoolExecutor(max_workers=1, thread_name_prefix='booker') as booker_pool:
            try:
                read = reader_pool.submit(lambda: server.app.test_client().get(
                    f"/api/personnel?unit={tree['child']}"))
                assert reader_has_first.wait(timeout=5), 'reader never locked first person'
                write = booker_pool.submit(lambda: server.app.test_client().post(
                    f'/api/personnel/{other_pid}/schedule', json={
                        'status': 'leave', 'from_date': today, 'to_date': tomorrow}))
                assert booking_has_second.wait(timeout=5), 'booking never locked sibling person'
                let_reader_continue.set()
                assert reader_wants_second.wait(timeout=5), 'reader never reached sibling person'
            finally:
                let_reader_continue.set()
                let_booking_commit.set()
            written = write.result(timeout=5)
            roster = read.result(timeout=5)
        assert written.status_code == 201, written.get_json()
        assert roster.status_code == 200, roster.get_json()
        with dbharness.owner_conn() as conn:
            row = conn.execute('SELECT status FROM personnel WHERE id = %s', (other_pid,)).fetchone()
            assert row['status'] == 'leave', row
            events = conn.execute(
                "SELECT status FROM scheduled_events WHERE person_id = %s AND state = 'active'",
                (other_pid,)).fetchall()
            assert [e['status'] for e in events] == ['leave'], events
    finally:
        server._sync_person_status = original_sync

    # Deletion must wait on the person before locking any absence children.
    # Otherwise a reconciliation holding this person and updating an event
    # would wait on the deleter, which in turn waits on this person.
    deleting = threading.Event()
    owner = dbharness.owner_conn()
    owner.execute("SET LOCAL lock_timeout = '2s'")
    owner.execute('SELECT id FROM personnel WHERE id = %s FOR UPDATE', (other_pid,))

    def observing_delete(conn, person_id, **kwargs):
        if person_id == other_pid and kwargs.get('for_update'):
            deleting.set()
        return original(conn, person_id, **kwargs)

    server._person_or_none = observing_delete
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            deletion = executor.submit(lambda: server.app.test_client().delete(
                f'/api/personnel/{other_pid}'))
            try:
                assert deleting.wait(timeout=5), 'delete did not lock the parent first'
                assert not deletion.done(), 'delete bypassed the person lock'
                # This would time out if deletion held the dependent event.
                owner.execute("UPDATE scheduled_events SET notes = 'concurrent reconcile' WHERE person_id = %s",
                              (other_pid,))
            finally:
                owner.commit()
            deleted = deletion.result(timeout=5)
        assert deleted.status_code == 200, deleted.get_json()
    finally:
        server._person_or_none = original
        owner.close()

    # The guard never widens unit access or exposes another tenant's person.
    other = dbharness.make_tree('Other Co')
    outsider = dbharness.make_user(other['root'], 'owner', 'other-owner')
    dbharness.as_user(outsider)
    assert client.put(url, json=guarded).status_code == 404
    print('ok')


if __name__ == '__main__':
    try:
        main()
    finally:
        dbharness.teardown(_schema)
