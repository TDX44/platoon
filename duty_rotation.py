"""Who takes the next turn of a duty. Pure: no Flask, no DB, no clock.

propose() is the whole rule behind "Generate rotation" on the duty roster:
for each day, the free soldier who has had this duty least often, then least
recently, gets it — never someone whose absence covers that day. Weekends
(and any holidays given) can keep a tally of their own, so a soldier who
pulled three Saturdays is not "even" with one who pulled three Tuesdays.
The route turns the answer into names; tests/test_duty_rotation.py drives it
directly.
"""
from datetime import date

WEEKDAY, WEEKEND, ALL = 'weekday', 'weekend', 'all'


def covers(row, day):
    """The availability rule: from_date <= day and (to_date = '' or to_date >= day).
    An empty from_date is "already started", the same as _derive_state()."""
    return (not row['from_date'] or row['from_date'] <= day) and \
        (not row['to_date'] or row['to_date'] >= day)


def category(day, holidays=(), separate_weekends=True):
    if not separate_weekends:
        return ALL
    return WEEKEND if date.fromisoformat(day).weekday() >= 5 or day in holidays else WEEKDAY


def propose(days, pool, history=(), absences=None, holidays=(), separate_weekends=True, taken=()):
    """One entry per day: {'date', 'category', 'person_id', 'reason'}.

    days      ISO dates, in order.
    pool      person ids; their order is the last tie-break.
    history   (person_id, date) for every earlier turn of this duty.
    absences  {person_id: [{'from_date', 'to_date'}, ...]}.
    taken     dates that already have this duty; left alone.

    person_id is None when the day is taken ('reason': 'taken') or nobody in
    the pool is free ('reason': 'nobody free').
    """
    absences = absences or {}
    holidays = set(holidays)
    order = {p: i for i, p in enumerate(pool)}
    count, last = {}, {}
    for person_id, day in history:
        if person_id not in order:
            continue
        key = (person_id, category(day, holidays, separate_weekends))
        count[key] = count.get(key, 0) + 1
        last[key] = max(last.get(key, ''), day)

    out, prev = [], None
    for day in days:
        cat = category(day, holidays, separate_weekends)
        entry = {'date': day, 'category': cat, 'person_id': None, 'reason': ''}
        out.append(entry)
        if day in taken:
            entry['reason'] = 'taken'
            continue
        free = [p for p in pool if not any(covers(r, day) for r in absences.get(p, ()))]
        if not free:
            entry['reason'] = 'nobody free'
            continue
        # Least often, then not two days running, then least recently
        # ('' = never), then the order the pool was given in.
        pick = min(free, key=lambda p: (count.get((p, cat), 0), p == prev, last.get((p, cat), ''), order[p]))
        count[(pick, cat)] = count.get((pick, cat), 0) + 1
        last[(pick, cat)] = day
        entry['person_id'] = prev = pick
    return out


if __name__ == '__main__':
    got = propose(['2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05'], [1, 2],
                  history=[(1, '2026-09-01')], absences={2: [{'from_date': '2026-10-05', 'to_date': ''}]})
    assert [e['person_id'] for e in got] == [2, 1, 2, 1], got
    print('ok')
