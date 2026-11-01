"""Quick add: a sentence like "Boundy is going to Fort Bliss for 2 weeks
starting Jan 1 for live aircraft" turned into proposed absences.

Pure, like billing_rules.py: no Flask, no DB, no network. server.py sends
build_request()'s body to the Claude Messages API and hands the reply to
read_proposals(). Claude only *reads* the sentence. Nothing here writes, and
the leader saves each proposal through the ordinary absence modal and POST
.../schedule, so every rule that route enforces still applies.

The model's output is untrusted: a person id that is not on the roster we
sent, a status we do not have or a date that is not a real YYYY-MM-DD drops
that proposal, or that field, rather than reaching the client.
"""
from datetime import date

MODEL = 'claude-haiku-5-5'
MAX_TEXT = 1000          # characters the leader may type
MAX_PROPOSALS = 20
STATUSES = ('tdy', 'leave', 'pass', 'other', 'ftr', 'late', 'excused')  # server.ABSENCE_STATUSES

_TOOL = {
    'name': 'propose_absences',
    'description': 'Record every absence the leader described, one entry per soldier per absence.',
    'input_schema': {
        'type': 'object',
        'properties': {
            'absences': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'properties': {
                        'person_id': {'type': ['integer', 'null'],
                                      'description': 'id from the roster; null if no soldier clearly matches'},
                        'name_as_written': {'type': 'string'},
                        'status': {'type': 'string', 'enum': list(STATUSES)},
                        'from_date': {'type': 'string', 'description': 'YYYY-MM-DD, first day away'},
                        'to_date': {'type': 'string', 'description': 'YYYY-MM-DD, last day away, inclusive; "" if unknown'},
                        'school': {'type': 'string', 'description': 'TDY only: the course or purpose'},
                        'location': {'type': 'string', 'description': 'TDY only: where'},
                        'reason': {'type': 'string', 'description': 'other/excused only: short reason'},
                        'question': {'type': 'string',
                                     'description': 'anything ambiguous the leader should check; "" if none'},
                    },
                    'required': ['person_id', 'name_as_written', 'status', 'from_date', 'to_date'],
                },
            },
        },
        'required': ['absences'],
    },
}


def _system(today, schools, locations):
    return (
        f"You turn a military leader's note into absence bookings for a unit roster. Today is {today} "
        f"({date.fromisoformat(today).strftime('%A')}).\n"
        "Statuses: tdy (temporary duty: schools, courses, training trips, travel orders), leave, pass, "
        "ftr (failure to report), late, excused, other.\n"
        "Dates: a date with no year is the next one on or after today. Dates are inclusive: "
        "'2 weeks starting Jan 1' is from Jan 1 to Jan 14; a return date given in the note is the to_date. "
        "Military dates like 31OCT2026 are day-month-year.\n"
        "Match each soldier to the roster by last name (first name or rank to break ties). "
        "If no one matches, or more than one does, set person_id null and say so in question.\n"
        "For TDY, use an entry from these lists when one fits, spelled exactly as listed; otherwise write it "
        "the way the leader did, short.\n"
        f"Schools/purposes: {', '.join(schools) or '(none)'}\n"
        f"Locations: {', '.join(locations) or '(none)'}\n"
        "Never invent an absence the note does not describe."
    )


def build_request(text, today, roster, schools, locations):
    """The Messages API body. roster: [{'id', 'rank', 'last', 'first'}]."""
    lines = '\n'.join(f"{p['id']}: {p['rank']} {p['last']}, {p['first']}" for p in roster)
    return {
        'model': MODEL,
        'max_tokens': 2000,
        'system': _system(today, schools, locations),
        'tools': [_TOOL],
        'tool_choice': {'type': 'tool', 'name': _TOOL['name']},
        'messages': [{'role': 'user', 'content': f'Roster (id: rank last, first):\n{lines}\n\nNote:\n{text}'}],
    }


def _iso(value):
    if not isinstance(value, str):
        return ''
    value = value.strip()
    try:
        return value if date.fromisoformat(value).isoformat() == value else ''
    except ValueError:
        return ''


def _text(value, limit=80):
    return ' '.join(value.split())[:limit] if isinstance(value, str) else ''


def read_proposals(reply, roster_ids, today):
    """Validated proposals out of a Messages API reply.

    Each is {person_id (int or None), name, status, from_date, to_date,
    school, location, reason, question}. A field the model got wrong is
    blanked and noted in question, so the leader sees it in the modal."""
    blocks = reply.get('content') if isinstance(reply, dict) else None
    raw = next((b.get('input', {}).get('absences') for b in blocks or []
                if isinstance(b, dict) and b.get('type') == 'tool_use'), None)
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw[:MAX_PROPOSALS]:
        if not isinstance(item, dict) or item.get('status') not in STATUSES:
            continue
        notes = [_text(item.get('question'), 200)] if _text(item.get('question'), 200) else []
        pid = item.get('person_id')
        if not isinstance(pid, int) or isinstance(pid, bool) or pid not in roster_ids:
            pid = None
        from_date, to_date = _iso(item.get('from_date')), _iso(item.get('to_date'))
        if not from_date:
            notes.append('Start date unclear.')
        elif from_date < today:
            notes.append('Start date is in the past.')
        if to_date and from_date and to_date < from_date:
            to_date = ''
            notes.append('End date was before the start.')
        out.append({
            'person_id': pid,
            'name': _text(item.get('name_as_written')),
            'status': item['status'],
            'from_date': from_date,
            'to_date': to_date,
            'school': _text(item.get('school')) if item['status'] == 'tdy' else '',
            'location': _text(item.get('location')) if item['status'] == 'tdy' else '',
            'reason': _text(item.get('reason'), 200),
            'question': ' '.join(notes),
        })
    return out


if __name__ == '__main__':
    roster = [{'id': 7, 'rank': 'CW2', 'last': 'Boundy', 'first': 'Braden'}]
    body = build_request('Boundy to Bliss', '2026-10-31', roster, ['Live Aircraft'], ['Bliss'])
    assert body['tool_choice']['name'] == 'propose_absences' and 'Boundy, Braden' in body['messages'][0]['content']
    reply = {'content': [{'type': 'tool_use', 'input': {'absences': [
        {'person_id': 7, 'name_as_written': 'Boundy', 'status': 'tdy', 'from_date': '2027-01-01',
         'to_date': '2027-01-14', 'school': 'Live Aircraft', 'location': 'Bliss', 'question': ''},
        {'person_id': 99, 'name_as_written': 'Ghost', 'status': 'tdy', 'from_date': '1/1', 'to_date': ''},
        {'person_id': 7, 'name_as_written': 'x', 'status': 'vacation', 'from_date': '2027-01-01', 'to_date': ''},
    ]}}]}
    got = read_proposals(reply, {7}, '2026-10-31')
    assert len(got) == 2, got
    assert got[0]['person_id'] == 7 and got[0]['to_date'] == '2027-01-14' and got[0]['question'] == ''
    assert got[1]['person_id'] is None and got[1]['from_date'] == '' and 'unclear' in got[1]['question']
    assert read_proposals({'content': [{'type': 'text', 'text': 'hi'}]}, {7}, '2026-10-31') == []
    print('ok')
