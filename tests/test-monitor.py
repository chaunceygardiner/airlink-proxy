#!/usr/bin/python3

# Copyright (c) 2026 John A Kline
# See the file LICENSE for your full rights.

"""Offline tests for the monitor and server code: database round trips, fetch
semantics (since_ts/max_ts/limit), the json conversions, the type 5 to type 6
conversion, the sanity checks and REST request parsing.  Needs no live sensor
(that path is covered by tests/test-live.py).

    python3 tests/test-monitor.py

Exits non-zero if any test fails.
"""

import copy
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airlinkproxy', 'bin'))

from fixtures import create_test_device_json, create_test_reading, float_eq
from monitor.database import Database
from monitor.model import (AVERAGED_FIELDS, READING_FIELDS, RecordType,
                           convert_data_structure_type_5_to_6, error_json,
                           reading_from_device_json, reading_to_json)
from monitor.service import Service
import monitor.service as service_mod
import server.server as srv

failures = []

def check(label: str, cond: bool, detail: str = '') -> None:
    if cond:
        print('PASS: %s' % label)
    else:
        failures.append(label)
        print('FAIL: %s %s' % (label, detail))

BASE_TS = 1600657320

# --- a database holding 5 archive readings, 60s apart ---------------------
tmp = tempfile.NamedTemporaryFile(prefix='airlink-proxy-test', suffix='.sdb', delete=False)
tmp.close()
os.unlink(tmp.name)
db = Database.create(tmp.name)

readings_in = []
for i in range(5):
    ts = BASE_TS + 60 * i
    r = create_test_reading(ts)
    db.save_archive_reading(ts, r)
    readings_in.append(r)

# 1. since_ts=0 returns everything, in order, unchanged.
out = list(db.fetch_archive_readings(0))
check('fetch all (since_ts=0) returns 5 readings', len(out) == 5, 'got %d' % len(out))
check('readings in timestamp order',
      [r.last_report_time for r in out] == [r.last_report_time for r in readings_in])
check('round trip equality', all(a == b for a, b in zip(readings_in, out)))

# 2. limit.
out = list(db.fetch_archive_readings(0, limit=3))
check('limit=3 returns 3 readings', len(out) == 3, 'got %d' % len(out))

# 3. max_ts is honored, alone and alongside limit.
cutoff = BASE_TS + 120
out = list(db.fetch_archive_readings(0, max_ts=cutoff))
check('max_ts returns 3 readings', len(out) == 3, 'got %d' % len(out))
out = list(db.fetch_archive_readings(0, max_ts=cutoff, limit=2))
check('max_ts+limit returns 2 readings', len(out) == 2, 'got %d' % len(out))

# 4. since_ts is exclusive.
out = list(db.fetch_archive_readings(BASE_TS))
check('since_ts exclusive', len(out) == 4, 'got %d' % len(out))

# 5. get_earliest_timestamp.
js = db.get_earliest_timestamp_as_json()
check('earliest timestamp', ('"timestamp": %d' % BASE_TS) in js, js)

# 6. CURRENT stays a single row; the latest write wins.
db.save_current_reading(readings_in[0])
db.save_current_reading(readings_in[1])
cur = list(db.fetch_current_readings())
check('current reading single row, latest wins',
      len(cur) == 1 and cur[0].last_report_time == readings_in[1].last_report_time)

# 7. Values the old string-built SQL would have mangled survive: a name
#    containing quotes, and a float with more than six decimal places.
tricky = create_test_reading(BASE_TS + 600)
tricky.name = 'a "quoted" name'
tricky.temp = 74.123456789
db.save_archive_reading(BASE_TS + 600, tricky)
got = list(db.fetch_archive_readings(BASE_TS + 599))[0]
check('quoted device name survives', got.name == tricky.name, repr(got.name))
check('float precision preserved', got.temp == 74.123456789, repr(got.temp))

# 8. The json the proxy serves is the json the device served.
device_json = create_test_device_json(BASE_TS)
round_tripped = json.loads(reading_to_json(reading_from_device_json(device_json)))
check('device json round trips unchanged', round_tripped == device_json)

# 9. A missing field becomes None rather than an error.
missing = copy.deepcopy(create_test_device_json(BASE_TS))
del missing['data']['conditions'][0]['wet_bulb']
r = reading_from_device_json(missing)
check('missing field becomes None', r.wet_bulb is None)
check('every other field still populated',
      all(getattr(r, f) is not None for f in READING_FIELDS if f != 'wet_bulb'))

# 10. Type 5 records are converted to type 6.
five = copy.deepcopy(create_test_device_json(BASE_TS))
c = five['data']['conditions'][0]
c['data_structure_type'] = 5
for suffix in ['', '_last_1_hour', '_last_3_hours', '_last_24_hours', '_nowcast']:
    c['pm_10p0%s' % suffix] = c.pop('pm_10%s' % suffix)
convert_data_structure_type_5_to_6(five)
c = five['data']['conditions'][0]
check('type 5 converted to type 6', c['data_structure_type'] == 6)
check('type 5 pm_10p0 renamed to pm_10', c['pm_10'] == 17.28, repr(c.get('pm_10')))
check('converted type 5 record is sane', Service.is_sane(five)[0])

# 11. Sanity checks.
check('well formed reading is sane', Service.is_sane(create_test_device_json(BASE_TS))[0])

bad = copy.deepcopy(create_test_device_json(BASE_TS))
bad['data']['conditions'][0]['temp'] = 'warm'
ok, reason = Service.is_sane(bad)
check('string temp rejected', not ok and 'temp' in reason, reason)

bad = copy.deepcopy(create_test_device_json(BASE_TS))
bad['data']['conditions'][0]['data_structure_type'] = 4
ok, reason = Service.is_sane(bad)
check('unknown data_structure_type rejected', not ok, reason)

bad = copy.deepcopy(create_test_device_json(BASE_TS))
bad['data']['conditions'] = []
ok, reason = Service.is_sane(bad)
check('empty conditions rejected', not ok, reason)

bad = copy.deepcopy(create_test_device_json(BASE_TS))
bad['error'] = {'code': 1, 'message': 'boom'}
ok, reason = Service.is_sane(bad)
check('device-reported error rejected', not ok, reason)

# 12. REST request parsing.
req = srv.Handler.parse_requestline('GET /fetch-archive-records?since_ts=0 HTTP/1.1')
check('since_ts=0 parses as FETCH_ARCHIVE_RECORDS',
      req.request_type == srv.RequestType.FETCH_ARCHIVE_RECORDS and req.since_ts == 0)
req = srv.Handler.parse_requestline('GET /fetch-archive-records?since_ts=5,max_ts=9,limit=2 HTTP/1.1')
check('full args parse', req.since_ts == 5 and req.max_ts == 9 and req.limit == 2)
req = srv.Handler.parse_requestline('GET /fetch-archive-records HTTP/1.1')
check('missing since_ts is an error', req.request_type == srv.RequestType.ERROR)
req = srv.Handler.parse_requestline('GET /fetch-archive-records?since_ts=abc HTTP/1.1')
check('non-integer since_ts is an error', req.request_type == srv.RequestType.ERROR)
req = srv.Handler.parse_requestline('GET /v1/current_conditions HTTP/1.1')
check('/v1/current_conditions is fetch-current',
      req.request_type == srv.RequestType.FETCH_CURRENT_RECORD)
req = srv.Handler.parse_requestline('GET /get-version HTTP/1.1')
check('/get-version parses', req.request_type == srv.RequestType.GET_VERSION)
req = srv.Handler.parse_requestline('GET /fetch-two-minute-record HTTP/1.1')
check('/fetch-two-minute-record parses',
      req.request_type == srv.RequestType.FETCH_TWO_MINUTE_RECORD)
req = srv.Handler.parse_requestline('GET /get-archive-interval-secs HTTP/1.1')
check('/get-archive-interval-secs parses',
      req.request_type == srv.RequestType.GET_ARCHIVE_INTERVAL)
req = srv.Handler.parse_requestline('GET /nonsense HTTP/1.1')
check('unknown command is an error', req.request_type == srv.RequestType.ERROR)
# The command set changed, so clients can discriminate on /get-version.
check('API command-set version is 2', srv.VERSION == '2', 'got %r' % srv.VERSION)

# 18. Error responses are valid json, in the device's own envelope.  Built with
# dumps because the message quotes the client's request text.
class _FakeWfile:
    def __init__(self): self.written = b''
    def write(self, b): self.written += b

class _FakeHandler(srv.Handler):
    def __init__(self):
        self.wfile = _FakeWfile()
        self.sent = []
    def send_response(self, code): self.sent.append(code)
    def send_header(self, k, v): pass
    def end_headers(self): pass

for message in ['fetch-archive-records requires since_ts argument',
                'Unknown command: /bo"gus.',
                'Unknown command: /back\\slash.']:
    h = _FakeHandler()
    h.respond_error(message)
    body = h.wfile.written.decode('ascii')
    parsed = json.loads(body)   # raises if not valid json
    check('error body parses as json (%s)' % message[:22], parsed['error'] is not None)
    check('error message survives intact (%s)' % message[:22],
          parsed['error']['message'] == message, 'got %r' % parsed['error']['message'])
    check('error body carries a null data member (%s)' % message[:22], parsed['data'] is None)
    check('error status is still 200 (%s)' % message[:22], h.sent == [200], 'got %r' % h.sent)

# 13. Poll scheduling.  The offset shifts each poll within its window; it must
# never change the poll PERIOD, and it must never suppress archive records.
# (Before 1.0 the offset moved the archive boundary instead, so any offset that
# was not a multiple of poll-freq-secs silently produced zero archive records.)
def simulate(pollfreq: int, arcint: int, offset: int, minutes: int = 60,
             poll_cost: float = 0.0):
    """Walk the scheduler forward, returning (poll_times, archive_timestamps).

    poll_cost models the time the sensor read itself takes.  It matters: adding
    the offset to the next boundary (rather than subtracting it first) skips a
    slot whenever the work leaves now past a boundary but short of
    boundary + offset, and a zero-cost poll never exposes that.
    """
    svc = Service.__new__(Service)
    svc.pollfreq_secs   = pollfreq
    svc.pollfreq_offset = offset
    svc.arcint_secs     = arcint

    now = float(BASE_TS)
    monkey = {'now': now}
    orig = service_mod.time.time
    service_mod.time.time = lambda: monkey['now']
    try:
        next_arc_ts = svc.next_archive_boundary()
        polls = []
        archives = []
        end = now + minutes * 60
        while monkey['now'] < end:
            monkey['now'] += svc.secs_to_next_poll()
            polls.append(monkey['now'])
            if monkey['now'] >= next_arc_ts:
                archives.append(int(monkey['now'] / arcint) * arcint)
                next_arc_ts = svc.next_archive_boundary()
            # The sensor read happens after the poll fires.
            monkey['now'] += poll_cost
    finally:
        service_mod.time.time = orig
    return polls, archives

for off in (0, 1, 2, 3, 4):
    polls, archives = simulate(5, 60, off)
    gaps = {round(b - a, 6) for a, b in zip(polls, polls[1:])}
    check('offset %d: poll period stays 5s' % off, gaps == {5.0},
          'gaps: %s' % gaps)
    check('offset %d: one archive per minute' % off, len(archives) == 60,
          'got %d' % len(archives))
    check('offset %d: archives on arcint boundaries' % off,
          all(a % 60 == 0 for a in archives))
    check('offset %d: archive timestamps are unique and ascending' % off,
          archives == sorted(set(archives)))
    check('offset %d: polls land at offset within the window' % off,
          all(round(t % 5, 6) == float(off % 5) for t in polls))

# The offset actually staggers: two proxies with different offsets never poll
# at the same instant.
polls_a, _ = simulate(5, 60, 0, minutes=5)
polls_b, _ = simulate(5, 60, 2, minutes=5)
check('different offsets never poll at the same instant',
      not (set(polls_a) & set(polls_b)))

# A slow read must delay at most its own poll, never cost a whole slot.  Costs
# are chosen to straddle the boundary for each offset (cost > pollfreq - offset).
for off, cost in ((1, 4.5), (2, 3.5), (3, 2.5), (4, 1.5)):
    polls, archives = simulate(5, 60, off, poll_cost=cost)
    gaps = {round(b - a, 6) for a, b in zip(polls, polls[1:])}
    check('offset %d with a %.1fs read: no slot skipped' % (off, cost),
          gaps == {5.0}, 'gaps: %s' % gaps)
    # Not an exact count: the read cost consumes wall time, so the run ends a
    # few seconds short of the last boundary.  Contiguity is the real property.
    check('offset %d with a %.1fs read: no archive boundary skipped' % (off, cost),
          {b - a for a, b in zip(archives, archives[1:])} == {60},
          'diffs: %s' % {b - a for a, b in zip(archives, archives[1:])})

# 14. --dump must not create the database.  sqlite3.connect() would create an
# empty file, and startup skips Database.create when the file exists, leaving
# every save to fail against a database with no Reading table.
import subprocess
bin_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       '..', 'home', 'airlinkproxy', 'bin')
missing_db = os.path.join(tempfile.mkdtemp(prefix='airlink-proxy-test'), 'absent.sdb')
conf_path = missing_db + '.conf'
with open(conf_path, 'w') as f:
    f.write('hostname = airlink\ndatabase-file = %s\n' % missing_db)
env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
proc = subprocess.run([sys.executable, os.path.join(bin_dir, 'airlinkproxyd'),
                       '--dump', conf_path],
                      capture_output=True, text=True, env=env, cwd=bin_dir)
check('--dump on a missing database exits non-zero', proc.returncode != 0,
      'rc=%d' % proc.returncode)
check('--dump on a missing database says so', 'No such database' in proc.stdout,
      repr(proc.stdout[:200]))
check('--dump does not create the database', not os.path.exists(missing_db))
os.unlink(conf_path)
os.rmdir(os.path.dirname(missing_db))

# 15. compute_avg: the archive and two-minute records are averages over their
# span, not the single poll that landed on the boundary.
def reading_with(ts: int, **overrides):
    j = create_test_device_json(ts)
    for k, v in overrides.items():
        j['data']['conditions'][0][k] = v
    return reading_from_device_json(j)

# pm_1_last 10/20/30 -> 20; pm_2p5 1.0/2.0/6.0 -> 3.0; temp 70/80/90 -> 80.
samples = [
    reading_with(BASE_TS + 0,  pm_1_last=10, pm_2p5=1.0, temp=70.0),
    reading_with(BASE_TS + 5,  pm_1_last=20, pm_2p5=2.0, temp=80.0),
    reading_with(BASE_TS + 10, pm_1_last=30, pm_2p5=6.0, temp=90.0),
]
avg = Service.compute_avg(samples)
check('compute_avg averages an integer field', avg.pm_1_last == 20, 'got %r' % avg.pm_1_last)
check('compute_avg averages a float field', float_eq(avg.pm_2p5, 3.0), 'got %r' % avg.pm_2p5)
check('compute_avg averages temp', float_eq(avg.temp, 80.0), 'got %r' % avg.temp)
check('compute_avg rounds integer fields to int', isinstance(avg.pm_1_last, int))
check('compute_avg carries the latest timestamp', avg.ts == BASE_TS + 10)
check('compute_avg carries identifiers', avg.did == samples[-1].did and avg.name == samples[-1].name)
# The device's rolling windows are carried, not averaged.
check('compute_avg does not average the device rolling windows',
      float_eq(avg.pm_2p5_last_1_hour, samples[-1].pm_2p5_last_1_hour)
      and avg.pct_pm_data_nowcast == samples[-1].pct_pm_data_nowcast)
check('rolling windows are not in AVERAGED_FIELDS',
      not [f for f in AVERAGED_FIELDS if 'hour' in f or 'nowcast' in f or f.startswith('pct_')])

# Integer rounding: 10 and 11 average to 10.5, stored as 11.
avg2 = Service.compute_avg([reading_with(BASE_TS, pm_1_last=10),
                            reading_with(BASE_TS + 5, pm_1_last=11)])
check('compute_avg rounds .5 up', avg2.pm_1_last == 11, 'got %r' % avg2.pm_1_last)

# A single sample averages to itself.
one = Service.compute_avg([samples[1]])
check('compute_avg of one reading is that reading',
      one.pm_1_last == 20 and float_eq(one.pm_2p5, 2.0))

# A field only some readings reported is averaged over those that did.
partial = Service.compute_avg([reading_with(BASE_TS, pm_1_last=10),
                               reading_with(BASE_TS + 5, pm_1_last=None),
                               reading_with(BASE_TS + 10, pm_1_last=20)])
check('compute_avg ignores None samples', partial.pm_1_last == 15, 'got %r' % partial.pm_1_last)

# A field no reading reported stays None rather than becoming 0.
absent = Service.compute_avg([reading_with(BASE_TS, pm_1_last=None),
                              reading_with(BASE_TS + 5, pm_1_last=None)])
check('compute_avg leaves an all-None field None', absent.pm_1_last is None,
      'got %r' % absent.pm_1_last)

# compute_avg must not mutate its inputs.
check('compute_avg does not mutate its inputs',
      samples[0].pm_1_last == 10 and samples[2].pm_1_last == 30)

# 16. trim_two_minute_readings drops only what is older than two minutes.
window = [reading_with(BASE_TS + t) for t in (0, 100, 119, 130, 240)]
Service.trim_two_minute_readings(window, BASE_TS + 240)
check('trim keeps exactly the last two minutes',
      [r.last_report_time - BASE_TS for r in window] == [130, 240],
      'kept %r' % [r.last_report_time - BASE_TS for r in window])

# 17. The two-minute record is a single row, like CURRENT.
two_min_db = tempfile.NamedTemporaryFile(prefix='airlink-proxy-test', suffix='.sdb', delete=False)
two_min_db.close()
os.unlink(two_min_db.name)
tdb = Database.create(two_min_db.name)
tdb.save_two_minute_reading(reading_with(BASE_TS, pm_1_last=10))
tdb.save_two_minute_reading(reading_with(BASE_TS + 5, pm_1_last=20))
rows = list(tdb.fetch_two_minute_readings())
check('two-minute record is single-row', len(rows) == 1, 'got %d' % len(rows))
check('two-minute record holds the latest write', rows[0].pm_1_last == 20)
check('two-minute record did not disturb the archive',
      len(list(tdb.fetch_archive_readings())) == 0)
check('RecordType.TWO_MINUTE is 2', RecordType.TWO_MINUTE == 2)
os.unlink(two_min_db.name)

# 18. json has one number type, so a float-valued field can arrive as int.
# Rejecting those threw away the whole reading.
int_j = create_test_device_json(BASE_TS)
int_j['data']['conditions'][0]['hum'] = 62            # int, not 62.0
int_j['data']['conditions'][0]['pm_10_nowcast'] = 0   # int zero
check('integer-valued float fields are sane', Service.is_sane(int_j)[0],
      Service.is_sane(int_j)[1])
bool_j = create_test_device_json(BASE_TS)
bool_j['data']['conditions'][0]['hum'] = True
check('bool is still rejected', not Service.is_sane(bool_j)[0])
str_j = create_test_device_json(BASE_TS)
str_j['data']['conditions'][0]['temp'] = 'warm'
check('string is still rejected', not Service.is_sane(str_j)[0])

# 19. Empty results answer in the device's error envelope, not '{}' -- a client
# written against the AirLink indexes into ['data']['conditions'] and would
# raise on an empty object.
empty_db = tempfile.NamedTemporaryFile(prefix='airlink-proxy-test', suffix='.sdb', delete=False)
empty_db.close()
os.unlink(empty_db.name)
edb = Database.create(empty_db.name)
for label, body in (('current', edb.fetch_current_reading_as_json()),
                    ('two-minute', edb.fetch_two_minute_reading_as_json())):
    parsed = json.loads(body)
    check('empty %s answers valid json' % label, parsed['data'] is None)
    check('empty %s reports an error' % label, parsed['error'] is not None)
    check('empty %s is not a bare {}' % label, body.strip() != '{}')
    # 500, not 400: the next successful poll answers it, so a client should
    # retry rather than treat it as a permanent bad request.
    check('empty %s reports code 500' % label, parsed['error']['code'] == 500,
          'got %r' % parsed['error']['code'])

# 20. Both single-row records are written in one transaction.
edb.save_poll_readings(create_test_reading(BASE_TS), create_test_reading(BASE_TS + 1))
check('batched write stored the current record',
      len(list(edb.fetch_current_readings())) == 1)
check('batched write stored the two-minute record',
      len(list(edb.fetch_two_minute_readings())) == 1)
edb.save_poll_readings(create_test_reading(BASE_TS + 5), None)
check('batched write leaves the two-minute record alone when it is None',
      len(list(edb.fetch_two_minute_readings())) == 1)
check('batched write replaced the current record (still single-row)',
      len(list(edb.fetch_current_readings())) == 1)
edb.save_poll_readings(None, None)   # must be a no-op, not an error
check('batched write with nothing to do is a no-op',
      len(list(edb.fetch_current_readings())) == 1)

# 21. When the two-minute window empties the row is cleared, so a stale average
# is not served indefinitely as though it were current.
edb.delete_two_minute_reading()
check('two-minute row is cleared', len(list(edb.fetch_two_minute_readings())) == 0)
check('clearing two-minute left the current record alone',
      len(list(edb.fetch_current_readings())) == 1)
check('cleared two-minute answers the error envelope',
      json.loads(edb.fetch_two_minute_reading_as_json())['error'] is not None)
os.unlink(empty_db.name)

# 21b. The two-minute record survives a restart while it is still recent, and
# is dropped once it has aged out.  A restart is seconds; the average is still
# good, so throwing it away would serve an error where a real value exists.
def two_minute_after_restart(row_age_secs, poll_succeeds):
    """Returns (action, rows) for a daemon start with a row of the given age."""
    path = os.path.join(tempfile.mkdtemp(prefix='airlink-proxy-test'), 'r.sdb')
    db = Database.create(path)
    now = int(time.time())
    if row_age_secs is not None:
        db.save_two_minute_reading(create_test_reading(now - row_age_secs))
    two_minute_ts = None
    for stored in db.fetch_two_minute_readings():
        two_minute_ts = stored.ts
    window = []
    reading = create_test_reading(now) if poll_succeeds else None
    if reading is not None:
        window.append(reading)
    Service.trim_two_minute_readings(window, now)
    two_min = Service.compute_avg(window) if window else None
    if reading is not None or two_min is not None:
        db.save_poll_readings(reading, two_min)
        action = 'wrote'
    elif two_minute_ts is not None and time.time() - two_minute_ts > 120:
        db.delete_two_minute_reading()
        action = 'cleared'
    else:
        action = 'kept'
    rows = len(list(db.fetch_two_minute_readings()))
    os.unlink(path)
    os.rmdir(os.path.dirname(path))
    return action, rows

check('restart keeps a 10s-old two-minute record',
      two_minute_after_restart(10, False) == ('kept', 1))
check('restart keeps a 119s-old two-minute record',
      two_minute_after_restart(119, False) == ('kept', 1))
check('restart drops a 121s-old two-minute record',
      two_minute_after_restart(121, False) == ('cleared', 0))
check('restart drops an hour-old two-minute record',
      two_minute_after_restart(3600, False) == ('cleared', 0))
check('a successful poll rewrites it regardless of age',
      two_minute_after_restart(3600, True) == ('wrote', 1))
check('fresh install writes on the first poll',
      two_minute_after_restart(None, True) == ('wrote', 1))

# 22. error_json escapes the request text it quotes.
parsed = json.loads(error_json('Unknown command: /bo"gus\\x.'))
check('error_json escapes quotes and backslashes',
      parsed['error']['message'] == 'Unknown command: /bo"gus\\x.')
check('error_json defaults to code 400', parsed['error']['code'] == 400)
check('error_json carries an explicit code',
      json.loads(error_json('boom', 500))['error']['code'] == 500)

# A bad request is 400; a failure inside the proxy is 500, so a client can tell
# "do not retry" from "try again".
for message, code, label in (('Unknown command: /bogus.', 400, 'bad request'),
                             ('Request failed: database is locked', 500, 'server failure')):
    h = _FakeHandler()
    h.respond_error(message, code)
    check('%s reports code %d' % (label, code),
          json.loads(h.wfile.written.decode('ascii'))['error']['code'] == code)
    check('%s still returns HTTP 200' % label, h.sent == [200])

os.unlink(tmp.name)
print()
if failures:
    print('%d FAILURES: %s' % (len(failures), failures))
    sys.exit(1)
print('ALL PASSED')
