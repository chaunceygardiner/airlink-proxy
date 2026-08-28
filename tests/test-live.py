#!/usr/bin/python3

# Copyright (c) 2026 John A Kline
# See the file LICENSE for your full rights.

"""Live tests: fetch real readings from the AirLink named in the configuration
file, sanity check them, and round trip them through the database and the json
conversions.

    python3 tests/test-live.py [<airlinkproxy-conf-file>]

The conf file defaults to home/airlinkproxy/airlinkproxy.conf.  A reachable
AirLink is required; everything that does not need one is in
tests/test-monitor.py.

Exits non-zero if any test fails.  These tests replaced the --test flag that
earlier versions carried inside the daemon.
"""

import copy
import json
import os
import sys
import tempfile
import time

from time import sleep
from typing import Optional, Tuple

import configobj
import requests

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airlinkproxy', 'bin'))

from monitor.database import Database
from monitor.model import (Reading, convert_data_structure_type_5_to_6,
                           reading_to_json, reading_from_device_json)
from monitor.service import Service

failures = []

def check(label: str, cond: bool, detail: str = '') -> None:
    if cond:
        print('PASS: %s' % label)
    else:
        failures.append(label)
        print('FAIL: %s %s' % (label, detail))

conf_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airlinkproxy',
    'airlinkproxy.conf')
config_dict = configobj.ConfigObj(conf_file, file_error=True, encoding='utf-8')
hostname     = config_dict.get('hostname')
port         = int(config_dict.get('port', 80))
timeout_secs = int(config_dict.get('timeout-secs', 3))

if not hostname:
    print('No hostname in %s' % conf_file)
    sys.exit(1)

print('Testing against AirLink at %s:%d (from %s)' % (hostname, port, conf_file))
print()

# --- collect two readings a second apart --------------------------------
def collect_two_readings_one_second_apart() -> Tuple[Optional[Reading], Optional[Reading]]:
    session = requests.Session()
    r1 = Service.collect_data(session, hostname, port, timeout_secs)
    sleep(1)  # to get a different time (to the second) on the second reading
    r2 = Service.collect_data(session, hostname, port, timeout_secs)
    return r1, r2

try:
    reading, reading2 = collect_two_readings_one_second_apart()
    check('two readings collected', reading is not None and reading2 is not None)
except Exception as e:
    check('two readings collected', False, repr(e))
    print('\nCannot reach the AirLink; the remaining tests need it.')
    sys.exit(1)

assert reading is not None and reading2 is not None

# --- the reading looks like a real one ----------------------------------
now = int(time.time())
one_minute_ago = now - 60

check('did present', reading.did is not None)
check('name present', reading.name is not None)
check('data_structure_type is 6', reading.data_structure_type == 6,
      repr(reading.data_structure_type))
check('ts is current', reading.ts is not None and one_minute_ago < reading.ts <= now,
      repr(reading.ts))
check('last_report_time is current',
      reading.last_report_time is not None
      and one_minute_ago < reading.last_report_time <= now,
      repr(reading.last_report_time))
check('temperature plausible', reading.temp is not None and -50 <= reading.temp <= 150,
      repr(reading.temp))
check('humidity plausible', reading.hum is not None and 0 <= reading.hum <= 100,
      repr(reading.hum))
for field in ['pm_1', 'pm_2p5', 'pm_10']:
    v = getattr(reading, field)
    check('%s plausible' % field, v is None or 0.0 <= v < 10000.0, repr(v))

# --- the device's own json survives a round trip through the model ------
session = requests.Session()
raw = session.get('http://%s:%d/v1/current_conditions' % (hostname, port),
                  timeout=timeout_secs).json()
if raw['data']['conditions'][0]['data_structure_type'] == 6:
    round_tripped = json.loads(reading_to_json(reading_from_device_json(raw)))
    check('live device json round trips unchanged', round_tripped == raw,
          'differs: %r' % [k for k in raw['data']['conditions'][0]
                           if raw['data']['conditions'][0].get(k)
                           != round_tripped['data']['conditions'][0].get(k)])
else:
    print('SKIP: device reports type 5; round trip check needs type 6')

# is_sane only accepts type 6, and the daemon converts before checking, so do
# the same here: a type 5 device is handled, not insane.
sane_input = copy.deepcopy(raw)
if sane_input['data']['conditions'][0]['data_structure_type'] == 5:
    convert_data_structure_type_5_to_6(sane_input)
    check('type 5 converted to type 6',
          sane_input['data']['conditions'][0]['data_structure_type'] == 6)
check('live reading is sane', Service.is_sane(sane_input)[0], Service.is_sane(sane_input)[1])

# --- database round trips with real data --------------------------------
tmp = tempfile.NamedTemporaryFile(prefix='airlink-proxy-live', suffix='.sdb', delete=False)
tmp.close()
os.unlink(tmp.name)
db = Database.create(tmp.name)

db.save_archive_reading(reading.ts, reading)
out = list(db.fetch_archive_readings(0))
check('archive round trip', len(out) == 1 and out[0] == reading,
      '%d rows' % len(out))

db.save_current_reading(reading)
db.save_current_reading(reading2)
cur = list(db.fetch_current_readings())
check('current is one row, latest wins',
      len(cur) == 1 and cur[0].last_report_time == reading2.last_report_time)

js = db.fetch_current_reading_as_json()
check('current record serializes to the device shape',
      '"conditions"' in js and '"error": null' in js, js[:120])

os.unlink(tmp.name)
print()
if failures:
    print('%d FAILURES: %s' % (len(failures), failures))
    sys.exit(1)
print('ALL PASSED')
