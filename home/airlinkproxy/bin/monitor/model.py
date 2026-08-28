#!/usr/bin/python3

# Copyright (c) 2020-2026 John A Kline
# See the file LICENSE for your full rights.

"""The AirLink reading, and the conversions in both directions between it and
json.

Both conversions live here, rather than in service.py, so that database.py can
turn a stored reading back into the device's own json without importing the
service (which would be a cycle).

A sample of the device's json, from
http://<airlink>/v1/current_conditions :

{ "data": { "did": "001D0A100214", "name": "paloaltoweather.com",
  "ts": 1600657321, "conditions": [{ "lsid": 347825,
  "data_structure_type": 6, "temp": 74.1, "hum": 62.8, "dew_point": 60.6,
  "wet_bulb": 64.2, "heat_index": 74.7, "pm_1_last": 11, "pm_2p5_last": 14,
  "pm_10_last": 17, "pm_1": 11.68, "pm_2p5": 15.55,
  "pm_2p5_last_1_hour": 19.35, "pm_2p5_last_3_hours": 20.54,
  "pm_2p5_last_24_hours": 18.59, "pm_2p5_nowcast": 21.25, "pm_10": 17.28,
  "pm_10_last_1_hour": 23.30, "pm_10_last_3_hours": 24.64,
  "pm_10_last_24_hours": 22.71, "pm_10_nowcast": 25.60,
  "last_report_time": 1600657321, "pct_pm_data_last_1_hour": 100,
  "pct_pm_data_last_3_hours": 100, "pct_pm_data_nowcast": 100,
  "pct_pm_data_last_24_hours": 100 }] }, "error": null }
"""

from dataclasses import dataclass
from json import dumps
from typing import Any, Dict, List, Optional

from monitor import log

class RecordType:
    CURRENT   : int = 0
    ARCHIVE   : int = 1
    TWO_MINUTE: int = 2

@dataclass
class Reading:
    did                      : Optional[str]
    name                     : Optional[str]
    ts                       : Optional[int]
    lsid                     : Optional[int]
    data_structure_type      : Optional[int]
    temp                     : Optional[float]
    hum                      : Optional[float]
    dew_point                : Optional[float]
    wet_bulb                 : Optional[float]
    heat_index               : Optional[float]
    pm_1_last                : Optional[int]
    pm_2p5_last              : Optional[int]
    pm_10_last               : Optional[int]
    pm_1                     : Optional[float]
    pm_2p5                   : Optional[float]
    pm_2p5_last_1_hour       : Optional[float]
    pm_2p5_last_3_hours      : Optional[float]
    pm_2p5_last_24_hours     : Optional[float]
    pm_2p5_nowcast           : Optional[float]
    pm_10                    : Optional[float]
    pm_10_last_1_hour        : Optional[float]
    pm_10_last_3_hours       : Optional[float]
    pm_10_last_24_hours      : Optional[float]
    pm_10_nowcast            : Optional[float]
    last_report_time         : Optional[int]
    pct_pm_data_last_1_hour  : Optional[int]
    pct_pm_data_last_3_hours : Optional[int]
    pct_pm_data_nowcast      : Optional[int]
    pct_pm_data_last_24_hours: Optional[int]

# The fields of Reading, in the order the database stores and returns them.
# database.py builds its column list and its row unpacking from this, so a new
# field is added in one place.
DATA_FIELDS: List[str] = ['did', 'name', 'ts']

CONDITION_FIELDS: List[str] = [
    'lsid', 'data_structure_type', 'temp', 'hum', 'dew_point', 'wet_bulb',
    'heat_index', 'pm_1_last', 'pm_2p5_last', 'pm_10_last', 'pm_1', 'pm_2p5',
    'pm_2p5_last_1_hour', 'pm_2p5_last_3_hours', 'pm_2p5_last_24_hours',
    'pm_2p5_nowcast', 'pm_10', 'pm_10_last_1_hour', 'pm_10_last_3_hours',
    'pm_10_last_24_hours', 'pm_10_nowcast', 'last_report_time',
    'pct_pm_data_last_1_hour', 'pct_pm_data_last_3_hours',
    'pct_pm_data_nowcast', 'pct_pm_data_last_24_hours']

READING_FIELDS: List[str] = DATA_FIELDS + CONDITION_FIELDS

# The fields Service.compute_avg averages when it builds an archive or
# two-minute record.  pm_1_last/pm_2p5_last/pm_10_last are the device's
# instantaneous readings and are the point of the exercise; temp through
# heat_index and the one-minute pm_1/pm_2p5/pm_10 are averaged for consistency.
#
# Everything NOT listed here is carried from the most recent reading in the
# span: the identifiers (did, name, lsid, data_structure_type), the timestamps
# (ts, last_report_time), and the device's own rolling windows
# (pm_2p5_last_1_hour and friends, pct_pm_data_*).  Averaging a rolling average
# across an interval does not mean anything, so those are left as the device
# last reported them.
AVERAGED_FIELDS: List[str] = [
    'temp', 'hum', 'dew_point', 'wet_bulb', 'heat_index',
    'pm_1_last', 'pm_2p5_last', 'pm_10_last', 'pm_1', 'pm_2p5', 'pm_10']

def convert_data_structure_type_5_to_6(j: Dict[str, Any]) -> None:
    """Rename the type 5 pm_10p0* fields to their type 6 pm_10* spellings, in
    place, and restamp the record as type 6.  Fields are renamed one at a time,
    so a record missing some of them is left half renamed with its
    data_structure_type still 5; that is deliberate -- the sanity check then
    rejects it, which is the same outcome as refusing to convert it at all."""
    try:
        conditions = j['data']['conditions'][0]
        for suffix in ['', '_last_1_hour', '_last_3_hours', '_last_24_hours', '_nowcast']:
            conditions['pm_10%s' % suffix] = conditions['pm_10p0%s' % suffix]
            conditions['pm_10p0%s' % suffix] = None
        conditions['data_structure_type'] = 6
        log.debug('Converted type 5 record to type 6.')
    except Exception as e:
        log.info('convert_data_structure_type_5_to_6: exception: %s' % e)
        # Let sanity check handle the issue.

def reading_from_device_json(j: Dict[str, Any]) -> Reading:
    """Build a Reading from the device's json.  Fields the device did not send
    come through as None, and are named in a single log line."""
    values: Dict[str, Any] = {}
    missed: List[str] = []

    data: Dict[str, Any] = j['data']
    conditions: Dict[str, Any] = data['conditions'][0]

    for field in DATA_FIELDS:
        if field in data:
            values[field] = data[field]
        else:
            values[field] = None
            missed.append(field)

    for field in CONDITION_FIELDS:
        if field in conditions:
            values[field] = conditions[field]
        else:
            values[field] = None
            missed.append(field)

    if missed:
        log.info("Sensor didn't report field(s): %s" % ','.join(missed))

    return Reading(**values)

def error_json(message: str, code: int = 400) -> str:
    """The device's own error envelope: a null data member and an error object
    carrying a code and a message.  Every response the proxy sends is valid
    json in this shape or the reading shape, so a client parses them alike.

    Built with dumps rather than interpolated: the message can quote the
    client's own request text, which would otherwise break the json the moment
    it contained a quote or a backslash."""
    return dumps({'data': None, 'error': {'code': code, 'message': message}})

def reading_to_json(reading: Reading) -> str:
    """Render a Reading as the json the device itself would have served, which
    is what clients of the proxy expect."""
    data: Dict[str, Any] = {field: getattr(reading, field) for field in DATA_FIELDS}
    condition_0: Dict[str, Any] = {field: getattr(reading, field) for field in CONDITION_FIELDS}
    data['conditions'] = [condition_0]
    return dumps({'data': data, 'error': None})
