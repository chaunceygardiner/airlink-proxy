#!/usr/bin/python3

# Copyright (c) 2020-2026 John A Kline
# See the file LICENSE for your full rights.

"""The main loop: poll the AirLink on poll-freq-secs boundaries (shifted by
poll-freq-offset), sanity check what comes back, save it as the current
reading, and on archive boundaries save it as an archive record too.

The daemon must never exit on a bad reading.  Sensor and database errors are
caught, logged and skipped, and the requests session is reset on a collection
error.
"""

import copy
import time

from datetime import datetime
from dateutil import tz
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

import requests

from monitor import log
from monitor.database import FIELD_TYPES, Database
from monitor.model import (AVERAGED_FIELDS, Reading,
                           convert_data_structure_type_5_to_6,
                           reading_from_device_json)

class Service(object):
    def __init__(self, hostname: str, port: int, timeout_secs: int,
                 pollfreq_secs: int, pollfreq_offset: int, arcint_secs: int,
                 database: Database) -> None:
        self.hostname        = hostname
        self.port            = port
        self.timeout_secs    = timeout_secs
        self.pollfreq_secs   = pollfreq_secs
        self.pollfreq_offset = pollfreq_offset
        self.arcint_secs     = arcint_secs
        self.database        = database

        log.debug('Service created')

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(tz=tz.gettz('UTC'))

    @staticmethod
    def datetime_display(ts: Optional[int]) -> str:
        if ts is None:
            return 'unknown'
        return '%s (%d)' % (time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(ts)), ts)

    @staticmethod
    def is_type(j: Dict[str, Any], t, name: str, none_ok: bool = False) -> bool:
        try:
            x = j[name]
            if x is None and none_ok:
                return True
            # json has a single number type, so a float-valued field can arrive
            # as int -- "hum": 62 rather than 62.0.  This firmware always emits
            # a decimal point, but a device that did not would have had its
            # whole reading rejected.  bool is a subclass of int, and is never a
            # valid number here.
            if isinstance(x, bool):
                log.debug('%s is a bool, not %s: %s' % (name, t, j[name]))
                return False
            if t is float and isinstance(x, int):
                return True
            if not isinstance(x, t):
                log.debug('%s is not an instance of %s: %s' % (name, t, j[name]))
                return False
            return True
        except KeyError as e:
            log.debug('is_type: could not find key: %s' % e)
            return False
        except Exception as e:
            log.debug('is_type: exception: %s' % e)
            return False

    @staticmethod
    def conditions_of(j: Any) -> Optional[Dict[str, Any]]:
        """conditions[0] when the response has the shape the device documents,
        otherwise None.  For use before is_sane has vetted the structure."""
        try:
            conditions = j['data']['conditions']
            if isinstance(conditions, list) and conditions and isinstance(conditions[0], dict):
                return conditions[0]
        except (KeyError, TypeError, IndexError):
            pass
        return None

    @staticmethod
    def is_sane(j: Dict[str, Any]) -> Tuple[bool, str]:
        # Everything below indexes into j, so the shape has to be established
        # first: a response that is not an object, or has no error member, is
        # malformed rather than unreadable, and saying so keeps it in the
        # insane-reading bucket instead of the fetch-failure one.
        if not isinstance(j, dict):
            return False, 'Response is not a json object'

        if 'error' not in j:
            return False, 'Missing "error" field'

        if j['error'] is not None:
            return False, 'Error: %s' % j['error']

        if not Service.is_type(j, dict, 'data'):
            return False, 'Missing or malformed "data" field'

        if not Service.is_type(j['data'], str, 'name'):
            return False, 'Missing or malformed "name" field'

        if not Service.is_type(j['data'], int, 'ts'):
            return False, 'Missing or malformed "ts" field'

        if not Service.is_type(j['data'], list, 'conditions'):
            return False, 'Missing or malformed "conditions" field'

        if len(j['data']['conditions']) == 0:
            return False, 'Expected one element in conditions array.'

        if not Service.is_type(j['data']['conditions'][0], int, 'data_structure_type'):
            return False, 'Missing or malformed "data_structure_type" field'

        if j['data']['conditions'][0]['data_structure_type'] != 6:
            return False, 'Expected data_structure_type of 6 (or type 5 auto converted to 6).'

        for name in ['pm_1_last', 'pm_2p5_last', 'pm_10_last', 'last_report_time',
                     'pct_pm_data_last_1_hour', 'pct_pm_data_last_3_hours',
                     'pct_pm_data_nowcast', 'pct_pm_data_last_24_hours']:
            if not Service.is_type(j['data']['conditions'][0], int, name, True):
                return False, 'Missing or malformed "%s" field' % name

        if not Service.is_type(j['data']['conditions'][0], int, 'lsid', True):
            return False, 'Missing or malformed "lsid" field'

        for name in ['temp', 'hum', 'dew_point', 'wet_bulb', 'heat_index']:
            if not Service.is_type(j['data']['conditions'][0], float, name):
                return False, 'Missing or malformed "%s" field' % name

        for name in ['pm_1', 'pm_2p5', 'pm_2p5_last_1_hour',
                     'pm_2p5_last_3_hours', 'pm_2p5_last_24_hours', 'pm_2p5_nowcast',
                     'pm_10', 'pm_10_last_1_hour', 'pm_10_last_3_hours',
                     'pm_10_last_24_hours', 'pm_10_nowcast']:
            if not Service.is_type(j['data']['conditions'][0], float, name, True):
                return False, 'Missing or malformed "%s" field' % name

        return True, ''

    @staticmethod
    def collect_data(session: requests.Session, hostname: str, port: int,
                     timeout_secs: int) -> Optional[Reading]:
        url = 'http://%s:%s/v1/current_conditions' % (hostname, port)

        # Fetch the reading.  If the machine was just rebooted, a temporary
        # failure in name resolution is likely; try three times on
        # ConnectionError before giving up on this poll.
        r: Optional[requests.Response] = None
        for i in range(3):
            try:
                start_time = time.time()
                r = session.get(url=url, timeout=timeout_secs)
                r.raise_for_status()
                elapsed_time = time.time() - start_time
                log.debug('collect_data: elapsed time: %f seconds.' % elapsed_time)
                if elapsed_time > 5:
                    log.info('Event took longer than expected: %f seconds.' % elapsed_time)
                break
            except requests.exceptions.ConnectionError as e:
                if i < 2:
                    log.info('%s: Retrying request.' % e)
                    sleep(1)
                else:
                    raise e

        j: Optional[Dict[str, Any]] = None
        time_of_reading: Optional[int] = None
        try:
            if r:
                j = r.json()
                log.debug('collect_data: json returned from %s is: %r' % (hostname, j))
                # Check for error
                if j is not None and 'error' in j and j['error'] is not None:
                    error = j['error']
                    log.info('%s returned error(%d): %s' % (url, error['code'], error['message']))
                    return None
                assert j is not None
                # If data structure type 5, convert it to 6.  This runs before
                # is_sane has vetted the structure, so reach in defensively and
                # let is_sane report anything malformed.
                conditions = Service.conditions_of(j)
                if conditions is not None and conditions.get('data_structure_type') == 5:
                    convert_data_structure_type_5_to_6(j)
                # Check for sanity
                sane, msg = Service.is_sane(j)
                if not sane:
                    log.info('Reading not sane:  %s (%s)' % (msg, j))
                    return None
                time_of_reading = j['data']['conditions'][0]['last_report_time']
                if time_of_reading is None:
                    log.info('Reading not sane:  Missing "last_report_time" field (%s)' % j)
                    return None
                # The reading could be old.  Check that it is not older than now - 60.
                age_of_reading = time.time() - time_of_reading
                if age_of_reading > 60:
                    # Perhaps the AirLink has rebooted.  If so, last_report_time
                    # will be seconds since boot until the device syncs its
                    # clock.  Check for that by looking at pm_1.
                    if j['data']['conditions'][0]['pm_1'] is None:
                        log.info('last_report_time must be time since boot: %d seconds.  Record: %s'
                                 % (time_of_reading, j))
                    else:
                        # Not current.  (Rarely, spurious timestamps -- e.g. 2016
                        # in 2020 -- have been observed, with both ts and
                        # last_report_time wrong.)
                        log.info('Ignoring reading from %s--age: %d seconds.  Record: %s'
                                 % (hostname, age_of_reading, j))
                    j = None
        except Exception as e:
            log.info('collect_data: Attempt to fetch from: %s failed: %s.' % (hostname, e))
            j = None

        if j is None or time_of_reading is None:
            return None

        log.debug('Successful read from %s.' % hostname)
        return reading_from_device_json(j)

    @staticmethod
    def compute_avg(readings: List[Reading]) -> Reading:
        """Average AVERAGED_FIELDS across readings; every other field is
        carried from the most recent reading in the span.  Callers guarantee at
        least one reading.

        A field is averaged over however many readings actually reported it, so
        a sensor that dropped a field for part of the span still contributes;
        a field no reading reported stays None.  Integer columns are rounded so
        the stored value keeps the type the device (and the json) uses."""
        avg: Reading = copy.deepcopy(readings[-1])

        for field in AVERAGED_FIELDS:
            values = [v for v in (getattr(r, field) for r in readings) if v is not None]
            if not values:
                setattr(avg, field, None)
                continue
            mean: float = sum(values) / float(len(values))
            setattr(avg, field, int(mean + 0.5) if FIELD_TYPES[field] == 'INTEGER' else mean)

        return avg

    @staticmethod
    def trim_two_minute_readings(readings: List[Reading], now: float) -> None:
        """Drop readings older than two minutes.  last_report_time is safe to
        key on because collect_data refuses a reading whose last_report_time is
        None -- is_sane itself allows None there (is_type with none_ok).  The
        loop stops at a leading None rather than dropping it, so relaxing that
        check in collect_data would grow this list without bound."""
        two_minutes_ago: float = now - 120
        while readings and readings[0].last_report_time is not None \
                and readings[0].last_report_time < two_minutes_ago:
            readings.pop(0)

    def secs_to_next_poll(self) -> float:
        # Polls land on pollfreq_secs boundaries shifted later by
        # pollfreq_offset, so two proxies polling one sensor with different
        # offsets never ask at the same moment.  Subtract the offset before
        # finding the boundary: adding it afterwards would skip a slot whenever
        # the previous poll's work left now past a boundary but short of
        # boundary + offset.  pollfreq_offset is validated to be less than
        # pollfreq_secs, so the poll period stays pollfreq_secs.
        now = time.time()
        next_poll_event = int((now - self.pollfreq_offset) / self.pollfreq_secs) \
            * self.pollfreq_secs + self.pollfreq_secs + self.pollfreq_offset
        secs_to_event = next_poll_event - now
        log.debug('Next poll in %f seconds' % secs_to_event)
        return secs_to_event

    def next_archive_boundary(self) -> float:
        return (int(time.time() / self.arcint_secs) + 1) * self.arcint_secs

    def do_loop(self) -> None:
        log.debug('Started main loop.')
        session: Optional[requests.Session] = None
        # Readings collected since the last archive record, and those from the
        # last two minutes.  The archive record is the average over its
        # interval, so a poll that fails no longer costs the whole record --
        # only the samples it would have contributed.
        archive_readings: List[Reading] = []
        two_minute_readings: List[Reading] = []
        # The timestamp of the two-minute row currently in the database, or
        # None if there is none.  A restart does not invalidate that row -- an
        # upgrade restart takes seconds and the average is still good -- so it
        # is read back here and kept until it actually ages out.  One rule
        # covers both cases: the stored record is never more than two minutes
        # old.
        two_minute_ts: Optional[int] = None
        try:
            for stored in self.database.fetch_two_minute_readings():
                two_minute_ts = stored.ts
                log.debug('Two-minute record carried over from %s.'
                          % Service.datetime_display(two_minute_ts))
        except Exception as e:
            log.error('Could not read the stored two-minute reading: %s' % e)
        # The archive record is written by the first poll at or past the
        # boundary (rather than requiring a poll to land exactly on it), so
        # any poll-freq-offset is safe and a slow sensor read that straddles
        # the boundary delays the record instead of skipping it.
        next_arc_ts: float = self.next_archive_boundary()
        while True:
            # Sleep until the next poll.
            sleep(self.secs_to_next_poll())

            # An archive record is due if this poll is at or past the boundary.
            archive_due: bool = time.time() >= next_arc_ts

            # Collect a reading.
            reading: Optional[Reading] = None
            try:
                start = Service.utc_now()
                if session is None:
                    session = requests.Session()

                reading = Service.collect_data(session, self.hostname, self.port, self.timeout_secs)
                log.debug('Read sensor in %d seconds.' % (Service.utc_now() - start).seconds)
            except Exception as e:
                log.error('Skipping reading because of: %s' % e)
                # It's probably a good idea to reset the session.
                try:
                    if session is not None:
                        session.close()
                except Exception as e:
                    log.info('Non-fatal: calling session.close(): %s' % e)
                finally:
                    session = None

            if reading is not None:
                archive_readings.append(reading)
                two_minute_readings.append(reading)

            # The two-minute average is recomputed every poll, over whatever is
            # still inside the window.
            Service.trim_two_minute_readings(two_minute_readings, time.time())
            two_minute: Optional[Reading] = (
                Service.compute_avg(two_minute_readings) if two_minute_readings else None)

            if reading is not None or two_minute is not None:
                # Both single-row records are written in one transaction: two
                # commits per poll would double the flash wear and the window
                # in which a REST reader hits 'database is locked'.
                try:
                    start = Service.utc_now()
                    self.database.save_poll_readings(reading, two_minute)
                    if two_minute is not None:
                        # is_sane requires an int ts, so this is set; fall back
                        # to now rather than leave the row unageable.
                        two_minute_ts = (two_minute.ts if two_minute.ts is not None
                                         else int(time.time()))
                    log.debug('Saved poll readings in %d seconds (two-minute over %d samples).'
                              % ((Service.utc_now() - start).seconds, len(two_minute_readings)))
                    if reading is not None:
                        log.debug('Saved current reading %s to database.'
                                  % Service.datetime_display(reading.last_report_time))
                except Exception as e:
                    log.critical('Could not save poll readings to database: %s: %s' % (self.database, e))
            elif two_minute_ts is not None and time.time() - two_minute_ts > 120:
                # Nothing to write and the stored average has aged out -- the
                # sensor has been unreadable for two minutes, across a restart
                # or not.  Clear it rather than let it be served as current.
                try:
                    self.database.delete_two_minute_reading()
                    two_minute_ts = None
                    log.info('Two-minute average cleared: no reading in two minutes.')
                except Exception as e:
                    log.critical('Could not clear two-minute reading from database: %s: %s' % (self.database, e))

            if archive_due:
                if archive_readings:
                    # The boundary just crossed, which is the timestamp the
                    # record is filed under.
                    archive_ts = int(time.time() / self.arcint_secs) * self.arcint_secs
                    try:
                        start = Service.utc_now()
                        self.database.save_archive_reading(
                            archive_ts, Service.compute_avg(archive_readings))
                        log.debug('Saved archive reading in %d seconds.' % (Service.utc_now() - start).seconds)
                        log.info('Added record %s to archive (%d samples).'
                                 % (Service.datetime_display(archive_ts), len(archive_readings)))
                    except Exception as e:
                        log.critical('Could not save archive reading to database: %s: %s' % (self.database, e))
                else:
                    log.error('Skipping archive record.')
                archive_readings = []
                next_arc_ts = self.next_archive_boundary()
