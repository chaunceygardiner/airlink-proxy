#!/usr/bin/python3

# Copyright (c) 2020 John A Kline
# See the file LICENSE for your full rights.

"""Make a rolling average of AirLink readings available.
Read from airlink sensor every 5 seconds and write current reading.
At 5s past the minute, write an archive reading.
"""

import optparse
import os
import requests
import sqlite3
import sys
import syslog
import tempfile
import time
import traceback

import server.server

import configobj

from datetime import datetime
from dateutil import tz
from dateutil.parser import parse
from enum import Enum
from json import dumps
from time import sleep

from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Tuple

AIRLINK_PROXY_VERSION = "0.1"

class Logger(object):
    def __init__(self, service_name: str, log_to_stdout: bool=False, debug_mode: bool=False):
        self.service_name = service_name
        self.log_to_stdout = log_to_stdout
        self.debug_mode = debug_mode
        if not log_to_stdout:
            syslog.openlog(service_name, syslog.LOG_PID | syslog.LOG_CONS)

    def logmsg(self, level: int, msg: str) -> None:
        if self.log_to_stdout:
            l: str
            if level == syslog.LOG_DEBUG:
                l = 'DEBUG'
            elif level == syslog.LOG_INFO:
                l = 'INFO'
            elif level == syslog.LOG_ERR:
                l = 'ERR'
            elif level == syslog.LOG_CRIT:
                l = 'CRIT'
            else:
                l = '%d' % level
            print('%s: %s: %s' % (l, self.service_name, msg))
        else:
            syslog.syslog(level, msg)

    def debug(self, msg: str) -> None:
        if self.debug_mode:
            self.logmsg(syslog.LOG_DEBUG, msg)

    def info(self, msg: str) -> None:
        self.logmsg(syslog.LOG_INFO, msg)

    def error(self, msg: str) -> None:
        self.logmsg(syslog.LOG_ERR, msg)

    def critical(self, msg: str) -> None:
        self.logmsg(syslog.LOG_CRIT, msg)

# Log to stdout until logger info is known.
log: Logger = Logger('monitor', log_to_stdout=True, debug_mode=False)

class Event(Enum):
    POLL = 1
    ARCHIVE = 2

#{ "data": { "did": "001D0A100214", "name": "paloaltoweather.com", "ts": 1600657321, "conditions": [{ "lsid": 347825, "data_structure_type": 6, "temp": 74.1, "hum": 62.8, "dew_point": 60.6, "wet_bulb": 64.2, "heat_index": 74.7, "pm_1_last": 11, "pm_2p5_last": 14, "pm_10_last": 17, "pm_1": 11.68, "pm_2p5": 15.55, "pm_2p5_last_1_hour": 19.35, "pm_2p5_last_3_hours": 20.54, "pm_2p5_last_24_hours": 18.59, "pm_2p5_nowcast": 21.25, "pm_10": 17.28, "pm_10_last_1_hour": 23.30, "pm_10_last_3_hours": 24.64, "pm_10_last_24_hours": 22.71, "pm_10_nowcast": 25.60, "last_report_time": 1600657321, "pct_pm_data_last_1_hour": 100, "pct_pm_data_last_3_hours": 100, "pct_pm_data_nowcast": 100, "pct_pm_data_last_24_hours": 100 }] }, "error": null }

@dataclass
class Reading:
    did                      : str
    name                     : str
    ts                       : int
    lsid                     : int
    data_structure_type      : int
    temp                     : float
    hum                      : float
    dew_point                : float
    wet_bulb                 : float
    heat_index               : float
    pm_1_last                : int
    pm_2p5_last              : int
    pm_10_last               : int
    pm_1                     : float
    pm_2p5                   : float
    pm_2p5_last_1_hour       : float
    pm_2p5_last_3_hours      : float
    pm_2p5_last_24_hours     : float
    pm_2p5_nowcast           : float
    pm_10                    : float
    pm_10_last_1_hour        : float
    pm_10_last_3_hours       : float
    pm_10_last_24_hours      : float
    pm_10_nowcast            : float
    last_report_time         : int
    pct_pm_data_last_1_hour  : int
    pct_pm_data_last_3_hours : int
    pct_pm_data_nowcast      : int
    pct_pm_data_last_24_hours: int

class DatabaseAlreadyExists(Exception):
    pass

class RecordType:
    CURRENT: int = 0
    ARCHIVE: int = 1

class Database(object):
    def __init__(self, db_file: str):
        self.db_file = db_file

    @staticmethod
    def create(db_file): # -> Database:
        if db_file != ':memory:' and os.path.exists(db_file):
            raise DatabaseAlreadyExists("Database %s already exists" % db_file)
        if db_file != ':memory:':
            # Create parent directories
            dir = os.path.dirname(db_file)
            if not os.path.exists(dir):
                os.makedirs(dir)

        create_reading_table: str = ('CREATE TABLE Reading ('
            ' record_type               INTEGER NOT NULL,'
            ' timestamp                 INTEGER NOT NULL,'
            ' did                       STRING,'
            ' name                      STRING,'
            ' ts                        INTEGER,'
            ' lsid                      INTEGER,'
            ' data_structure_type       INTEGER,'
            ' temp                      FLOAT,'
            ' hum                       FLOAT,'
            ' dew_point                 FLOAT,'
            ' wet_bulb                  FLOAT,'
            ' heat_index                FLOAT,'
            ' pm_1_last                 INTEGER,'
            ' pm_2p5_last               INTEGER,'
            ' pm_10_last                INTEGER,'
            ' pm_1                      FLOAT,'
            ' pm_2p5                    FLOAT,'
            ' pm_2p5_last_1_hour        FLOAT,'
            ' pm_2p5_last_3_hours       FLOAT,'
            ' pm_2p5_last_24_hours      FLOAT,'
            ' pm_2p5_nowcast            FLOAT,'
            ' pm_10                     FLOAT,'
            ' pm_10_last_1_hour         FLOAT,'
            ' pm_10_last_3_hours        FLOAT,'
            ' pm_10_last_24_hours       FLOAT,'
            ' pm_10_nowcast             FLOAT,'
            ' last_report_time          INTEGER,'
            ' pct_pm_data_last_1_hour   INTEGER,'
            ' pct_pm_data_last_3_hours  INTEGER,'
            ' pct_pm_data_nowcast       INTEGER,'
            ' pct_pm_data_last_24_hours INTEGER,'
            ' PRIMARY KEY (record_type, timestamp));')

        with sqlite3.connect(db_file, timeout=5) as conn:
            cursor = conn.cursor()
            cursor.execute(create_reading_table)
            cursor.close()

        return Database(db_file)

    def save_current_reading(self, r: Reading) -> None:
        self.save_reading(RecordType.CURRENT, r.ts, r)

    def save_archive_reading(self, timestamp: int, r: Reading) -> None:
        self.save_reading(RecordType.ARCHIVE, timestamp, r)

    @staticmethod
    def add_string(fields: str, values: str, field: str, str_val: str) -> Tuple[str, str]:
        if str_val is not None:
            return '%s, %s' % (fields, field), '%s, "%s"' % (values, str_val)

    @staticmethod
    def add_int(fields: str, values: str, field: str, int_val: int) -> Tuple[str, str]:
        if int_val is not None:
            return '%s, %s' % (fields, field), '%s, %d' % (values, int_val)

    @staticmethod
    def add_float(fields: str, values: str, field: str, float_val: float) -> Tuple[str, str]:
        if float_val is not None:
            return '%s, %s' % (fields, field), '%s, %f' % (values, float_val)

    @staticmethod
    def build_insert_statement(record_type: int, timestamp: int, r: Reading) -> str:
        fields: str = 'record_type, timestamp'
        values: str = '%d, %d' % (record_type, timestamp) 
        if r.did is not None:
            fields, values = Database.add_string(fields, values, 'did', r.did)
        if r.name is not None:
            fields, values = Database.add_string(fields, values, 'name', r.name)
        if r.ts is not None:
            fields, values = Database.add_int(fields, values, 'ts', r.ts)
        if r.lsid is not None:
            fields, values = Database.add_int(fields, values, 'lsid', r.lsid)
        if r.data_structure_type is not None:
            fields, values = Database.add_int(fields, values, 'data_structure_type', r.data_structure_type)
        if r.temp is not None:
            fields, values = Database.add_float(fields, values, 'temp', r.temp)
        if r.hum is not None:
            fields, values = Database.add_float(fields, values, 'hum', r.hum)
        if r.dew_point is not None:
            fields, values = Database.add_float(fields, values, 'dew_point', r.dew_point)
        if r.wet_bulb is not None:
            fields, values = Database.add_float(fields, values, 'wet_bulb', r.wet_bulb)
        if r.heat_index is not None:
            fields, values = Database.add_float(fields, values, 'heat_index', r.heat_index)
        if r.pm_1_last is not None:
            fields, values = Database.add_int(fields, values, 'pm_1_last', r.pm_1_last)
        if r.pm_2p5_last is not None:
            fields, values = Database.add_int(fields, values, 'pm_2p5_last', r.pm_2p5_last)
        if r.pm_10_last is not None:
            fields, values = Database.add_int(fields, values, 'pm_10_last', r.pm_10_last)
        if r.pm_1 is not None:
            fields, values = Database.add_float(fields, values, 'pm_1', r.pm_1)
        if r.pm_2p5 is not None:
            fields, values = Database.add_float(fields, values, 'pm_2p5', r.pm_2p5)
        if r.pm_2p5_last_1_hour is not None:
            fields, values = Database.add_float(fields, values, 'pm_2p5_last_1_hour', r.pm_2p5_last_1_hour)
        if r.pm_2p5_last_3_hours is not None:
            fields, values = Database.add_float(fields, values, 'pm_2p5_last_3_hours', r.pm_2p5_last_3_hours)
        if r.pm_2p5_last_24_hours is not None:
            fields, values = Database.add_float(fields, values, 'pm_2p5_last_24_hours', r.pm_2p5_last_24_hours)
        if r.pm_2p5_nowcast is not None:
            fields, values = Database.add_float(fields, values, 'pm_2p5_nowcast', r.pm_2p5_nowcast)
        if r.pm_10 is not None:
            fields, values = Database.add_float(fields, values, 'pm_10', r.pm_10)
        if r.pm_10_last_1_hour is not None:
            fields, values = Database.add_float(fields, values, 'pm_10_last_1_hour', r.pm_10_last_1_hour)
        if r.pm_10_last_3_hours is not None:
            fields, values = Database.add_float(fields, values, 'pm_10_last_3_hours', r.pm_10_last_3_hours)
        if r.pm_10_last_24_hours is not None:
            fields, values = Database.add_float(fields, values, 'pm_10_last_24_hours', r.pm_10_last_24_hours)
        if r.pm_10_nowcast is not None:
            fields, values = Database.add_float(fields, values, 'pm_10_nowcast', r.pm_10_nowcast)
        if r.last_report_time is not None:
            fields, values = Database.add_int(fields, values, 'last_report_time', r.last_report_time)
        if r.pct_pm_data_last_1_hour is not None:
            fields, values = Database.add_int(fields, values, 'pct_pm_data_last_1_hour', r.pct_pm_data_last_1_hour)
        if r.pct_pm_data_last_3_hours is not None:
            fields, values = Database.add_int(fields, values, 'pct_pm_data_last_3_hours', r.pct_pm_data_last_3_hours)
        if r.pct_pm_data_nowcast is not None:
            fields, values = Database.add_int(fields, values, 'pct_pm_data_nowcast', r.pct_pm_data_nowcast)
        if r.pct_pm_data_last_24_hours is not None:
            fields, values = Database.add_int(fields, values, 'pct_pm_data_last_24_hours', r.pct_pm_data_last_24_hours)

        return 'INSERT INTO Reading (%s) VALUES(%s);' % (fields, values)

    def save_reading(self, record_type: int, timestamp: int, r: Reading) -> None:
        insert_reading_sql = Database.build_insert_statement(record_type, timestamp, r)
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            # if a current record, delete previous current.
            if record_type == RecordType.CURRENT:
                cursor.execute('DELETE FROM Reading where record_type = %d;' % RecordType.CURRENT)
            # Now insert.
            log.debug('inserting record: %s' % insert_reading_sql)
            cursor.execute(insert_reading_sql)

    def fetch_current_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.CURRENT, 0)

    def fetch_current_reading_as_json(self) -> str:
        for reading in self.fetch_current_readings():
            return Service.convert_to_json(reading)
        return '{}'

    def get_earliest_timestamp_as_json(self) -> str:
        select: str = ('SELECT timestamp FROM Reading WHERE record_type = %d'
            ' ORDER BY timestamp LIMIT 1') % RecordType.ARCHIVE
        log.debug('get-earliest-timestamp: select: %s' % select)
        resp = {}
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select):
                log.debug('get-earliest-timestamp: returned %s' % row[0])
                resp['timestamp'] = row[0]
                break
        log.debug('get-earliest-timestamp: returning: %s' % dumps(resp))
        return dumps(resp)

    def fetch_archive_readings(self, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.ARCHIVE, since_ts, max_ts, limit)

    def fetch_archive_readings_as_json(self, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> str:
        contents = ''
        for reading in self.fetch_archive_readings(since_ts, max_ts, limit):
            if contents != '':
                contents += ','
            contents += Service.convert_to_json(reading)
        return '[  %s ]' % contents

    def fetch_readings(self, record_type: int, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Reading]:
        select: str = ('SELECT did, name, ts, lsid, data_structure_type,'
            ' temp, hum, dew_point, wet_bulb, heat_index,'
            ' pm_1_last, pm_2p5_last, pm_10_last,'
            ' pm_1,'
            ' pm_2p5, pm_2p5_last_1_hour, pm_2p5_last_3_hours, pm_2p5_last_24_hours, pm_2p5_nowcast,'
            ' pm_10, pm_10_last_1_hour, pm_10_last_3_hours, pm_10_last_24_hours, pm_10_nowcast,'
            ' last_report_time, pct_pm_data_last_1_hour, pct_pm_data_last_3_hours,'
            ' pct_pm_data_nowcast, pct_pm_data_last_24_hours FROM Reading'
            ' WHERE record_type = %d AND timestamp > %d') % (record_type, since_ts)
        if max_ts is not None:
            select = '%s AND timestamp <= %d' % (select, max_ts)
        select += ' ORDER BY timestamp'
        if limit is not None:
            select = '%s LIMIT %d' % (select, limit)
        select += ';'
        log.debug('fetch_readings: select: %s' % select)
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select):
                reading = Database.create_reading_from_row(row)
                yield reading

    @staticmethod
    def create_reading_from_row(row) -> Reading:
        return Reading(
            did                       = row[0],
            name                      = row[1],
            ts                        = row[2],
            lsid                      = row[3],
            data_structure_type       = row[4],
            temp                      = row[5],
            hum                       = row[6],
            dew_point                 = row[7],
            wet_bulb                  = row[8],
            heat_index                = row[9],
            pm_1_last                 = row[10],
            pm_2p5_last               = row[11],
            pm_10_last                = row[12],
            pm_1                      = row[13],
            pm_2p5                    = row[14],
            pm_2p5_last_1_hour        = row[15],
            pm_2p5_last_3_hours       = row[16],
            pm_2p5_last_24_hours      = row[17],
            pm_2p5_nowcast            = row[18],
            pm_10                     = row[19],
            pm_10_last_1_hour         = row[20],
            pm_10_last_3_hours        = row[21],
            pm_10_last_24_hours       = row[22],
            pm_10_nowcast             = row[23],
            last_report_time          = row[24],
            pct_pm_data_last_1_hour   = row[25],
            pct_pm_data_last_3_hours  = row[26],
            pct_pm_data_nowcast       = row[27],
            pct_pm_data_last_24_hours = row[28])

class Service(object):
    def __init__(self, hostname: str, port: int, timeout_secs: int,
                 database: Database) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout_secs    = timeout_secs
        self.pollfreq_secs   = 5
        self.pollfreq_offset = 5
        self.arcint_secs     = 60
        self.database        = database

        log.debug('Service created')

    @staticmethod
    def collect_data(session: requests.Session, hostname: str, port:int, timeout_secs:int) -> Optional[Reading]:
    
        j = None
        url = 'http://%s:%s/v1/current_conditions' % (hostname, port)
    
        # fetch data
        # If the machine was just rebooted, a temporary failure in name
        # resolution is likely.  As such, try three times on ConnectionError.
        for i in range(3):
            try:
                start_time = time.time()
                r: requests.Response = session.get(url="http://%s:%s/v1/current_conditions" % (hostname, port), timeout=timeout_secs)
                r.raise_for_status()
                elapsed_time = time.time() - start_time
                log.debug('collect_data: elapsed time: %f seconds.' % elapsed_time)
                if elapsed_time > 1.0:
                    log.info('Event took longer than expected: %f seconds.' % elapsed_time)
                break
            except requests.exceptions.ConnectionError as e:
                if i < 2:
                    log.info('%s: Retrying request.' % e)
                    sleep(1)
                else:
                    raise e

        try:
            if r:
                # convert to json
                j = r.json()
                log.debug('collect_data: json returned from %s is: %r' % (hostname, j))
                # Check for error
                if 'error' in j and j['error'] is not None:
                    error = j['error']
                    code = error['code']
                    message = error['message']
                    log.info('%s returned error(%d): %s' % (url, code, message))
                    return None
                # If data structure type 5, convert it to 6.
                if j['data']['conditions'][0]['data_structure_type'] == 5:
                    Service.convert_data_structure_type_5_to_6(j)
                # Check for sanity
                sane, msg = Service.is_sane(j)
                if not sane:
                    log.info('Reading not sane:  %s (%s)' % (msg, j))
                    return None
                time_of_reading = j['data']['conditions'][0]['last_report_time']
                # The reading could be old.
                # Check that it's not older than now - 60
                age_of_reading = time.time() - time_of_reading
                if age_of_reading > 60:
                    # Perhaps the AirLink has rebooted.  If so, the last_report_time
                    # will be seconds from boot time (until the device syncs
                    # time.  Check for this by checking if concentrations.pm_1
                    # is None.
                    if j['data']['conditions'][0]['pm_1'] is None:
                        log.info('last_report_time must be time since boot: %d seconds.  Record: %s'
                                 % (time_of_reading, j))
                    else:
                        # Not current.  (Note: Rarely, spurious timestamps (e.g., 2016 in 2020)
                        # have been observed.  Both the ts and last_report_time fields are incorrect.
                        # Example on Oct 10 21:11:38:
                        # {'data': {'did': '001D0A100214', 'name': 'airlink', 'ts': 1461926887,
                        # 'conditions': [{'lsid': 349506, 'data_structure_type': 6, 'temp': 67.7,
                        # 'hum': 72.2, 'dew_point': 58.4, 'wet_bulb': 61.2, 'heat_index': 68.1,
                        # 'pm_1_last': 0, 'pm_2p5_last': 0, 'pm_10_last': 0, 'pm_1': 0.0,
                        # 'pm_2p5': 0.0, 'pm_2p5_last_1_hour': 0.13, 'pm_2p5_last_3_hours': 0.27,
                        # 'pm_2p5_last_24_hours': 0.43, 'pm_2p5_nowcast': 0.23, 'pm_10': 1.09,
                        # 'pm_10_last_1_hour': 0.64, 'pm_10_last_3_hours': 0.89,
                        # 'pm_10_last_24_hours': 1.02, 'pm_10_nowcast': 0.84,
                        # 'last_report_time': 1461926886, 'pct_pm_data_last_1_hour': 100,
                        # 'pct_pm_data_last_3_hours': 100, 'pct_pm_data_nowcast': 100,
                        # 'pct_pm_data_last_24_hours': 100}]}, 'error': None}
                        log.info('Ignoring reading from %s--age: %d seconds.  Record: %s'
                                 % (hostname, age_of_reading, j))
                    j = None
        except Exception as e:
            log.info('collect_data: Attempt to fetch from: %s failed: %s.' % (hostname, e))
            j = None
    
    
        if j is None:
            return None
    
        # Create a Reading
        log.debug('Successful read from %s.' % hostname)
        return Service.populate_record(time_of_reading, j)
    
    @staticmethod
    def populate_record(ts, j) -> Reading:
        record = dict()
    
        # put items into record
        missed = []
    
        def get_and_update_missed_data(key):
            if key in j['data']:
                return j['data'][key]
            else:
                missed.append(key)
                return None
    
        def get_and_update_missed_data_conditions(key):
            if key in j['data']['conditions'][0]:
                return j['data']['conditions'][0][key]
            else:
                missed.append(key)
                return None

        record['did'] = get_and_update_missed_data('did')
        record['name'] = get_and_update_missed_data('name')
        record['ts'] = get_and_update_missed_data('ts')

        record['lsid'] = get_and_update_missed_data_conditions('lsid')
        record['data_structure_type'] = get_and_update_missed_data_conditions('data_structure_type')
        record['last_report_time'] = get_and_update_missed_data_conditions('last_report_time')

        record['temp'] = get_and_update_missed_data_conditions('temp')
        record['hum'] = get_and_update_missed_data_conditions('hum')
        record['dew_point'] = get_and_update_missed_data_conditions('dew_point')
        record['wet_bulb'] = get_and_update_missed_data_conditions('wet_bulb')
        record['heat_index'] = get_and_update_missed_data_conditions('heat_index')

        record['pct_pm_data_last_1_hour'] = get_and_update_missed_data_conditions('pct_pm_data_last_1_hour')
        record['pct_pm_data_last_3_hours'] = get_and_update_missed_data_conditions('pct_pm_data_last_3_hours')
        record['pct_pm_data_nowcast'] = get_and_update_missed_data_conditions('pct_pm_data_nowcast')
        record['pct_pm_data_last_24_hours'] = get_and_update_missed_data_conditions('pct_pm_data_last_24_hours')
    
        record['pm1_0'] = get_and_update_missed_data_conditions('pm_1_last')
        record['pm2_5'] = get_and_update_missed_data_conditions('pm_2p5_last')
        record['pm10_0'] = get_and_update_missed_data_conditions('pm_10_last')
    
        # Copy in all of the concentrations.
        record['pm_1'] = get_and_update_missed_data_conditions('pm_1')
        record['pm_1_last'] = get_and_update_missed_data_conditions('pm_1_last')
        for prefix in ['pm_2p5', 'pm_10']:
            key = prefix + '_last'
            record[key] = get_and_update_missed_data_conditions(key)
            key = prefix
            record[key] = get_and_update_missed_data_conditions(key)
            key = prefix + '_last_1_hour'
            record[key] = get_and_update_missed_data_conditions(key)
            key = prefix + '_last_3_hours'
            record[key] = get_and_update_missed_data_conditions(key)
            key = prefix + '_last_24_hours'
            record[key] = get_and_update_missed_data_conditions(key)
            key = prefix + '_nowcast'
            record[key] = get_and_update_missed_data_conditions(key)
    
        if missed:
            log.info("Sensor didn't report field(s): %s" % ','.join(missed))

        return Reading(
            did                       = record['did'],
            name                      = record['name'],
            ts                        = record['ts'],
            lsid                      = record['lsid'],
            data_structure_type       = record['data_structure_type'],
            temp                      = record['temp'],
            hum                       = record['hum'],
            dew_point                 = record['dew_point'],
            wet_bulb                  = record['wet_bulb'],
            heat_index                = record['heat_index'],
            pm_1_last                 = record['pm_1_last'],
            pm_2p5_last               = record['pm_2p5_last'],
            pm_10_last                = record['pm_10_last'],
            pm_1                      = record['pm_1'],
            pm_2p5                    = record['pm_2p5'],
            pm_2p5_last_1_hour        = record['pm_2p5_last_1_hour'],
            pm_2p5_last_3_hours       = record['pm_2p5_last_3_hours'],
            pm_2p5_last_24_hours      = record['pm_2p5_last_24_hours'],
            pm_2p5_nowcast            = record['pm_2p5_nowcast'],
            pm_10                     = record['pm_10'],
            pm_10_last_1_hour         = record['pm_10_last_1_hour'],
            pm_10_last_3_hours        = record['pm_10_last_3_hours'],
            pm_10_last_24_hours       = record['pm_10_last_24_hours'],
            pm_10_nowcast             = record['pm_10_nowcast'],
            last_report_time          = record['last_report_time'],
            pct_pm_data_last_1_hour   = record['pct_pm_data_last_1_hour'],
            pct_pm_data_last_3_hours  = record['pct_pm_data_last_3_hours'],
            pct_pm_data_nowcast       = record['pct_pm_data_nowcast'],
            pct_pm_data_last_24_hours = record['pct_pm_data_last_24_hours'])

    @staticmethod
    def datetime_display(ts: int) -> str:
        return "%s (%d)" % (time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(ts)), ts)

    @staticmethod
    def convert_to_json(reading: Reading) -> str:
        data: Dict[str, Any] = {
            'did'                      : reading.did,
            'name'                     : reading.name,
            'ts'                       : reading.ts}
        condition_0: Dict[str, Any] = {
            'lsid'                     : reading.lsid,
            'data_structure_type'      : reading.data_structure_type,
            'temp'                     : reading.temp,
            'hum'                      : reading.hum,
            'dew_point'                : reading.dew_point,
            'wet_bulb'                 : reading.wet_bulb,
            'heat_index'               : reading.heat_index,
            'pm_1_last'                : reading.pm_1_last,
            'pm_2p5_last'              : reading.pm_2p5_last,
            'pm_10_last'               : reading.pm_10_last,
            'pm_1'                     : reading.pm_1,
            'pm_2p5'                   : reading.pm_2p5,
            'pm_2p5_last_1_hour'       : reading.pm_2p5_last_1_hour,
            'pm_2p5_last_3_hours'      : reading.pm_2p5_last_3_hours,
            'pm_2p5_last_24_hours'     : reading.pm_2p5_last_24_hours,
            'pm_2p5_nowcast'           : reading.pm_2p5_nowcast,
            'pm_10'                    : reading.pm_10,
            'pm_10_last_1_hour'        : reading.pm_10_last_1_hour,
            'pm_10_last_3_hours'       : reading.pm_10_last_3_hours,
            'pm_10_last_24_hours'      : reading.pm_10_last_24_hours,
            'pm_10_nowcast'            : reading.pm_10_nowcast,
            'last_report_time'         : reading.last_report_time,
            'pct_pm_data_last_1_hour'  : reading.pct_pm_data_last_1_hour,
            'pct_pm_data_last_3_hours' : reading.pct_pm_data_last_3_hours,
            'pct_pm_data_nowcast'      : reading.pct_pm_data_nowcast,
            'pct_pm_data_last_24_hours': reading.pct_pm_data_last_24_hours}
        data['conditions'] = [ condition_0 ]
        reading_dict: Dict[str, Any] = { 'data': data, 'error': None }

        return dumps(reading_dict)

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(tz=tz.gettz('UTC'))

    @staticmethod
    def convert_data_structure_type_5_to_6(j: Dict[str, Any]) -> None:
        # Fix up these names and change data_structure_type to 6
        try:
            j['data']['conditions'][0]['pm_10'] = j['data']['conditions'][0]['pm_10p0']
            j['data']['conditions'][0]['pm_10p0'] = None
            j['data']['conditions'][0]['pm_10_last_1_hour'] = j['data']['conditions'][0]['pm_10p0_last_1_hour']
            j['data']['conditions'][0]['pm_10p0_last_1_hour'] = None
            j['data']['conditions'][0]['pm_10_last_3_hours'] = j['data']['conditions'][0]['pm_10p0_last_3_hours']
            j['data']['conditions'][0]['pm_10p0_last_3_hours'] = None
            j['data']['conditions'][0]['pm_10_last_24_hours'] = j['data']['conditions'][0]['pm_10p0_last_24_hours']
            j['data']['conditions'][0]['pm_10p0_last_24_hours'] = None
            j['data']['conditions'][0]['pm_10_nowcast'] = j['data']['conditions'][0]['pm_10p0_nowcast']
            j['data']['conditions'][0]['pm_10p0_nowcast'] = None
    
            j['data']['conditions'][0]['data_structure_type'] = 6
            log.debug('Converted type 5 record to type 6.')
        except Exception as e:
            log.info('convert_data_structure_type_5_to_6: exception: %s' % e)
            # Let sanity check handle the issue.

    @staticmethod
    def is_type(j: Dict[str, Any], t, name: str, none_ok: bool = False) -> bool:
        try:
            x = j[name]
            if x is None and none_ok:
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
    def is_sane(j: Dict[str, Any]) -> Tuple[bool, str]:
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
            return False, 'Expected data_structure_type of 6 (or type 5 auto converted to 6.'
    
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

    def compute_next_event(self, first_time: bool) -> Tuple[Event, float]:
        now = time.time()
        next_poll_event = int(now / self.pollfreq_secs) * self.pollfreq_secs + self.pollfreq_secs
        log.debug('next_poll_event: %f' % next_poll_event)
        next_arc_event = int((now - self.pollfreq_offset) / self.arcint_secs) * self.arcint_secs + self.arcint_secs + self.pollfreq_offset
        log.debug('next_arc_event: %f' % next_arc_event)
        event = Event.ARCHIVE if next_poll_event == next_arc_event else Event.POLL
        secs_to_event = next_poll_event - now
        log.debug('Next event: %r in %f seconds' % (event, secs_to_event))
        return event, secs_to_event

    def do_loop(self) -> None:
        first_time: bool = True
        log.debug('Started main loop.')
        session: Optional[requests.Session] = None
        while True:
            # sleep until next event
            event, secs_to_event = self.compute_next_event(first_time)
            first_time = False
            sleep(secs_to_event)

            # Write a reading and possibly write an archive record.
            try:
                start = Service.utc_now()
                if session is None:
                    session= requests.Session()

                reading: Optional[Reading] = Service.collect_data(session, self.hostname, self.port, self.timeout_secs)
                log.debug('Read sensor in %d seconds.' % (Service.utc_now() - start).seconds)
            except Exception as e:
                log.error('Skipping reading because of: %s' % e)
                # It's probably a good idea to reset the session
                try:
                    if session is not None:
                        session.close()
                except Exception as e:
                    log.info('Non-fatal: calling session.close(): %s' % e)
                finally:
                    session = None
                if reading is None and event == event.ARCHIVE:
                    log.error('Skipping archive record.')

            # May or may not have a new reading.
            if reading is not None:
                if event == event.ARCHIVE:
                    # The plus five seconds is to guard against this routine
                    # running a few seconds early.
                    archive_ts = int((time.time() + 5) / self.arcint_secs) * self.arcint_secs
                try:
                    start = Service.utc_now()
                    self.database.save_current_reading(reading)
                    log.debug('Saved current reading in %d seconds.' % (Service.utc_now() - start).seconds)
                    log.debug('Saved current reading %s to database.' % Service.datetime_display(reading.last_report_time))
                except Exception as e:
                    log.critical('Could not save current reading to database: %s: %s' % (self.database, e))
                if event == event.ARCHIVE:
                    try:
                        start = Service.utc_now()
                        self.database.save_archive_reading(archive_ts, reading)
                        log.debug('Saved archive reading in %d seconds.' % (Service.utc_now() - start).seconds)
                        log.info('Added record %s to archive.' % Service.datetime_display(archive_ts))
                    except Exception as e:
                        log.critical('Could not save archive reading to database: %s: %s' % (self.database, e))

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_passed() -> None:
    print(bcolors.OKGREEN + 'PASSED' + bcolors.ENDC)

def print_failed(e: Exception) -> None:
    print(bcolors.FAIL + 'FAILED' + bcolors.ENDC)
    print(traceback.format_exc())

def collect_two_readings_one_second_apart(hostname: str, port: int, timeout_secs:int) -> Tuple[Optional[Reading], Optional[Reading]]:
    try:
        session: requests.Session = requests.Session()
        print('collect_two_readings_one_seconds_apart...', end='')
        reading1: Optional[Reading] = Service.collect_data(session, hostname, port, timeout_secs)
        sleep(1) # to get a different time (to the second) on reading2
        reading2: Optional[Reading] = Service.collect_data(session, hostname, port, timeout_secs)
        print_passed()
        return reading1, reading2
    except Exception as e:
        print_failed(e)
        raise e

def run_tests(service_name: str, hostname: str, port: int, timeout_secs: int) -> None:
    reading, reading2 = collect_two_readings_one_second_apart(hostname, port, timeout_secs)
    test_db_archive_records(service_name, reading)
    test_db_current_records(service_name, reading, reading2)
    sanity_check_reading(reading)
    test_convert_to_json(reading, reading2)

def sanity_check_reading(reading: Optional[Reading]) -> None:
    try:
        print('sanity_check_reading....', end='')
        now: float = int(time.time())
        one_minute_ago: int = int(time.time() - 60)

        assert reading is not None
        assert reading.did is not None
        assert reading.name is not None
        assert reading.ts > one_minute_ago and reading.ts < now, 'Reading returned insane time (%r).' % reading.ts
        assert reading.temp >= 0 and reading.temp <= 100, 'Reading returned insane temperature (%d).' % reading.temp
        assert reading.hum >= 0 and reading.hum <= 100, 'Reading returned insane humidity (%d).' % reading.hum
        assert reading.last_report_time > one_minute_ago and reading.last_report_time < now, 'Reading returned insane time (%r).' % reading.last_report_time
        assert reading.pm_1 >= 0.0 and reading.pm_1 < 10000.0, 'Reading returned insane pm_1: %f' % reading.pm_1
        assert reading.pm_2p5 >= 0.0 and reading.pm_2p5 < 10000.0, 'Reading returned insane pm_2p5: %f' % reading.pm_2p5
        assert reading.pm_10 >= 0.0 and reading.pm_10 < 10000.0, 'Reading returned insane pm_10: %f' % reading.pm_10

        print_passed()
    except Exception as e:
        print_failed(e)

def create_test_reading(dt: datetime) -> Reading:
    return Reading(
        did                       = 'abc',
        name                      = 'foo',
        ts                        = int(datetime.timestamp(dt)),
        lsid                      = 123,
        data_structure_type       = 6,
        temp                      = 100,
        hum                       = 90,
        dew_point                 = 85,
        wet_bulb                  = 66,
        heat_index                = 99,
        pm_1_last                 = 1,
        pm_2p5_last               = 3,
        pm_10_last                = 10,
        pm_1                      = 1.1,
        pm_2p5                    = 2.5,
        pm_2p5_last_1_hour        = 2.1,
        pm_2p5_last_3_hours       = 2.3,
        pm_2p5_last_24_hours      = 2.24,
        pm_2p5_nowcast            = 2.22,
        pm_10                     = 10.0,
        pm_10_last_1_hour         = 10.1,
        pm_10_last_3_hours        = 10.3,
        pm_10_last_24_hours       = 10.24,
        pm_10_nowcast             = 10.1010,
        last_report_time          = int(datetime.timestamp(dt)),
        pct_pm_data_last_1_hour   = 94,
        pct_pm_data_last_3_hours  = 97,
        pct_pm_data_nowcast       = 100,
        pct_pm_data_last_24_hours = 76)

def float_eq(v1: float, v2: float) -> bool:
    return abs(v1 - v2) < 0.0001

def test_db_archive_records(service_name: str, reading_in: Optional[Reading]) -> None:
    try:
        print('test_db_archive_records....', end='')
        assert reading_in is not None
        tmp_db = tempfile.NamedTemporaryFile(
            prefix='tmp-test-db-archive-%s.sdb' % service_name, delete=False)
        tmp_db.close()
        os.unlink(tmp_db.name)
        db = Database.create(tmp_db.name)
        db.save_archive_reading((reading_in.ts + 60) / 60 * 60, reading_in)
        cnt = 0
        for reading_out in db.fetch_archive_readings(0):
            #print(reading_out)
            if reading_in != reading_out:
                print('test_db_archive_records failed: in: %r, out: %r' % (reading_in, reading_out))
            cnt += 1
        if cnt != 1:
            print('test_db_archive_records failed with count: %d' % cnt)
        print_passed()
    except Exception as e:
        print('test_db_archive_records failed: %s' % e)
        raise e
    finally:
        os.unlink(tmp_db.name)

def test_db_current_records(service_name: str, reading_in_1: Optional[Reading], reading_in_2: Optional[Reading]) -> None:
    try:
        print('test_db_current_records....', end='')
        tmp_db = tempfile.NamedTemporaryFile(
            prefix='tmp-test-db-current-%s.sdb' % service_name, delete=False)
        tmp_db.close()
        os.unlink(tmp_db.name)
        db = Database.create(tmp_db.name)
        db.save_current_reading(reading_in_1)
        db.save_current_reading(reading_in_2)
        cnt = 0
        for reading_out in db.fetch_current_readings():
            #print(reading_out)
            if reading_in_2 != reading_out:
                print('test_db_current_records failed: in: %r, out: %r' % (reading_in_2, reading_out))
            cnt += 1
        if cnt != 1:
            print('test_db_current_records failed with count: %d' % cnt)
        print_passed()
    except Exception as e:
        print('test_db_current_records failed: %s' % e)
        raise e
    finally:
        os.unlink(tmp_db.name)

def test_convert_to_json(reading1: Optional[Reading], reading2: Optional[Reading]) -> None:
    try:
        print('test_convert_to_json....', end='')

        assert reading1 is not None
        assert reading2 is not None
        Service.convert_to_json(reading1)
        Service.convert_to_json(reading2)

        tzinfos = {'CST': tz.gettz("UTC")}
        reading = create_test_reading(parse('2019/12/15T03:43:05UTC', tzinfos=tzinfos))
        json_reading: str = Service.convert_to_json(reading)

        expected = '{"data": {"did": "abc", "name": "foo", "ts": 1576381385, "conditions": [{"lsid": 123, "data_structure_type": 6, "temp": 100, "hum": 90, "dew_point": 85, "wet_bulb": 66, "heat_index": 99, "pm_1_last": 1, "pm_2p5_last": 3, "pm_10_last": 10, "pm_1": 1.1, "pm_2p5": 2.5, "pm_2p5_last_1_hour": 2.1, "pm_2p5_last_3_hours": 2.3, "pm_2p5_last_24_hours": 2.24, "pm_2p5_nowcast": 2.22, "pm_10": 10.0, "pm_10_last_1_hour": 10.1, "pm_10_last_3_hours": 10.3, "pm_10_last_24_hours": 10.24, "pm_10_nowcast": 10.101, "last_report_time": 1576381385, "pct_pm_data_last_1_hour": 94, "pct_pm_data_last_3_hours": 97, "pct_pm_data_nowcast": 100, "pct_pm_data_last_24_hours": 76}]}, "error": null}'

        assert json_reading == expected, 'Expected json: %s, found: %s' % (expected, json_reading)
        print_passed()
    except Exception as e:
        print_failed(e)

def dump_database(db_file: str) -> None:
    start = Service.utc_now()
    database: Database = Database(db_file)
    print('----------------------------')
    print('* Dumping current reading  *')
    print('----------------------------')
    for reading in database.fetch_current_readings():
        print(reading)
        print('---')
    print('----------------------------')
    print('* Dumping archive readings *')
    print('----------------------------')
    for reading in database.fetch_archive_readings():
        print(reading)
        print('---')
    print('Dumped database in %d seconds.' % (Service.utc_now() - start).seconds)

class UnexpectedSensorRecord(Exception):
    pass

class CantOpenConfigFile(Exception):
    pass

class CantParseConfigFile(Exception):
    pass

def get_configuration(config_file):
    try:
        config_dict = configobj.ConfigObj(config_file, file_error=True, encoding='utf-8')
    except IOError:
        raise CantOpenConfigFile("Unable to open configuration file %s" % config_file)
    except configobj.ConfigObjError:
        raise CantParseConfigFile("Error parsing configuration file %s", config_file)

    return config_dict

def start(args):
    usage = """%prog [--help] [--test | --dump] [--pidfile <pidfile>] <airlinkproxy-conf-file>"""
    parser: str = optparse.OptionParser(usage=usage)

    parser.add_option('-p', '--pidfile', dest='pidfile', action='store',
                      type=str, default=None,
                      help='When running as a daemon, pidfile in which to write pid.  Default is None.')
    parser.add_option('-t', '--test', dest='test', action='store_true', default=False,
                      help='Run tests and then exit. Default is False')
    parser.add_option('-d', '--dump', dest='dump', action='store_true', default=False,
                      help='Dump database and then exit. Default is False')

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error('Usage: [--pidfile <pidfile>] [--test | --dump] <airlinkproxy-conf-file>')

    conf_file: str = os.path.abspath(args[0])
    config_dict    = get_configuration(conf_file)

    debug          : bool           = int(config_dict.get('debug', 0))
    log_to_stdout  : bool           = int(config_dict.get('log-to-stdout', 0))
    service_name   : str            = config_dict.get('service-name', 'airlink-proxy')
    hostname       : Optional[str]  = config_dict.get('hostname', None)
    port           : int            = int(config_dict.get('port', 80))
    server_port    : int            = int(config_dict.get('server-port', 8000))
    timeout_secs   : int            = int(config_dict.get('timeout-secs', 15))
    pollfreq_secs  : int            = int(config_dict.get('poll-freq-secs', 5))
    pollfreq_offset: int            = int(config_dict.get('poll-freq-offset', 5))
    arcint_secs    : int            = int(config_dict.get('archive-interval-secs', 60))
    db_file        : Optional[str]  = config_dict.get('database-file', None)

    global log
    log = Logger(service_name, log_to_stdout=log_to_stdout, debug_mode=debug)

    log.info('debug          : %r'    % debug)
    log.info('log_to_stdout  : %r'    % log_to_stdout)
    log.info('conf_file      : %s'    % conf_file)
    log.info('Version        : %s'    % AIRLINK_PROXY_VERSION)
    log.info('host:port      : %s:%s' % (hostname, port))
    log.info('server_port    : %s'    % server_port)
    log.info('timeout_secs   : %d'    % timeout_secs)
    log.info('pollfreq_secs  : %d'    % pollfreq_secs)
    log.info('pollfreq_offset: %d'    % pollfreq_offset)
    log.info('arcint_secs    : %d'    % arcint_secs)
    log.info('db_file        : %s'    % db_file)
    log.info('service_name   : %s'    % service_name)
    log.info('pidfile        : %s'    % options.pidfile)

    if options.test and options.dump:
        parser.error('At most one of --test and --dump can be specified.')

    if options.test is True:
        if not hostname:
            parser.error('hostname must be specified in the config file')
        run_tests(service_name, hostname, port, timeout_secs)
        sys.exit(0)

    if options.dump is True:
        if not db_file:
            parser.error('database-file must be specified in the config file')
        dump_database(db_file)
        sys.exit(0)

    if not hostname:
        parser.error('hostname must be specified in the config file')

    if not db_file:
        parser.error('database-file must be specified in the config file')

    # arcint must be a multilpe of pollfreq
    if arcint_secs % pollfreq_secs != 0:
        parser.error('archive-interval-secs must be a multiple of poll-frequency-secs')

    if options.pidfile is not None:
        pid: str = str(os.getpid())
        with open(options.pidfile, 'w') as f:
            f.write(pid+'\n')
            os.fsync(f)

    # Create database if it does not yet exist.
    if not os.path.exists(db_file):
        log.debug('Creating database: %s' % db_file)
        database: Database = Database.create(db_file)
    else:
        database: Database = Database(db_file)

    airlinkproxy_service = Service(hostname, port, timeout_secs, database)

    log.debug('Staring server on port %d.' % server_port)
    server.server.serve_requests(server_port, db_file)

    log.debug('Staring mainloop.')
    airlinkproxy_service.do_loop()
