#!/usr/bin/python3

# Copyright (c) 2020-2026 John A Kline
# See the file LICENSE for your full rights.

"""The sqlite database: one Reading table holding the current reading
(record_type 0, a single row that is replaced on every poll), the archive
history (record_type 1, one row per archive interval, averaged over the
interval) and the two-minute average (record_type 2, another single row
replaced on every poll).  CURRENT and TWO_MINUTE are single-row record types
(delete+insert); only ARCHIVE accumulates.

All SQL is parameterized.  Values were previously interpolated into the
statement text, which quoted the device-supplied name field by hand and
rounded every float to six decimal places on its way into the database.
"""

import os
import sqlite3

from json import dumps
from typing import Any, Dict, Iterator, List, Optional

from monitor import log
from monitor.model import (READING_FIELDS, Reading, RecordType, error_json,
                           reading_to_json)

# The sqlite type of each stored field, for CREATE TABLE.  The names and types
# match what earlier versions created, so an existing database needs no
# migration.
FIELD_TYPES: Dict[str, str] = {
    'did'                      : 'STRING',
    'name'                     : 'STRING',
    'ts'                       : 'INTEGER',
    'lsid'                     : 'INTEGER',
    'data_structure_type'      : 'INTEGER',
    'temp'                     : 'FLOAT',
    'hum'                      : 'FLOAT',
    'dew_point'                : 'FLOAT',
    'wet_bulb'                 : 'FLOAT',
    'heat_index'               : 'FLOAT',
    'pm_1_last'                : 'INTEGER',
    'pm_2p5_last'              : 'INTEGER',
    'pm_10_last'               : 'INTEGER',
    'pm_1'                     : 'FLOAT',
    'pm_2p5'                   : 'FLOAT',
    'pm_2p5_last_1_hour'       : 'FLOAT',
    'pm_2p5_last_3_hours'      : 'FLOAT',
    'pm_2p5_last_24_hours'     : 'FLOAT',
    'pm_2p5_nowcast'           : 'FLOAT',
    'pm_10'                    : 'FLOAT',
    'pm_10_last_1_hour'        : 'FLOAT',
    'pm_10_last_3_hours'       : 'FLOAT',
    'pm_10_last_24_hours'      : 'FLOAT',
    'pm_10_nowcast'            : 'FLOAT',
    'last_report_time'         : 'INTEGER',
    'pct_pm_data_last_1_hour'  : 'INTEGER',
    'pct_pm_data_last_3_hours' : 'INTEGER',
    'pct_pm_data_nowcast'      : 'INTEGER',
    'pct_pm_data_last_24_hours': 'INTEGER',
}

class DatabaseAlreadyExists(Exception):
    pass

class Database(object):
    def __init__(self, db_file: str):
        self.db_file = db_file

    def __repr__(self) -> str:
        return 'Database(%s)' % self.db_file

    @staticmethod
    def create(db_file: str) -> 'Database':
        if db_file != ':memory:' and os.path.exists(db_file):
            raise DatabaseAlreadyExists('Database %s already exists' % db_file)
        if db_file != ':memory:':
            # Create parent directories
            directory = os.path.dirname(db_file)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)

        columns: str = ''.join(
            ' %-25s %s,' % (field, FIELD_TYPES[field]) for field in READING_FIELDS)
        create_reading_table: str = (
            'CREATE TABLE Reading ('
            ' record_type               INTEGER NOT NULL,'
            ' timestamp                 INTEGER NOT NULL,'
            '%s'
            ' PRIMARY KEY (record_type, timestamp));' % columns)

        with sqlite3.connect(db_file, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute(create_reading_table)
            cursor.close()

        return Database(db_file)

    def save_current_reading(self, r: Reading) -> None:
        self.save_poll_readings(r, None)

    def save_archive_reading(self, timestamp: int, r: Reading) -> None:
        self.save_reading(RecordType.ARCHIVE, timestamp, r)

    def save_two_minute_reading(self, r: Reading) -> None:
        self.save_poll_readings(None, r)

    @staticmethod
    def _insert_sql() -> str:
        columns: List[str] = ['record_type', 'timestamp'] + READING_FIELDS
        return 'INSERT INTO Reading (%s) VALUES(%s);' % (
            ', '.join(columns), ', '.join('?' * len(columns)))

    @staticmethod
    def _insert_values(record_type: int, timestamp: Optional[int], r: Reading) -> List[Any]:
        return [record_type, timestamp] + [getattr(r, field) for field in READING_FIELDS]

    def save_poll_readings(self, current: Optional[Reading],
                           two_minute: Optional[Reading]) -> None:
        """Write the current reading and the two-minute average in ONE
        transaction.  Each poll writes both, so doing them separately meant two
        commits every poll-freq-secs -- double the flash wear on the SD cards
        this runs on, and double the window in which a REST reader can hit
        'database is locked'.  Either may be None, in which case that record is
        left alone."""
        if current is None and two_minute is None:
            return
        insert: str = Database._insert_sql()
        with sqlite3.connect(self.db_file, timeout=15) as conn:
            cursor = conn.cursor()
            for record_type, reading, timestamp in (
                    (RecordType.CURRENT, current, current.ts if current else None),
                    (RecordType.TWO_MINUTE, two_minute, two_minute.ts if two_minute else None)):
                if reading is None:
                    continue
                cursor.execute('DELETE FROM Reading WHERE record_type = ?;', (record_type,))
                values = Database._insert_values(record_type, timestamp, reading)
                log.debug('inserting record: %s %r' % (insert, values))
                cursor.execute(insert, values)
            cursor.close()

    def delete_two_minute_reading(self) -> None:
        """Drop the two-minute row.  Called when the two-minute window empties
        -- otherwise the last average computed would sit there indefinitely and
        be served as though it were current."""
        with sqlite3.connect(self.db_file, timeout=15) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM Reading WHERE record_type = ?;',
                           (RecordType.TWO_MINUTE,))
            cursor.close()

    def save_reading(self, record_type: int, timestamp: Optional[int], r: Reading) -> None:
        # Only ARCHIVE reaches here; the single-row record types go through
        # save_poll_readings, which owns the delete-then-insert rule.  The
        # statement and its values come from the same two helpers that path
        # uses, so the column list and the placeholders cannot drift apart.
        insert: str = Database._insert_sql()
        values: List[Any] = Database._insert_values(record_type, timestamp, r)
        with sqlite3.connect(self.db_file, timeout=15) as conn:
            cursor = conn.cursor()
            log.debug('inserting record: %s %r' % (insert, values))
            cursor.execute(insert, values)
            cursor.close()

    def fetch_current_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.CURRENT, 0)

    def fetch_current_reading_as_json(self) -> str:
        for reading in self.fetch_current_readings():
            return reading_to_json(reading)
        # No reading yet: a fresh install, or a proxy that has never had a
        # successful poll.  This path stands in for the device's own URL, so
        # answer in the device's error shape rather than an empty object a
        # client would index into and raise on.
        # 500, not the default 400: nothing is wrong with the request and the
        # next successful poll answers it.  400 tells a client not to retry.
        return error_json('No current reading yet.', 500)

    def fetch_two_minute_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.TWO_MINUTE, 0)

    def fetch_two_minute_reading_as_json(self) -> str:
        for reading in self.fetch_two_minute_readings():
            return reading_to_json(reading)
        return error_json('No two-minute average available.', 500)

    def get_earliest_timestamp_as_json(self) -> str:
        select: str = ('SELECT timestamp FROM Reading WHERE record_type = ?'
                       ' ORDER BY timestamp LIMIT 1')
        log.debug('get-earliest-timestamp: select: %s' % select)
        resp: Dict[str, Any] = {}
        with sqlite3.connect(self.db_file, timeout=10) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select, (RecordType.ARCHIVE,)):
                log.debug('get-earliest-timestamp: returned %s' % row[0])
                resp['timestamp'] = row[0]
                break
            cursor.close()
        log.debug('get-earliest-timestamp: returning: %s' % dumps(resp))
        return dumps(resp)

    def fetch_archive_readings(self, since_ts: int = 0, max_ts: Optional[int] = None,
                               limit: Optional[int] = None) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.ARCHIVE, since_ts, max_ts, limit)

    def fetch_archive_readings_as_json(self, since_ts: int = 0, max_ts: Optional[int] = None,
                                       limit: Optional[int] = None) -> str:
        contents: str = ''
        for reading in self.fetch_archive_readings(since_ts, max_ts, limit):
            if contents != '':
                contents += ','
            contents += reading_to_json(reading)
        return '[  %s ]' % contents

    def fetch_readings(self, record_type: int, since_ts: int = 0, max_ts: Optional[int] = None,
                       limit: Optional[int] = None) -> Iterator[Reading]:
        select: str = 'SELECT %s FROM Reading WHERE record_type = ? AND timestamp > ?' % (
            ', '.join(READING_FIELDS))
        args: List[Any] = [record_type, since_ts]
        if max_ts is not None:
            select += ' AND timestamp <= ?'
            args.append(max_ts)
        select += ' ORDER BY timestamp'
        if limit is not None:
            select += ' LIMIT ?'
            args.append(limit)
        select += ';'
        log.debug('fetch_readings: select: %s %r' % (select, args))
        with sqlite3.connect(self.db_file, timeout=10) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select, args):
                yield Database.create_reading_from_row(row)
            cursor.close()

    @staticmethod
    def create_reading_from_row(row) -> Reading:
        return Reading(**dict(zip(READING_FIELDS, row)))
