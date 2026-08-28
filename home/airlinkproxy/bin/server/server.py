#!/usr/bin/python3

# Copyright (c) 2020 John A Kline
# See the file LICENSE for your full rights.

import http.server
import optparse
import os
import socket
import sys
import threading

from enum import Enum
from dataclasses import dataclass
from json import dumps
from typing import Dict, List, Optional

from monitor import log
from monitor.database import Database
from monitor.model import error_json

# The API command-set version, NOT the program version.  Bumped to '2' when
# /fetch-two-minute-record was added and archive records became averages over
# the interval rather than the single poll that landed on the boundary; a
# client can use /get-version to tell the two contracts apart.
VERSION = '2'

class RequestType(Enum):
    ERROR                   = 0
    GET_VERSION             = 1
    GET_EARLIEST_TIMESTAMP  = 2
    FETCH_CURRENT_RECORD    = 3
    FETCH_ARCHIVE_RECORDS   = 4
    FETCH_TWO_MINUTE_RECORD = 5
    GET_ARCHIVE_INTERVAL    = 6

@dataclass
class Request:
    request_type: RequestType
    since_ts    : Optional[int]
    max_ts      : Optional[int]
    limit       : Optional[int]
    error       : Optional[str]
    request     : str

class Handler(http.server.BaseHTTPRequestHandler):
    """Handle requests in a separate thread."""

    def log_message(self, format: str, *args) -> None:
        """Silence the per-request access line BaseHTTPRequestHandler writes to
        stderr.  Requests are logged by do_GET at info level instead, which is
        what the logwatch classifier counts; letting the default through would
        put a duplicate in the journal under programname python3, outside the
        reach of the airlink-proxy rsyslog rule."""
        log.debug('%s - %s' % (self.address_string(), format % args))

    def do_GET(self):
        try:
            self.dispatch(Handler.parse_requestline(self.requestline))
        except Exception as e:
            # Without this, an exception (sqlite3.OperationalError: database is
            # locked, say) unwinds into socketserver, which prints a traceback
            # to stderr and closes the socket: the client gets an empty reply
            # instead of the documented error envelope, and the traceback lands
            # in the journal under programname python3, where neither the
            # rsyslog rule nor the logwatch classifier can see it.
            log.critical('request_error: %s failed: %s' % (self.requestline, e))
            try:
                self.respond_error('Request failed: %s' % e, 500)
            except Exception as e2:
                log.info('Non-fatal: could not send error response: %s' % e2)

    def dispatch(self, request: Request) -> None:
        # serve_requests sets db_file before the server thread starts, so this
        # cannot fire; raising rather than asserting means do_GET turns it into
        # a proper error response instead of an empty reply.
        if db_file is None:
            raise RuntimeError('server started without a database file')
        if request.request_type == RequestType.GET_VERSION:
            log.info('get-version: %s' % VERSION)
            self.respond_success(dumps({'version': VERSION}))
        elif request.request_type == RequestType.GET_EARLIEST_TIMESTAMP:
            log.info('get-earliest-timestamp')
            self.respond_success(Database(db_file).get_earliest_timestamp_as_json())
        elif request.request_type == RequestType.FETCH_CURRENT_RECORD:
            log.info('fetch-current-record')
            self.respond_success(Database(db_file).fetch_current_reading_as_json())
        elif request.request_type == RequestType.GET_ARCHIVE_INTERVAL:
            log.info('get-archive-interval-secs: %r' % archive_interval_secs)
            self.respond_success(dumps({'archive-interval-secs': archive_interval_secs}))
        elif request.request_type == RequestType.FETCH_TWO_MINUTE_RECORD:
            log.info('fetch-two-minute-record')
            self.respond_success(Database(db_file).fetch_two_minute_reading_as_json())
        elif request.request_type == RequestType.FETCH_ARCHIVE_RECORDS:
            log.info('fetch-archive-records: since_ts: %r, max_ts: %r, limit: %r'
                     % (request.since_ts, request.max_ts, request.limit))
            # parse_requestline makes a missing since_ts an ERROR, so it is set
            # here.  Raise rather than defaulting to 0, which would silently
            # return the entire history.
            if request.since_ts is None:
                raise RuntimeError('fetch-archive-records reached dispatch without since_ts')
            self.respond_success(Database(db_file).fetch_archive_readings_as_json(
                request.since_ts, request.max_ts, request.limit))
        else:
            log.info('request_error: %s' % request.error)
            self.respond_error(request.error)

    def respond_success(self, json: str) -> None:
        self.send_response(200)
        self.send_header('Accept', 'application/json')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.encode('ascii'))

    def respond_error(self, error: Optional[str], code: int = 400):
        """Errors use the device's own envelope: a null data member and an
        error object carrying a code and a message.  The status stays 200, as
        it always has, so check the error member rather than the status.

        The code distinguishes a bad request (400) from a failure inside the
        proxy (500), so a client can tell "you asked wrong, do not retry" from
        "I could not answer just now, try again".

        Built with dumps rather than interpolated: the message quotes the
        client's own request text, which would otherwise break the json (or
        inject into it) the moment it contained a quote or a backslash."""
        self.send_response(200)
        self.send_header('Accept', 'application/json')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(error_json(error if error is not None else 'Request failed.', code)
                         .encode('ascii'))

    @staticmethod
    def parse_args(args_in: str) -> Dict[str, str]:
        args_dict: Dict[str, str] = {}
        args: List[str] = args_in.split(',')
        for arg in args:
            if '=' in arg:
                key_value: List[str] = arg.split('=')
                if key_value[0] != '':
                    args_dict[key_value[0]] = key_value[1]
        return args_dict

    @staticmethod
    def parse_requestline(requestline: str) -> Request:
        request: str = requestline.split(' ')[1]
        args: str = ''
        if '?' in request:
            cmd = request.split('?')[0]
            args = request.split('?')[1]
        else:
            cmd = request
        request_type = RequestType.ERROR
        since_ts: Optional[int] = None
        max_ts: Optional[int] = None
        limit: Optional[int] = None
        error: Optional[str] = None
        if cmd == '/get-version':
            request_type = RequestType.GET_VERSION
        elif cmd == '/get-earliest-timestamp':
            request_type = RequestType.GET_EARLIEST_TIMESTAMP
        elif cmd == '/v1/current_conditions':
            request_type = RequestType.FETCH_CURRENT_RECORD
        elif cmd == '/fetch-archive-records':
            request_type = RequestType.FETCH_ARCHIVE_RECORDS
        elif cmd == '/fetch-two-minute-record':
            request_type = RequestType.FETCH_TWO_MINUTE_RECORD
        elif cmd == '/get-archive-interval-secs':
            request_type = RequestType.GET_ARCHIVE_INTERVAL
        elif cmd == '/':
            error = 'A command must be specified.'
        else:
            error = 'Unknown command: %s.' % cmd
        if request_type != RequestType.ERROR:
            args_dict: Dict[str, str] = Handler.parse_args(args)
            if request_type == RequestType.FETCH_ARCHIVE_RECORDS:
                if 'since_ts' in args_dict:
                    try:
                        since_ts = int(args_dict['since_ts'])
                    except Exception:
                        request_type = RequestType.ERROR
                        error =  "The since_ts argument must be an integer, found: '%s'." % args_dict['since_ts']
                    if 'max_ts' in args_dict:
                        try:
                            max_ts = int(args_dict['max_ts'])
                        except Exception:
                            request_type = RequestType.ERROR
                            error =  "The max_ts argument must be an integer, found: '%s'." % args_dict['max_ts']
                    if 'limit' in args_dict:
                        try:
                            limit = int(args_dict['limit'])
                        except Exception:
                            request_type = RequestType.ERROR
                            error =  "The limit argument must be an integer, found: '%s'." % args_dict['limit']
                else:
                    request_type = RequestType.ERROR
                    error =  'fetch-archive-records requires since_ts argument'
        return Request(
            request_type = request_type,
            since_ts     = since_ts,
            max_ts       = max_ts,
            limit        = limit,
            error        = error,
            request      = request)

db_file: Optional[str] = None
# The daemon's archive-interval-secs, served on /get-archive-interval-secs so a
# client (weewx-airlink's catch-up, for one) can see whether the proxy's archive
# cadence matches its own rather than having to infer it.
archive_interval_secs: Optional[int] = None

def start_server(port: int):
    class ThreadingHTTPServer6(http.server.ThreadingHTTPServer):
        address_family = socket.AF_INET6
    try:
        with ThreadingHTTPServer6(('::', port), Handler) as server:
            server.serve_forever()
    except Exception as e:
        # This runs on a daemon thread: without this, the thread dies quietly
        # and the daemon keeps polling with nothing listening, so systemd sees
        # a healthy service while every client gets connection-refused.  Take
        # the whole process down so Restart=on-failure applies.
        log.critical('REST server on port %d died: %s.  Exiting.' % (port, e))
        # os._exit skips stdio flushing, which would swallow the message above
        # when logging to stdout.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)

def serve_requests(port: int, db_file_in: str, arcint_secs: Optional[int] = None):
    global db_file
    global archive_interval_secs
    db_file = db_file_in
    archive_interval_secs = arcint_secs
    daemon = threading.Thread(name='airlinkproxy_daemon_server',
                              target=start_server,
                              args=[port])
    daemon.daemon = True # Set as a daemon so it will be killed once the main thread is dead.
    daemon.start()

if __name__ == '__main__':
    def main():
        # Run from the bin directory as:
        #   python3 -m server.server --db-file <db-file> --port <port>
        # (running server/server.py directly leaves bin off sys.path, so
        # "from monitor import log" fails before this point is reached).
        usage = """python3 -m server.server [--help] --db-file <db-file> --port <port>"""

        parser: optparse.OptionParser = optparse.OptionParser(usage=usage)
        parser.add_option('--db-file', dest='db_file', action='store',
                          type=str, default=None,
                          help='The database file from which to serve readings.  --db-file must be specified.')
        parser.add_option("--port", dest="port", type=int, default=None,
                          help="The port on which to serve.  --port must be specified.")

        (options, args) = parser.parse_args()

        if options.db_file is None:
            parser.error('db-file must be specified.')

        if options.port is None:
            parser.error('port must be specified.')

        log.reconfigure('server.py', log_to_stdout=True)
        serve_requests(options.port, options.db_file)
        print('Hit return to exit...', end='')
        _ = input()

    main()
