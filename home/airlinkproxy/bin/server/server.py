#!/usr/bin/python3

# Copyright (c) 2020 John A Kline
# See the file LICENSE for your full rights.

import http.server
import threading

import monitor.monitor

from enum import Enum
from dataclasses import dataclass
from json import dumps
from typing import Dict, List, Optional

VERSION = '1'

class RequestType(Enum):
    ERROR                  = 0
    GET_VERSION            = 1
    GET_EARLIEST_TIMESTAMP = 2
    FETCH_CURRENT_RECORD   = 3
    FETCH_ARCHIVE_RECORDS  = 4

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
    def do_GET(self):
        request =  Handler.parse_requestline(self.requestline)
        if request.request_type == RequestType.GET_VERSION:
            self.respond_success(dumps({'version': VERSION}))
        elif request.request_type == RequestType.GET_EARLIEST_TIMESTAMP:
            self.respond_success(monitor.monitor.Database(db_file).get_earliest_timestamp_as_json())
        elif request.request_type == RequestType.FETCH_CURRENT_RECORD:
            self.respond_success(monitor.monitor.Database(db_file).fetch_current_reading_as_json())
        elif request.request_type == RequestType.FETCH_ARCHIVE_RECORDS:
            self.respond_success(monitor.monitor.Database(db_file).fetch_archive_readings_as_json(request.since_ts, request.max_ts, request.limit))
        else:
            self.respond_error(request.error)

    def respond_success(self, json: str) -> None:
        self.send_response(200)
        self.send_header('Accept', 'application/json')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.encode('ascii'))

    def respond_error(self, error: str):
        self.send_response(200)
        self.send_header('Accept', 'application/json')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(('{ error : "%s" }' % error).encode('ascii'))

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
        elif cmd == '/':
            error = 'A command must be specified.'
        else:
            error = 'Unknown command: %s.' % cmd
        if cmd != RequestType.ERROR:
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

def start_server(port: int):
    with http.server.ThreadingHTTPServer(('', port), Handler) as server:
        server.serve_forever()

def serve_requests(port: int, db_file_in: str):
    global db_file
    db_file = db_file_in
    daemon = threading.Thread(name='airlinkproxy_daemon_server',
                              target=start_server,
                              args=[port])
    daemon.setDaemon(True) # Set as a daemon so it will be killed once the main thread is dead.
    daemon.start()

if __name__ == '__main__':
    def main():
        usage = """%prog [--help] --db-file <db-file> --port <port>"""

        parser: str = optparse.OptionParser(usage=usage)
        parser.add_option('--db-file', dest='db_file', action='store',
                          type=str, default=None,
                          help='The database file from which to serve readings.  --db-file must be specified.')
        parser.add_option("--port", dest="port", type=int, default=None,
                          help="The port on which to serve.  --port must be specified.")

        if options.db_file is None:
            parser.error('db-file must be specified.')

        if options.port is None:
            parser.error('port must be specified.')

        (options, args) = parser.parse_args()

        serve_requests(options.port, options.db_file)
        print('Hit return to exit...', end='')
        _ = input()
