#!/usr/bin/python3

# Copyright (c) 2020-2026 John A Kline
# See the file LICENSE for your full rights.

import syslog

class Logger(object):
    def __init__(self, service_name: str, log_to_stdout: bool=False, debug_mode: bool=False):
        self.reconfigure(service_name, log_to_stdout, debug_mode)

    def reconfigure(self, service_name: str, log_to_stdout: bool=False, debug_mode: bool=False) -> None:
        """Repoint the logger; called by airlinkproxyd once the config file
        has been read.  The shared `log` object below is mutated in place so
        every module sees the change."""
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
            # flush: under systemd stdout is a pipe, so print() would block
            # buffer and deliver the log in 8K bursts, losing whatever is still
            # buffered when the daemon is stopped.
            print('%s: %s: %s' % (l, self.service_name, msg), flush=True)
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

# The shared logger.  Logs to stdout until airlinkproxyd reconfigures it from
# the config file.
log: Logger = Logger('monitor', log_to_stdout=True, debug_mode=False)
