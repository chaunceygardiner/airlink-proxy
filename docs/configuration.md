---
title: Configuration
layout: default
nav_order: 4
---

# Configuration

[airlink-proxy manual](https://chaunceygardiner.github.io/airlink-proxy/) · [airlink-proxy on GitHub](https://github.com/chaunceygardiner/airlink-proxy) · [Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)

---

The daemon reads one file, `<target-dir>/airlinkproxy.conf` (by default
`/home/airlinkproxy/airlinkproxy.conf`).  It is a flat `key = value` file — no
sections, no nesting:

```
debug                 = 0
log-to-stdout         = 0
service-name          = airlink-proxy
hostname              = airlink
port                  = 80
timeout-secs          = 1
server-port           = 8040
poll-freq-secs        = 5
poll-freq-offset      = 0
archive-interval-secs = 300
database-file         = /home/airlinkproxy/archive/airlinkproxy.sdb
```

The install script writes this file and migrates it on upgrade, so the normal
way to change a setting is to re-run the installer with the matching option
(`./install -h` lists them).  Editing the file by hand works too; restart the
service afterwards.

| Key | Default | Description |
| --- | --- | --- |
| [`debug`](#debug) | 0 | Log debug messages. |
| [`log-to-stdout`](#log-to-stdout) | 0 | Log to stdout instead of syslog. |
| [`service-name`](#service-name) | airlink-proxy | Syslog program name. |
| [`hostname`](#hostname) | (required) | DNS name or IP address of the AirLink. |
| [`port`](#port) | 80 | Port of the AirLink. |
| [`timeout-secs`](#timeout-secs) | 1 | Timeout for device reads. |
| [`server-port`](#server-port) | 8040 | Port the proxy's REST API listens on. |
| [`poll-freq-secs`](#poll-freq-secs) | 5 | How often to poll the AirLink. |
| [`poll-freq-offset`](#poll-freq-offset) | 0 | Shift each poll this many seconds later within its window. |
| [`archive-interval-secs`](#archive-interval-secs) | 300 | How often to write an archive record. |
| [`database-file`](#database-file) | (required) | Path of the sqlite database. |

## The settings

### debug

`0` or `1`.  With `1` the daemon logs a great deal more, including a line for
every poll and every sql statement.  Useful while setting up; noisy afterwards.

### log-to-stdout

`0` or `1`.  With `1` the daemon writes its log to stdout instead of syslog.
Under systemd that means the journal.  The normal value is `0`, which logs to
syslog and lets rsyslog route it to `/var/log/airlink-proxy.log`.

### service-name

The program name the daemon logs under.  This is the name rsyslog, logrotate and
logwatch key on.

{: .important }
Change this and the shipped log configuration no longer matches: the log stops
arriving in `/var/log/airlink-proxy.log` and the logwatch section stops
appearing.  The install script warns when the name is not `airlink-proxy`.
There is very little reason to change it.

### hostname

The DNS name or IP address of the AirLink on your network.  This is the one
setting with no useful default; the installer asks for it.  The daemon fetches
`http://<hostname>:<port>/v1/current_conditions`.

### port

The AirLink's HTTP port.  The device serves on 80 and there is rarely a reason
to change this.

### timeout-secs

How long to wait for a device read before giving up.  A read that times out is
logged and skipped; the next poll tries again with a fresh session.  The default
is 1 second: an AirLink on the same LAN answers a small json document in well
under a tenth of a second, so a read still outstanding after a full second is
failing rather than being slow, and waiting longer only delays the retry.

Keeping it short also bounds what a dead sensor costs.  A `ConnectionError` is
retried twice a second apart, so the worst case is
`3 x timeout-secs + 2` seconds; at the default that is 5 seconds, which fits
inside one `poll-freq-secs` interval instead of overrunning several.

{: .note }
A `ConnectionError` — as opposed to a timeout — is retried twice within the same
poll, a second apart, before the poll is abandoned.  That exists for the case of
a just-rebooted machine whose name resolution is not up yet.

### server-port

The port the proxy's own REST API listens on.  The server binds `::`, so it
answers on IPv6 and IPv4 both, and it is threaded — requests do not queue behind
one another.

It must be between 1024 and 65535; the installer refuses anything else.  The
daemon runs as an unprivileged user and cannot bind a privileged port, and a
bind that fails takes the daemon down rather than leaving it running with
nothing listening.

{: .note }
The sibling proxies default to different ports so all three can run on one
machine: purple-proxy 8000, airlink-proxy 8040, airgradient-proxy 8080.  If you
change this, keep it clear of the others.

### poll-freq-secs

How often the AirLink is polled, in seconds.  Polls are scheduled on boundaries
of this value rather than "every N seconds from whenever the daemon started", so
with the default of 5 a poll lands on every fifth second of the minute.

`archive-interval-secs` must be a whole multiple of this value, and
`poll-freq-offset` must be smaller than it.  Both the installer and the daemon
refuse a combination that is not, so a hand-edited conf with a bad pairing fails
at startup rather than quietly misbehaving.

### poll-freq-offset

Shifts each poll later by this many seconds within its poll window.  With the
default `poll-freq-secs` of 5, an offset of 0 polls on every fifth second and an
offset of 2 polls two seconds after each of those.  That staggers two proxies
polling one device so they never ask at the same moment, and it lets the reading
be taken a moment into the interval, once the AirLink has refreshed.

It must be less than `poll-freq-secs`; at or beyond that it would push each poll
into the following window and slow polling to a multiple of `poll-freq-secs`.
The default is 0.

The offset does not move the archive boundary.  Archive records are still filed
on `archive-interval-secs` boundaries; the first poll at or after a boundary
writes the record, so any valid offset is safe.

### archive-interval-secs

How often an archive record is written to the database.  It should match the
archive interval of the weather software that reads it; the default here is 300
(five minutes), which is the usual WeeWX archive interval.

The record is the average of every reading collected during the interval, so it
describes the whole span rather than the single poll that landed on the
boundary.  The device's own rolling windows (`pm_2p5_last_1_hour` and friends,
`pct_pm_data_*`) are carried from the last reading in the span rather than
averaged.

{: .important }
If you use [weewx-airlink](https://github.com/chaunceygardiner/weewx-airlink)'s
archive backfill, set this to your WeeWX archive interval.  Backfill asks the
proxy for the records covering a WeeWX archive period; if this interval is
longer than that period, a WeeWX period can fall entirely inside one proxy
record and there is nothing to fill it from.

{: .note }
A record is written if any poll during the interval produced a usable reading,
so a single failed poll costs a sample rather than the whole record.  Only when
every poll in the interval failed is no record written, and
`Skipping archive record.` logged.  The gap is real; inventing a value to fill
it would be worse.

### database-file

Path of the sqlite database holding the current reading and the archive history.
The installer's default is `<target-dir>/archive/airlinkproxy.sdb`.  It grows
with the archive history and is not pruned; nothing here deletes old records,
since they are the history that makes backfilling possible.

{: .note }
At a one-minute archive interval this grows about five times faster than at the
default five.  Match `archive-interval-secs` to your weather software's archive
interval rather than keeping minute records you will never read.
