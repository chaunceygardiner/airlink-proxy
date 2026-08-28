---
title: Running the proxy
layout: default
nav_order: 6
---

# Running the proxy

[airlink-proxy manual](https://chaunceygardiner.github.io/airlink-proxy/) · [airlink-proxy on GitHub](https://github.com/chaunceygardiner/airlink-proxy) · [Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)

---

## The service

airlink-proxy runs as a systemd unit named `airlink-proxy`, as the
`airlinkproxy` system user, restarting on failure after ten seconds.

```sh
sudo systemctl status airlink-proxy
sudo systemctl restart airlink-proxy
sudo systemctl stop airlink-proxy
sudo systemctl start airlink-proxy
```

The install script enables and starts the unit; you should not normally need to
touch it, and an upgrade restarts it only if something actually changed.

## The log

Two places, and they hold different things:

```sh
sudo journalctl -u airlink-proxy     # service-level messages: starts, stops, crashes
tail -f /var/log/airlink-proxy.log   # the daemon's own log: readings, requests, errors
```

The second exists because rsyslog is configured to route everything logged under
the program name `airlink-proxy` into that file, and then to stop processing it
— so the daemon's chatter does not also land in `/var/log/syslog`.  If rsyslog
is not installed, the daemon's log has nowhere to go but the journal.

The log is rotated weekly with 52 rotations kept, compressed with
`delaycompress`.  Rotation uses `create` rather than `copytruncate`, so
continuity across a rotation depends on rsyslog being signalled -- which
rsyslog's own logrotate configuration does, in the same run.

A healthy log is quiet.  At the default settings the only routine entry is one
`Added record ... to archive (N samples).` per archive interval, plus a line per REST
request served.  Individual polls are logged only with
[`debug`](configuration.md#debug) turned on.

## The logwatch report

If logwatch is present when you install, a classifier script is installed with
it and an airlink-proxy section appears in the regular logwatch report.  It
counts what happened rather than reprinting the log: startups, archive records
added, long device reads, each REST request by name, and errors by kind —
skipped readings, insane readings, old readings, readings the device timestamped
as seconds since boot, readings missing fields, type 5 conversion errors,
current, two-minute and archive database insert errors, skipped archive records,
sensor fetch failures, retried sensor requests, errors the sensor itself
reported, name resolution failures, no route to host, connection refused,
connection timeouts, REST server failures, and request errors.

A device that is quietly getting worse shows up here as a rising count long
before anything visibly breaks.

{: .important }
The classifier matches the daemon's log messages verbatim, so the two ship
together and an upgrade always refreshes the script.  If you customize the
logwatch conf files, note that they — unlike the classifier script — are never
overwritten once installed.

## Dumping the database

The daemon takes the configuration file as its argument and accepts two options;
`airlinkproxyd --help` lists them.

`--dump` prints the current reading, the two-minute average and every archive record held in the
database, then exits.  That is the entire history, so page through it:

```sh
python3 /home/airlinkproxy/bin/airlinkproxyd --dump \
    /home/airlinkproxy/airlinkproxy.conf | less
```

It only reads the database, so the service can be left running.

`--pidfile <path>` writes the process id to a file.  It is a holdover from the
days of the SysV init script; the systemd unit does not use it.

## Where everything lives

| Path | What it is |
| --- | --- |
| `/home/airlinkproxy/bin/airlinkproxyd` | The daemon. |
| `/home/airlinkproxy/airlinkproxy.conf` | Its configuration. |
| `/home/airlinkproxy/archive/airlinkproxy.sdb` | The sqlite database. |
| `/etc/systemd/system/airlink-proxy.service` | The systemd unit. |
| `/etc/rsyslog.d/airlink-proxy.conf` | Routes the log to its own file. |
| `/etc/logrotate.d/airlink-proxy` | Rotates that file weekly. |
| `/etc/logwatch/conf/logfiles/airlink-proxy.conf` | Tells logwatch where the log is. |
| `/etc/logwatch/conf/services/airlink-proxy.conf` | Declares the logwatch service. |
| `/etc/logwatch/scripts/services/airlink-proxy` | Classifies the log. |
| `/var/log/airlink-proxy.log` | The daemon's log. |

The target directory defaults to `/home/airlinkproxy` but is yours to choose at
install time; the unit is rewritten to match.
