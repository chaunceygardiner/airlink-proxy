---
title: Home
layout: default
nav_order: 1
permalink: /
---

# airlink-proxy — one poller in front of the AirLink

[View on GitHub](https://github.com/chaunceygardiner/airlink-proxy){: .btn .btn-primary }
[Download the source](https://github.com/chaunceygardiner/airlink-proxy/releases/latest/download/airlink-proxy.zip){: .btn }
[Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues){: .btn }

airlink-proxy is a small daemon that sits between a
[Davis AirLink](https://www.davisinstruments.com/) air quality sensor and
everything that wants to read it.  It polls the device on the local network at
a steady rate, sanity checks each reading, keeps the latest one, maintains a
rolling two-minute average, writes an archive record every archive interval into
a sqlite database, and answers a handful of REST requests over HTTP.

It is a standalone program.  There is no WeeWX code in it and it does not need
WeeWX to run, though it was written with WeeWX in mind.

**Requirements:** Debian or Raspberry Pi OS, systemd, and Python 3 with the
`python3-configobj`, `python3-dateutil` and `python3-requests` packages.  See
[Installation](installation.md).

## Why not query the AirLink directly?

**The device is a small embedded server.**  Point several clients at it on a
short interval and reads start timing out.  The proxy is the only thing that
talks to the AirLink, at one steady rate you choose, no matter how many clients
ask the proxy.

**Only the proxy has a history.**  The AirLink remembers nothing across a
request: ask it a question and it tells you about right now.  The proxy writes
an archive record every archive interval and keeps them, so a period your
weather software missed can be recovered afterwards.  Ask the device directly
and there is nothing to recover from.

**Every reading is checked before it is believed.**  Fields are type checked, a
reading whose `last_report_time` is more than 60 seconds old is rejected, and a
device that has rebooted (and is reporting seconds-since-boot as its timestamp)
is recognised and called out rather than silently stored.  A bad reading is
logged and skipped — the daemon does not pass it on and does not exit.

**Clients need not know it is there.**  The proxy answers the current reading on
`/v1/current_conditions`, the AirLink's own path, with the AirLink's own json.
Pointing a client at the proxy instead of the device is a hostname change.

{: .note }
The current reading is stored exactly as the device reported it.  Archive
records and the two-minute record are averages the proxy computes over their
span — of the device's instantaneous `pm_1_last`, `pm_2p5_last` and `pm_10_last`,
its one-minute `pm_1`, `pm_2p5` and `pm_10`, and the temperature and humidity
fields — the same way
[purple-proxy](https://github.com/chaunceygardiner/purple-proxy) does.  The
AirLink's longer rolling windows (`pm_2p5_last_1_hour`, `_last_3_hours`,
`_last_24_hours`, `_nowcast`) and the `pct_pm_data_*` counters are carried from
the last reading in the span rather than averaged.

## Where to go next

* [Installation](installation.md) — get the source, install the service.
* [Upgrading](upgrading.md) — what a re-install does and does not touch.
* [Configuration](configuration.md) — every setting, with its default.
* [REST API](rest-api.md) — the requests the proxy answers.
* [Running the proxy](running.md) — service, logs, logwatch, dumping the database.
* [Troubleshooting](troubleshooting.md) — when readings stop arriving.
