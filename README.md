# airlink-proxy

[![Read the manual](assets/btn-manual.svg)](https://chaunceygardiner.github.io/airlink-proxy/)
[![Download airlink-proxy.zip](assets/btn-download.svg)](https://github.com/chaunceygardiner/airlink-proxy/releases/latest/download/airlink-proxy.zip)
[![Report an issue](assets/btn-issue.svg)](https://github.com/chaunceygardiner/airlink-proxy/issues)

A proxy and archiver for the [Davis AirLink](https://www.davisinstruments.com/) air
quality sensor.  airlink-proxy runs as a daemon that polls an AirLink on the local
network, sanity checks each reading, stores readings in a sqlite database, and serves
everything through a small REST API.

**[Read the manual](https://chaunceygardiner.github.io/airlink-proxy/)** for the full
documentation: installation, configuration, the REST API, and troubleshooting.

## Why not query the AirLink directly?

* The device is a small embedded server.  Point several clients at it on a short
  interval and reads start timing out.  The proxy is the only thing that talks to the
  AirLink, at one steady rate, no matter how many clients ask the proxy.
* Only the proxy has a history.  It writes an archive record every archive interval and
  keeps them, so a period your weather software missed can be recovered afterwards.
  The device itself remembers nothing.
* Every reading is sanity checked before it is accepted: fields are type checked,
  readings more than 60 seconds old are rejected, and a device reporting
  seconds-since-boot after a reboot is recognized and called out.
* Clients need not know the proxy is there.  It answers the current reading on
  `/v1/current_conditions`, the AirLink's own path, with the AirLink's own json.

The current reading is stored exactly as the device reported it.  Archive records and the
two-minute record are averages the proxy computes over their span, from the AirLink's
instantaneous `pm_1_last`, `pm_2p5_last` and `pm_10_last`, its one-minute `pm_1`, `pm_2p5`
and `pm_10`, and the temperature and humidity fields.  The device's longer rolling windows
(`pm_2p5_last_1_hour`, `_last_3_hours`, `_last_24_hours`, `_nowcast`) and the
`pct_pm_data_*` counters are carried from the last reading in the span rather than
averaged.

## REST API

* `/v1/current_conditions` — the latest reading held, in exactly the form the AirLink
  serves it.
* `/fetch-two-minute-record` — the average over the last two minutes.
* `/get-archive-interval-secs` — the proxy's archive interval, so a client can
  check it against its own.
* `/get-version` — the version of the proxy command set (currently `2`).
* `/get-earliest-timestamp` — the timestamp of the oldest archive record in the database.
* `/fetch-archive-records?since_ts=<since_ts>` — all archive records with timestamp
  greater than `<since_ts>` (seconds since the epoch; `since_ts=0` fetches everything).
* `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>` — limits the records
  returned to timestamps less than or equal to `<max_ts>`.
* `/fetch-archive-records?since_ts=<since_ts>,limit=<count>` — returns at most `<count>`
  records.
* `max_ts` and `limit` may be combined.

Arguments are separated by commas, not ampersands.  See the
[REST API page](https://chaunceygardiner.github.io/airlink-proxy/rest-api.html) for
responses, and for how errors are reported.

## Requirements

* Debian or Raspberry Pi OS (tested there; on other platforms these instructions and
  the install script serve as a specification of the steps needed).
* systemd (the service is installed as a systemd unit).
* Python 3 with the `python3-configobj`, `python3-dateutil` and `python3-requests`
  packages.
* rsyslog (recommended: it routes the daemon's log to `/var/log/airlink-proxy.log`;
  without it the log is only in the systemd journal).
* logwatch (optional; a log classifier is installed if logwatch is present).

## Installation

Clone the repository:

```sh
git clone https://github.com/chaunceygardiner/airlink-proxy
```

or download and unzip
[the latest release](https://github.com/chaunceygardiner/airlink-proxy/releases/latest/download/airlink-proxy.zip),
which unpacks to `airlink-proxy-master`.  Then:

```sh
sudo apt install rsyslog python3-configobj python3-dateutil python3-requests
cd <airlink-proxy-src-dir>
sudo ./install
```

Every setting can be given as a command line option; on a fresh install, the script asks
for anything not specified (press Enter to accept the shown default), and `-y` accepts
the default for everything not specified.  `./install -h` lists all options.

```sh
# Scripted; defaults for everything not given:
sudo ./install --sensor airlink -y

# Upgrade an existing installation (settings come from the installed conf):
sudo ./install -y
```

Re-running the script upgrades in place: `airlinkproxy.conf` is rewritten from your
existing settings (your values are kept, options new to the version are added with
their defaults, options the version no longer knows are dropped, and the previous file
is saved as `.bak` -- so hand-added comments do not survive), other conf files are never
overwritten, program files are refreshed, and the daemon is restarted only if something
it uses actually changed.  A pre-1.0 SysV installation is migrated to the systemd unit
automatically.  See
[Upgrading](https://chaunceygardiner.github.io/airlink-proxy/upgrading.html).

To uninstall (the target directory, with its configuration and database, is left in
place):

```sh
sudo ./install --uninstall [<target-dir>]
```

## Testing

```sh
tests/test-install             # install script tests; runs unprivileged in a sandbox
python3 tests/test-monitor.py  # offline tests: database, model, sanity checks, REST parsing
python3 tests/test-live.py     # live tests against a real AirLink (hostname from the conf)
```
