---
title: Upgrading
layout: default
nav_order: 3
---

# Upgrading

[airlink-proxy manual](https://chaunceygardiner.github.io/airlink-proxy/) · [airlink-proxy on GitHub](https://github.com/chaunceygardiner/airlink-proxy) · [Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)

---

Upgrading is the same script, run again:

```sh
cd <airlink-proxy-src-dir>
sudo ./install -y
```

An upgrade never prompts.  Settings come from the installed
`airlinkproxy.conf`; anything given on the command line overrides them.

## What the upgrade touches

**`airlinkproxy.conf` is rewritten from your existing settings.**  Existing
values are kept, options given on the command line win, options new to this
version are added with their defaults, and options this version no longer knows
about are dropped.  The previous file is saved as `airlinkproxy.conf.bak`.

{: .note }
Hand-written comments in `airlinkproxy.conf` do not survive migration.  Values
do — a per-machine `server-port` is carried across upgrades.

**Every other conf file is left alone.**  The rsyslog, logrotate and the two
logwatch conf files are installed only when they are absent; once installed they
are yours to customize and no upgrade overwrites them.  When the version shipped
in a release differs from the one installed, your file is left in place and the
shipped version is written next to it as `<file>.dpkg-new` for you to merge by
hand.  Once the installed file matches what ships, the `.dpkg-new` file is
removed.

**Program files are refreshed every time.**  So is the logwatch classifier
script, which matches the daemon's log messages verbatim and therefore has to
ship with the daemon that writes them.

**A symlinked destination is always left in place.**  Files symlinked to a
source checkout keep working, and a symlinked `airlinkproxy.conf` is not
migrated — edit the file it points at.

**The daemon is disturbed as little as possible.**  It is not stopped up front.
At the end it is started if it was not running, and restarted only if the
program files, `airlinkproxy.conf` or the systemd unit actually changed.  An
install that changed nothing leaves the running daemon alone.  rsyslog is
restarted only when its conf was newly installed.

## Upgrading from before 1.0

Version 1.0 replaced the SysV init script and its nohup wrapper with a systemd
unit running as the `airlinkproxy` system user, and split the daemon into
modules.  The install script migrates such an installation automatically: it
stops and removes the old init script, removes the wrapper and the pre-1.0
`monitor/monitor.py`, then installs, enables and starts the unit.  Nothing is
asked of you.

{: .note }
The database is unchanged.  1.0 generates exactly the schema earlier versions
created, so an existing `airlinkproxy.sdb` is carried forward with its whole
history and needs no migration.

Three settings that earlier versions read from the conf, logged at startup and
then ignored — `poll-freq-secs`, `poll-freq-offset` and
`archive-interval-secs` — now actually take effect.  `poll-freq-secs` and
`archive-interval-secs` were hardcoded to 5 and 60.  `poll-freq-secs` still
defaults to 5, so a conf that did not set it behaves exactly as before.  A conf
that did set these will now be obeyed; check [Configuration](configuration.md)
if yours does.

{: .important }
**Archive records are now averages, and the default
`archive-interval-secs` is 300 for new installations.**  An archive record used
to be the single poll that landed on the boundary; it is now the average of the
readings collected during the interval.

Your archive interval does not change when you upgrade.  A configuration file
that sets `archive-interval-secs` keeps its value, and a pre-1.0 file that never
set one has 60 written into it -- the cadence that version actually ran at --
rather than the new default.  Only a fresh install gets 300.

{: .important }
If you use weewx-airlink's archive backfill, set `archive-interval-secs` to your
WeeWX archive interval.  If the proxy's interval is longer than a WeeWX archive
period, a period can fall entirely inside one proxy record with nothing to fill
it from.

A new `/fetch-two-minute-record` command serves a two-minute average, error
responses are now valid json, and `/get-version` reports `2` so a client can
tell the two contracts apart.

{: .important }
**`poll-freq-offset` has changed meaning and its default is now 0.**  It used to
shift the archive boundary; it now shifts each poll within its window, which is
what the documentation always claimed it did.  It must be less than
`poll-freq-secs`.  The old shipped default was 5, equal to the default
`poll-freq-secs`, so the installer resets any inherited value at or above
`poll-freq-secs` to 0 and says so.  Set a new, smaller offset only if you are
staggering two proxies against one AirLink.
