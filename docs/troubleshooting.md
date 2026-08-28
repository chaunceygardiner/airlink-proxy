---
title: Troubleshooting
layout: default
nav_order: 7
---

# Troubleshooting

[airlink-proxy manual](https://chaunceygardiner.github.io/airlink-proxy/) · [airlink-proxy on GitHub](https://github.com/chaunceygardiner/airlink-proxy) · [Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)

---

Almost everything shows up in `/var/log/airlink-proxy.log`.  Start there.

```sh
tail -f /var/log/airlink-proxy.log
```

## The service will not start

```sh
sudo systemctl status airlink-proxy
sudo journalctl -u airlink-proxy -n 50
```

The daemon reads its configuration once, at startup, and exits if it cannot.
The usual causes are a `database-file` in a directory the `airlinkproxy` user
cannot write or traverse, a missing `hostname`, an `archive-interval-secs` that
is not a whole multiple of `poll-freq-secs`, or a Python package that was never
installed (`python3-configobj`, `python3-dateutil`, `python3-requests`).

Because the unit is `Restart=on-failure` with a ten second delay, a daemon that
fails at startup produces a start-fail-wait-start cycle in the journal rather
than a single error.  Look at the first failure, not the latest.

## No readings are arriving

Check the device by hand, from the machine the proxy runs on:

```sh
curl 'http://<airlink-hostname>/v1/current_conditions'
```

The log distinguishes the ways this fails.  These are the words the daemon
writes; the logwatch report gives the same failures friendlier names, so grep
the log for these, not for the report's:

| In the log | What it means |
| --- | --- |
| `Name or service not known`, `Temporary failure in name resolution` | DNS cannot resolve `hostname`.  Check the spelling and that the name resolves from *this* machine. |
| `No route to host` | The AirLink is on a network this machine cannot reach. |
| `Connection refused` | Something answered at that address, but nothing is listening on `port`. |
| `Connection to ... timed out` | Nothing accepted the connection within `timeout-secs`.  The address may be right and the device off, or a firewall may be dropping the packets. |
| `Read timed out` | The device accepted the connection and then did not answer within `timeout-secs`. |
| `Skipping reading because of:` | The read raised; the reason follows.  The session is reset and the next poll tries again. |

None of these stop the daemon.  Each is logged, the reading is skipped, the
requests session is reset, and the next poll tries again.

{: .note }
A `ConnectionError` is retried twice inside the same poll, a second apart,
before the poll gives up — that is what `Retrying request` in the log means.  It
exists for the just-rebooted machine whose resolver is not up yet.

## Readings arrive but are rejected

```
Reading not sane:  <reason> (<the reading>)
```

The daemon checked the reading and refused to believe it.  The reason names the
field: a value of the wrong type, a `data_structure_type` that is neither 5 nor
6, an empty `conditions` array, or an `error` the device itself reported.

An occasional rejected reading is normal and harmless.  A steady stream of them
is a device that needs attention, and the logwatch report is where the trend
shows.

## Readings are old

```
Ignoring reading from <host>--age: <n> seconds.  Record: ...
```

The device answered, but the reading it returned is more than 60 seconds old, so
it is not stored.  Usually the AirLink has stopped updating and is serving a
stale record.

A related case is called out separately:

```
last_report_time must be time since boot: <n> seconds.  Record: ...
```

The AirLink has rebooted and has not yet synchronised its clock, so it is
reporting seconds-since-boot as a timestamp.  It sorts itself out once the
device gets the time; if it does not, the device is not reaching a time source.

## A gap in the archive

```
Skipping archive record.
```

Every poll during that archive interval failed, so there was nothing to average
and no record was written.  This is intentional: the interval genuinely has no
data, and writing an invented value would be worse.  A single failed poll does
not cause this -- the record is the average of whatever polls did succeed.

If gaps are frequent, the cause is upstream — see the two sections above.

## Nothing answers on the REST port

```sh
curl 'http://localhost:8040/get-version'
```

If it is refused, check first whether the service is running at all.  A port the
daemon cannot bind -- one already taken, or below 1024, which it cannot use
because it runs unprivileged -- makes the daemon log
`REST server on port N died` and exit rather than run on with nothing
listening, so `systemctl status airlink-proxy` will show it failed or
restarting.  If the service *is* running and the port still does not answer,
check that `server-port` is what you think it is.

{: .note }
purple-proxy defaults to 8000 and airgradient-proxy to 8080.  If you run more
than one of them, give each its own port.

Remember the argument separator is a comma: `?since_ts=0,limit=10`.  With an
ampersand, the proxy sees one long malformed argument and answers with an error.

{: .note }
Errors come back with HTTP status 200 and a json body whose `error` member is
non-null.  A client that checks only the status code will not notice.  See
[REST API](rest-api.md#errors).

## The log is not in `/var/log/airlink-proxy.log`

Either rsyslog is not installed, or `service-name` has been changed from
`airlink-proxy`.  The rsyslog rule keys on the program name; change the name and
the rule no longer matches.  The install script warns when it sees a
`service-name` the shipped configuration does not route.

With `log-to-stdout = 1` the daemon logs to stdout instead of syslog, which
under systemd means the journal and not the file.

## The logwatch section is missing

logwatch must be installed **before** airlink-proxy, since the install script
only lays down the logwatch configuration if logwatch is present.  Install
logwatch, then re-run `sudo ./install -y`.

## After an upgrade, a setting went back to its default

Check for `airlinkproxy.conf.bak` next to the conf: migration keeps existing
values, so a value that reverted was probably spelled in a way this version no
longer recognizes and was dropped as deprecated.  The install prints
`Removing deprecated option: <key>.` when that happens.

{: .note }
If `airlinkproxy.conf` is a symlink, it is deliberately **not** migrated — the
installer leaves the symlink alone and says so.  Edit the file it points at.

## Still stuck

Turn on debug logging, restart, and watch a few polls:

```
debug = 1
```

Then [open an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)
with the relevant part of the log.
