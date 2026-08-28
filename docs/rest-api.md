---
title: REST API
layout: default
nav_order: 5
---

# REST API

[airlink-proxy manual](https://chaunceygardiner.github.io/airlink-proxy/) · [airlink-proxy on GitHub](https://github.com/chaunceygardiner/airlink-proxy) · [Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)

---

The proxy answers plain HTTP GETs on
[`server-port`](configuration.md#server-port) (8040 by default).  The server
binds `::`, so it answers over IPv6 and IPv4 both, and it is threaded — requests
do not queue behind one another.

{: .note }
Arguments are separated by commas, not ampersands:
`?since_ts=0,limit=10`.  This is not the usual query-string convention, but it
is what the proxy parses.

## The current reading

### `/v1/current_conditions`

The most recent reading the proxy holds, in exactly the form the AirLink itself
serves it — same path, same json.  A client can be pointed at the proxy instead
of the device by changing the hostname and nothing else.

```
$ curl 'http://localhost:8040/v1/current_conditions'
{"data": {"did": "001D0A100214", "name": "airlink", "ts": 1600657500,
 "conditions": [{"lsid": 347825, "data_structure_type": 6, "temp": 74.1,
 "hum": 62.8, "dew_point": 60.6, "wet_bulb": 64.2, "heat_index": 74.7,
 "pm_1_last": 11, "pm_2p5_last": 14, "pm_10_last": 17, "pm_1": 11.68,
 "pm_2p5": 15.55, "pm_2p5_last_1_hour": 19.35, "pm_2p5_last_3_hours": 20.54,
 "pm_2p5_last_24_hours": 18.59, "pm_2p5_nowcast": 21.25, "pm_10": 17.28,
 "pm_10_last_1_hour": 23.3, "pm_10_last_3_hours": 24.64,
 "pm_10_last_24_hours": 22.71, "pm_10_nowcast": 25.6,
 "last_report_time": 1600657500, "pct_pm_data_last_1_hour": 100,
 "pct_pm_data_last_3_hours": 100, "pct_pm_data_nowcast": 100,
 "pct_pm_data_last_24_hours": 100}]}, "error": null}
```

A field the device did not report comes back as `null` rather than being
omitted.  `data_structure_type` is always 6: a device that reports the older
type 5 has its `pm_10p0*` fields renamed to their type 6 `pm_10*` spellings
before the reading is stored, so clients never see type 5.

Before the first reading is stored -- a fresh install, or a proxy that has never
had a successful poll -- there is nothing to return, and the response is the
error envelope described under [Errors](#errors) rather than a reading.  A
client written against the AirLink's own URL should check that `error` is null
before reaching into `data`.

## History

### `/get-earliest-timestamp`

The timestamp of the oldest archive record held, in seconds since the epoch:

```
$ curl 'http://localhost:8040/get-earliest-timestamp'
{"timestamp": 1600657320}
```

### `/fetch-two-minute-record`

The average of the readings collected over the last two minutes, as a single
reading object in the same shape as
[`/v1/current_conditions`](#v1current_conditions).  Recomputed on every poll.

This is a smoother value than the current reading and a fresher one than an
archive record.  It is what a client should fall back to for a loop packet when
no archive record covers the period it wants.

The record is never more than two minutes old.  Once two minutes pass with no
usable reading it is cleared, and the response becomes the error envelope below
rather than the last average computed.  The same applies before the first
reading is collected.

Restarting the daemon does not discard it.  A restart takes seconds and the
average is still good, so the stored record keeps being served until the first
successful poll replaces it -- an upgrade does not leave a client without a
value.  If polling stays down it is kept until it ages out at two minutes, then
cleared as above.  The record that replaces it averages only what has been
collected since the restart, so for the first two minutes it spans less than
its full window.

### `/fetch-archive-records?since_ts=<since_ts>`

Every archive record with a timestamp **greater than** `<since_ts>`, as a json
array of the same reading objects.  `since_ts=0` fetches the entire history,
which on a long-running proxy is a great many records.

Each record is the **average** over its
[`archive-interval-secs`](configuration.md#archive-interval-secs) span, not a
single instantaneous reading; the device's own rolling windows
(`pm_2p5_last_1_hour` and friends) are carried from the last reading in the span
rather than averaged.

Two optional arguments narrow it:

| Argument | Effect |
| --- | --- |
| `max_ts=<max_ts>` | Only records with timestamp **less than or equal to** `<max_ts>`. |
| `limit=<count>` | At most `<count>` records. |

They combine, in any order:

```
/fetch-archive-records?since_ts=1600657320,max_ts=1600657500
/fetch-archive-records?since_ts=0,limit=10
/fetch-archive-records?since_ts=0,max_ts=1600657500,limit=10
```

A single archive period is the `since_ts`/`max_ts` pair of that period's bounds,
which is how weather software backfills one missed interval.

```
$ curl 'http://localhost:8040/fetch-archive-records?since_ts=0,limit=1'
[  {"data": {"did": "001D0A100214", "name": "airlink", "ts": 1600657320, ... }]
```

## Version

### `/get-version`

```
$ curl 'http://localhost:8040/get-version'
{"version": "2"}
```

{: .important }
This is the version of the **command set** — the API described on this page —
not the version of the program.  It changes only when the requests themselves
change.  For the program version, look at the `Version` line the daemon logs at
startup.

Version `2` made archive records averages over their interval (in version `1` an
archive record was the single poll that landed on the boundary), added
[`/fetch-two-minute-record`](#fetch-two-minute-record) and
[`/get-archive-interval-secs`](#get-archive-interval-secs), and made error
responses valid json.  A client that cares about the difference — a WeeWX
extension backfilling archive records, say — can tell the two apart with this
command, since the proxy and the client are upgraded independently.

### `/get-archive-interval-secs`

The daemon's [`archive-interval-secs`](configuration.md#archive-interval-secs),
as an integer:

```
$ curl 'http://localhost:8040/get-archive-interval-secs'
{"archive-interval-secs": 300}
```

The value is the proxy's running configuration, not a default, so a client can
check it against its own archive interval.
[weewx-airlink](https://github.com/chaunceygardiner/weewx-airlink) uses it to
refuse catch-up outright when the two disagree, rather than backfilling from
records that do not line up.  There is no way to infer this from the records
themselves: they carry no per-record boundary timestamp, an interval in which
every poll failed produces no record at all, and the archive is never pruned, so
a long span can contain records written under a different interval.

## Errors

A malformed or unknown request is answered with the reason, in the same
envelope the AirLink itself uses for errors — a null `data` member and an
`error` object carrying a `code` and a `message`:

```
$ curl 'http://localhost:8040/fetch-archive-records'
{"data": null, "error": {"code": 400, "message": "fetch-archive-records requires since_ts argument"}}

$ curl 'http://localhost:8040/bogus'
{"data": null, "error": {"code": 400, "message": "Unknown command: /bogus."}}

$ curl 'http://localhost:8040/'
{"data": null, "error": {"code": 400, "message": "A command must be specified."}}
```

{: .important }
The status is **200**, not 4xx — so a client that checks only the status code
will treat an error as success.  Check whether the `error` member is null, which
is the same check that works against the device itself.

The `code` tells the two kinds of failure apart: **400** means the request was
wrong and repeating it unchanged will fail again; **500** means the proxy could
not answer this time — a database that was momentarily locked, say — and the
same request may well succeed on a retry.

{: .note }
Before command-set version `2` the error body was `{ error : "..." }`, with an
unquoted key, and did not parse as json at all.  A client that had to work
around that can stop.

Every request is logged at info level, which is what lets the logwatch report
count how many of each kind arrived.  See [Running the proxy](running.md).
