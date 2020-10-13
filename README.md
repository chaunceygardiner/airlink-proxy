# airlink-proxy
Proxy server for AirLink air quality sensor.  Serves current and archived readings which are averaged over a specified interval.

## What? Why?

airlink-proxy works in the backround querying the AirLink sensor and answers queries from clients about air quality.

### Why not query the sensor directly?
* The proxy will query the AirLink every 5s and provide the latest of these queries to
* using a URL and response identical to the actual AirLink device.
* The proxy can handle a higher load, even when running on a Raspberry Pi.
* The proxy will archive the AirLiink's 1m readings (at 5s past the minute).

### Rest API
* `/v1/current_conditions` Identical to quering the device directly.  Returns the latest response on record.
* `/get-version' Returns the version of the proxy command set (currently, '1').
* `/get-earliest-timestamp' Returns the the timestamp of the oldest record in the database.
* `/fetch-archive-records?since_ts=<timestamp>` Fetches all archive records >= <timestamp> (i.e., seconds since the epoch).
* `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>` Fetches all archive records > <since_ts> and <= <max_ts>.
* `/fetch-archive-records?since_ts=<since_ts>,limit=<count>` Fetches up to <count> records  > <since_ts>.
* `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>,limit=<count>` Fetches up to <count> archive records > <since_ts> and <= <max_ts>.

### Json Specification
See the AirLink spec for the json.

## Installation Instructions

Note: Tested under Debian and Raspbian.  For other platorms,
these instructions and the install script serve as a specification
for what steps are needed to install.

```
sudo <airlink-proxy-src-dir>/install <airlink-proxy-src-dir> <target-dir> <airlink-dns-name-or-ip-address>
```

### Example installation commands:
```
cd ~/software/airlink-proxy
sudo ./install . /home/airlinkproxy airlink
```
