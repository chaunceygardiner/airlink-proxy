---
title: Installation
layout: default
nav_order: 2
---

# Installation

[airlink-proxy manual](https://chaunceygardiner.github.io/airlink-proxy/) · [airlink-proxy on GitHub](https://github.com/chaunceygardiner/airlink-proxy) · [Report an issue](https://github.com/chaunceygardiner/airlink-proxy/issues)

---

**Requirements:** Debian or Raspberry Pi OS, systemd, and Python 3 with
`python3-configobj`, `python3-dateutil` and `python3-requests`.  There is no
virtual environment and nothing is installed with pip.  rsyslog is strongly
recommended — it is what routes the daemon's log to
`/var/log/airlink-proxy.log`; without it the log lives only in the systemd
journal.  logwatch is optional; if it is present, a log classifier is installed
with the daemon.

## 1. Get the source

Clone the repository:

```sh
git clone https://github.com/chaunceygardiner/airlink-proxy
```

or download and unzip
[the latest release](https://github.com/chaunceygardiner/airlink-proxy/releases/latest/download/airlink-proxy.zip),
which unpacks to `airlink-proxy-master`.

## 2. Install the packages

```sh
sudo apt install rsyslog python3-configobj python3-dateutil python3-requests
```

## 3. Run the install script

```sh
cd <airlink-proxy-src-dir>
sudo ./install
```

Every setting is a named command line option; `./install -h` lists them all.  On
a fresh install the script prompts for anything not given on the command line,
showing the default — press Enter to accept it.  `-y` accepts every default
silently.

```sh
# Fully interactive:
sudo ./install

# Scripted; defaults for everything not given:
sudo ./install --sensor airlink --server-port 8040 -y
```

{: .important }
The pre-1.0 positional form (`sudo ./install <src> <target> <dns-name>`) is
rejected.  If you use it, the script prints the equivalent flags and exits
without changing anything.

On a fresh install the script:

* creates an `airlinkproxy` system user for the daemon to run as;
* copies the program to the target directory (default `/home/airlinkproxy`) and
  chowns it to that user;
* generates `<target-dir>/airlinkproxy.conf` from the settings chosen;
* installs the rsyslog, logrotate and logwatch configuration;
* installs, enables and starts the `airlink-proxy` systemd service.

## 4. Check that it is running

```sh
sudo systemctl status airlink-proxy
curl 'http://localhost:8040/get-version'
```

The second command answers `{"version": "2"}` -- that is the version of the REST
command set, not of the program, so it does not track the release number.
Within a poll interval or two,
`curl 'http://localhost:8040/v1/current_conditions'` returns a reading.  See
[Running the proxy](running.md) for the log and
[Troubleshooting](troubleshooting.md) if nothing arrives.

## Uninstalling

```sh
sudo ./install --uninstall [<target-dir>]
```

The target directory is left in place, with its configuration and its database —
the archive history is not thrown away by an uninstall.
