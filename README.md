# tsdb-migrate

This is a command-line tool for offline migration of [Prometheus](https://prometheus.io) 1.x "local storage" databases to the new [TSDB](https://github.com/prometheus/tsdb) format of Prometheus 2.0 (which is not released yet).

**Warning:** This is still in the very early stages, so please do not use it for anything important yet!

## Installation

Install Go and then `go get` the package:

```
go get github.com/xperimental/tsdb-migrate
```

As this is pre-release software no binaries are provided yet.

## Usage

```
Usage of tsdb-migrate:
  -i, --input string         Directory of local storage to convert.
  -o, --output string        Directory for new TSDB database.
  -r, --retention duration   Retention time of new database. (default 360h0m0s)
  -s, --start-time string    Starting time for conversion process. (default "2016-07-18T14:37:00Z")
      --step-time duration   Time slice to use for copying values. (default 24h0m0s)
```