# Overview
[![Build Status](https://travis-ci.org/istreamdata/orientgo.svg?branch=master)](https://travis-ci.org/istreamdata/orientgo)
[![GoDoc](https://godoc.org/gopkg.in/istreamdata/orientgo.v2?status.svg)](https://godoc.org/gopkg.in/istreamdata/orientgo.v2)

**OrientGo** is a Go client for the [OrientDB](http://orientdb.com/orientdb/) database.

# Status

OrientDB versions supported: **2.0 - 2.1.2**

Driver is under active development. API in `orientgo` is potentially unstable (though getting more stable now).

Early adopters are welcome to try it out and report any problems found.

### Supported features:
- Mostly any SQL [queries](http://godoc.org/gopkg.in/istreamdata/orientgo.v2#SQLQuery), [commands](http://godoc.org/gopkg.in/istreamdata/orientgo.v2#SQLCommand) and [batch requests](http://godoc.org/gopkg.in/istreamdata/orientgo.v2#ScriptCommand).
- Server-side scripts (via [ScriptCommand](http://godoc.org/gopkg.in/istreamdata/orientgo.v2#ScriptCommand) or [functions](http://godoc.org/gopkg.in/istreamdata/orientgo.v2#Function)).
- Command results conversion to custom types via [mapstructure](http://github.com/mitchellh/mapstructure).
- Direct CRUD operations on `ODocument` objects with `CreateRecord`/`UpdateRecord`/`DeleteRecord`.
- Management of databases and record clusters.
- Can be used for the golang `database/sql` API, with some cautions (see below).
- Only supports OrientDB 2.x series.

### Not supported yet:
- OrientDB 1.x.
- Servers with cluster configuration (not tested).
- Fetch plans are temporary disabled due to internal changes.
- RidBags (LinkBags) support is temporary disabled.
- Transactions in Go. Transactions in JS can be used instead.
- Live queries.
- Command results streaming.
- Non-integer OrientDB DECIMAL values. Integers are ok.
- OrientDB CUSTOM type.
- ORM-like API. See Issue #6.

#### Caveat on using OrientGo as a database/sql API driver

The golang `database/sql` API has some constraints that can be make it painful to work with OrientDB. For example:

* When you insert a record, the Go `database/sql` API only allows one to return a single int64 identifier for the record, but OrientDB uses as a compound int16:int64 RID, so getting the RID of records you just inserted requires another round trip to the database to query the RID.

Also, since OrientDB transactions are not supported, the `Tx` portion of the `database/sql` API is not yet implemented.

# Development

You are welcome to initiate pull request and suggest a more user-friendly API. We will try to review them ASAP.

## How to run functional tests:

1) Install [Docker](https://docs.docker.com)

2) Pull OrientDB image: `docker pull dennwc/orientdb:2.1.2`

3) `go test -v ./...`

## LICENSE

The MIT License
