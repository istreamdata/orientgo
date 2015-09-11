# Overview
[![Build Status](https://travis-ci.org/istreamdata/orientgo.svg?branch=master)](https://travis-ci.org/istreamdata/orientgo)

**OrientGo** is a Go client for the [OrientDB](http://orientdb.com/orientdb/) database, a fork of [Ogonori](https://github.com/quux00/ogonori).

## Changes to Ogonori
- Uses varint encoding/decoding functions from stdlib `binary` package (which is more robust)
- Glog for logging instead of custom package
- Uses [MapStructure](http://github.com/mitchellh/mapstructure) lib for record deserialization
- User can omit deserialization of query/command results to `ODocument` and provide custom values for deserialization (array/struct)
- Connection pooling, `orient.Database` is safe for concurrent use
- Supports Orient functions ( SQL/JS(/Groovy?) )
- Works with OrientDB 2.1.2

# Status

The primary focus of ogonori is to build a Go (golang) client supporting the OrientDB version 2 Network Binary Protocol for both Document and Graph databases.

Driver is under active development: it is in an alpha-state for the core features and the API is potentially unstable (though getting more stable now).

Here's what you can do with it right now:

- Do most any OrientDB SQL statements via `SQLQuery` and `SQLCommand`, including support for OrientDB fetch plans.
- Create `oschema.ODocument` objects and create them in the DB via `CreateRecord`.
- Update fields on `oschema.ODocument` objects and update them in the DB via `UpdateRecord`.
- Use the ogonori driver for the golang `database/sql` API, with some cautions (see below).
- Use it with either document or graph databases.
- Supports OrientDB 2.1.x series. OrientDB 1.x and 2.0 is not supported.

Early adopters are welcome to try it out and report any problems found.  You are also welcome to suggest a more user-friendly API on top of the low-level `obinary` one.


What is not yet supported:

- Transactions (that is next on my TODO list)
- A more user-friendly Document or Graph API, perhaps with JSON marshaling/unmarshaling.  If you want to help design that see Issue #6.
- OrientDB DECIMAL and CUSTOM types.
- Insertion/retrieval of "large" records into OrientDB.  In some cases a few hundred or even a few dozen KB will cause a problem - see Issue #7.
- Some edge cases around RidBags (LinkBags) - the library will panic at present if you hit these. That obviously is not proper behavior, but since this is alpha (or pre-alpha) that's what I'm doing right now.


*Documentation Note*: Eventually I will write a detailed wiki on using ogonori with OrientDB, but that will have to wait until the API is stable.  For now the code in the client.go file, plus the godoc for the code is the documentation you'll need to access to see how to use it.


#### Caveat on using ogonori as a golang database/sql API driver

The golang `database/sql` API has some constraints that can be make it painful to work with OrientDB.  For example:

* when you insert a record, the Go `database/sql` API only allows one to return a single int64 identifier for the record, but OrientDB uses as a compound int16:int64 RID, so getting the RID of records you just inserted requires another round trip to the database to query the RID.
* there is no way (that I know of) to specify an OrientDB fetch plan in the SQL only, and the `database/sql` package provides no affordance for adding this. So if you want to pull in additional linked records using a fetch plan (such as `*:-1`), then you'll need to use the ogonori native low-level `obinary` API.

Also, since I don't yet support OrientDB transactions, the `Tx` portion of the `database/sql` API is not yet implemented.

<br/>

## How to run tests:

1) Install Docker

2) Pull OrientDB image: `docker pull dennwc/orientdb:2.1.2`

3) `go test -v ./...`


<br/>
# LICENSE

The MIT License
