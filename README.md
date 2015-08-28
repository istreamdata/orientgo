# Overview

**OrientGo** is a Go client for the [OrientDB](http://orientdb.com/orientdb/) database, a fork of [Ogonori](https://github.com/quux00/ogonori).

[![Build Status](https://travis-ci.org/dyy18/orientgo.svg?branch=master)](https://travis-ci.org/dyy18/orientgo)

<br/>

## Changes to Ogonori
- Uses varint encoding/decoding functions from stdlib `binary` package (which is more robust)
- Uses Glog for logging instead of custom package
- Uses [MapStructure](http://github.com/mitchellh/mapstructure) lib for record deserialization
- User can omit deserialization of query/command results to `ODocument` and provide value for deserialization (array/struct)
- Supports Orient functions ( SQL/JS(/Groovy?) )
- Works with OrientDB 2.1-rc3

# Status

The primary focus of ogonori is to build a Go (golang) client supporting the OrientDB version 2 Network Binary Protocol for both Document and Graph databases.

Driver is under active development: it is in an alpha-state for the core features and the API is potentially unstable (though getting more stable now).

Here's what you can do with it right now:

- Do most any OrientDB SQL statements via `obinary.SQLQuery` and `obinary.SQLCommand`, including support for OrientDB fetch plans.
- Create `oschema.ODocument` objects and create them in the DB via `obinary.CreateRecord`.
- Update fields on `oschema.ODocument` objects and update them in the DB via `obinary.UpdateRecord`.
- Use the ogonori driver for the golang `database/sql` API, with some cautions (see below).
- Use it with either document or graph databases.
- Use it with the OrientDB 2.0-2.1 series. OrientDB 1.x is not supported.
- Run it with multiple goroutines - the unit of thread safety is the `obinary.DBClient`.  As long as each goroutine uses its own `DBClient`, it should work based on my design and testing so far.

Early adopters are welcome to try it out and report any problems found.  You are also welcome to suggest a more user-friendly API on top of the low-level `obinary` one.


What is not yet supported:

- Transactions (that is next on my TODO list)
- A more user-friendly Document or Graph API, perhaps with JSON marshaling/unmarshaling.  If you want to help design that see Issue #6.
- OrientDB DECIMAL and CUSTOM types.
- Insertion/retrieval of "large" records into OrientDB.  In some cases a few hundred or even a few dozen KB will cause a problem - see Issue #7.
- Some edge cases around RidBags (LinkBags) - the library will panic at present if you hit these. That obviously is not proper behavior, but since this is alpha (or pre-alpha) that's what I'm doing right now.
- A `DBClient` connection pool.  Right now you have to create your DBClients afresh (or find a way to reuse them).


*Documentation Note*: Eventually I will write a detailed wiki on using ogonori with OrientDB, but that will have to wait until the API is stable.  For now the code in the client.go file, plus the godoc for the code is the documentation you'll need to access to see how to use it.


#### Caveat on using ogonori as a golang database/sql API driver

The golang `database/sql` API has some constraints that can be make it painful to work with OrientDB.  For example:

* when you insert a record, the Go `database/sql` API only allows one to return a single int64 identifier for the record, but OrientDB uses as a compound int16:int64 RID, so getting the RID of records you just inserted requires another round trip to the database to query the RID.
* there is no way (that I know of) to specify an OrientDB fetch plan in the SQL only, and the `database/sql` package provides no affordance for adding this. So if you want to pull in additional linked records using a fetch plan (such as `*:-1`), then you'll need to use the ogonori native low-level `obinary` API.

Also, since I don't yet support OrientDB transactions, the `Tx` portion of the `database/sql` API is not yet implemented.

<br/>
## Next TODO

* transactions - transactions in OrientDB are done via optimistic concurrency control (version checking), so the client has to do most of the work; thus, this will take some time

<br/>
## Timeline

I have no projection for when this will be in a ready state.


<br/>
## Development

I am testing on Linux and Windows 7.  I do not have access to any Mac OS X machines, so if someone wants to run the client.go tests on a Mac and tell me the results, that would be helpful.


Right now I have unit tests for the following packages:

* `github.com/dyy18/orientgo/obinary/binserde/varint`
* `github.com/dyy18/orientgo/obinary/rw`
* `github.com/dyy18/orientgo/oschema`

For the higher level functionality I'm using a running functional test - the top-level `client.go`.  Right now to use it you need to have OrientDB 2.x installed and running.

#### How to run client.go:

**OPTION 1**: Set up before hand and only run data statements, not DDL

Before running this test, you can to run the scripts/ogonori-setup.sql
with the `console.sh` program of OrientDB:
  
     ./console.sh ogonori-setup.sql

Then run this code with:

     ./ogonori  (or go run client.go)

**OPTION 2**: Run full DDL - create and drop the database, in between
run the data statements

    ./ogonori full

**OPTION 3**: Run create DDL, but not the drop

    ./ogonori create

After doing this then you can run with

    ./ogonori

to test the data statements only


If that finishes without error, then the test is passing.  If it fails, it should clean up after itself.  If it doesn't you'll need to do:

    ./console.sh
    > connect remote:localhost/ogonoriTest admin admin
    orientdb {db=ogonoriTest}> delete from Cat where name <> 'Linus' AND name <> 'Keiko'

This will be more automated in the future, but is what I have for now.


<br/>
# LICENSE

The MIT License
