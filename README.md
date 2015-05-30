# Overview

**Ogonori** is a Go client for the [OrientDB](http://orientdb.com/orientdb/) database.

<br/>
# Status

This project is in early stages and the API is unstable.

However, the SQL statements that a project will typically need to do can be done now using either the golang `database/sql` API or the current function-based API in the ogonori/obinary package.  The obinary package is intended as low-level and eventually a friendlier object-based API will probably be built on top of it.

Note that the database/sql API has some constraints that can be make it painful to work with OrientDB.  For example:
* when you insert a record, the Go `database/sql` API only allows one to return a single int64 identifier for the record, but OrientDB uses as a compound int16:int64 RID, so getting the RID of records you just inserted requires another round trip to the database to query the RID.
* there is no way (that I know of) to specify an OrientDB fetch plan in the SQL only, and the `database/sql` package provides no affordance for adding this. So if you want to pull in additional linked records using a fetch plan (such as `*:-1`), then you'll need to use the ogonori native low-level `obinary` API.

Early adopters are welcome to try it out and report any problems found.  You are also welcome to suggest a more user-friendly API on top of the low-level `obinary` one.

Little work has been done on the serialization front, so supporting object creation on the client side followed by saves (standalone or in transactions) are not yet supported.

The primary focus is of ogonori is to build a Go (golang) client supporting OrientDB version 2 Network Binary Protocol for both Document and Graph databases.

*Documentation*: Eventually I will write a detailed wiki on using ogonori with OrientDB, but that will have to wait until the API is stable.  For now the code in the client.go file, plus the godoc for the code is the documentation you'll need to access to see how to use it.


#### [Update: 29-May-2015]

__Highlights__

* Support for LinkBags (RidBags) are now in place.  Graph databases tend to make heavy use of LinkBags and the earlier defect account in Issue #3 is now resolved.
* Settled on "Fetch" to mean methods that pull from the database server and "Get" to mean methods that return values in the local objects.  
* Created new `oschema.ORID` struct and stopped using string RIDs in the ogonori code base

__Details__

In order to support LinkBags I needed a "seekable" ByteBuffer, so I wrote obuf.ByteBuf, which is currently a read-only buffer with `Seek` (absolute) and `Skip` (relative) methods.  The Deserializer now takes obuf.ByteBuf rather than the stdlib bytes.Buffer, which is not seekable.

Fetch vs. Get: The OrientDB Java client is not at all transparent about which operations cause database lookups.  Ogonori will strive to be transparent on this front.



<br/>
#### [Update: 03-May-2015]

__Highlights__

* Support for DATE and DATETIME now in place. (Deserialization only)
* Support for OrientDB LINKs and fetch plans in place. (Deserialization only)
* The client.go functional test does a better job of clean up, but it's getting very large and needs to be refactored itself.
* A large portion of OrientDB DDL and SQL now supported.  See client.go test for examples.

__Details__

I've added support for OrientDB LINKs and fetch plans.  LINK, LINKLIST, LINKSET and LINKMAP are all supported and fetch plans that pull extended or "supplementary" links in are hydrated into Documents from the query.

The OrientDB Java driver puts "supplementary documents" into a shared LRU cache. For now, I have chosen not to go this route.  Instead, I resolve all the references in the query before the documents/records are returned from the `SQLQuery` command.  For the cases I've tested so far, this seems to work fine.  I've tested circular links and it works fine.  Where it will be problematic is when a query returns a large dataset - resolving all of them could be slow or incomplete if paging (limits) are used.  I may decide to add in a LRU cache later.  I plan on looking at [https://github.com/coocood/freecache](https://github.com/coocood/freecache) for clever ideas on ways to do this.

So far I've tested the LINKs via a Document Database only.  Most work on Graph DBs has been deferred.

The Deserialization code is much cleaner now, though parts are still missing, such as handling "flat data" and CUSTOM, LINKBAG and DECIMAL types.  Those are advanced features I haven't seen in action yet.

I also discovered that the headers returned by the network binary serialization format can be mixed - some headers have both property names and property ids in the same header.  That was unexpected, so I had to make a number of changes to support that.  I believe that is now properly handled.


<br/>
#### [Update: 18-Apr-2015]

I had to take a few weeks off to work on something else, but now I'm back.  The most recent changes include:

* Support for database/sql is partially in place.  The part that is lacking is Tx (transactions).  That is deferred until I have a chance to review how I will implement optimistic currency control transactions in ogonori.  (I will borrow heavily from how the Java client does it.)

* The obinary.SQLCommand and obinary.SQLQuery methods (the primary workhorses) were refactored
now that I better understand the various "data structures" that the OrientDB server can return.  The ODocument and OField model I'm using seems to have held up well, so still using it (including the distinction between OField and OProperty).

* Significantly enhanced the `client.go` functional test to have a wide range of DDL and SQL/data statements, including use of the Go sql driver that is currently supported.


<br/>
#### [Update: 08-Mar-2015]

I have much of the low level binary protocol implemented, but still missing some key types in the Deserializer, such as LINK, LINKLIST, DECIMAL, DATE.  The ogonori driver supports queries against document databases.  Creates and updates are not yet supported.  No higher level API work has started yet - for example no conversion from the ogonori document/field structs to JSON.


<br/>

## Near-term Priorities

* Refactor client.go functional test into smaller more readable pieces
* Start on serialization features
* Work out what the "native" ogonori API is going to be - right now most everything is done via DDL and SQL strings.  The serialization features will be needed to support this.

## TODOs

* transactions - transactions in OrientDB are done via optimistic concurrency control (version checking), so the client has to do most of the work; thus, this will take some time
* support for graph databases (focusing on document dbs first)
* marshal and unmarshal Go structs to OrientDB documents and data structures
 * intend to look closely at the mgo (mongo DB) Go driver for API ideas/compatibility

## Longer-term

* May add support for OrientDB 1.7.x - which requires implementing the CSV serialization format


## Timeline

I have no projection for when this will be in a ready state.


## Development

So far I've been testing on Linux only.  I will start testing on Windows soon.  I do not have access to any Mac OS X machines, so if someone wants to run the client.go tests on a Mac and tell me the results, that would be helpful.


Right now I have high coverage unit tests for the following packages:

* `github.com/quux00/ogonori/obinary/binserde/varint`
* `github.com/quux00/ogonori/obinary/rw`
* `github.com/quux00/ogonori/oschema` (EmbeddedMap only)

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
