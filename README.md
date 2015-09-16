# Overview
[![Build Status](https://travis-ci.org/istreamdata/orientgo.svg?branch=master)](https://travis-ci.org/istreamdata/orientgo)

**OrientGo** is a Go client for the [OrientDB](http://orientdb.com/orientdb/) database.

# Important Status Update

**Ownership of the ogonori library transferred to the istreamdata team**

Soon [orientgo](https://github.com/dennwc/orientgo) will be merged as `v2` version of API. You can still use ogonori API using `gopkg.in/istreamdata/orientgo.v1` import path.

----

In Sept 2015, this library will transfer to their ownership.  They have already advanced the codebase, added new features and changed some of the API.  You can get a view of what they have done here:  [dennwc/orientgo](https://github.com/dennwc/orientgo).

I have marked this last commit to quux00/ogonori with a "v1" tag, so if you need pull that down for a transition period, you can use that tag.

Thanks for the support and help from those that have contributed ideas and code.  I will likely be contributing to the istreamdata ogonori codebase in the near future, so my vote is that we rally around their repo as the canonical Golang OrientDB driver.  Once it gets to a solid production state, we can recommend that OrientDB post it on their website as the recommended Go driver for OrientDB.

-Michael Peterson

----


<br/>
# Status

The primary focus of ogonori is to build a Go (golang) client supporting the OrientDB version 2 Network Binary Protocol for both Document and Graph databases.

The istreamdata/orientgo driver is under active development.

As of the v1 tag, here's what you can do with it:

1. Do most any OrientDB SQL statements via `obinary.SQLQuery` and `obinary.SQLCommand`, including support for OrientDB fetch plans.
2. Create `oschema.ODocument` objects and create them in the DB via `obinary.CreateRecord`.
3. Update fields on `oschema.ODocument` objects and update them in the DB via `obinary.UpdateRecord`.
4. Use the ogonori driver for the golang `database/sql` API, with some cautions (see below).
5. Use it with either document or graph databases.
6. Use it with the OrientDB 2.0.x series.  Some of my tests fail with the recent 2.1.x series and I haven't yet worked out the issues, so OrientDB 2.1 is currently not supported.  OrientDB 1.x is also not supported.
7. Run it with multiple goroutines - the unit of thread safety is the `obinary.DBClient`.  As long as each goroutine uses its own `DBClient`, it should work based on my design and testing so far.

Early adopters are welcome to try it out and report any problems found.  You are also welcome to suggest a more user-friendly API on top of the low-level `obinary` one.


What is not yet supported:

1. Transactions (that is next on my TODO list)
2. A more user-friendly Document or Graph API, perhaps with JSON marshaling/unmarshaling.  If you want to help design that see Issue #6.
3. OrientDB DECIMAL and CUSTOM types.
4. Insertion/retrieval of "large" records into OrientDB.  In some cases a few hundred or even a few dozen KB will cause a problem - see Issue #7.
5. Some edge cases around RidBags (LinkBags) - the library will panic at present if you hit these. That obviously is not proper behavior, but since this is alpha (or pre-alpha) that's what I'm doing right now.
6. A `DBClient` connection pool.  Right now you have to create your DBClients afresh (or find a way to reuse them).
7. OrientDB Functions - I haven't looked at these at all, so they might work insofar as you can create and use them only via OrientDB SQL.


*Documentation Note*: Eventually I will write a detailed wiki on using ogonori with OrientDB, but that will have to wait until the API is stable.  For now the code in the client.go file, plus the godoc for the code is the documentation you'll need to access to see how to use it.


#### Caveat on using ogonori as a golang database/sql API driver

The golang `database/sql` API has some constraints that can be make it painful to work with OrientDB.  For example:

* when you insert a record, the Go `database/sql` API only allows one to return a single int64 identifier for the record, but OrientDB uses as a compound int16:int64 RID, so getting the RID of records you just inserted requires another round trip to the database to query the RID.
* there is no way (that I know of) to specify an OrientDB fetch plan in the SQL only, and the `database/sql` package provides no affordance for adding this. So if you want to pull in additional linked records using a fetch plan (such as `*:-1`), then you'll need to use the ogonori native low-level `obinary` API.

Also, since I don't yet support OrientDB transactions, the `Tx` portion of the `database/sql` API is not yet implemented.

<br>
#### [Update: 16-Aug-2015]

* Updates of `oschema.ODocument` fields using `obinary.UpdateRecord` are supported for all datatypes except DECIMAL, CUSTOM and possibly some edge cases around RidBags (not well tested yet.)
* Added concurrent_client test and it passes.  DBClient-per-goroutine model looks to be safe.


<br>
#### [Update: 07-Aug-2015]

* Serialization for most datatypes is done - only DECIMAL and CUSTOM remain - there are Help Wanted Issues for those.
* I have now tested up the 2.0 series on Windows and tests should pass there.  I develop on Linux.  I would love to have someone run the tests on Mac OS X.
* I have started to add support for Updates of records (`REQUEST_RECORD_UPDATE` in binary protocol speak) - these are not well tested and what I'm working on currently
* [ernestas-poskus](https://github.com/ernestas-poskus) is adding travis support and refactoring client.go (yay!), so that should be improved soon.


<br>
#### [Update: 26-July-2015]

Sorry for a bit of a hiatus - I am in the midst of finding a new job so I've spent the last three weekends preparing for technical interviews.  


* Serialization support is now well underway.
* Have serialization implemented for most basic types (INTEGER, DOUBLE, STRING, BYTE, etc)
* Have serialization implemented for DATE and DATETIME
* Have serialization implemented for embedded types: Embedded Records, Embedded Maps, Embedded Lists (and Sets, which are just lists in ogonori)
* Fixed defect in the varint encoder/decoder. It can now handle all 64 bit numbers (where the varint version expands to 9 or 10 bytes long).
* Added `obuf.WriteBuf`, a seekable WriteBuffer, which makes serialization (especially recursive serialization) much cleaner than with `bytes.Buffer`.

* **Note:** I have started to create a list of "Help Wanted" features in the GitHub Issues section.  If you want to contribute ogonori, please take a look at that.  As my job search winds down and I get back to ogonori, I will update that with more features/todos.


<br>
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
## Next TODO

* transactions - transactions in OrientDB are done via optimistic concurrency control (version checking), so the client has to do most of the work; thus, this will take some time

<br/>
## Timeline

I have no projection for when this will be in a ready state.


<br/>
## Development

I am testing on Linux and Windows 7.  I do not have access to any Mac OS X machines, so if someone wants to run the client.go tests on a Mac and tell me the results, that would be helpful.


Right now I have unit tests for the following packages:

* `gopkg.in/istreamdata/orientgo.v1/obinary/binserde/varint`
* `gopkg.in/istreamdata/orientgo.v1/obinary/rw`
* `gopkg.in/istreamdata/orientgo.v1/obuf`
* `gopkg.in/istreamdata/orientgo.v1/oschema`

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
