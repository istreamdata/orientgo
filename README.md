# Overview

**Ogonori** is a Go client for the OrientDB database.

<br/>
# Status

This project is in early stages and not usable yet.

The primary focus is to build an implementation of the Network Binary Protocol for OrientDB version 2, eventually supporting both Document and Graph DBs

*[Update: 03-May-2015]*:

I've added support for OrientDB LINKs and fetch plans.  LINK, LINKLIST, LINKSET and LINKMAP are all supported and fetch plans that pull extended or "supplementary" links in are hydrated into Documents from the query.

The OrientDB Java driver puts "supplementary documents" into a shared LRU cache. For now, I have chosen not to go this route.  Instead, I resolve all the references in the query before the documents/records are returned from the `SQLQuery` command.  For the cases I've tested so far, this seems to work fine.  I've tested circular links and it works fine.  Where it will be problematic is when a query returns a large dataset - resolving all of them could be slow or incomplete if paging (limits) are used.  I may decide to add in a LRU cache later.  I plan on looking at https://github.com/coocood/freecache for clever ideas on ways to do this.




*[Update: 18-Apr-2015]*:

I had to take a few weeks off to work on something else, but now I'm back.  The most recent changes include:

* Support for database/sql is partially in place.  The part that is lacking is Tx (transactions).  That is deferred until I have a chance to review how I will implement optimistic currency control transactions in ogonori.  (I will borrow heavily from how the Java client does it.)

* The obinary.SQLCommand and obinary.SQLQuery methods (the primary workhorses) were refactored
now that I better understand the various "data structures" that the OrientDB server can return.  The ODocument and OField model I'm using seems to have held up well, so still using it (including the distinction between OField and OProperty).

* Significantly enhanced the `client.go` functional test to have a wide range of DDL and SQL/data statements, including use of the Go sql driver that is currently supported.


*[Update: 08-Mar-2015]*:

I have much of the low level binary protocol implemented, but still missing some key types in the Deserializer, such as LINK, LINKLIST, DECIMAL, DATE.  The ogonori driver supports queries against document databases.  Creates and updates are not yet supported.  No higher level API work has started yet - for example no conversion from the ogonori document/field structs to JSON.



## Near-term Priorities

* Continue to test database/sql implementation
* Continue filling in missing deserialization features
* Start on serialization features, which will allow INSERT/UPDATE statements to the database

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

     ./ogonori

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
