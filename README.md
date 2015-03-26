# Overview

**Ogonori** is a Go client for the OrientDB database.

<br/>
# Status

This project is in early stages and not usable yet.

The primary focus is to build an implementation of the Network Binary Protocol for OrientDB version 2, eventually supporting both Document and Graph DBs

*[Update: 08-Mar-2015]*:

I have much of the low level binary protocol implemented, but still missing some key types in the Deserializer, such as LINK, LINKLIST, DECIMAL, DATE.  The ogonori driver supports queries against document databases.  Creates and updates are not yet supported.  No higher level API work has started yet - for example no conversion from the ogonori document/field structs to JSON.



## Near-term Priorities

* Support database/sql interface (implement database/sql/driver)
* Continue filling in missing deserialization features
* Start on serialization features, which will allow INSERT/UPDATE statements to the database

## TODOs

* support for graph databases (focusing on document dbs first)
* marshal and unmarshal Go structs to OrientDB documents and data structures
 * intend to look closely at the mgo (mongo DB) Go driver for API ideas/compatibility
* transactions - transactions in OrientDB are done via optimistic concurrency control (version checking), so the client has to do most of the work; thus, this will take some time

## Longer-term

* May add support for OrientDB 1.7.x - which requires implementing the CSV serialization format
* May implement support for the HTTP Protocol.


## Timeline

I have no projection for when this will be in a ready state.


## Development

Right now I have high coverage unit tests for the following packages:

* `github.com/quux00/ogonori/obinary/binserde/varint`
* `github.com/quux00/ogonori/obinary/rw`
* `github.com/quux00/ogonori/oschema` (EmbeddedMap only)

For the higher level functionality I'm using a running functional test - the top-level `client.go`.  Right now to use it you need to have OrientDB 2.x installed and running.  And then run one of the OrientDB sql scripts in the `scripts` directory.  Example:

    cd $ORIENTDB_BIN
    ./server.sh
    
    cd $ORIENTDB_BIN
    ./console.sh /path/to/ogonori/scripts/ogonori-setup.sql
    # (or use ogonori-setup-with-drop.sql if you've already run the above)
    cd /path/to/ogonori
    go build
    ./ogonori
    
If that finishes without error, then the test is passing.  If it fails, right now, you'll need to run:

    ./console.sh
    > connect remote:localhost/ogonoriTest admin admin
    orientdb {db=ogonoriTest}> delete from Cat where name <> 'Linus' AND name <> 'Keiko'

This will be more automated in the future, but is what I have for now.


<br/>
# LICENSE

The MIT License
