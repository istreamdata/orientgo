# Overview

**Ogonori** is a Go client for the OrientDB database.

<br/>
# Status

This project is in early stages and not usable yet.

The primary focus is to build an implementation of the Network Binary Protocol for OrientDB version 2, eventually supporting both Document and Graph DBs

*[Update: 08-Mar-2015]*:

I have much of the low level binary protocol implemented, but still missing some key types in the Deserializer, such as LINK, LINKLIST, DECIMAL, DATE.  The ogonori driver supports queries against document databases.  Creates and updates are not yet supported.  No higher level API work has started yet - for example no conversion from the ogonori document/field structs to JSON.



## Near-term Priorities

* Continue filling in missing deserialization features
* Start on serialization features, which will allow INSERT/UPDATE statements to the database

## TODOs

* support for graph databases (focusing on document dbs first)
* marshal and unmarshal Go structs to OrientDB documents and data structures
 * intend to look closely at the mgo (mongo DB) Go driver for API ideas/compatibility
* support database/sql interface (implement database/sql/driver)
* transactions - transactions in OrientDB are done via optimistic concurrency control (version checking), so the client has to do most of the work; thus, this will take some time

## Longer-term

* May add support for OrientDB 1.7.x - which requires implementing the CSV serialization format
* May implement support for the HTTP Protocol.


## Timeline

I have no projection for when this will be in a ready state.

<br/>
# LICENSE

The MIT License
