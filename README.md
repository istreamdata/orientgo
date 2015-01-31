# Overview

**Ogonori** is a Go client for the OrientDB database.

# Status

This project is in early stages and not usable yet.

I am starting with an implementation of the Binary Protocol for OrientDB version 2 and OrientDB 1.7.x - there are two serialization formats to implement for that also.

Things on the overall todo list:

* marshal and unmarshal Go structs to OrientDB documents and data structures
** intend to look closely at the mgo (mongo DB) Go driver for ideas/compatibility
* support database/sql interface (implement database/sql/driver)
* implement support for the HTTP Protocol.
* add support for older versions of OrientDB.

I have no projection for when this will be in a ready state.


# LICENSE

The MIT License
