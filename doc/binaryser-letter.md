Hello,

I am continuing work on a Golang driver for OrientDB and working on the binary network protocol. I am wondering if someone can help me interpret the output of a REQUEST_RECORD_LOAD request.

I am using orientdb-community-2.0-rc2 and orientdb-community-2.1 to test against.

For reference when I do the query with the command line client here's the result:

orientdb {db=cars}> load record #11:0

+---------------------------------------------------------------------+
| Document - @class: Person             @rid: #11:0      @version: 1  |
+---------------------------------------------------------------------+
|                     Name | Value                                    |
+---------------------------------------------------------------------+
|                     name | Luke                                     |
+---------------------------------------------------------------------+

When I do it with my golang client specifying the SCHEMALESS binary serialization format, here's what the server sends back (my annotations included):

Reading byte (1 byte)... [OChannelBinaryServer]
Read byte: 30 [OChannelBinaryServer]  => REQUEST_RECORD_LOAD
Reading int (4 bytes)... [OChannelBinaryServer]
Read int: 59 [OChannelBinaryServer]   => session-id
Reading short (2 bytes)... [OChannelBinaryServer]
Read short: 11 [OChannelBinaryServer] => cluster-id
Reading long (8 bytes)... [OChannelBinaryServer]
Read long: 0 [OChannelBinaryServer]   => cluster-position
Reading string (4+N bytes)... [OChannelBinaryServer]
Read string:  [OChannelBinaryServer]  => fetch plan (empty string)
Reading byte (1 byte)... [OChannelBinaryServer]
Read byte: 0 [OChannelBinaryServer]   => ignore-cache
Reading byte (1 byte)... [OChannelBinaryServer]
Read byte: 0 [OChannelBinaryServer]   => load-tombstones

Writing byte (1 byte): 0 [OChannelBinaryServer]   => status: SUCCESS
Writing int (4 bytes): 59 [OChannelBinaryServer]  => session-id
Writing byte (1 byte): 1 [OChannelBinaryServer]   => payload-status: record=resultset
Writing byte (1 byte): 100 [OChannelBinaryServer] => record-type: 'd' (ascii 100) = document
Writing int (4 bytes): 1 [OChannelBinaryServer]   => record-version 
Writing bytes (4+19=23 bytes): [0, 12, 80, 101, 114, 115, 111, 110, 1, 0, 0, 0, 14, 0, 8, 76, 117, 107, 101] [OChannelBinaryServer]  => record-content (see below)
Writing byte (1 byte): 0 [OChannelBinaryServer] => payload-status: no more records

Everything looks good except for how to interpret the record-content bytes.  They don't look like what I would expect from this spec: https://raw.githubusercontent.com/wiki/orientechnologies/orientdb/Record-Schemaless-Binary-Serialization.md

      Version
      |---|----------Classname-----------|------Header--------|--------Data--------|
           len |-------- string ---------|  ?  ?  ?  ? ptr  ? |len |----string-----|
            6   P    e    r    s   o    n                      4   L   u    k    e
bytes: [0, 12, 80, 101, 114, 115, 111, 110, 1, 0, 0, 0, 14, 0, 8, 76, 117, 107, 101]
idx  :  0   1   2    3    4    5    6    7  8  9 10 11  12 13 14  15   16   17   18

The version, classname and data sections look right.  But I can't figure out the header piece.  It is supposed to be

    +--------------------------+-------------------+-------------------------------+----------------+
    | field_name_length:varint | field_name:byte[] | pointer_to_data_structure:int | data_type:byte |
    +--------------------------+-------------------+-------------------------------+----------------+

But the field_name_length and field_name seem to be missing.  The ptr-to-data looks right (idx 12 is "14", which points to the start of the data section).

The last byte of the header (idx 13) is 0 and that maps to "boolean" type according to this page: https://github.com/orientechnologies/orientdb/wiki/Types, but that is wrong, since the data type is of type string


I also tried to compare it to what this proposal doc says: https://groups.google.com/forum/#!searchin/orient-database/varint$20variable$20length$20int/orient-database/8r1ES_LEDxE/rwdpxjMr-BQJ

but I am having trouble making that work.

Please help.

-Michael
