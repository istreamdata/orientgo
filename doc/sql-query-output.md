### When using Java client

    /127.0.0.1:44332 - Read byte: 41 [OChannelBinaryServer]
    /127.0.0.1:44332 - Reading int (4 bytes)... [OChannelBinaryServer]
    /127.0.0.1:44332 - Read int: 112 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Reading byte (1 byte)... [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Read byte: 115 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Read chunk lenght: 69 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Reading 69 bytes... [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Read 69 bytes: q(select * from Person where name = 'Luke'���� [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing int (4 bytes): 112 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing byte (1 byte): 108 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing short (2 bytes): 0 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing byte (1 byte): 100 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing short (2 bytes): 11 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing long (8 bytes): 0 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing bytes (4+19=23 bytes): [0, 12, 80, 101, 114, 115, 111, 110, 1, 0, 0, 0, 14, 0, 8, 76, 117, 107, 101] [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    {db=cars} /127.0.0.1:44332 - Flush [OChannelBinaryServer]
    /127.0.0.1:44332 - Reading byte (1 byte)... [OChannelBinaryServer]




### Bytes Written by go client
                                                    
    [113 0 0 0 40 115 101 108 101 99 116 32 42 32 102 114 111 109 32 80 101 114 115 111 110
     32 119 104 101 114 101 32 110 97 109 101 32 61 32 39 76 117 107 101 39 
     255 255 255 255 0 0 0 0 255 255 255 255]

### Bytes Written by Java client

               sz |----------- bytes array --->
        [0 0 0 40 115 101 108 101 99 116 32 42 32 102 114 111 109 32 80 101 114 115 111 110 
     32 119 104 101 114 101 32 110 97 109 101 32 61 32 39 76 117 107 101 39 
     -1 -1 -1 -1    0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 ...
     |--limit--|   fetch-plan serialized-params
                                                    
     1 2 3  4   5   6   7   8  9  10 11 12 13  14  15  16  17 18 19  20  21  22  23  24 25  26  27  28  29  30
    [0 0 0 40 115 101 108 101 99 116 32 42 32 102 114 111 109 32 80 101 114 115 111 110 32 119 104 101 114 101
     31  32 33  34  35 36 37 38 39 40  41  42  43 44 45 46 47 48 49  51  53  55  57  59  61  63 64
    [32 110 97 109 101 32 61 32 39 76 117 107 101 39 -1 -1 -1 -1  0 0 0 0 0 0 0 0 0 0 0 0 0 0 0  0
    
    [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
                                                    

objectContent.toStream() ->
[0, 0, 0, 40, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 80, 101, 114, 115, 111, 110, 32, 119, 104, 101, 114, 101, 32, 110, 97, 109, 101, 32, 61, 32, 39, 76, 117, 107, 101, 39, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]    
    
final bytes returned:

    |--- classname--|
    |--- len --| "p"
    [0, 0, 0, 1, 113, 

    |------------------ SQL query ----> 
    |--- len --| "select * from Person where name = 'Luke'"
    0, 0, 0, 40, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 80, 101, 114, 115, 111, 110, 32, 119, 104, 101, 114, 101, 32, 110, 97, 109, 101, 32, 61, 32, 39, 76, 117, 107, 101, 39,
    
    |--- limit --| (48)
    -1, -1, -1, -1, 
    
    |-fetchplan-| (52)   
    0, 0, 0, 0,     
    
    |-qparams-| (56)
    0, 0, 0, 0,   
    
    |-nextPageRID-| (60)
    0,  0,  0,  0,

    |-prev qparams-| (64)
    0,  0,  0,  0 ]
    
    
[41 0 0 0 28 115 
[0, 0, 0, 1, 113, 
 0  0  0  1  113 
 
0, 0, 0, 40, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 80, 101, 114, 115, 111, 110, 32, 119, 104, 101, 114, 101, 32, 110, 97, 109, 101, 32, 61, 32, 39, 76, 117, 107, 101, 39, 
0  0  0  40  115  101  108  101  99  116  32  42  32  102  114  111  109  32  80  101  114  115  111  110  32  119  104  101  114  101  32  110  97  109  101  32  61  32  39  76  117  107  101  39

-1, -1, -1, -1
255 255 255 255

0, 0, 0, 0,
0  0  0  0 

0, 0, 0, 0,
0  0  0  0 

0, 0, 0, 0
0  0  0  0 

0, 0, 0, 0 ]
0  0  0  0]    
    
    
    
    
    final OMemoryStream buffer = super.queryToStream();

    buffer.setUtf8(nextPageRID != null ? nextPageRID.toString() : "");

    // extras added here
    final byte[] queryParams = serializeQueryParameters(previousQueryParams);
    buffer.set(queryParams);

    return buffer;






Writing byte (1 byte): 0 [OChannelBinaryServer]    => SUCCESS
Writing int (4 bytes): 36 [OChannelBinaryServer]   => session-id
Writing byte (1 byte): 108 [OChannelBinaryServer]  => 'l' (Collection)
Writing int (4 bytes): 1 [OChannelBinaryServer]    => collection-size (in resultset)
Writing short (2 bytes): 0 [OChannelBinaryServer]  => short:record-type (always zero for record? not sure how to interpret)
Writing byte (1 byte): 100 [OChannelBinaryServer]  => byte:record-type 'd' == document
Writing short (2 bytes): 11 [OChannelBinaryServer] => short:cluster-id
Writing long (8 bytes): 0 [OChannelBinaryServer]   => long:cluster-pos, so RID is #11:0
Writing int (4 bytes): 1 [OChannelBinaryServer]    => int:record-version
                                V   6   P    e    r    s    o    n     <-- ptr -->     4   L   u    k    e
Writing bytes (4+19=23 bytes): [0, 12, 80, 101, 114, 115, 111, 110, 1, 0, 0, 0, 14, 0, 8, 76, 117, 107, 101] [OChannelBinaryServer]  => record-content                                            ^              EOH
Writing byte (1 byte): 0 [OChannelBinaryServer] => EOColl??         field-id=0



### query with 2 records returning

Read 38 bytes: qselect * from Foo���� [OChannelBinaryServer]
Writing byte (1 byte): 0 [OChannelBinaryServer]
Writing int (4 bytes): 67 [OChannelBinaryServer]
Writing byte (1 byte): 108 [OChannelBinaryServer]  -> 'l' (Collection)
Writing int (4 bytes): 2 [OChannelBinaryServer]    -> 2 records
Writing short (2 bytes): 0 [OChannelBinaryServer]
Writing byte (1 byte): 100 [OChannelBinaryServer]  -> record-type 'd'
Writing short (2 bytes): 12 [OChannelBinaryServer] -> cluster-id
Writing long (8 bytes): 0 [OChannelBinaryServer]    -> cluster-pos (#12:0)
Writing int (4 bytes): 1 [OChannelBinaryServer]
Writing bytes (4+25=29 bytes): 
  [0, 6, 70, 111, 111, 43, 0, 0, 0, 16, 45, 0, 0, 0, 24, 0, 14, 75, 111, 111, 45, 108, 105, 111, 10] [OChannelBinaryServer]
Writing short (2 bytes): 0 [OChannelBinaryServer]  -> short=0 start of new record
Writing byte (1 byte): 100 [OChannelBinaryServer]  -> record-type 'd'
Writing short (2 bytes): 12 [OChannelBinaryServer]
Writing long (8 bytes): 1 [OChannelBinaryServer]   (#12:1)
Writing int (4 bytes): 1 [OChannelBinaryServer]    -> version 1
Writing bytes (4+25=29 bytes): 
  [0, 6, 70, 111, 111, 43, 0, 0, 0, 16, 45, 0, 0, 0, 24, 0, 14, 77, 105, 99, 104, 97, 101, 108, 92] [OChannelBinaryServer]
Writing byte (1 byte): 0 [OChannelBinaryServer]



## select make from Carz => doesn't return a Document (has no "Class")

     orientdb {db=cars}> select make from Carz;
     ----+------+---------
     #   |@CLASS|make     
     ----+------+---------
     0   |null  |Honda    
     1   |null  |Chevrolet
     ----+------+---------

Read 42 bytes: qselect make from Carz���� [OChannelBinaryServer]
Writing byte (1 byte): 0 [OChannelBinaryServer]  -> status
Writing int (4 bytes): 85 [OChannelBinaryServer]  -> session
Writing byte (1 byte): 108 [OChannelBinaryServer]  -> result-type: 'l' (Collection)
Writing int (4 bytes): 2 [OChannelBinaryServer]   -> result-set-size: 2
Writing short (2 bytes): 0 [OChannelBinaryServer]  -> short=0 (means "record", not null or "RID")
Writing byte (1 byte): 100 [OChannelBinaryServer]  -> record-type = 'd' 
Writing short (2 bytes): -2 [OChannelBinaryServer] -> cluster-id -2 => means ??
Writing long (8 bytes): 0 [OChannelBinaryServer]  -> cluster-pos
Writing int (4 bytes): 0 [OChannelBinaryServer]    -> record-version
Writing bytes (4+19=23 bytes): 
    V  ?  4   m    a    k    e <----ptr--->  ?  ?  5   H    o    n    d   a
   [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 13, 7, 0, 10, 72, 111, 110, 100, 97] [OChannelBinaryServer]
idx 0                   5                10        13 
Writing short (2 bytes): 0 [OChannelBinaryServer]
Writing byte (1 byte): 100 [OChannelBinaryServer]
Writing short (2 bytes): -2 [OChannelBinaryServer]
Writing long (8 bytes): 1 [OChannelBinaryServer]
Writing int (4 bytes): 0 [OChannelBinaryServer]
Writing bytes (4+23=27 bytes): 
  [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 13, 7, 0, 18, 67, 104, 101, 118, 114, 111, 108, 101, 116] [OChannelBinaryServer]
Writing byte (1 byte): 0 [OChannelBinaryServer]
Flush [OChannelBinaryServer]




Hello,

I am continuing to work on a Go client and trying to implement the Network Binary Protocol, but I've hit another response I don't understand.

I am doing a query using REQUEST_COMMAND using synchronous command and querying a Document (not using Graphs yet).  When I query for the full document the serialized record seems to be of a slightly different format than when I query for a field of the record.

Here's the query in the Java shell for background:

    orientdb {db=cars}> select * from Carz;   
    ----+-----+------+---------+------
    #   |@RID |@CLASS|make     |model 
    ----+-----+------+---------+------
    0   |#13:0|Carz  |Honda    |Accord
    1   |#13:1|Carz  |Chevrolet|Tahoe 
    ----+-----+------+---------+------
    
    2 item(s) found. Query executed in 0.006 sec(s).
    orientdb {db=cars}> select make from Carz;
    ----+------+---------
    #   |@CLASS|make     
    ----+------+---------
    0   |null  |Honda    
    1   |null  |Chevrolet
    ----+------+---------


When I do a REQUEST_COMMAND query with the query "select * from Carz" I get back a serialized record that does not quite follow the for Schemaless Serialization (https://raw.githubusercontent.com/wiki/orientechnologies/orientdb/Record-Schemaless-Binary-Serialization.md).  Instead I get back the "alternative" serialization format I outlined in my previous posting: https://groups.google.com/d/msg/orient-database/IDItY72Ze6U/pP4lgfT8S1UJ

But when I do a REQUEST_COMMAND query with the query "select make from Carz", I now get back a serialized record that looks like it exactly matches the documented Schemaless Serialization, rather than the "alternative" serialization format (I don't know what else to call it, since it appears to be undocumented.)  

Why is there an inconsistency and can this be fixed?  It's very unclear what is going on.

Here's the breakdown:


    select * from Carz

    Read 39 bytes: qselect * from Carz���� [OChannelBinaryServer]
    Writing byte (1 byte): 0 [OChannelBinaryServer]   -> status                                      
    Writing int (4 bytes): 87 [OChannelBinaryServer]  -> session                                     
    Writing byte (1 byte): 108 [OChannelBinaryServer] -> result-type: 'l' (Collection)               
    Writing int (4 bytes): 2 [OChannelBinaryServer]   -> result-set-size: 2                          
    Writing short (2 bytes): 0 [OChannelBinaryServer] -> short=0 (means "record", not null or "RID") 
    Writing byte (1 byte): 100 [OChannelBinaryServer] -> record-type = 'd'                           
    Writing short (2 bytes): 13 [OChannelBinaryServer]-> cluster-id (13)
    Writing long (8 bytes): 0 [OChannelBinaryServer]  -> cluster-pos , so rid is #13:0                                
    Writing int (4 bytes): 1 [OChannelBinaryServer]   -> record-version                              
    Writing bytes (4+30=34 bytes): 
     [0, 8, 67, 97, 114, 122, 47, 0, 0, 0, 17, 49, 0, 0, 0, 23, 0, 10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100] [OChannelBinaryServer]
    Writing short (2 bytes): 0 [OChannelBinaryServer]
    Writing byte (1 byte): 100 [OChannelBinaryServer]
    Writing short (2 bytes): 13 [OChannelBinaryServer]
    Writing long (8 bytes): 1 [OChannelBinaryServer]
    Writing int (4 bytes): 1 [OChannelBinaryServer]
    Writing bytes (4+33=37 bytes): 
     [0, 8, 67, 97, 114, 122, 47, 0, 0, 0, 17, 49, 0, 0, 0, 27, 0, 18, 67, 104, 101, 118, 114, 111, 108, 101, 116, 10, 84, 97, 104, 111, 101] [OChannelBinaryServer]
    Writing byte (1 byte): 0 [OChannelBinaryServer]
    Flush [OChannelBinaryServer]

    
Analyzing the first serialized record, this is the "alternative" serialized format:

   Version
   |---|-----Classname------|--------------Header-----------------| ... cont'd below ...
        len |---- string ---| PID <----ptr--> PID <----ptr---> EOH
         4   C  a    r    z       n                               
     [0, 8, 67, 97, 114, 122, 47, 0, 0, 0, 17, 49, 0, 0, 0, 23, 0,
idx:  0  1   2   3    4    5   6  7  8  9  10  11 12 13 14  15 16 

   |---------------------------Data-------------------------| 
   |len |-------string------| len |---------string----------| 
     5   H   o    n    d   a       A   c   c    o    r    d   
    10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100] 
    17  18                 22  23  24                     29  


The header here is what is "alternative" - instead of reguilar zigzag encoding it uses the formala:

    zigzagEncode( (fieldId+1) * -1 )
    
    to encode the Property/field ID, but not the name of the Property/field.



Compare that to:
    
    select make from Carz
    Read 42 bytes: qselect make from Carz���� [OChannelBinaryServer]
    Writing byte (1 byte): 0 [OChannelBinaryServer]    -> status
    Writing int (4 bytes): 85 [OChannelBinaryServer]   -> session
    Writing byte (1 byte): 108 [OChannelBinaryServer]  -> result-type: 'l' (Collection)
    Writing int (4 bytes): 2 [OChannelBinaryServer]    -> result-set-size: 2
    Writing short (2 bytes): 0 [OChannelBinaryServer]  -> short=0 (means "record", not null or "RID")
    Writing byte (1 byte): 100 [OChannelBinaryServer]  -> record-type = 'd' 
    Writing short (2 bytes): -2 [OChannelBinaryServer] -> cluster-id -2 => means ????
    Writing long (8 bytes): 0 [OChannelBinaryServer]   -> cluster-pos ??
    Writing int (4 bytes): 0 [OChannelBinaryServer]    -> record-version                              
    Writing bytes (4+19=23 bytes): 
        V  ?  4   m    a    k    e <----ptr--->  ?  ?  5   H    o    n    d   a
       [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 13, 7, 0, 10, 72, 111, 110, 100, 97] [OChannelBinaryServer]
    idx 0                   5                10        13 
    Writing short (2 bytes): 0 [OChannelBinaryServer]
    Writing byte (1 byte): 100 [OChannelBinaryServer]
    Writing short (2 bytes): -2 [OChannelBinaryServer]
    Writing long (8 bytes): 1 [OChannelBinaryServer]    -> cluster-pos ??
    Writing int (4 bytes): 0 [OChannelBinaryServer]
    Writing bytes (4+23=27 bytes): 
      [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 13, 7, 0, 18, 67, 104, 101, 118, 114, 111, 108, 101, 116] [OChannelBinaryServer]
    Writing byte (1 byte): 0 [OChannelBinaryServer]
    Flush [OChannelBinaryServer]
    

Analyzing the first serialized record, this looks like the documented serialization format:

       |-|--|--------------- Header -----------------|---------- Data ---------|
        V CN  4   m    a    k    e <----ptr--->  ?  ?  5   H    o    n    d   a
       [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 13, 7, 0, 10, 72, 111, 110, 100, 97]
    idx 0  1  2   3              6  7        10 11 12  13  14                 18
    
    
idx 0   : serialization version (0)
idx 1   : classname => string of length 0, no classname
idx 2   : "normally" encoded varint (rather than the strange version used for the full Document), sz = 4
idx 3-6 : bytes for "make" (the field name)
idx 7-10: int - ptr to data
idx 11  : data_type (7 = string)
idx 12  : ? end of header, I guess
idx 13  : "normally" encoded varint, sz = 5
idx 14-18: bytes for "Honda", the value for field "make"

So here the Header uses regular zigzag encoding and gives the field name, NOT the field id.


So to summarize my questions:

* is my interpretation of the serialized formats above correct?
* why are we using two different serialization formats?
* how is my driver to know which serialization format is being returned?  The only difference is the cluster-id is -2.  I haven't located any documentation as to what that value means.


      |-|--|------------------------------- Header -----------------------------------------------|--------------------------- Data -----------------------|
       V CN  4   m    a    k    e <----ptr---> str 5    m    o    d    e    l <----ptr---> str EOH 5   H    o    n    d   a   6   A   c   c    o    r    d

      [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 24, 7, 10, 109, 111, 100, 101, 108, 0, 0, 0, 30, 7, 0, 10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100] [OChannelBinaryServer]
