--------------------------------------
-- Alternative serialization format -- => used for Property
--------------------------------------
-- PID=property-id and it is an odd number

       Version
       |---|-----Classname------|--------------Header-----------------| ...
            len |---- string ---| PID <----ptr--> PID <----ptr---> EOH
             4   C  a    r    z       n                              
         [0, 8, 67, 97, 114, 122, 47, 0, 0, 0, 17, 49, 0, 0, 0, 23, 0,
    idx:  0  1   2   3    4    5   6  7  8  9  10  11 12 13 14  15 16
    
       |---------------------------Data-------------------------|
       |len |-------string------| len |---------string----------|
         5   H   o    n    d   a       A   c   c    o    r    d  
        10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100]
        17  18                 22  23  24                     29  

-------------------------------------
-- Schemaless serialization format -- => used for Document
-------------------------------------

       |-|--|--------------- Header -------------------|---------- Data ---------|
        V CN  4   m    a    k    e <----ptr---> TYP EOH  5   H    o    n    d   a
       [0, 0, 8, 109, 97, 107, 101, 0, 0, 0, 13, 7,  0, 10, 72, 111, 110, 100, 97]
    idx 0  1  2   3              6  7        10 11  12  13  14                 18

EOH = end of head er
TYP = data type (7=string)
CN here is 0 (no classname), but is a typical string (len, followed by chars)



==================
# LINKMAP
==================

    2015-04-24 21:35:07:977 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Read 108 bytes: caINSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": #10:0, "mom": #10:1} [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing int (4 bytes): 2293 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing byte (1 byte): 114 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing short (2 bytes): 0 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing byte (1 byte): 100 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing short (2 bytes): 10 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing long (8 bytes): 6 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing bytes (4+55=59 bytes): 

       |--Classname--|----------------------------- Header ---------------------------------|
     V  3   C   a   t  Pid<---ptr----> Pid <---ptr----> Pid <---ptr----> Pid <---ptr----> EOH
    [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 34, 43, 0, 0, 0, 35, 57, 0, 0, 0, 40, 0,  
    
    len|-----------string-------------| age len|----string------|len  TYP  len |---string--| <RID> TYP  len |--string---| <RID>
     7   C   h   a    r    l    i    e    5  4  A    n    n   a   2  STRNG  3   b   f    f   10  0 STRNG 3   m    o    m   10  1
    14, 67, 104, 97, 114, 108, 105, 101, 10, 8, 65, 110, 110, 97, 4,  7,    6, 98, 102, 102, 20, 0, 7,   6, 109, 111, 109, 20, 2] 
    
    [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    2015-04-24 21:35:07:996 INFO  {db=ogonoriTest} /127.0.0.1:35726 - Flush [OChannelBinaryServer]


    2015-04-24 22:35:12:580 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Read 105 bytes: c^INSERT INTO Cat SET name='Charlie', age=5, caretaker='Anna', notes = {"bff": #10:0, 30: #10:1} [OChannelBinaryServer]

    [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 34, 43, 0, 0, 0, 35, 57, 0, 0, 0, 40, 0, 
    
    len|-----------string-------------| age len|----string------|len  TYP  len |---string--| <RID> TYP  len |--string--| <RID>
     7   C   h   a    r    l    i    e    5  4  A    n    n   a   2  STRNG  3   b   f    f   10  0 STRNG 2   "3  0"      10  1
    14, 67, 104, 97, 114, 108, 105, 101, 10, 8, 65, 110, 110, 97, 4, 7,     6, 98, 102, 102, 20, 0, 7,   4,  51, 48,     20, 2]


    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing int (4 bytes): 2338 [OChannelBinaryServer]
    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing byte (1 byte): 114 [OChannelBinaryServer]
    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing short (2 bytes): 0 [OChannelBinaryServer]
    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing byte (1 byte): 100 [OChannelBinaryServer]
    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing short (2 bytes): 10 [OChannelBinaryServer]
    2015-04-24 22:35:12:601 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing long (8 bytes): 6 [OChannelBinaryServer]
    2015-04-24 22:35:12:602 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    2015-04-24 22:35:12:602 INFO  {db=ogonoriTest} /127.0.0.1:36084 - Writing bytes (4+54=58 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 34, 43, 0, 0, 0, 35, 57, 0, 0, 0, 40, 0, 14, 67, 104, 97, 114, 108, 105, 101, 10, 8, 65, 110, 110, 97, 4, 7, 6, 98, 102, 102, 20, 0, 7, 4, 51, 48, 20, 2] [OChannelBinaryServer]





###
 EMBEDDED
###

Recursively calls serialize, since it is an ODocument

2015-07-04 20:37:18:581 INFO  /127.0.0.1:39660 - Read byte: 31 [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  /127.0.0.1:39660 - Reading int (4 bytes)... [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  /127.0.0.1:39660 - Read int: 1517 [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Reading short (2 bytes)... [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Read short: -1 [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Read chunk lenght: 88 [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Reading 88 bytes... [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Read 88 bytes: 
Dalename$episode+],
                    dalek88
DingnamePageScashTddXB� [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Reading byte (1 byte)... [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Read byte: 100 [OChannelBinaryServer]
2015-07-04 20:37:18:581 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Reading byte (1 byte)... [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Read byte: 0 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Writing byte (1 byte): 0 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Writing int (4 bytes): 1517 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Writing short (2 bytes): 12 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Writing long (8 bytes): 2 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Writing int (4 bytes): 3 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Writing int (4 bytes): 0 [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  {db=ogonoriTest} /127.0.0.1:39660 - Flush [OChannelBinaryServer]
2015-07-04 20:37:18:582 INFO  /127.0.0.1:39660 - Reading byte (1 byte)... [OChannelBinaryServer]
2015-07-04 20:37:18:584 INFO  /127.0.0.1:39660 - Read byte: 5 [OChannelBinaryServer]
2015-07-04 20:37:18:584 INFO  /127.0.0.1:39660 - Reading int (4 bytes)... [OChannelBinaryServer]
2015-07-04 20:37:18:584 INFO  /127.0.0.1:39660 - Read int: 1517 [OChannelBinaryServer]
2015-07-04 20:37:18:584 INFO  /127.0.0.1:39660 - Reading byte (1 byte)... [OChannelBinaryServer]


    ODocument dingo = new ODocument("Dingo");
    dingo.field("name", "dd", OType.STRING);
    dingo.field("age", 44, OType.INTEGER);
    dingo.field("cash", 37.73, OType.FLOAT);
    
    ODocument doc = new ODocument("Dalek");
    doc.field("name", "dalek8");
    doc.field("EE", dingo, OType.EMBEDDED);

Serialization of EMBEDDED


 V   5   D   a   l    e    k   4   n    a   m    e   <-- ptr --> TYP  ?  <-- ptr --> EOH   6   d    a   l    e    k    8   5   D   i    n    g    o
[0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 23, 7, 93, 0, 0, 0, 30, 0,  12, 100, 97, 108, 101, 107, 56, 10, 68, 105, 110, 103, 111,

4   n    a   m    e   <-- ptr --> TYP  3   a   g    e   <-- ptr --> TYP  4   c   a   s    h   <-- ptr --> TYP EOH  2   d    d  <69> <=== float =====>
8, 110, 97, 109, 101, 0, 0, 0, 66, 7,  6, 97, 103, 101, 0, 0, 0, 69, 1,  8, 99, 97, 115, 104, 0, 0, 0, 70, 4,  0,  4, 100, 100, 88, 66, 22, -21, -123]


 V   5   D   a   l    e    k   4   n    a   m    e   <-- ptr --> TYP  ?  <-- ptr --> EOH   6   d    a   l    e    k    8   5   D   i    n    g    o
[0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 23, 7, 95, 0, 0, 0, 30, 0,  12, 100, 97, 108, 101, 107, 56, 10, 68, 105, 110, 103, 111,

4   n    a   m    e   <-- ptr --> TYP  3   a   g    e   <-- ptr --> TYP EOH  2   f    o    o  vint
8, 110, 97, 109, 101, 0, 0, 0, 56, 7,  6, 97, 103, 101, 0, 0, 0, 60, 1,  0,  6, 102, 111, 111, 88]


[0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 23, 7, 95, 0, 0, 0, 30, 0, 12, 100, 97, 108, 101, 107, 56, 10, 68, 105, 110, 103, 111, 8, 110, 97, 109, 101, 0, 0, 0, 56, 7, 6, 97, 103, 101, 0, 0, 0, 60, 1, 0, 6, 102, 111, 111, 88]


[  0  10  68  97 108 101 107   8 110  97 109 101   0   0   0  23   7  95   0   0   0  30   0  12 100  97 108 101 107  56  10  68 105 110 103 111   8 110  97 109 101   0   0   0  56   7   6  97 103 101   0   0   0  60   1   0   6 102 111 111  88 ]
[  0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33  34  35  36  37  38  39  40  41  42  43  44  45  46  47  48  49  50  51  52  53  54  55  56  57  58  59  60 ]



