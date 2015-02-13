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
