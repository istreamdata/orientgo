Read byte: 41 [OChannelBinaryServer]
Reading int (4 bytes)... [OChannelBinaryServer]
Read int: 2853 [OChannelBinaryServer]
/127.0.0.1:40420 - Reading byte (1 byte)... [OChannelBinaryServer]
/127.0.0.1:40420 - Read byte: 115 [OChannelBinaryServer]
/127.0.0.1:40420 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
/127.0.0.1:40420 - Read chunk lenght: 83 [OChannelBinaryServer]
/127.0.0.1:40420 - Reading 83 bytes... [OChannelBinaryServer]
/127.0.0.1:40420 - Read 83 bytes: qBSELECT from Person where any() traverse(0,2) (firstName = 'Abbie')���� [OChannelBinaryServer]
/127.0.0.1:40420 - Writing byte (1 byte): 0 [OChannelBinaryServer]     <- status OK
/127.0.0.1:40420 - Writing int (4 bytes): 2853 [OChannelBinaryServer]  <- session-id
/127.0.0.1:40420 - Writing byte (1 byte): 108 [OChannelBinaryServer]   <- 'l' collection of records
/127.0.0.1:40420 - Writing int (4 bytes): 2 [OChannelBinaryServer]     <- collection size
/127.0.0.1:40420 - Writing short (2 bytes): 0 [OChannelBinaryServer]   <- 0=full record
/127.0.0.1:40420 - Writing byte (1 byte): 100 [OChannelBinaryServer]   <- 'd' (document)
/127.0.0.1:40420 - Writing short (2 bytes): 11 [OChannelBinaryServer]  <- cluster-id (RID p1)
/127.0.0.1:40420 - Writing long (8 bytes): 1 [OChannelBinaryServer]    <- cluster-pos (RID p2)
/127.0.0.1:40420 - Writing int (4 bytes): 3 [OChannelBinaryServer]     <- version
/127.0.0.1:40420 - Writing bytes (4+103=107 bytes): [0, 12, 80, 101, 114, 115, 111, 110, 18, 102, 105, 114, 115, 116, 78, 97, 109, 101, 0, 0, 0, 63, 7, 16, 108, 97, 115, 116, 78, 97, 109, 101, 0, 0, 0, 69, 7, 6, 83, 83, 78, 0, 0, 0, 76, 7, 20, 111, 117, 116, 95, 70, 114, 105, 101, 110, 100, 0, 0, 0, 88, 22, 0, 10, 65, 98, 98, 105, 101, 12, 87, 105, 108, 115, 111, 110, 22, 53, 53, 53, 45, 53, 53, 45, 53, 53, 53, 53, 1, 0, 0, 0, 1, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0] [OChannelBinaryServer]
/127.0.0.1:40420 - Writing short (2 bytes): 0 [OChannelBinaryServer]
/127.0.0.1:40420 - Writing byte (1 byte): 100 [OChannelBinaryServer]
/127.0.0.1:40420 - Writing short (2 bytes): 11 [OChannelBinaryServer]
/127.0.0.1:40420 - Writing long (8 bytes): 2 [OChannelBinaryServer]
/127.0.0.1:40420 - Writing int (4 bytes): 2 [OChannelBinaryServer]
/127.0.0.1:40420 - Writing bytes (4+100=104 bytes): [0, 12, 80, 101, 114, 115, 111, 110, 18, 102, 105, 114, 115, 116, 78, 97, 109, 101, 0, 0, 0, 62, 7, 16, 108, 97, 115, 116, 78, 97, 109, 101, 0, 0, 0, 67, 7, 6, 83, 83, 78, 0, 0, 0, 73, 7, 18, 105, 110, 95, 70, 114, 105, 101, 110, 100, 0, 0, 0, 85, 22, 0, 8, 90, 101, 107, 101, 10, 82, 111, 115, 115, 105, 22, 52, 52, 52, 45, 52, 52, 45, 52, 52, 52, 52, 1, 0, 0, 0, 1, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0] [OChannelBinaryServer]
/127.0.0.1:40420 - Writing byte (1 byte): 0 [OChannelBinaryServer]


===========================
   | -------- classname ---------- |
 V       P   e    r    s    o    n    9   f    i    r    s    t    N   a   m    e  <--- ptr --> TYP 
[0, 12, 80, 101, 114, 115, 111, 110, 18, 102, 105, 114, 115, 116, 78, 97, 109, 101, 0, 0, 0, 63, 7,

 8   l    a   s    t    N   a   m    e  <--- ptr --> TYP|3   S   S   N <--- ptr --> TYP
16, 108, 97, 115, 116, 78, 97, 109, 101, 0, 0, 0, 69, 7, 6, 83, 83, 78, 0, 0, 0, 76, 7,

        HEADER, contd ------------------------------------------------|
                                                                LNKBAG
|10   o    u    t   _    F   r    i    e    n    d <--- ptr --> TYP EOH
20, 111, 117, 116, 95, 70, 114, 105, 101, 110, 100, 0, 0, 0, 88, 22, 0,


| ------------------------ DATA SECTION ---------------------------- >>>>
 5   A   b   b   i    e    6   W   i    l    s    o    n   11   5   5   5   -   5   5   -  5   5   5   5   
10, 65, 98, 98, 105, 101, 12, 87, 105, 108, 115, 111, 110, 22, 53, 53, 53, 45, 53, 53, 45, 53, 53, 53, 53,

(---- LINK BAG -----------------------------)
           int32     in16          int64
 EMBDD  (----1----) ( 12 ) (-------- 0 ---------)
  1,    0, 0, 0, 1, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0]


