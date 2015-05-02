with fetchPlan: *:-1"
2015-04-27 08:00:27:103 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Read 69 bytes: q$select * from Cat where name='Tilde'����*:-1 [OChannelBinaryServer]
2015-04-27 08:00:27:104 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing byte (1 byte): 0 [OChannelBinaryServer]
2015-04-27 08:00:27:104 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing int (4 bytes): 11908 [OChannelBinaryServer]
2015-04-27 08:00:27:104 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing byte (1 byte): 108 [OChannelBinaryServer]
2015-04-27 08:00:27:104 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing int (4 bytes): 1 [OChannelBinaryServer]
2015-04-27 08:00:27:104 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing short (2 bytes): 0 [OChannelBinaryServer]
2015-04-27 08:00:27:104 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing byte (1 byte): 100 [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing short (2 bytes): 10 [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing long (8 bytes): 13 [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing int (4 bytes): 4 [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing bytes (4+40=44 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 32, 43, 0, 0, 0, 33, 53, 0, 0, 0, 38, 0, 10, 84, 105, 108, 100, 101, 16, 8, 69, 97, 114, 108, 20, 0] [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing byte (1 byte): 2 [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing short (2 bytes): 0 [OChannelBinaryServer]   <- classid (not RID, not null)
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing byte (1 byte): 100 [OChannelBinaryServer]   <- type 'd'
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing short (2 bytes): 10 [OChannelBinaryServer]  <- RID
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing long (8 bytes): 0 [OChannelBinaryServer]    <- RID (10:0)
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing int (4 bytes): 2 [OChannelBinaryServer]     <- record version
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing bytes (4+36=40 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 27, 43, 0, 0, 0, 28, 0, 10, 76, 105, 110, 117, 115, 30, 14, 77, 105, 99, 104, 97, 101, 108] [OChannelBinaryServer]
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Writing byte (1 byte): 0 [OChannelBinaryServer]    <- EOM
2015-04-27 08:00:27:105 INFO  {db=ogonoriTest} /127.0.0.1:58049 - Flush [OChannelBinaryServer]
2015-04-27 08:00:27:106 INFO  /127.0.0.1:58049 - Reading byte (1 byte)... [OChannelBinaryServer]
2015-04-27 08:00:48:110 INFO  /127.0.0.1:58049 - Flush [OChannelBinaryServer]
2015-04-27 08:00:48:110 INFO  /127.0.0.1:58049 - Closing socket... [OChannelBinaryServer]




### Code to handle if the byte after the serialized is not zero

if status byte == 2 then they put it in the local cache

     if (network.getSrvProtocolVersion() >= 17) {
       // LOAD THE FETCHED RECORDS IN CACHE
       byte status;
       while ((status = network.readByte()) > 0) {
         final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
         if (record != null && status == 2)
           // PUT IN THE CLIENT LOCAL CACHE
           database.getLocalCache().updateRecord(record);
       }
     }
              
              
with fetchPlan: "cat:0"              
2015-04-27 08:12:39:514 INFO  /127.0.0.1:58134 - Read byte: 41 [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  /127.0.0.1:58134 - Reading int (4 bytes)... [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  /127.0.0.1:58134 - Read int: 11993 [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Reading byte (1 byte)... [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Read byte: 115 [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Read chunk lenght: 70 [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Reading 70 bytes... [OChannelBinaryServer]
2015-04-27 08:12:39:515 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Read 70 bytes: q$select * from Cat where name='Tilde'����cat:0 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing byte (1 byte): 0 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing int (4 bytes): 11993 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing byte (1 byte): 108 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing int (4 bytes): 1 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing short (2 bytes): 0 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing byte (1 byte): 100 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing short (2 bytes): 10 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing long (8 bytes): 13 [OChannelBinaryServer]
2015-04-27 08:12:39:516 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing int (4 bytes): 4 [OChannelBinaryServer]
2015-04-27 08:12:39:517 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing bytes (4+40=44 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 32, 43, 0, 0, 0, 33, 53, 0, 0, 0, 38, 0, 10, 84, 105, 108, 100, 101, 16, 8, 69, 97, 114, 108, 20, 0] [OChannelBinaryServer]
2015-04-27 08:12:39:517 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Writing byte (1 byte): 0 [OChannelBinaryServer]
2015-04-27 08:12:39:517 INFO  {db=ogonoriTest} /127.0.0.1:58134 - Flush [OChannelBinaryServer]
2015-04-27 08:12:39:517 INFO  /127.0.0.1:58134 - Reading byte (1 byte)... [OChannelBinaryServer]
              





## multiple "primary" docs in the list and multiple "secondary" docs in the "cache"

2015-04-27 19:52:20:130 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Read 74 bytes: q)select * from Cat where buddy is not null����*:-1 [OChannelBinaryServer]
2015-04-27 19:52:20:133 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 0 [OChannelBinaryServer]     <- status OK
2015-04-27 19:52:20:133 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing int (4 bytes): 8 [OChannelBinaryServer]     <- session-id (double check ??)
2015-04-27 19:52:20:133 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 108 [OChannelBinaryServer]   <- 'l' (Collection of docs)
2015-04-27 19:52:20:133 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing int (4 bytes): 2 [OChannelBinaryServer]     <- number of documents
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 0 [OChannelBinaryServer]   <- classid (not RID, not null)
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 100 [OChannelBinaryServer]   <- type 'd'
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 10 [OChannelBinaryServer]  <- RID
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing long (8 bytes): 13 [OChannelBinaryServer]   <- RID (10:13)
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing int (4 bytes): 4 [OChannelBinaryServer]     <- record version
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing bytes (4+40=44 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 32, 43, 0, 0, 0, 33, 53, 0, 0, 0, 38, 0, 10, 84, 105, 108, 100, 101, 16, 8, 69, 97, 114, 108, 20, 0] [OChannelBinaryServer]
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 0 [OChannelBinaryServer]
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 100 [OChannelBinaryServer]
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 10 [OChannelBinaryServer]
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing long (8 bytes): 14 [OChannelBinaryServer]
2015-04-27 19:52:20:134 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing int (4 bytes): 4 [OChannelBinaryServer]    
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing bytes (4+41=45 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 32, 43, 0, 0, 0, 33, 53, 0, 0, 0, 39, 0, 10, 70, 101, 108, 105, 120, 18, 10, 83, 97, 110, 100, 121, 20, 2] [OChannelBinaryServer]     <- start of supplementary records
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 2 [OChannelBinaryServer]    <- document, not EOT (end of transmission)
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 0 [OChannelBinaryServer]  <- classid (not RID, not null)
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 100 [OChannelBinaryServer]  <- type 'd'      
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 10 [OChannelBinaryServer] <- RID => clusterId           
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing long (8 bytes): 0 [OChannelBinaryServer]   <- RID (10:13) => clusterPos
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing int (4 bytes): 2 [OChannelBinaryServer]    <- record version                  
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing bytes (4+36=40 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 27, 43, 0, 0, 0, 28, 0, 10, 76, 105, 110, 117, 115, 30, 14, 77, 105, 99, 104, 97, 101, 108] [OChannelBinaryServer]
2015-04-27 19:52:20:135 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 2 [OChannelBinaryServer]    <- document, not null, RID or EOR (??)
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 0 [OChannelBinaryServer]  <- classid (not RID, not null)     
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 100 [OChannelBinaryServer]  <- type 'd'                        
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing short (2 bytes): 10 [OChannelBinaryServer] <- RID                             
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing long (8 bytes): 1 [OChannelBinaryServer]   <- RID (10:13)                     
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing int (4 bytes): 2 [OChannelBinaryServer]    <- record version                  
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing bytes (4+33=37 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 27, 43, 0, 0, 0, 28, 0, 10, 75, 101, 105, 107, 111, 20, 8, 65, 110, 110, 97] [OChannelBinaryServer]
2015-04-27 19:52:20:136 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Writing byte (1 byte): 0 [OChannelBinaryServer]    <- EOR (end of records)
2015-04-27 19:52:20:137 INFO  {db=ogonoriTest} /127.0.0.1:49420 - Flush [OChannelBinaryServer]
2015-04-27 19:52:20:137 INFO  /127.0.0.1:49420 - Reading byte (1 byte)... [OChannelBinaryServer]





2015-05-02 10:58:37:571 INFO  /127.0.0.1:45555 - Read byte: 41 [OChannelBinaryServer]
2015-05-02 10:58:37:571 INFO  /127.0.0.1:45555 - Reading int (4 bytes)... [OChannelBinaryServer]
2015-05-02 10:58:37:571 INFO  /127.0.0.1:45555 - Read int: 115 [OChannelBinaryServer]
2015-05-02 10:58:37:571 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Reading byte (1 byte)... [OChannelBinaryServer]
2015-05-02 10:58:37:572 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Read byte: 115 [OChannelBinaryServer]
2015-05-02 10:58:37:572 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
2015-05-02 10:58:37:572 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Read chunk lenght: 62 [OChannelBinaryServer]
2015-05-02 10:58:37:572 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Reading 62 bytes... [OChannelBinaryServer]
2015-05-02 10:58:37:572 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Read 62 bytes: q)select * from Cat where buddy is not null����*:-1 [OChannelBinaryServer]
2015-05-02 10:58:37:573 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing byte (1 byte): 0 [OChannelBinaryServer]
2015-05-02 10:58:37:574 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing int (4 bytes): 115 [OChannelBinaryServer]
2015-05-02 10:58:37:574 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing byte (1 byte): 108 [OChannelBinaryServer]
2015-05-02 10:58:37:574 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing int (4 bytes): 1 [OChannelBinaryServer]
2015-05-02 10:58:37:574 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing short (2 bytes): 0 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing byte (1 byte): 100 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing short (2 bytes): 10 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing long (8 bytes): 15 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing int (4 bytes): 6 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing bytes (4+40=44 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 26, 41, 0, 0, 0, 32, 43, 0, 0, 0, 33, 53, 0, 0, 0, 38, 0, 10, 84, 105, 108, 100, 101, 16, 8, 69, 97, 114, 108, 20, 0] [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing byte (1 byte): 2 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing short (2 bytes): 0 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing byte (1 byte): 100 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing short (2 bytes): 10 [OChannelBinaryServer]
2015-05-02 10:58:37:575 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing long (8 bytes): 0 [OChannelBinaryServer]
2015-05-02 10:58:37:576 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing int (4 bytes): 2 [OChannelBinaryServer]
2015-05-02 10:58:37:576 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing bytes (4+36=40 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 27, 43, 0, 0, 0, 28, 0, 10, 76, 105, 110, 117, 115, 30, 14, 77, 105, 99, 104, 97, 101, 108] [OChannelBinaryServer]
2015-05-02 10:58:37:576 INFO  {db=ogonoriTest} /127.0.0.1:45555 - Writing byte (1 byte): 0 [OChannelBinaryServer]



