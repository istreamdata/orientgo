### SQLQuery returning type 'l' (no supplementary docs from an extended fetchPlan)

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 360 [OChannelBinaryServer]
    /127.0.0.1:51647 - Reading byte (1 byte)... [OChannelBinaryServer]
    /127.0.0.1:51647 - Read byte: 115 [OChannelBinaryServer]
    /127.0.0.1:51647 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
    /127.0.0.1:51647 - Read chunk lenght: 52 [OChannelBinaryServer]
    /127.0.0.1:51647 - Reading 52 bytes... [OChannelBinaryServer]
    /127.0.0.1:51647 - Read 52 bytes: q#select * from Cat order by name asc���� [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing int (4 bytes): 360 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing byte (1 byte): 108 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing int (4 bytes): 3 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing short (2 bytes): 0 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing byte (1 byte): 100 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing short (2 bytes): 10 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing long (8 bytes): 1 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing bytes (4+33=37 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 27, 43, 0, 0, 0, 28, 0, 10, 75, 101, 105, 107, 111, 20, 8, 65, 110, 110, 97] [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing short (2 bytes): 0 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing byte (1 byte): 100 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing short (2 bytes): 10 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing long (8 bytes): 0 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing bytes (4+36=40 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 27, 43, 0, 0, 0, 28, 0, 10, 76, 105, 110, 117, 115, 30, 14, 77, 105, 99, 104, 97, 101, 108] [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing short (2 bytes): 0 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing byte (1 byte): 100 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing short (2 bytes): 10 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing long (8 bytes): 2 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing int (4 bytes): 1 [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing bytes (4+31=35 bytes): [0, 6, 67, 97, 116, 1, 0, 0, 0, 21, 41, 0, 0, 0, 25, 43, 0, 0, 0, 26, 0, 6, 90, 101, 100, 6, 8, 83, 104, 97, 119] [OChannelBinaryServer]
    /127.0.0.1:51647 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    /127.0.0.1:51647 - Flush [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]




### SQLCommand returning type 'l' (no supplementary docs from an extended fetchPlan)

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 391 [OChannelBinaryServer]
    27.0.0.1:51659 - Reading byte (1 byte)... [OChannelBinaryServer]
    27.0.0.1:51659 - Read byte: 115 [OChannelBinaryServer]
    27.0.0.1:51659 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
    27.0.0.1:51659 - Read chunk lenght: 91 [OChannelBinaryServer]
    27.0.0.1:51659 - Reading 91 bytes... [OChannelBinaryServer]
    27.0.0.1:51659 - Read 91 bytes: cPINSERT INTO Patient (name, married) VALUES ('Hank', 'true'), ('Martha', 'false') [OChannelBinaryServer]
    27.0.0.1:51659 - Writing byte (1 byte): 0 [OChannelBinaryServer]      <- status OK
    27.0.0.1:51659 - Writing int (4 bytes): 391 [OChannelBinaryServer]    <- session-id
    27.0.0.1:51659 - Writing byte (1 byte): 108 [OChannelBinaryServer]    <- 'l' (type=collection of records)
    27.0.0.1:51659 - Writing int (4 bytes): 2 [OChannelBinaryServer]      <- collection size (num recs)
    27.0.0.1:51659 - Writing short (2 bytes): 0 [OChannelBinaryServer]    <- 0=full record (this starts the "record" section)
    27.0.0.1:51659 - Writing byte (1 byte): 100 [OChannelBinaryServer]    <- 'd'=document
    27.0.0.1:51659 - Writing short (2 bytes): 11 [OChannelBinaryServer]   <- cluster-id (RID part 1)
    27.0.0.1:51659 - Writing long (8 bytes): 0 [OChannelBinaryServer]     <- cluster-pos (RID part 2)
    27.0.0.1:51659 - Writing int (4 bytes): 1 [OChannelBinaryServer]      <- version                                           
    27.0.0.1:51659 - Writing bytes (4+26=30 bytes): [0, 14, 80, 97, 116, 105, 101, 110, 116, 1, 0, 0, 0, 20, 45, 0, 0, 0, 25, 0, 8, 72, 97, 110, 107, 1] [OChannelBinaryServer]
    27.0.0.1:51659 - Writing short (2 bytes): 0 [OChannelBinaryServer]    <- 0=full record (this starts the "record" section)  
    27.0.0.1:51659 - Writing byte (1 byte): 100 [OChannelBinaryServer]    <- 'd'=document                                      
    27.0.0.1:51659 - Writing short (2 bytes): 11 [OChannelBinaryServer]   <- cluster-id (RID part 1)                           
    27.0.0.1:51659 - Writing long (8 bytes): 1 [OChannelBinaryServer]     <- cluster-pos (RID part 2)                          
    27.0.0.1:51659 - Writing int (4 bytes): 1 [OChannelBinaryServer]      <- version                                           
    27.0.0.1:51659 - Writing bytes (4+28=32 bytes): [0, 14, 80, 97, 116, 105, 101, 110, 116, 1, 0, 0, 0, 20, 45, 0, 0, 0, 27, 0, 12, 77, 97, 114, 116, 104, 97, 0] [OChannelBinaryServer]
    27.0.0.1:51659 - Writing byte (1 byte): 0 [OChannelBinaryServer]      <- EOT
    27.0.0.1:51659 - Flush [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]



### SQLCommand returning type 'r' (no supplementary docs from an extended fetchPlan)

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 421 [OChannelBinaryServer]
    27.0.0.1:51671 - Reading byte (1 byte)... [OChannelBinaryServer]
    27.0.0.1:51671 - Read byte: 115 [OChannelBinaryServer]
    27.0.0.1:51671 - Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
    27.0.0.1:51671 - Read chunk lenght: 70 [OChannelBinaryServer]
    27.0.0.1:51671 - Reading 70 bytes... [OChannelBinaryServer]
    27.0.0.1:51671 - Read 70 bytes: c;INSERT INTO Patient (name, married) VALUES ('Hank', 'true') [OChannelBinaryServer]
    27.0.0.1:51671 - Writing byte (1 byte): 0 [OChannelBinaryServer]     <- status OK
    27.0.0.1:51671 - Writing int (4 bytes): 421 [OChannelBinaryServer]   <- session-id
    27.0.0.1:51671 - Writing byte (1 byte): 114 [OChannelBinaryServer]   <- 'r' (type=single-record)
    27.0.0.1:51671 - Writing short (2 bytes): 0 [OChannelBinaryServer]   <- 0=full record  (-2=null, -3=RID only)
    27.0.0.1:51671 - Writing byte (1 byte): 100 [OChannelBinaryServer]   <- 'd'=document ('f'=flat data, 'b'=raw bytes)
    27.0.0.1:51671 - Writing short (2 bytes): 11 [OChannelBinaryServer]  <- cluster-id  (RID part 1)
    27.0.0.1:51671 - Writing long (8 bytes): 0 [OChannelBinaryServer]    <- cluster-pos (RID part 2)
    27.0.0.1:51671 - Writing int (4 bytes): 1 [OChannelBinaryServer]     <- version
    27.0.0.1:51671 - Writing bytes (4+26=30 bytes): [0, 14, 80, 97, 116, 105, 101, 110, 116, 1, 0, 0, 0, 20, 45, 0, 0, 0, 25, 0, 8, 72, 97, 110, 107, 1] [OChannelBinaryServer]
    27.0.0.1:51671 - Writing byte (1 byte): 0 [OChannelBinaryServer]     <- EOT
    27.0.0.1:51671 - Flush [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]



### SQLCommand returning type 'a' CREATE CLASS Animal => serializedRec 'a'

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 476 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading byte (1 byte)... [OChannelBinaryServer]
    27.0.0.1:52161 - Read byte: 115 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading chunk of bytes. Reading chunk length as int (4 bytes)...[OChannelBinaryServer]
    27.0.0.1:52161 - Read chunk lenght: 30 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading 30 bytes... [OChannelBinaryServer]
    27.0.0.1:52161 - Read 30 bytes: cCREATE CLASS Animal [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing int (4 bytes): 476 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 97 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing string (4+1=5 bytes): 9 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    27.0.0.1:52161 - Flush [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]



### SQLCommand returning type 'a': delete from Cat where name ='June' => serializedRec 'a'

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 479 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading byte (1 byte)... [OChannelBinaryServer]
    27.0.0.1:52161 - Read byte: 115 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading chunk of bytes. Reading chunk length as int (4 bytes)...[OChannelBinaryServer]
    27.0.0.1:52161 - Read chunk lenght: 45 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading 45 bytes... [OChannelBinaryServer]
    27.0.0.1:52161 - Read 45 bytes: c"delete from Cat where name ='June' [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing int (4 bytes): 479 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 97 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing string (4+1=5 bytes): 1 [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 0 [OChannelBinaryServer]
    27.0.0.1:52161 - Flush [OChannelBinaryServer]


### SQLCommand returning type 'a': TRUNCATE CLASS Patient => serializedRec 'a'

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 479 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading byte (1 byte)... [OChannelBinaryServer]
    27.0.0.1:52161 - Read byte: 115 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading chunk of bytes. Reading chunk length as int (4 bytes)...[OChannelBinaryServer]
    27.0.0.1:52161 - Read chunk lenght: 33 [OChannelBinaryServer]
    27.0.0.1:52161 - Reading 33 bytes... [OChannelBinaryServer]
    27.0.0.1:52161 - Read 33 bytes: cTRUNCATE CLASS Patient [OChannelBinaryServer]
    27.0.0.1:52161 - Writing byte (1 byte): 0 [OChannelBinaryServer]           <- status OK
    27.0.0.1:52161 - Writing int (4 bytes): 479 [OChannelBinaryServer]         <- session-id
    27.0.0.1:52161 - Writing byte (1 byte): 97 [OChannelBinaryServer]          <- 'a' (type=serialized record)
    27.0.0.1:52161 - Writing string (4+2=6 bytes): 1l [OChannelBinaryServer]   <- serialized record
    27.0.0.1:52161 - Writing byte (1 byte): 0 [OChannelBinaryServer]           <- EOT
    27.0.0.1:52161 - Flush [OChannelBinaryServer]
