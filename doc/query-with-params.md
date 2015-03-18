
## With Java client

Read byte: 41 [OChannelBinaryServer]
Reading int (4 bytes)... [OChannelBinaryServer]
Read int: 276 [OChannelBinaryServer]
Reading byte (1 byte)... [OChannelBinaryServer]
Read byte: 115 [OChannelBinaryServer]
Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
Read chunk lenght: 117 [OChannelBinaryServer]
Reading 117 bytes... [OChannelBinaryServer]
Read 117 bytes: q+select * from Carz where make=? and model=?����-

params
      0 1&
Honda
     Accord [OChannelBinaryServer]
Writing byte (1 byte): 0 [OChannelBinaryServer]
Writing int (4 bytes): 276 [OChannelBinaryServer] -> session-id
Writing byte (1 byte): 108 [OChannelBinaryServer] -> 'l'
Writing int (4 bytes): 1 [OChannelBinaryServer]
Writing short (2 bytes): 0 [OChannelBinaryServer]
Writing byte (1 byte): 100 [OChannelBinaryServer]
Writing short (2 bytes): 13 [OChannelBinaryServer]
Writing long (8 bytes): 0 [OChannelBinaryServer]
Writing int (4 bytes): 1 [OChannelBinaryServer]
Writing bytes (4+30=34 bytes): [0, 8, 67, 97, 114, 122, 47, 0, 0, 0, 17, 49, 0, 0, 0, 23, 0, 10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100] [OChannelBinaryServer]
Writing byte (1 byte): 0 [OChannelBinaryServer]
Flush [OChannelBinaryServer]



#-- serialized bytes in Java client --#
           + s e l e c t   *   f r o m   C a r z   w h e r e   m a k e = ?   a n d   m o d e l = ?
[0, 0, 0, 43, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 67, 97, 114, 122, 32, 119, 104, 101, 114, 101, 32, 109, 97, 107, 101, 61, 63, 32, 97, 110, 100, 32, 109, 111, 100, 101, 108, 61, 63, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 45, 
0, 0, 12, 112, 97, 114, 97, 109, 115, 0, 0, 0, 15, 12, 0, 4, 7, 2, 48, 0, 0, 0, 32, 7, 7, 2, 49, 0, 0, 0, 38, 7, 10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100, 0, 0, 0, 0, 0, 0, 0, 0]

       |---------- EMBDDED-MAP Header---------------------|    |------Key 1 header---------|---------Key 2 header----------|
                                                   12=EMBEDDEDMAP
                                                   data  EOH n-entries                   data  hdr                        data
        6   p    a   r    a   m    s   <---ptr---> TYP       2 TYP len=1  "0" <---ptr---> TYP  TYP len=1 "1"  <---ptr---> TYP?
[0, 0, 12, 112, 97, 114, 97, 109, 115, 0, 0, 0, 15, 12,  0,  4, 7,     2, 48, 0, 0, 0, 32, 7,   7,     2, 49, 0, 0, 0, 38, 7, 
        2        4        6         8    10     12       14 15            18    20     22      24         26    28     30  
     H   o    n    d    a   6   A   c   c   o    r    d  
10, 72, 111, 110, 100, 97, 12, 65, 99, 99, 111, 114, 100,
32       34        36      38


[0 0 12 112 97 114 97 109 115 0 0 0 15 12 0 4 7 2 48 0 0 0 32 7 7 2 49 0 0 0 37 7 8 74 117 110 101 14 67 108 101 97 118 101 114]


# Step by Step #
           6   p   a  r   a  m   s  <--ptr--> TYP EOH  2 KTYP 1  "0" <--ptr-->  DTYP  KTYP 1  "1" <--ptr-->  DTYP  |------ data -------------|
sbuf: [0 0 12 112 97 114 97 109 115 <0 0 0 0>  12  0   4, 7   2, 48  <0 0 0 17>   7    7   2  49  <0 0 0 22>   7   8 J u n e 14 C l e a v e r]
       0    2      4      6      8    10  12      14  15     17      19     22        24      26         30       32         37

ptrPos: [9]             -> [9  19 27]
ptrVal: [0] +15 -> [15] -> [15 32 37]

dbufPtrPos: [4  12] + 15 -> [19, 27]
dbufPtrVal: [17 22] + 15 -> [32, 37]

endHdrPos = 15


       nentries
       2 KTYP 1  "0" <--ptr-->  DTYP  KTYP 1  "1" <--ptr-->  DTYP  |------ data -------------|
dbuf: [4, 7   2, 48  <0 0 0 17>   7    7   2  49  <0 0 0 22>   7   8 J u n e 14 C l e a v e r]
       0      2       4   6       8       10      12  14      16


ptrPos: [4 12]
ptrVal: [17  22]
endHdrPos = 17

dbuf2: [8 J u n e 14 C l e a v e r]

      6   p  a  r   a  m   s  <---ptr--->  TYP EOH          2 TYP len=1  "0"  <---ptr--->TYP  TYP len=1 "1"  <---ptr---> TYP?
[0 0 12 112  0  0   0  15 115 0 0 0 0       12   0           4 7      2  48  0 0 0 17     7    7      2  49  0 0 0 22     7

      6   p  a  r   a  m   s  <---ptr--->  TYP EOH          2 TYP len=1  "0"  <---ptr--->TYP  TYP len=1 "1"  <---ptr---> TYP?
[0 0 12 112 97 114 97 109 115 0 0 0 15      12   0           4  7     2  48  0 0 0 17     7    7      2  49  0 0 0 22     7
                                                           >15                           >23             >26             >31  
 8 74 117 110 101 14 67 108 101 97 118 101 114]
32
 8 74 117 110 101 14 67 108 101 97 118 101 114]
      6   p   a  r   a  m   s  <---ptr--->  TYP EOH  2 TYP len=1  "0"  <---ptr--->TYP  TYP len=1 "1"  <---ptr---> TYP?
[0 0 12 112 97 114 97 109 115 0 0 0 15      12   0   4 7       2  49 0 0 0 25 7 7 2 48 0 0 0 30 7 14 67 108 101 97 118 101 114 8 74 117 110 101]


      6   p   a  r   a  m   s  <---ptr--->  TYP EOH  
[0 0 12 112 97 114 97 109 115 0 0 0 15      12  0    2 7 12 112 97 114 97 109 115 0 0 0 44 12 4 7 2 48 0 0 0 22 7 7 2 49 0 0 0 30 7 8 74 117 110 101 14 67 108 101 97 118 101 114]


     6   p   a  r   a  m   s  <---ptr--->  TYP=>WRONG!!
[0 0 12 112 97 114 97 109 115 0 0 0 15     23 0]


|----------Key 1 header--------------|---------Key 2 header----------|
      TYP len=1  "0"  <---ptr--->TYP  TYP len=1 "1"  <---ptr---> TYP?
[      7      2  48   0 0 0 32    7    7      2 49   0 0 0 38      7   X  10 72 111 110 100 97 12 65 99 99 111 114 100 0 0 0 0 0 0 0 0]
[0 0   X      2  48   0 0 0 17    7    X      2 49   0 0 0 22      7   0   8 74 117 110 101 14 67 108 101 97 118 101 114]




+--------------+------------------+----------+-----------+
| keyType:byte | keyValue:byte[]  | data-ptr | data-type |    
+--------------+------------------+----------+-----------+


when the 
