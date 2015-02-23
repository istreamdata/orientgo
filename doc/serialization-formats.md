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
# load record #0:1
==================
