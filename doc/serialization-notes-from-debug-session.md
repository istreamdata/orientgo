Serialization in Java client

ORecordSerializer#toStream
creates BytesContainer
bytes[0] = CURRENT_RECORD_VERSION => 0 (binarySerializationVersion)
serializer#serialize(ODocument, bytesContainer, classOnly=false)
.. serializeClass(document, bytesContainer)
.... writeString(bytesContainer, className:string)  (varint encoding ???)
.. create pos []int of length of number of fields
.. writeString(bytesContainer, propName:string)  (using varint encoding)

     5   D   a   l    e    k   4   n    a   m    e
[0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ... ]
 0       2       4         6       8        10      12
[2]pos = {12, 0}
<-
     5   D   a   l    e    k   4   n    a   m    e   <-futptr-> FTYP  7   e    p    i    s    o    d    e   <-futptr-> FTYPE EOH?? 
[0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 0, 0,  14, 101, 112, 105, 115, 111, 100, 101, 0, 0, 0, 0, 0,   0,    0, 0, ... ]
 0       2       4         6       8        10      12    14    16       18        20        22        24 25
[2]pos = {12, 25}




.. try to look up the type of the field from the document fieldType (will have it if already cached)

when it gets to the values - looks up property type first from document.fieldType(), which returns null
so then it looks in properties in the ORecordSerializerV0 class => so that must be the GlobalProperties ??->
but it is STILL NULL, because it hasn't been created in the DB yet!
So then, it calls `OType.getTypeByValue` where it uses a combo precomputed hash (based on class type) to
look up the type -> also has special handling of EMBEDDEDSET/LIST/MAP (not sure what it is doing there).

and then calls `ORecordSerializerV0#writeSingleValue`

[0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 0, 0, 14, 101, 112, 105, 115, 111, 100, 101,  0, 0, 0, 0, 0,   0,

12, 100, 97, 108, 101, 107, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 


ORecordSerializerV0#writeSingleValue returns the int value of the offset ("pointer") the data value was written to.
with this method:

    public void serializeLiteral(final int value, final byte[] stream, final int startPosition) {
      stream[startPosition] = (byte) ((value >>> 24) & 0xFF);
      stream[startPosition + 1] = (byte) ((value >>> 16) & 0xFF);
      stream[startPosition + 2] = (byte) ((value >>> 8) & 0xFF);
      stream[startPosition + 3] = (byte) ((value >>> 0) & 0xFF);
    }

Once the bytes are serialized, then `OStorageRemoteThread#createRecord` is called->

# SupportingNotes - part of another example header

    +-----------------------------+-------------------+-------------------------------+----------------+
    | field_name_length|id:varint | field_name:byte[] | pointer_to_data_structure:int | data_type:byte |
    +-----------------------------+-------------------+-------------------------------+----------------+

    VN CN? len                                                      12=EMBEDDEDMAP     KEY                                   VAL
            9   d    a   t   a   b   a    s    e    s   <---ptr---> TYP EOH NumEntries TYP len  c   a   r    s  <----ptr---> TYP
    [0, 0, 18, 100, 97, 116, 97, 98, 97, 115, 101, 115, 0, 0, 0, 18, 12, 0, 6,          7,  8, 99, 97, 114, 115, 0, 0, 0, 69,  7, 




OVarIntSerializer.write(bytes, (properties[i].getId() + 1) * -1);

        final OChannelBinaryAsynchClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
        lastNetworkUsed = network;

OStorageRemote#createRecord (line 346):

        try {
          network.writeShort((short) iRid.clusterId);
          network.writeBytes(iContent);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode);


The server log

    Read byte: 31 [OChannelBinaryServer]  <- REQUEST_RECORD_CREATE
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 2 [OChannelBinaryServer]    <- session-id ??
    Reading short (2 bytes)... [OChannelBinaryServer]
    Read short: -1 [OChannelBinaryServer]    <- cluster-id
    Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]  <- record-content
    Read chunk lenght: 39 [OChannelBinaryServer]
    Reading 39 bytes... [OChannelBinaryServer]
    Read 39 bytes: 
    Dalenameepisode&
                      dalek40 [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]     
    Read byte: 100 [OChannelBinaryServer]               <- record-type ('d'=document)
    Reading byte (1 byte)... [OChannelBinaryServer]
    Read byte: 0 [OChannelBinaryServer]                 <- mode (0=synchronous)
    Writing byte (1 byte): 0 [OChannelBinaryServer]     <- SUCCESS
    Writing int (4 bytes): 2 [OChannelBinaryServer]     <- session-id
    Writing short (2 bytes): 11 [OChannelBinaryServer]  <- cluster-id
    Writing long (8 bytes): 0 [OChannelBinaryServer]    <- cluster-pos
    Writing int (4 bytes): 1 [OChannelBinaryServer]     <- record-version
    Writing int (4 bytes): 0 [OChannelBinaryServer]     <- count of collection changes (if non-zero has RidBag data following)
    Flush [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]


I missed where in the the unwinding back to the save call, the Java client also had this interaction (before the `save()` finished)

    Read byte: 41 [OChannelBinaryServer]
    Reading int (4 bytes)... [OChannelBinaryServer]
    Read int: 4 [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]
    Read byte: 115 [OChannelBinaryServer]
    Reading chunk of bytes. Reading chunk length as int (4 bytes)... [OChannelBinaryServer]
    Read chunk lenght: 49 [OChannelBinaryServer]
    Reading 49 bytes... [OChannelBinaryServer]
    Read 49 bytes: qselect from Dalek*:1 [OChannelBinaryServer]
    Writing byte (1 byte): 0 [OChannelBinaryServer]
    Writing int (4 bytes): 4 [OChannelBinaryServer]
    Writing byte (1 byte): 108 [OChannelBinaryServer]
    Writing int (4 bytes): 1 [OChannelBinaryServer]
    Writing short (2 bytes): 0 [OChannelBinaryServer]
    Writing byte (1 byte): 100 [OChannelBinaryServer]
    Writing short (2 bytes): 11 [OChannelBinaryServer]
    Writing long (8 bytes): 0 [OChannelBinaryServer]
    Writing int (4 bytes): 1 [OChannelBinaryServer]
    Writing bytes (4+39=43 bytes): [0, 10, 68, 97, 108, 101, 107, 8, 110, 97, 109, 101, 0, 0, 0, 31, 7, 14, 101, 112, 105, 115, 111, 100, 101, 0, 0, 0, 38, 1, 0, 12, 100, 97, 108, 101, 107, 52, 48] [OChannelBinaryServer]
    Writing byte (1 byte): 0 [OChannelBinaryServer]
    Flush [OChannelBinaryServer]
    Reading byte (1 byte)... [OChannelBinaryServer]




## commit

returned immediately after OTx.isActive() returned false.

So you must have to have txs on for save not to do any saves / commits?

