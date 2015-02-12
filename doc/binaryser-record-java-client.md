

ODatabaseDocument#executeSaveRecord(ln 1616)
ORecordSerializer#toStream(ln 84)
      serializerByVersion[CURRENT_RECORD_VERSION].serialize((ODocument) iSource, container, false); (ln 104)
      
ORecordSerializerBinaryV0#
      wrote Byte (0)
      wrote classname as string
          OVarIntSerializer.write(bytes, nameBytes.length);
      

BytesBuffer so far


I have done some additional analysis by stepping through the Java client code to see what it writes out.

For this code:

    ODocument person = new ODocument("Person");
    person.field("name", "Han");
    person.field("surname", "Solo");
    person.save();


The serialized record {Person[name=Han, surname=Solo]} gets written out like this:

               |---------- className ---------|                                  
            V   6   P    e    r    s    o    n     
    bytes: [0, 12, 80, 101, 114, 115, 111, 110, 
    idx:    0   1   2    3    4    5    6    7  

             |------------- Header ------------|---------------- Data ---------------|
             0  <---ptr--->  20  <---ptr---> EOH 3   H   a    n  4   S    o    l    o      
    bytes:   1, 0, 0, 0, 19, 41, 0, 0, 0, 23, 0, 6, 72, 97, 110, 8, 83, 111, 108, 111]
    idx:     8  9 10 11  12  13 14 15 16  17 18 19  20  21   22 23  24   25   26   27


--------------------------------------------------------------------------
      
Positions 9-12 and 14-17 are non-varint integers that are pointers to the values in the data section.  And position 18 is an end-of-header marker. While this doesn't match the documentation, those make sense to me.

What I don't get the byte before the pointers - position 8 and 13.  According to the Java client, these values (oddly) encoded values of the Property id.  In my case the "name" property has id 0 and the "surname" property has id 20.  The encoding is this:

    zigzagEncode( (propertyId+1) * -1 )

so, going in reverse, 1 (val of idx 8) is -1 when zigzag decoded and working backwards:

(p + 1) * -1 = -1
p + 1 = -1/-1 = 1
p = 1 - 1 = 0, the ID of property 'name'

and 41 (val of idx 13)
zigzagDecode(41) = -21
-21 + 1 => 20, the ID of property 'surname'

The reverse formula:

   nv = zigzagDecode(v)  // 41 -> -21
   pid = (-1 * nv) + 1  // pid = (-1 * -21) + 1
   


So three things:

1. the schemaless serialization documentation needs to be updated
2. what is the logic for the way the property id is encoded?  Why add 1 and then make it negative?
3. how do I obtain the ID of a property?  what call do I make in the binary protocol to get that?

Thank you
-Michael


      
# Misc Notes

    /**
     * Encodes a value using the variable-length encoding from <a
     * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>. It uses zig-zag encoding to
     * efficiently encode signed values. If values are known to be nonnegative, {@link #writeUnsignedVarLong(long, DataOutput)} should
     * be used.
     * 
     * @param value
     *          value to encode
     * @param out
     *          to write bytes to
     * @throws IOException
     *           if {@link DataOutput} throws {@link IOException}
     */
    private static long signedToUnsigned(long value) {
      return (value << 1) ^ (value >> 63);
    }      
