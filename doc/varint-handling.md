### Masks for VarInt shifts (>>1, >>2 not shown)

       0        1        2         3       4        5        6         7
    1aaaaaaa 1bbbbbbb 1ccccccc 1ddddddd 1eeeeeee 1fffffff 1ggggggg 0hhhhhhh ORIG
    1111aaaa aaa1bbbb bbb1cccc ccc1dddd ddd1eeee eee1ffff fff1gggg ggg0hhhh >>3
    00000000 00000000 00000000 00000000 00001111 11100000 00000000 00000000
       0x0     0x0      0x0       0x0     0x0f     0xe0     0x0      0x0
    
    11111aaa aaaa1bbb bbbb1ccc cccc1ddd dddd1eee eeee1fff ffff1ggg gggg0hhh >>4
    00000000 00000000 00000000 00000111 11110000 00000000 00000000 00000000
       0x0     0x0      0x0      0x07     0xf0      0x0     0x0      0x0
    
    111111aa aaaaa1bb bbbbb1cc ccccc1dd ddddd1ee eeeee1ff fffff1gg ggggg0hh >>5
    00000000 00000000 00000011 11111000 00000000 00000000 00000000 00000000
       0x0     0x00     0x03     0xf8      0x0      0x0     0x0      0x0
    
    1111111a aaaaaa1b bbbbbb1c cccccc1d dddddd1e eeeeee1f ffffff1g gggggg0h >>6
    00000000 00000001 11111100 00000000 00000000 00000000 00000000 00000000
       0x0     0x01     0xfa      0x0      0x0      0x0     0x0      0x0
    
    11111111 aaaaaaa1 bbbbbbb1 ccccccc1 ddddddd1 eeeeeee1 fffffff1 ggggggg0 >>7
    00000000 11111110 00000000 00000000 00000000 00000000 00000000 00000000
       0x0     0xfe     0x0       0x0      0x0      0x0     0x0      0x0


