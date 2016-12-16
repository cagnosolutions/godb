package msgpack

var (

	// nil
	Nil byte = 0xc0

	// NEVER USED
	// UserDefined byte =0xc1

	// boolean
	False byte = 0xc2
	True  byte = 0xc3

	// unsigned / signed int max
	PosFixedNumHigh byte = 0x7f
	NegFixedNumLow  byte = 0xe0

	// unsigned int
	Uint8  byte = 0xcc
	Uint16 byte = 0xcd
	Uint32 byte = 0xce
	Uint64 byte = 0xcf

	// signed int
	Int8  byte = 0xd0
	Int16 byte = 0xd1
	Int32 byte = 0xd2
	Int64 byte = 0xd3

	// float
	Float  byte = 0xca
	Double byte = 0xcb

	// string
	FixedStrLow  byte = 0xa0
	FixedStrHigh byte = 0xbf
	FixedStrMask byte = 0x1f
	Str8         byte = 0xd9
	Str16        byte = 0xda
	Str32        byte = 0xdb

	// binary
	Bin8  byte = 0xc4
	Bin16 byte = 0xc5
	Bin32 byte = 0xc6

	// array
	FixedArrayLow  byte = 0x90
	FixedArrayHigh byte = 0x9f
	FixedArrayMask byte = 0xf
	Array16        byte = 0xdc
	Array32        byte = 0xdd

	// map
	FixedMapLow  byte = 0x80
	FixedMapHigh byte = 0x8f
	FixedMapMask byte = 0xf
	Map16        byte = 0xde
	Map32        byte = 0xdf

	// ext (tuples)
	FixExt1  byte = 0xd4
	FixExt2  byte = 0xd5
	FixExt4  byte = 0xd6
	FixExt8  byte = 0xd7
	FixExt16 byte = 0xd8
	Ext8     byte = 0xc7
	Ext16    byte = 0xc8
	Ext32    byte = 0xc9
)

func IsNil(c byte) bool {
	return c == Nil
}

func IsBool(c byte) bool {
	return c == False || c == True
}

func IsTrue(c byte) bool {
	return c == True
}

func IsFalse(c byte) bool {
	return c == False
}

func IsFixedNum(c byte) bool {
	return c <= PosFixedNumHigh || c >= NegFixedNumLow
}

func IsFixedString(c byte) bool {
	return c >= FixedStrLow && c <= FixedStrHigh
}

func IsFixedArray(c byte) bool {
	return c >= FixedArrayLow && c <= FixedArrayHigh
}

func IsFixedMap(c byte) bool {
	return c >= FixedMapLow && c <= FixedMapHigh
}

func IsExt(c byte) bool {
	return (c >= FixExt1 && c <= FixExt16) || (c >= Ext8 && c <= Ext32)
}
