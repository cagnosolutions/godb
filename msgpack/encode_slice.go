package msgpack

import "reflect"

func encodeStringValue(e *Encoder, v reflect.Value) error {
	return e.EncodeString(v.String())
}

func encodeByteSliceValue(e *Encoder, v reflect.Value) error {
	return e.EncodeBytes(v.Bytes())
}

func encodeByteArrayValue(e *Encoder, v reflect.Value) error {
	if err := e.encodeBytesLen(v.Len()); err != nil {
		return err
	}

	if v.CanAddr() {
		b := v.Slice(0, v.Len()).Bytes()
		return e.write(b)
	}

	b := make([]byte, v.Len())
	reflect.Copy(reflect.ValueOf(b), v)
	return e.write(b)
}

func (e *Encoder) encodeBytesLen(l int) error {
	if l < 256 {
		return e.write1(Bin8, uint64(l))
	}
	if l < 65536 {
		return e.write2(Bin16, uint64(l))
	}
	return e.write4(Bin32, uint64(l))
}

func (e *Encoder) encodeStrLen(l int) error {
	if l < 32 {
		return e.w.WriteByte(FixedStrLow | uint8(l))
	}
	if l < 256 {
		return e.write1(Str8, uint64(l))
	}
	if l < 65536 {
		return e.write2(Str16, uint64(l))
	}
	return e.write4(Str32, uint64(l))
}

func (e *Encoder) EncodeString(v string) error {
	if err := e.encodeStrLen(len(v)); err != nil {
		return err
	}
	return e.writeString(v)
}

func (e *Encoder) EncodeBytes(v []byte) error {
	if v == nil {
		return e.EncodeNil()
	}
	if err := e.encodeBytesLen(len(v)); err != nil {
		return err
	}
	return e.write(v)
}

func (e *Encoder) EncodeArrayLen(l int) error {
	if l < 16 {
		return e.w.WriteByte(FixedArrayLow | byte(l))
	}
	if l < 65536 {
		return e.write2(Array16, uint64(l))
	}
	return e.write4(Array32, uint64(l))
}

// Deprecated. Use EncodeArrayLen instead.
func (e *Encoder) EncodeSliceLen(l int) error {
	return e.EncodeArrayLen(l)
}

func (e *Encoder) encodeStringSlice(s []string) error {
	if s == nil {
		return e.EncodeNil()
	}
	if err := e.EncodeArrayLen(len(s)); err != nil {
		return err
	}
	for _, v := range s {
		if err := e.EncodeString(v); err != nil {
			return err
		}
	}
	return nil
}

func encodeSliceValue(e *Encoder, v reflect.Value) error {
	if v.IsNil() {
		return e.EncodeNil()
	}
	return encodeArrayValue(e, v)
}

func encodeArrayValue(e *Encoder, v reflect.Value) error {
	l := v.Len()
	if err := e.EncodeSliceLen(l); err != nil {
		return err
	}
	for i := 0; i < l; i++ {
		if err := e.EncodeValue(v.Index(i)); err != nil {
			return err
		}
	}
	return nil
}
