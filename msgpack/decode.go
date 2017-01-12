package msgpack

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
)

const bytesAllocLimit = 1024 * 1024 // 1mb
const sliceAllocLimit = 1e4
const mapAllocLimit = 1e4

const seekStart = 0 // for backwards compatibility; use io.SeekStart const in the future.

func Unmarshal(b []byte, v ...interface{}) error {
	if len(v) == 1 && v[0] != nil {
		unmarshaler, ok := v[0].(Unmarshaler)
		if ok {
			return unmarshaler.UnmarshalMsgpack(b)
		}
	}
	return NewDecoder(bytes.NewReader(b)).Decode(v...)
}

type Decoder struct {
	DecodeMapFunc func(*Decoder) (interface{}, error)

	r   *bytes.Reader
	buf []byte
}

func NewDecoder(r *bytes.Reader) *Decoder {
	return &Decoder{
		DecodeMapFunc: decodeMap,

		r:   r,
		buf: make([]byte, 64),
	}
}

func (d *Decoder) Reset(r *bytes.Reader) error {
	d.r = r
	return nil
}

func (d *Decoder) Rewind() error {
	_, err := d.r.Seek(0, seekStart) 
	return err
}

func (d *Decoder) Decode(v ...interface{}) error {
	for _, vv := range v {
		if err := d.decode(vv); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) decode(dst interface{}) error {
	var err error
	switch v := dst.(type) {
	case *string:
		if v != nil {
			*v, err = d.DecodeString()
			return err
		}
	case *[]byte:
		if v != nil {
			return d.decodeBytesPtr(v)
		}
	case *int:
		if v != nil {
			*v, err = d.DecodeInt()
			return err
		}
	case *int8:
		if v != nil {
			*v, err = d.DecodeInt8()
			return err
		}
	case *int16:
		if v != nil {
			*v, err = d.DecodeInt16()
			return err
		}
	case *int32:
		if v != nil {
			*v, err = d.DecodeInt32()
			return err
		}
	case *int64:
		if v != nil {
			*v, err = d.DecodeInt64()
			return err
		}
	case *uint:
		if v != nil {
			*v, err = d.DecodeUint()
			return err
		}
	case *uint8:
		if v != nil {
			*v, err = d.DecodeUint8()
			return err
		}
	case *uint16:
		if v != nil {
			*v, err = d.DecodeUint16()
			return err
		}
	case *uint32:
		if v != nil {
			*v, err = d.DecodeUint32()
			return err
		}
	case *uint64:
		if v != nil {
			*v, err = d.DecodeUint64()
			return err
		}
	case *bool:
		if v != nil {
			*v, err = d.DecodeBool()
			return err
		}
	case *float32:
		if v != nil {
			*v, err = d.DecodeFloat32()
			return err
		}
	case *float64:
		if v != nil {
			*v, err = d.DecodeFloat64()
			return err
		}
	case *[]string:
		return d.decodeStringSlicePtr(v)
	case *map[string]string:
		return d.decodeMapStringStringPtr(v)
	case *map[string]interface{}:
		return d.decodeMapStringInterfacePtr(v)
	case *time.Duration:
		if v != nil {
			vv, err := d.DecodeInt64()
			*v = time.Duration(vv)
			return err
		}
	case *time.Time:
		if v != nil {
			*v, err = d.DecodeTime()
			return err
		}
	case CustomDecoder:
		if v != nil {
			return v.DecodeMsgpack(d)
		}
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return errors.New("msgpack: Decode(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("msgpack: Decode(nonsettable %T)", dst)
	}
	v = v.Elem()
	if !v.IsValid() {
		return fmt.Errorf("msgpack: Decode(nonsettable %T)", dst)
	}
	return d.DecodeValue(v)
}

func (d *Decoder) DecodeValue(v reflect.Value) error {
	decode := getDecoder(v.Type())
	return decode(d, v)
}

func (d *Decoder) DecodeNil() error {
	c, err := d.r.ReadByte()
	if err != nil {
		return err
	}
	if c != Nil {
		return fmt.Errorf("msgpack: invalid code %x decoding nil", c)
	}
	return nil
}

func (d *Decoder) DecodeBool() (bool, error) {
	c, err := d.r.ReadByte()
	if err != nil {
		return false, err
	}
	return d.bool(c)
}

func (d *Decoder) bool(c byte) (bool, error) {
	if c == False {
		return false, nil
	}
	if c == True {
		return true, nil
	}
	return false, fmt.Errorf("msgpack: invalid code %x decoding bool", c)
}

func (d *Decoder) interfaceValue(v reflect.Value) error {
	vv, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	if vv != nil {
		if v.Type() == errorType {
			if vv, ok := vv.(string); ok {
				v.Set(reflect.ValueOf(errors.New(vv)))
				return nil
			}
		}

		v.Set(reflect.ValueOf(vv))
	}
	return nil
}

// DecodeInterface decodes value into interface. Possible value types are:
//   - nil,
//   - bool,
//   - int64 for negative numbers,
//   - uint64 for positive numbers,
//   - float32 and float64,
//   - string,
//   - slices of any of the above,
//   - maps of any of the above.
func (d *Decoder) DecodeInterface() (interface{}, error) {
	c, err := d.r.ReadByte()
	if err != nil {
		return nil, err
	}

	if IsFixedNum(c) {
		if int8(c) < 0 {
			return d.int(c)
		}
		return d.uint(c)
	}
	if IsFixedMap(c) {
		d.r.UnreadByte()
		return d.DecodeMap()
	}
	if IsFixedArray(c) {
		return d.decodeSlice(c)
	}
	if IsFixedString(c) {
		return d.string(c)
	}

	switch c {
	case Nil:
		return nil, nil
	case False, True:
		return d.bool(c)
	case Float:
		return d.float32(c)
	case Double:
		return d.float64(c)
	case Uint8, Uint16, Uint32, Uint64:
		return d.uint(c)
	case Int8, Int16, Int32, Int64:
		return d.int(c)
	case Bin8, Bin16, Bin32:
		return d.bytes(c, nil)
	case Str8, Str16, Str32:
		return d.string(c)
	case Array16, Array32:
		return d.decodeSlice(c)
	case Map16, Map32:
		d.r.UnreadByte()
		return d.DecodeMap()
	case FixExt1, FixExt2, FixExt4, FixExt8, FixExt16,
		Ext8, Ext16, Ext32:
		return d.ext(c)
	}

	return 0, fmt.Errorf("msgpack: unknown code %x decoding interface{}", c)
}

// Skip skips next value.
func (d *Decoder) Skip() error {
	c, err := d.r.ReadByte()
	if err != nil {
		return err
	}

	if IsFixedNum(c) {
		return nil
	} else if IsFixedMap(c) {
		return d.skipMap(c)
	} else if IsFixedArray(c) {
		return d.skipSlice(c)
	} else if IsFixedString(c) {
		return d.skipBytes(c)
	}

	switch c {
	case Nil, False, True:
		return nil
	case Uint8, Int8:
		return d.skipN(1)
	case Uint16, Int16:
		return d.skipN(2)
	case Uint32, Int32, Float:
		return d.skipN(4)
	case Uint64, Int64, Double:
		return d.skipN(8)
	case Bin8, Bin16, Bin32:
		return d.skipBytes(c)
	case Str8, Str16, Str32:
		return d.skipBytes(c)
	case Array16, Array32:
		return d.skipSlice(c)
	case Map16, Map32:
		return d.skipMap(c)
	case FixExt1, FixExt2, FixExt4, FixExt8, FixExt16, Ext8, Ext16, Ext32:
		return d.skipExt(c)
	}

	return fmt.Errorf("msgpack: unknown code %x", c)
}

// peekCode returns the next Msgpack code. See
// https://github.com/msgpack/msgpack/blob/master/spec.md#formats for details.
func (d *Decoder) PeekCode() (code byte, err error) {
	code, err = d.r.ReadByte()
	if err != nil {
		return 0, err
	}
	return code, d.r.UnreadByte()
}

func (d *Decoder) gotNilCode() bool {
	code, err := d.PeekCode()
	return err == nil && code == Nil
}

func (d *Decoder) readN(n int) ([]byte, error) {
	var err error
	d.buf, err = readN(d.r, d.buf, n)
	return d.buf, err
}

func readN(r *bytes.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 && b == nil {
		return make([]byte, 0), nil
	}

	if cap(b) >= n {
		b = b[:n]
		_, err := io.ReadFull(r, b)
		return b, err
	}
	b = b[:cap(b)]

	pos := 0
	for len(b) < n {
		diff := n - len(b)
		if diff > bytesAllocLimit {
			diff = bytesAllocLimit
		}
		b = append(b, make([]byte, diff)...)

		_, err := io.ReadFull(r, b[pos:])
		if err != nil {
			return nil, err
		}

		pos = len(b)
	}

	return b, nil
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
