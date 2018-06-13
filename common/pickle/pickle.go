// Copyright (c) 2015 Rackspace
// Copyright (c) 2016-2018 iQIYI.com.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pickle

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

func pickleString(val string, buf *bytes.Buffer, scratch []byte) {
	length := len(val)
	if length < 256 {
		buf.WriteByte('U') // SHORT_BINSTRING
		buf.WriteByte(byte(length))
		buf.WriteString(val)
	} else {
		scratch[0] = 'T' // BINSTRING
		binary.LittleEndian.PutUint32(scratch[1:5], uint32(length))
		buf.Write(scratch[0:5])
		buf.WriteString(val)
	}
}

func pickleInt(val int64, buf *bytes.Buffer, scratch []byte) {
	if int64(uint8(val)) == val {
		buf.WriteByte('K') // BININT1
		buf.WriteByte(byte(val))
	} else if int64(uint16(val)) == val {
		scratch[0] = 'M' // BININT2
		binary.LittleEndian.PutUint16(scratch[1:3], uint16(val))
		buf.Write(scratch[0:3])
	} else if int64(int32(val)) == val {
		scratch[0] = 'J' // BININT
		binary.LittleEndian.PutUint32(scratch[1:5], uint32(val))
		buf.Write(scratch[0:5])
	} else {
		scratch[0] = '\x8a' // LONG1
		scratch[1] = 8      // 8 bytes
		binary.LittleEndian.PutUint64(scratch[2:10], uint64(val))
		buf.Write(scratch[0:10])
	}
}

func pickleObj(o interface{}, buf *bytes.Buffer, scratch []byte) error {
	v := reflect.ValueOf(o)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			buf.WriteByte('N') // NONE
		} else {
			return pickleObj(v.Elem().Interface(), buf, scratch)
		}
	case reflect.Invalid:
		buf.WriteByte('N') // NONE
	case reflect.Bool:
		if v.Bool() {
			buf.WriteByte('\x88') // NEWTRUE
		} else {
			buf.WriteByte('\x89') // NEWFALSE
		}
	case reflect.String:
		pickleString(v.String(), buf, scratch)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		pickleInt(v.Int(), buf, scratch)
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		pickleInt(int64(v.Uint()), buf, scratch)
	case reflect.Float32, reflect.Float64:
		scratch[0] = 'G' // BINFLOAT
		bits := math.Float64bits(v.Float())
		binary.BigEndian.PutUint64(scratch[1:9], bits)
		buf.Write(scratch[0:9])
	case reflect.Slice, reflect.Array:
		buf.WriteByte('(') // MARK
		for i := 0; i < v.Len(); i++ {
			if err := pickleObj(v.Index(i).Interface(), buf, scratch); err != nil {
				return err
			}
		}
		buf.WriteByte('l') // LIST
	case reflect.Map:
		buf.WriteByte('(') // MARK
		// Using "range" is way faster than iterating over maps with reflection,
		// but it's verbose and has to be a type we know ahead of time.
		switch o := o.(type) {
		case map[string]string:
			for k, v := range o {
				pickleString(k, buf, scratch)
				pickleString(v, buf, scratch)
			}
		case map[string]interface{}:
			for k, v := range o {
				pickleString(k, buf, scratch)
				if err := pickleObj(v, buf, scratch); err != nil {
					return err
				}
			}
		case map[interface{}]interface{}:
			for k, v := range o {
				if err := pickleObj(k, buf, scratch); err != nil {
					return err
				}
				if err := pickleObj(v, buf, scratch); err != nil {
					return err
				}
			}
		default:
			for _, k := range v.MapKeys() {
				if err := pickleObj(k.Interface(), buf, scratch); err != nil {
					return err
				}
				err := pickleObj(v.MapIndex(k).Interface(), buf, scratch)
				if err != nil {
					return err
				}
			}
		}
		buf.WriteByte('d') // DICT
	case reflect.Struct:
		switch o := o.(type) {
		case PickleTuple:
			buf.WriteByte('(') // MARK
			for _, to := range []interface{}{o.A, o.B, o.C, o.D}[:o.Len] {
				if err := pickleObj(to, buf, scratch); err != nil {
					return err
				}
			}
			buf.WriteByte('l') // TUPLE
		case PickleArray:
			buf.WriteString("carray\narray\n")
			buf.WriteByte('(') // MARK
			if err := pickleObj(o.Type, buf, scratch); err != nil {
				return err
			}
			if err := pickleObj(o.Data, buf, scratch); err != nil {
				return err
			}
			buf.WriteByte('l') // TUPLE
			buf.WriteByte('R') // REDUCE
		default: // why not serialize arbitrary structs as dicts while we're here
			if v.NumField() == 2 && v.Type().Field(0).Name == "ArrayType" {
				buf.WriteString("carray\narray\n")
				buf.WriteByte('(') // MARK
				if err := pickleObj(v.Field(0).Interface(), buf, scratch); err != nil {
					return err
				}
				if err := pickleObj(v.Field(1).Interface(), buf, scratch); err != nil {
					return err
				}
				buf.WriteByte('t') // TUPLE
				buf.WriteByte('R') // REDUCE
				return nil
			}
			buf.WriteByte('(') // MARK
			for i := 0; i < v.Type().NumField(); i++ {
				if !v.Field(i).CanInterface() {
					continue
				}
				field := v.Type().Field(i)
				if tag := field.Tag.Get("pickle"); tag != "" {
					pickleString(tag, buf, scratch)
				} else {
					pickleString(v.Type().Field(i).Name, buf, scratch)
				}
				if err := pickleObj(v.Field(i).Interface(), buf, scratch); err != nil {
					return err
				}
			}
			buf.WriteByte('d') // DICT
		}
	default:
		return fmt.Errorf("Unknown object type in pickle: %v", v)
	}
	return nil
}

func PickleDumps(o interface{}) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte('\x80') // PROTO
	buf.WriteByte(2)      // Protocol 2
	scratch := make([]byte, 10)
	if err := pickleObj(o, buf, scratch); err != nil {
		panic(err.Error())
	}
	buf.WriteByte('.')
	return buf.Bytes()
}

func Marshal(v interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteByte('\x80') // PROTO
	buf.WriteByte(2)      // Protocol 2
	scratch := make([]byte, 10)
	if err := pickleObj(v, buf, scratch); err != nil {
		return nil, err
	}
	buf.WriteByte('.')
	return buf.Bytes(), nil
}
