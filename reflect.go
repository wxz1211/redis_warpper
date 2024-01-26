package redis_warpper

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type decoderFunc func(reflect.Value, string) error

var decoders = []decoderFunc{
	reflect.Bool:          decodeBool,
	reflect.Int:           decodeInt,
	reflect.Int8:          decodeInt8,
	reflect.Int16:         decodeInt16,
	reflect.Int32:         decodeInt32,
	reflect.Int64:         decodeInt64,
	reflect.Uint:          decodeUint,
	reflect.Uint8:         decodeUint8,
	reflect.Uint16:        decodeUint16,
	reflect.Uint32:        decodeUint32,
	reflect.Uint64:        decodeUint64,
	reflect.Float32:       decodeFloat32,
	reflect.Float64:       decodeFloat64,
	reflect.Complex64:     nil,
	reflect.Complex128:    nil,
	reflect.Array:         nil,
	reflect.Chan:          nil,
	reflect.Func:          nil,
	reflect.Interface:     nil,
	reflect.Map:           nil,
	reflect.Ptr:           nil,
	reflect.Slice:         nil,
	reflect.String:        decodeString,
	reflect.Struct:        nil,
	reflect.UnsafePointer: nil,
}

func requiredStruct(data interface{}) bool {
	srcType := indirectType(reflect.TypeOf(data))
	return srcType.Kind() == reflect.Struct
}

func indirectType(reflectType reflect.Type) (_ reflect.Type) {
	for reflectType.Kind() == reflect.Ptr || reflectType.Kind() == reflect.Slice {
		reflectType = reflectType.Elem()
	}
	return reflectType
}

func newModalInst(modal interface{}) interface{} {
	srcType := indirectType(reflect.TypeOf(modal))
	ins := reflect.New(srcType)
	return ins.Interface()
}

func indirect(reflectValue reflect.Value) reflect.Value {
	for reflectValue.Kind() == reflect.Ptr {
		reflectValue = reflectValue.Elem()
	}
	return reflectValue
}

func getJsonDataType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.String:
		return "string"
	default:
		return ""
	}

}

func getFieldName(ft reflect.StructField) (string, bool) {
	tag := ft.Tag.Get("json")
	omitEmpty := false
	if strings.HasSuffix(tag, "omitempty") {
		omitEmpty = true
		idx := strings.Index(tag, ",")
		if idx > 0 {
			tag = tag[:idx]
		} else {
			tag = ft.Name
		}
	}
	return tag, omitEmpty
}

func getStructFieldName(data interface{}) []string {
	srcType := indirectType(reflect.TypeOf(data))
	ret := []string{}
	for m := 0; m < srcType.NumField(); m++ {
		field := srcType.Field(m)
		fieldName, _ := getFieldName(field)
		jsonTypeName := getJsonDataType(field.Type)
		if jsonTypeName == "" {
			continue
		}
		ret = append(ret, fieldName)
	}
	return ret
}

func structToMap(data interface{}) (map[string]interface{}, error) {
	srcValue := indirect(reflect.ValueOf(data))
	srcType := indirectType(reflect.TypeOf(data))
	ret := map[string]interface{}{}
	for m := 0; m < srcType.NumField(); m++ {
		field := srcType.Field(m)
		fieldName, _ := getFieldName(field)
		jsonTypeName := getJsonDataType(field.Type)
		if jsonTypeName == "" {
			continue
		}
		ret[fieldName] = srcValue.Field(m).Interface()
	}
	return ret, nil
}

func scanMapToStruct(dest interface{}, vals map[string]interface{}) error {
	srcValue := indirect(reflect.ValueOf(dest))
	srcType := indirectType(reflect.TypeOf(dest))

	for m := 0; m < srcType.NumField(); m++ {
		field := srcType.Field(m)
		fieldName, _ := getFieldName(field)
		jsonTypeName := getJsonDataType(field.Type)
		if jsonTypeName == "" {
			continue
		}
		v, ok := vals[fieldName].(string)
		if !ok {
			continue
		}
		dec := decoders[field.Type.Kind()]
		if dec == nil {
			continue
		}
		err := dec(srcValue.Field(m), v)
		if err != nil {
			fmt.Printf("set field(%s)=%s err:%v\n", fieldName, v, err)
			continue
		}
	}
	return nil
}

func decodeBool(f reflect.Value, s string) error {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	f.SetBool(b)
	return nil
}

func decodeInt8(f reflect.Value, s string) error {
	return decodeNumber(f, s, 8)
}

func decodeInt16(f reflect.Value, s string) error {
	return decodeNumber(f, s, 16)
}

func decodeInt32(f reflect.Value, s string) error {
	return decodeNumber(f, s, 32)
}

func decodeInt64(f reflect.Value, s string) error {
	return decodeNumber(f, s, 64)
}

func decodeInt(f reflect.Value, s string) error {
	return decodeNumber(f, s, 0)
}

func decodeNumber(f reflect.Value, s string, bitSize int) error {
	v, err := strconv.ParseInt(s, 10, bitSize)
	if err != nil {
		return err
	}
	f.SetInt(v)
	return nil
}

func decodeUint8(f reflect.Value, s string) error {
	return decodeUnsignedNumber(f, s, 8)
}

func decodeUint16(f reflect.Value, s string) error {
	return decodeUnsignedNumber(f, s, 16)
}

func decodeUint32(f reflect.Value, s string) error {
	return decodeUnsignedNumber(f, s, 32)
}

func decodeUint64(f reflect.Value, s string) error {
	return decodeUnsignedNumber(f, s, 64)
}

func decodeUint(f reflect.Value, s string) error {
	return decodeUnsignedNumber(f, s, 0)
}

func decodeUnsignedNumber(f reflect.Value, s string, bitSize int) error {
	v, err := strconv.ParseUint(s, 10, bitSize)
	if err != nil {
		return err
	}
	f.SetUint(v)
	return nil
}

func decodeFloat32(f reflect.Value, s string) error {
	v, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return err
	}
	f.SetFloat(v)
	return nil
}

// although the default is float64, but we better define it.
func decodeFloat64(f reflect.Value, s string) error {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	f.SetFloat(v)
	return nil
}

func decodeString(f reflect.Value, s string) error {
	f.SetString(s)
	return nil
}
