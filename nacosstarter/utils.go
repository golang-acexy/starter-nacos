package nacosstarter

import (
	"reflect"
)

func clearSliceAndMapInPlace(v any) {
	clearSliceAndMap(reflect.ValueOf(v))
}

func clearSliceAndMap(v reflect.Value) {
	if !v.IsValid() {
		return
	}

	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if field.CanSet() {
				clearSliceAndMap(field)
			}
		}
	case reflect.Slice:
		if v.CanSet() {
			v.SetLen(0) // 保持容量和底层数组
		}
	case reflect.Map:
		if v.CanSet() && v.Len() > 0 {
			// 快速清空map
			keys := v.MapKeys()
			for _, key := range keys {
				v.SetMapIndex(key, reflect.Value{})
			}
		}
	}
}
