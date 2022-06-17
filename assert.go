package netx

import "reflect"

func assert(v interface{}, errmsg string) {
	if isnil(v) {
		panic(errmsg)
	}
}

func isnil(v interface{}) bool {
	if v == nil {
		return true
	}
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map,
		reflect.Interface, reflect.Slice,
		reflect.Ptr, reflect.UnsafePointer:
		return value.IsNil()
	}
	return false
}
