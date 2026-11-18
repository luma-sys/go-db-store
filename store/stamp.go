package store

import (
	"fmt"
	"reflect"
	"time"
)

var (
	timeType    = reflect.TypeFor[time.Time]()
	timePtrType = reflect.TypeFor[*time.Time]()
)

// entityValue devolve a struct apontada por e; reflect em nil ou não-struct entraria em pânico.
func entityValue(e any) (reflect.Value, error) {
	v := reflect.ValueOf(e)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}, fmt.Errorf("entidade nil")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("entidade deve ser struct, recebeu %T", e)
	}
	return v, nil
}

// stamp grava now nos campos de data presentes em v; keep preserva o valor já preenchido.
func stamp(v reflect.Value, now time.Time, keep bool, names ...string) error {
	for _, name := range names {
		f := v.FieldByName(name)
		if !f.IsValid() || (keep && !f.IsZero()) {
			continue
		}
		if !f.CanSet() {
			return fmt.Errorf("campo %s não pode ser gravado", name)
		}
		switch f.Type() {
		case timeType:
			f.Set(reflect.ValueOf(now))
		case timePtrType:
			t := now
			f.Set(reflect.ValueOf(&t))
		default:
			return fmt.Errorf("campo %s deve ser time.Time ou *time.Time, recebeu %s", name, f.Type())
		}
	}
	return nil
}
