package gorm

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/pub-burrito/pq/arrays"
	"github.com/pub-burrito/pq/hstore"
)

type postgres struct {
}

func (s *postgres) BinVar(i int) string {
	return fmt.Sprintf("$%v", i)
}

func intSliceToDbValue(s interface{}) interface{} {
	var cc string
	rv := reflect.ValueOf(s)
	switch l := rv.Len(); {
	case l == 0:
		cc = "{}"
	case l == 1:
		cc = fmt.Sprintf("{%d}", rv.Index(0).Int())
	default:
		cc = "{"
		for i := 0; i < l-1; i++ {
			cc = fmt.Sprintf("%s%d,", cc, rv.Index(i).Int())
		}
		cc = fmt.Sprintf("%s%d}", cc, rv.Index(l-1).Int())
	}
	return cc
}

func floatSliceToDbValue(s interface{}) interface{} {
	var cc string
	rv := reflect.ValueOf(s)
	switch l := rv.Len(); {
	case l == 0:
		cc = "{}"
	case l == 1:
		cc = fmt.Sprintf("{%f}", rv.Index(0).Float())
	default:
		cc = "{"
		for i := 0; i < l-1; i++ {
			cc = fmt.Sprintf("%s%f,", cc, rv.Index(i).Float())
		}
		cc = fmt.Sprintf("%s%f}", cc, rv.Index(l-1).Float())
	}
	return cc
}

func stringSliceToDbValue(s interface{}) interface{} {
	var cc string
	rv := reflect.ValueOf(s)
	switch l := rv.Len(); {
	case l == 0:
		cc = "{}"
	case l == 1:
		cc = fmt.Sprintf("{\"%s\"}", rv.Index(0).String())
	default:
		cc = "{"
		for i := 0; i < l-1; i++ {
			cc = fmt.Sprintf("%s\"%s\",", cc, rv.Index(i).String())
		}
		cc = fmt.Sprintf("%s\"%s\"}", cc, rv.Index(l-1).String())
	}
	return cc
}

func (s *postgres) DbValue(value interface{}) interface{} {
	if reflect.ValueOf(value).Kind() == reflect.Slice {
		switch reflect.TypeOf(value).Elem().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return intSliceToDbValue(value)
		case reflect.String:
			return stringSliceToDbValue(value)
		case reflect.Float32, reflect.Float64:
			return floatSliceToDbValue(value)
		default:
			return value
		}

	}
	return value
}

func (s *postgres) SupportLastInsertId() bool {
	return false
}

func (d *postgres) SqlTag(value reflect.Value, size int) string {
	switch value.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		return "integer"
	case reflect.Int64, reflect.Uint64:
		return "bigint"
	case reflect.Float32, reflect.Float64:
		return "numeric"
	case reflect.String:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("varchar(%d)", size)
		}
		return "text"
	case reflect.Struct:
		if value.Type() == timeType {
			return "timestamp with time zone"
		}
	case reflect.Map:
		if value.Type() == hstoreType {
			return "hstore"
		}
	default:
		if _, ok := value.Interface().([]byte); ok {
			return "bytea"
		}
	}
	panic(fmt.Sprintf("invalid sql type %s (%s) for postgres", value.Type().Name(), value.Kind().String()))
}

func (s *postgres) PrimaryKeyTag(value reflect.Value, size int) string {
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		return "serial PRIMARY KEY"
	case reflect.Int64, reflect.Uint64:
		return "bigserial PRIMARY KEY"
	default:
		panic("Invalid primary key type")
	}
}

func (s *postgres) ReturningStr(key string) string {
	return fmt.Sprintf("RETURNING \"%v\"", key)
}

func (s *postgres) Quote(key string) string {
	return fmt.Sprintf("\"%s\"", key)
}

func (s *postgres) HasTable(scope *Scope, tableName string) bool {
	var count int
	newScope := scope.New(nil)
	newScope.Raw(fmt.Sprintf("SELECT count(*) FROM INFORMATION_SCHEMA.tables where table_name = %v", newScope.AddToVars(tableName)))
	newScope.DB().QueryRow(newScope.Sql, newScope.SqlVars...).Scan(&count)
	return count > 0
}

func (s *postgres) HasColumn(scope *Scope, tableName string, columnName string) bool {
	var count int
	newScope := scope.New(nil)
	newScope.Raw(fmt.Sprintf("SELECT count(*) FROM information_schema.columns WHERE table_name = %v AND column_name = %v",
		newScope.AddToVars(tableName),
		newScope.AddToVars(columnName),
	))
	newScope.DB().QueryRow(newScope.Sql, newScope.SqlVars...).Scan(&count)
	return count > 0
}

var hstoreType = reflect.TypeOf(Hstore{})

type Hstore map[string]*string

func (h Hstore) Value() (driver.Value, error) {
	hstore := hstore.Hstore{Map: map[string]sql.NullString{}}
	if len(h) == 0 {
		return nil, nil
	}

	for key, value := range h {
		hstore.Map[key] = sql.NullString{String: *value, Valid: true}
	}
	return hstore.Value()
}

func (h *Hstore) Scan(value interface{}) error {
	hstore := hstore.Hstore{}

	if err := hstore.Scan(value); err != nil {
		return err
	}

	if len(hstore.Map) == 0 {
		return nil
	}

	*h = Hstore{}
	for k := range hstore.Map {
		if hstore.Map[k].Valid {
			s := hstore.Map[k].String
			(*h)[k] = &s
		} else {
			(*h)[k] = nil
		}
	}

	return nil
}

type ArrayType struct {
	ActualDest reflect.Value
}

func NewArrayType(dest reflect.Value) *ArrayType {
	return &ArrayType{ActualDest: dest}
}

func (a *ArrayType) Scan(value interface{}) error {
	switch a.ActualDest.Type().Elem().Kind() {
	case reflect.String:
		arrays.Unmarshal(value.([]byte), a.ActualDest.Addr().Interface())
	case reflect.Int, reflect.Int8, reflect.Int64:
		if st := reflect.TypeOf(value); st.Kind() == reflect.Slice {
			sv := reflect.ValueOf(value)
			switch st.Elem().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				l := sv.Len()
				a.ActualDest.Set(reflect.MakeSlice(a.ActualDest.Type(), l, l))
				for i := 0; i < l; i++ {
					a.ActualDest.Index(i).SetInt(sv.Index(i).Int())
				}
			}
		}
	}
	// TODO: there are some error conditions!
	return nil
}
