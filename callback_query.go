package gorm

import (
	"reflect"
	"strings"
	"time"

	"github.com/pub-burrito/pq/arrays"
)

func Query(scope *Scope) {
	defer scope.Trace(time.Now())

	var (
		isSlice        bool
		isPtr          bool
		anyRecordFound bool
		destType       reflect.Type
	)

	var dest = reflect.Indirect(reflect.ValueOf(scope.Value))
	if value, ok := scope.Get("gorm:query_destination"); ok {
		dest = reflect.Indirect(reflect.ValueOf(value))
	}

	if dest.Kind() == reflect.Slice {
		isSlice = true
		destType = dest.Type().Elem()
		if destType.Kind() == reflect.Ptr {
			isPtr = true
			destType = destType.Elem()
		}
	} else {
		scope.Search = scope.Search.clone().limit(1)
	}

	scope.prepareQuerySql()

	if !scope.HasError() {
		rows, err := scope.DB().Query(scope.Sql, scope.SqlVars...)

		if scope.Err(err) != nil {
			return
		}

		defer rows.Close()
		for rows.Next() {
			anyRecordFound = true
			elem := dest
			if isSlice {
				elem = reflect.New(destType).Elem()
			}

			columns, _ := rows.Columns()
			var values []interface{}
			slices := make(map[reflect.Value]*interface{})
			for _, value := range columns {
				field := elem.FieldByName(snakeToUpperCamel(strings.ToLower(value)))
				if field.IsValid() {
					if field.Kind() == reflect.Slice {
						var intr interface{}
						slices[field] = &intr
						values = append(values, &intr)
					} else {
						values = append(values, field.Addr().Interface())
					}
				} else {
					var ignore interface{}
					values = append(values, &ignore)
				}
			}
			scope.Err(rows.Scan(values...))

			// TODO: PJW This should be done in postgres.go!!!
			for k, v := range slices {
				// should probably switch on source type as that is what drives this difference.  Integer type arrays
				// seem to come back as arrays; string arrays come back as a postgres type string "{abc, def, ghi}", which
				// must be parsed by pg/arrays.  This string is represented as a byte array.
				switch k.Type().Elem().Kind() {
				case reflect.String:
					arrays.Unmarshal((*v).([]byte), k.Addr().Interface())
				case reflect.Int, reflect.Int8, reflect.Int64:
					if st := reflect.TypeOf(*v); st.Kind() == reflect.Slice {
						sv := reflect.ValueOf(*v)
						switch st.Elem().Kind() {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
							reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
							l := sv.Len()
							k.Set(reflect.MakeSlice(k.Type(), l, l))
							for i := 0; i < l; i++ {
								k.Index(i).SetInt(sv.Index(i).Int())
							}
						}
					}
				}
			}
			if isSlice {
				if isPtr {
					dest.Set(reflect.Append(dest, elem.Addr()))
				} else {
					dest.Set(reflect.Append(dest, elem))
				}
			}
		}

		if !anyRecordFound {
			scope.Err(RecordNotFound)
		}
	}
}

func AfterQuery(scope *Scope) {
	scope.CallMethod("AfterFind")
}

func init() {
	DefaultCallback.Query().Register("gorm:query", Query)
	DefaultCallback.Query().Register("gorm:after_query", AfterQuery)
}
