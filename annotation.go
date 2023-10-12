package db

import (
	"log"
	"reflect"
)

type Column struct {
	AttributeName   string
	ColumnString    string
	TypeString      string
	ForeignKey      string
	ReferenceString string
	PrimaryKey      string
	NullString      string
	DefaultString   string
	JoinString      string
	TableName       string
	SQLDefinition   string
}

func processTypeForColumns(t reflect.Type, columns map[string][]Column, key string) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("panic at %s:%s", key, err)
		}
	}()
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			_, skip := t.Field(i).Tag.Lookup("dbskip")
			if skip {
				continue
			}
			columns = regularLogic(t, columns, key, i)
		}
	}

}

func regularLogic(t reflect.Type, columns map[string][]Column, key string, idx int) map[string][]Column {
	field := t.Field(idx)
	_, dbskipOk := field.Tag.Lookup("dbskip")
	if dbskipOk {
		columns[key] = append(columns[key], Column{})
		return columns
	}
	columnString, columnOk := field.Tag.Lookup("column")
	datatypeString, datatypeOk := field.Tag.Lookup("datatype")
	primaryKeyString, primaryKeyOk := field.Tag.Lookup("primaryKey")
	foreignKeyString, foreignKeyOk := field.Tag.Lookup("foreignKey")
	referencesString, referencesOk := field.Tag.Lookup("references")
	nullString, nullStringOk := field.Tag.Lookup("null")
	defaultString, defaultOk := field.Tag.Lookup("default")
	joinString, joinOk := field.Tag.Lookup("join")
	tableNameString, tableNameOk := field.Tag.Lookup("tableName")

	if !columnOk {
		columns[key] = append(columns[key], Column{})
		return columns
	}
	column := Column{
		AttributeName: field.Name,
		ColumnString:  columnString,
	}
	if tableNameOk {
		column.TableName = tableNameString
	}
	switch field.Type.Kind() {
	case reflect.String:
		if datatypeOk {
			if datatypeString == "uuid.UUID" {
				column.TypeString = "varchar(35)"
				if primaryKeyOk {
					column.PrimaryKey = primaryKeyString
				}
				if nullStringOk {
					column.NullString = nullString
				}
				if defaultOk {
					column.DefaultString = defaultString
				}
				if foreignKeyOk {
					column.ForeignKey = foreignKeyString
				}
				if referencesOk {
					column.ReferenceString = referencesString
				}
			} else if datatypeString == "time.TIME" {
				column.TypeString = "datetime"
				if defaultOk {
					column.DefaultString = defaultString
				}
				if nullStringOk {
					column.NullString = nullString
				}
			} else {
				column.TypeString = datatypeString
			}
		} else {
			column.TypeString = "varchar(255)"
		}
		if nullStringOk && nullString == "true" {
			column.NullString = "null"
		} else if nullStringOk && nullString == "false" {
			column.NullString = "not null"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "integer"
		}
		if nullStringOk && nullString == "true" {
			column.NullString = "null"
		} else if nullStringOk && nullString == "false" {
			column.NullString = "not null"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Bool:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "tinyint(1)"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "float(8,2)"
		}
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Uint:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "integer"
		}
		if nullStringOk && nullString == "true" {
			column.NullString = "null"
		} else if nullStringOk && nullString == "false" {
			column.NullString = "not null"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		if joinOk {
			column.JoinString = joinString
		}
	case reflect.Ptr:
		ptrField := field.Type.Elem()
		if ptrField.Kind() == reflect.Array || ptrField.Kind() == reflect.Slice {
			if joinOk {
				column.JoinString = joinString
			}
		}
		if ptrField.Kind() == reflect.Struct {
			column.TypeString = "varchar(35)"
			if foreignKeyOk {
				column.ForeignKey = foreignKeyString
			}
			if referencesOk {
				column.ReferenceString = referencesString
			}
			if nullStringOk {
				column.NullString = nullString
			}
		}
	case reflect.Struct:
		processTypeForColumns(field.Type, columns, field.Name)
	default:
		columns[key] = append(columns[key], column)
		return columns
	}
	columns[key] = append(columns[key], column)
	return columns
}

func ptrLogic(t reflect.Type, columns map[string][]Column, key string, idx int) map[string][]Column {
	field := t.Field(idx)
	_, dbskipOk := field.Tag.Lookup("dbskip")
	if dbskipOk {
		columns[key] = append(columns[key], Column{})
		return columns
	}
	columnString, columnOk := field.Tag.Lookup("column")
	datatypeString, datatypeOk := field.Tag.Lookup("datatype")
	primaryKeyString, primaryKeyOk := field.Tag.Lookup("primaryKey")
	foreignKeyString, foreignKeyOk := field.Tag.Lookup("foreignKey")
	referencesString, referencesOk := field.Tag.Lookup("references")
	nullString, nullStringOk := field.Tag.Lookup("null")
	defaultString, defaultOk := field.Tag.Lookup("default")
	joinString, joinOk := field.Tag.Lookup("join")
	tableNameString, tableNameOk := field.Tag.Lookup("tableName")

	if !columnOk {
		columns[key] = append(columns[key], Column{})
		return columns
	}
	column := Column{
		AttributeName: field.Name,
		ColumnString:  columnString,
	}
	if tableNameOk {
		column.TableName = tableNameString
	}
	switch field.Type.Kind() {
	case reflect.String:
		if datatypeOk {
			if datatypeString == "uuid.UUID" {
				column.TypeString = "varchar(35)"
				if primaryKeyOk {
					column.PrimaryKey = primaryKeyString
				}
				if nullStringOk {
					column.NullString = nullString
				}
				if defaultOk {
					column.DefaultString = defaultString
				}
				if foreignKeyOk {
					column.ForeignKey = foreignKeyString
				}
				if referencesOk {
					column.ReferenceString = referencesString
				}
			} else if datatypeString == "time.TIME" {
				column.TypeString = "datetime"
				if defaultOk {
					column.DefaultString = defaultString
				}
				if nullStringOk {
					column.NullString = nullString
				}
			} else {
				column.TypeString = datatypeString
			}
		} else {
			column.TypeString = "varchar(255)"
		}
		if nullStringOk && nullString == "true" {
			column.NullString = "null"
		} else if nullStringOk && nullString == "false" {
			column.NullString = "not null"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "integer"
		}
		if nullStringOk && nullString == "true" {
			column.NullString = "null"
		} else if nullStringOk && nullString == "false" {
			column.NullString = "not null"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Bool:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "tinyint(1)"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "float(8,2)"
		}
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Uint:
		if datatypeOk {
			column.TypeString = datatypeString
		} else {
			column.TypeString = "integer"
		}
		if nullStringOk && nullString == "true" {
			column.NullString = "null"
		} else if nullStringOk && nullString == "false" {
			column.NullString = "not null"
		}
		if defaultOk {
			column.DefaultString = defaultString
		}
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		if joinOk {
			column.JoinString = joinString
		}
	case reflect.Ptr:
		ptrField := field.Type.Elem()
		if ptrField.Kind() == reflect.Array || ptrField.Kind() == reflect.Slice {
			if joinOk {
				column.JoinString = joinString
			}
		}
		if ptrField.Kind() == reflect.Struct {
			column.TypeString = "varchar(35)"
			if foreignKeyOk {
				column.ForeignKey = foreignKeyString
			}
			if referencesOk {
				column.ReferenceString = referencesString
			}
			if nullStringOk {
				column.NullString = nullString
			}
		}
	default:
		columns[key] = append(columns[key], column)
		return columns
	}
	columns[key] = append(columns[key], column)
	return columns
}
