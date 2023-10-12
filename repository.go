package db

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
)

const SqlUuid string = "varchar(35) not null"
const SqlUuidPk string = "varchar(35) not null primary key"
const SqlString string = "varchar(255) not null"
const SqlLong string = "text"
const SqlInt string = "int not null default 0"

type Repository struct {
	DB     *DB
	Tables []Entity
}

type KVP struct {
	Key   string
	Value interface{}
}

type FieldDataTypes struct {
	Name     string
	DataType string
}

type JoinTable struct {
	FirstTable  string
	SecondTable string
	FirstKey    string
	SecondKey   string
	SQL         string
}

type Alters struct {
	Table      string
	Key        string
	ForeignKey string
	Reference  string
	SQL        string
}

type Entity interface {
	Scan(*sql.Rows, []Entity) error
	ScanLocal(*sql.Rows, Entity) error
	GetTable() string
	SetCreateTable(map[string][]Column) Entity
	GetCreateTable() map[string][]Column
	GetID() (string, error)
	GetChildren() ([]Entity, error)
	GetJoin(Entity) (IJoinTable, error)
}

type IJoinTable interface {
	GetTable() string
	GetID() (string, error)
	GetParentTable() string
	GetChildTable() string
}

func (c Repository) Select(ent Entity, id string) ([]Entity, error) {
	results := make([]Entity, 0)
	rows, err := c.DB.Conn.Query("SELECT * FROM "+ent.GetTable()+" WHERE ID = ?", id)
	defer rows.Close()
	if err != nil {
		return nil, fmt.Errorf(ent.GetTable()+" %q: %v", id, err)
	}
	err = ent.Scan(rows, results)
	if err = handleSQLError(rows, ent, "SELECT", err, id); err != nil {
		return nil, err
	}
	return results, nil
}

func (c Repository) SelectIn(ent Entity, ids []string) ([]Entity, error) {
	results := make([]Entity, 0)
	placeholders := make([]string, 0)
	for i := 0; i < len(ids); i++ {
		placeholders = append(placeholders, "?")
	}
	rows, err := c.DB.Conn.Query(fmt.Sprintf("SELECT * FROM %s WHERE ID IN (%s)", ent.GetTable(), strings.Join(placeholders, ",")), ids)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	err = ent.Scan(rows, results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (c Repository) Take(result Entity, id string) error {
	rows, err := c.DB.Conn.Query("SELECT * FROM "+result.GetTable()+" WHERE ID = ? LIMIT 1", id)
	if err != nil {
		return fmt.Errorf(result.GetTable()+" %q: %v", id, err)
	}
	defer rows.Close()
	results := make([]Entity, 1)
	err = result.Scan(rows, results)
	if err = handleSQLError(rows, result, "SELECT", err, id); err != nil {
		return err
	}
	return nil
}

func (c Repository) Find(ent Entity) ([]Entity, error) {
	ret := make([]Entity, 0)
	id, err := ent.GetID()
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.Conn.Query(selectQuery(ent.GetTable()), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := ent.Scan(rows, ret); err != nil {
			return nil, fmt.Errorf("scan %v", err)
		}
		ret = append(ret, ret...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func (c Repository) All(ids []string, ent Entity) ([]Entity, error) {
	results := make([]Entity, 0)
	placeholders := make([]string, 0)
	for _, v := range ids {
		placeholders = append(placeholders, v)
	}
	row, err := c.DB.Conn.Query(`SELECT * FROM %s t WHERE t.id IN (%s)`,
		ent.GetTable(), strings.Join(placeholders, ","), strings.Join(ids, ","),
	)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	for row.Next() {
		var result Entity
		if err := result.Scan(row, results); err != nil {
			return nil, fmt.Errorf("scan %v", err)
		}
		results = append(results, result)
	}
	if err := row.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (c Repository) Save(ent Entity) error {
	q := insertQuery(strings.Join(GetColumns(ent), ","), strings.Join(GetPlaceholders(ent), ","), ent.GetTable())
	_, err := c.DB.Conn.Query(fmt.Sprintf(q), GetValues(ent)...)
	if err != nil {
		return err
	}
	ents, err := ent.GetChildren()
	if err != nil {
		return err
	}
	err = c.SaveAll(ents)
	if err != nil {
		return err
	}
	return err
}

func (c Repository) SaveAll(ents []Entity) error {
	save := make([]Entity, 0)
	joins := make([]IJoinTable, 0)
	for _, v := range ents {
		placeholders := make([]string, 0)
		values := make([]interface{}, 0)
		save = append(save, v)
		placeholders = append(placeholders, strings.Join(GetPlaceholders(v), ","))
		values = append(values, GetValues(v))
		children, err := v.GetChildren()
		if err != nil {
			return err
		}
		err = c.Save(v)
		if err != nil {
			return err
		}
		save, joins, err = c.SaveChildren(v, children, save)
	}
	for _, j := range joins {
		if e, ok := j.(Entity); ok {
			err := c.Save(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c Repository) SaveChildren(parent Entity, children []Entity, save []Entity) ([]Entity, []IJoinTable, error) {
	joins := make([]IJoinTable, 0)
	for _, s := range children {
		c.Save(s)

		join, err := parent.GetJoin(s)
		if err != nil {
			return nil, nil, err
		}
		if j, ok := join.(Entity); ok {
			err = c.Save(j)
		}
		childrenWithJoins, err := s.GetChildren()
		if err != nil {
			return nil, nil, err
		}
		if len(childrenWithJoins) > 0 {
			c.SaveChildren(s, childrenWithJoins, save)
		}
	}
	return save, joins, nil
}

func (c Repository) Update(e Entity, id string, updates []KVP) error {
	query := "UPDATE TABLE " + e.GetTable()
	values := make([]interface{}, 0)
	for _, kvp := range updates {
		query += " SET " + kvp.Key + " = ?"
		values = append(values, kvp.Value)
	}
	query += " WHERE ID = ?"
	values = append(values, id)
	rows, err := c.DB.Conn.Query(query, values...)
	defer rows.Close()
	return handleSQLError(rows, e, "UPDATE", err, id)
}

func (c Repository) Insert(e Entity) error {
	rows, err := c.DB.Conn.Query("INSERT INTO "+e.GetTable()+" ("+strings.Join(GetColumns(e), ",")+") VALUES ("+strings.Join(GetPlaceholders(e), ", ")+")", GetValues(e)...)
	defer rows.Close()
	return handleSQLError(rows, e, "INSERT", err, "")
}

func (c Repository) Delete(e Entity) error {
	id, err := e.GetID()
	if err != nil {
		return err
	}
	rows, err := c.DB.Conn.Query("DELETE FROM "+e.GetTable()+" WHERE ID = ?", id)
	defer rows.Close()
	return handleSQLError(rows, e, "DELETE", err, "")
}

func handleSQLError(rows *sql.Rows, e Entity, action string, err error, id string) error {
	if id == "" {
		if err != nil {
			return fmt.Errorf(e.GetTable()+" %s: %v", action, err)
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf(e.GetTable()+" %s: %v", action, err)
		}
	} else {
		if err != nil {
			return fmt.Errorf(e.GetTable()+" %s %q: %v", action, id, err)
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf(e.GetTable()+" %s %q: %v", action, id, err)
		}
	}
	return nil
}

func GetFieldDataTypes(f []FieldDataTypes) string {
	s := make([]string, 0)
	for _, d := range f {
		switch d.DataType {
		case "string":
			s = append(s, strings.ToUpper(d.Name)+" "+SqlString)
		case "uuid":
			s = append(s, strings.ToUpper(d.Name)+" "+SqlUuid)
		case "uuidpk":
			s = append(s, strings.ToUpper(d.Name)+" "+SqlUuidPk)
		case "int":
			s = append(s, strings.ToUpper(d.Name)+" "+SqlInt)
		case "long":
			s = append(s, strings.ToUpper(d.Name)+" "+SqlLong)
		default:
			s = append(s, strings.ToUpper(d.Name)+" "+d.DataType)
		}
	}
	return strings.Join(s, ",\n")
}

func (c Repository) CreateTableSQL(table string, fields []FieldDataTypes) string {
	base := fmt.Sprintf(`CREATE TABLE %s (
		%s
	)`, table, GetFieldDataTypes(fields))
	return base
}

func (c Repository) CreateTableSQLFromAnnotations(e Entity) string {
	columns := make(map[string][]Column)
	t := reflect.TypeOf(e)
	processTypeForColumns(t, columns, "")
	return ""
}

func (c Repository) NewFieldDataType(name string, datatype string) FieldDataTypes {
	return FieldDataTypes{
		Name:     name,
		DataType: datatype,
	}
}

func (c Repository) NewFieldDataTypes() []FieldDataTypes {
	return make([]FieldDataTypes, 0)
}

func (c *Repository) RegisterTable(ent ...Entity) {
	for _, e := range ent {
		c.Tables = append(c.Tables, e)
	}
}

func (c Repository) GetChildIds(parent Entity, child Entity) ([]string, error) {
	parentName := CamelToSnake(reflect.TypeOf(parent).Name())
	childName := CamelToSnake(reflect.TypeOf(child).Name())
	parentId, err := parent.GetID()
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.Conn.Query(fmt.Sprintf("SELECT %s_id FROM %s_%s WHERE %s_id = ?", childName, parentName, childName, parentName), parentId)
	results := make([]string, 0)
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return nil, fmt.Errorf("%s GetChildIds:%v", parentName, err)
		}
		results = append(results, result)
	}
	return results, nil
}

func (c Repository) GetChildren(parentType Entity, childType Entity) ([]Entity, error) {
	childIds, err := c.GetChildIds(parentType, childType)
	if err != nil {
		return nil, fmt.Errorf("get child ids: %v", err)
	}
	children, err := c.All(childIds, childType)
	if err != nil {
		return nil, fmt.Errorf("get child objects: %v", err)
	}
	return children, nil
}

func (c *Alters) GenerateSQL(tableName string) {
	tableName = CamelToSnake(tableName)
	c.SQL = fmt.Sprintf("ALTER TABLE `%s` ADD CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`id`)", tableName, fmt.Sprintf("fk_%s_%s", tableName, c.Reference), c.ForeignKey, c.Reference)
}

func (c Repository) CreateTables() error {
	out := make(chan map[string][]Column, len(c.Tables))
	var wg sync.WaitGroup
	cols := readAnnotations(c, &wg, out)

	joins := make([]JoinTable, 0)
	alters := make([]Alters, 0)
	tables := make([]string, 0)
	tableName := ""
	for _, columns := range cols {
		for key, attributes := range columns {
			columns := make([]string, 0)
			for _, attribute := range attributes {
				if attribute.TableName != "" {
					tableName = attribute.TableName
				}
				var column string
				if attribute.ColumnString != "" && attribute.JoinString == "" && attribute.TypeString != "" {
					attribute.GenerateSQL()
					column = attribute.SQLDefinition
				}
				if attribute.ColumnString != "" && attribute.JoinString != "" {
					first := strings.Split(attribute.JoinString, ",")
					t1 := strings.Split(first[0], ":")
					t2 := strings.Split(first[1], ":")
					jt := JoinTable{
						FirstTable:  t1[0],
						FirstKey:    t1[1],
						SecondTable: t2[0],
						SecondKey:   t2[1],
					}
					if tableName == "" {
						tableName = CamelToSnake(fmt.Sprintf("%s_%s", jt.FirstTable, jt.SecondTable))
					}
					jt.SQL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s varchar(35) not null, %s varchar(35) not null)", tableName, jt.FirstKey, jt.SecondKey)
					alter1 := Alters{
						Table:      tableName,
						Reference:  jt.FirstTable,
						ForeignKey: jt.FirstKey,
					}
					alter1.GenerateSQL(tableName)
					alter2 := Alters{
						Table:      tableName,
						Reference:  jt.SecondTable,
						ForeignKey: jt.SecondKey,
					}
					alter2.GenerateSQL(tableName)
					alters = append(alters, alter1, alter2)
					joins = append(joins, jt)
				}
				if attribute.ReferenceString != "" {
					if tableName == "" {
						tableName = CamelToSnake(key)
					}
					at := Alters{
						Table:     tableName,
						Reference: attribute.ReferenceString,
						Key:       attribute.ColumnString,
					}
					if attribute.ForeignKey != "" {
						at.ForeignKey = attribute.ForeignKey
					}
					at.GenerateSQL(at.Table)
					alters = append(alters, at)
				}
				if column != "" {
					columns = append(columns, column)
				}
			}
			if len(columns) > 0 {
				tables = append(tables, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\r\n%s\r\n)\r\n", CamelToSnake(key), strings.Join(columns, ",\r\n")))
			}
		}
	}
	erchan := make(chan string)
	for _, k := range tables {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			_, err := c.DB.Conn.Query(s)
			if err != nil {
				erchan <- fmt.Sprintf("%s:%s", s, err.Error())
			}
		}(k)
	}
	for _, k := range joins {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			_, err := c.DB.Conn.Query(s)
			if err != nil {
				erchan <- fmt.Sprintf("%s:%s", s, err.Error())
			}
		}(k.SQL)
	}
	wg.Wait()
	for _, k := range alters {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			_, err := c.DB.Conn.Query(s)
			if err != nil {
				erchan <- fmt.Sprintf("%s:%s", s, err.Error())
			}
		}(k.SQL)
	}
	go func() {
		wg.Wait()
		defer close(erchan)
	}()
	log.Printf("error count: %d", len(erchan))
	open := true
	i := 0
	for open {
		er, open := <-erchan
		log.Println(er)
		i++
		if !open {
			break
		}
	}
	return nil
}

func CamelToSnake(s string) string {
	low := "abcdefghijklmnopqrstuvwxyz"
	upper := strings.ToUpper(low)
	ret := ""
	for i, r := range s {
		if strings.Contains(upper, string(r)) {
			idx := strings.Index(upper, string(r))
			rep := string(low[idx])
			if i == 0 {
				ret += rep
			} else {
				ret += "_" + rep
			}
		} else {
			ret += string(r)
		}
	}
	return ret
}

func (c *Column) GenerateSQL() {
	if c.ColumnString != "" {
		c.SQLDefinition = fmt.Sprintf("    %s", c.ColumnString)
		if c.TypeString != "" {
			ds := ""
			if c.DefaultString != "" {
				ds = fmt.Sprintf(" DEFAULT %s", c.DefaultString)
			}
			switch c.TypeString {
			case "varchar(35)":
				fallthrough
			case "uuid.UUID":
				c.SQLDefinition += " varchar(35)"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
				if c.PrimaryKey != "" {
					c.SQLDefinition += " PRIMARY KEY"
				}
			case "datetime":
				fallthrough
			case "time.TIME":
				c.SQLDefinition += " DATETIME"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
			case "bool":
				c.SQLDefinition += " TINYINT(1)"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
			case "float":
				c.SQLDefinition += " FLOAT(8, 2)"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
			case "int":
				fallthrough
			case "time.Duration":
				c.SQLDefinition += " INT"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
			case "long":
				c.SQLDefinition += " TEXT"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
			case "string":
				fallthrough
			default:
				c.SQLDefinition += " varchar(255)"
				if c.NullString != "" {
					c.SQLDefinition += " NOT NULL"
				}
				c.SQLDefinition += ds
			}
		}
	}
}

func readAnnotations(c Repository, wg *sync.WaitGroup, out chan map[string][]Column) []map[string][]Column {
	cols := make([]map[string][]Column, 0)
	if len(c.Tables) > 0 {
		for i := 0; i < len(c.Tables); i++ {
			wg.Add(1)
			go func(e Entity, idx int, out chan map[string][]Column) {
				defer wg.Done()
				columns := make(map[string][]Column)
				t := reflect.TypeOf(e)
				var name string
				if t.Kind() == reflect.Ptr {
					name = t.Elem().Name()
				} else {
					name = t.Name()
				}
				processTypeForColumns(t, columns, name)
				out <- columns
			}(c.Tables[i], i, out)
		}
		wg.Wait()
		go func(out chan map[string][]Column) {
			defer close(out)
		}(out)
		open := true
		for open {
			col, open := <-out
			cols = append(cols, col)
			if !open {
				break
			}
		}
	}
	return cols
}

func GetColumns(ent Entity) []string {
	results := make([]string, 0)
	r := reflect.TypeOf(ent)
	for i := 0; i < r.NumField(); i++ {
		field := r.Field(i)
		if f, ok := field.Tag.Lookup("column"); ok {
			results = append(results, f)
		}
	}
	return results
}

func GetPlaceholders(ent Entity) []string {
	results := make([]string, 0)
	r := reflect.TypeOf(ent)
	for i := 0; i < r.NumField(); i++ {
		field := r.Field(i)
		if _, ok := field.Tag.Lookup("column"); ok {
			results = append(results, "?")
		}
	}
	return results
}

func GetValues(ent Entity) []interface{} {
	results := make([]interface{}, 0)
	v := reflect.ValueOf(ent).Elem()
	t := reflect.TypeOf(ent)
	for i := 0; i < v.NumField(); i++ {
		if _, ok := t.Field(i).Tag.Lookup("column"); ok {
			results = append(results, v.Field(i).Interface())
		}
	}
	return results
}

func GetField(v Entity, fd string) interface{} {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(fd)
	return f
}

func insertQuery(columns, placeholders, table string) string {
	return fmt.Sprintf("INSERT (%s) VALUES (%s) INTO %s ON DUPLICATE KEY UPDATE", columns, placeholders, table)
}

func selectQuery(table string) string {
	return fmt.Sprintf("SELECT * FROM %s t WHERE t.id = ?`", table)
}

func selectInQuery(table, placeholders string) string {
	return fmt.Sprintf("SELECT * FROM %s t WHERE t.id IN (%s)", table, placeholders)
}

func deleteQuery(table string) string {
	return fmt.Sprintf("DELETE FROM %s t WHERE t.id = ?`", table)
}
