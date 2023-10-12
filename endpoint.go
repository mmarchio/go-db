package db

import (
	"errors"
	"log"
	"strings"
)

func (db DB) Take(dst *Entity, params ...string) error {
	var e Entity
	if dst != nil {
		e = *dst
	}
	var table, column, value string
	rows, err := db.Conn.Query("SELECT * FROM ? WHERE ? = ?", table, column, value)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	results := make([]Entity, 0)
	if err := e.Scan(rows, results); err != nil {
		return err
	}
	if len(results) > 0 {
		dst = &results[0]
	} else {
		return errors.New("0 results found")
	}
	return nil
}

func (db *DB) QueryBuilder(dst *Entity) *DB {
	db.query = ""
	return db
}

func (db *DB) Select(table, tableAlias, columnAlias string, column []string) *DB {
	db.query = "SELECT "
	for i, v := range column {
		column[i] = columnAlias + "." + v
	}
	db.query += strings.Join(column, ", ")
	db.query += " FROM " + table + " " + tableAlias
	return db
}

type Table struct {
	Name  string
	Alias string
	Key   string
}

func (db *DB) Join(joinType string, table1, table2 Table) *DB {
	db.query += strings.ToUpper(joinType) + " " + table1.Name + " " + table1.Alias + " ON " + table1.Alias + "." + table1.Key + " = " + table2.Alias + "." + table2.Key
	return db
}

func (db *DB) Where(t Table, v string, o string) *DB {
	db.query += " WHERE " + t.Alias + "." + t.Key + " " + o + " " + v
	return db
}

func (db *DB) And() *DB {
	db.query += " AND"
	return db
}

func (db *DB) Or() *DB {
	db.query += " OR"
	return db
}
