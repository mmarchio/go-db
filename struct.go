package db

import (
	"database/sql"
	"log"

	"github.com/go-sql-driver/mysql"
)

type DB struct {
	Conn  *sql.DB
	user  string
	pass  string
	net   string
	addr  string
	dbn   string
	query string
}

func (d *DB) SetUser(v string) {
	d.user = v
}

func (d DB) GetUser() string {
	return d.user
}

func (d *DB) SetPass(v string) {
	d.pass = v
}

func (d DB) GetPass() string {
	return d.pass
}

func (d *DB) SetNet(v string) {
	d.net = v
}

func (d DB) GetNet() string {
	return d.net
}

func (d *DB) SetAddr(v string) {
	d.addr = v
}

func (d DB) GetAddr() string {
	return d.addr
}

func (d *DB) SetDBN(v string) {
	d.dbn = v
}

func (d DB) GetDBN() string {
	return d.dbn
}

func (d DB) GetCfg() *mysql.Config {
	return &mysql.Config{
		User:                 d.GetUser(),
		Passwd:               d.GetPass(),
		Net:                  d.GetNet(),
		Addr:                 d.GetAddr(),
		DBName:               d.GetDBN(),
		AllowNativePasswords: true,
	}
}

func (d *DB) Connect() {
	db, err := sql.Open("mysql", d.GetCfg().FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	d.Conn = db
	pingTest := d.Conn.Ping()
	if pingTest != nil {
		log.Fatal(err)
	}
}

type Model struct {
	ID      string `json:"id" column:"id" datatype:"uuid.UUID" null:"false" primaryKey:"true"`
	Created string `json:"created" column:"created" datatype:"time.TIME" null:"false" default:"NOW()"`
	Updated string `json:"updated" column:"updated" datatype:"time.TIME" null:"false" default:"NOW()"`
}
