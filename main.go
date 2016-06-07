package main

import (
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"github.com/pkg/errors"
)

var checkPidSQL = `select pg_backend_pid()`

// Queryer is an interface used by Get and Select
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Execer is an interface used by MustExec and LoadFile
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Ext is a union interface which can bind, query, and exec, used by
// NamedQuery and NamedExec.
type Ext interface {
	Queryer
	Execer
}

// DB db struct
type DB struct {
	*sql.DB
}

// DBConfig db config
type DBConfig struct {
	Host     string
	User     string
	Pass     string
	Database string
	Port     uint16
	NumConn  int
}

// NewDB create DB
func NewDB(config *DBConfig) (*DB, error) {
	poolcfg := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     config.Host,
			User:     config.User,
			Password: config.Pass,
			Database: config.Database,
			Port:     config.Port,
		},
		MaxConnections: config.NumConn,
	}
	pool, err := pgx.NewConnPool(poolcfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed create pgx pool")
	}
	db, err := stdlib.OpenFromConnPool(pool)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create db")
	}
	return &DB{db}, nil
}

func selectTime(ext Ext) (time.Time, error) {
	var tm time.Time
	err := ext.QueryRow(`select now()`).Scan(&tm)
	if err != nil {
		return tm, errors.Wrap(err, "failed to select time")
	}
	return tm, nil
}

type nullTime struct {
	Time  time.Time
	Valid bool
}

func (nt *nullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

func (nt nullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

func insertValuePlainSQL(ext Ext, t nullTime) error {
	_, err := ext.Exec(`INSERT INTO test (t) values ($1)`, t)
	if err != nil {
		return err
	}
	return nil
}

func insertValuePgxSQL(ext *sql.DB, t pgx.NullTime) error {
	if driver, ok := ext.Driver().(*stdlib.Driver); ok && driver.Pool != nil {
		_, err := driver.Pool.Exec(`INSERT INTO test (t) values ($1)`, t)
		if err != nil {
			return err
		}
	}
	return nil
}
