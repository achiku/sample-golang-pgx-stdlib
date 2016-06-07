package main

import (
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

func testCreateDB(t *testing.T) (*DB, func()) {
	cfg := &DBConfig{
		Host:     "localhost",
		User:     "pgtest",
		Pass:     "",
		Database: "pgtest",
		Port:     5432,
	}
	db, err := NewDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS test (
		id SERIAL
		,t TIMESTAMP WITH TIME ZONE
	)
	`)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		db.Exec(`DROP TABLE test`)
		db.Close()
	}
	return db, cleanup
}

func TestNewDB(t *testing.T) {
	cfg := &DBConfig{
		Host:     "localhost",
		User:     "pgtest",
		Pass:     "",
		Database: "pgtest",
		Port:     5432,
	}
	db, err := NewDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Error(err)
	}
}

func TestSelectRow(t *testing.T) {
	db, cleanup := testCreateDB(t)
	defer cleanup()

	tm, err := selectTime(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tm)
}

func TestInsertValuePlainSQL(t *testing.T) {
	db, cleanup := testCreateDB(t)
	defer cleanup()

	tm := pgx.NullTime{
		Valid: false,
	}
	if err := insertValuePlainSQL(db, tm); err != nil {
		t.Fatal(err)
	}
}

func TestInsertValuePgxSQL(t *testing.T) {
	db, cleanup := testCreateDB(t)
	defer cleanup()

	tm := pgx.NullTime{
		Valid: false,
	}
	if err := insertValuePgxSQL(db.DB, tm); err != nil {
		t.Fatal(err)
	}
	if driver, ok := db.Driver().(*stdlib.Driver); ok && driver.Pool != nil {
		rows, err := driver.Pool.Query(`SELECT id, t FROM test`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		var i int
		var ntm pgx.NullTime
		for rows.Next() {
			err := rows.Scan(&i, &ntm)
			if err != nil {
				t.Fatal(err)
			}
			log.Printf("%d %v", i, ntm)
		}
	}
}

func TestConnectionAcquire(t *testing.T) {
	db, cleanup := testCreateDB(t)
	defer cleanup()

	var tm time.Time
	err := db.QueryRow(`SELECT now()`).Scan(&tm)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(tm)

	res, err := db.Exec(`INSERT INTO test (t) VALUES ($1)`, tm)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%+v", res)

	if driver, ok := db.Driver().(*stdlib.Driver); ok && driver.Pool != nil {
		log.Printf("none ava:%d, cur:%d, max:%d",
			driver.Pool.Stat().AvailableConnections, driver.Pool.Stat().CurrentConnections,
			driver.Pool.Stat().MaxConnections)
		conn1, err := driver.Pool.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		defer conn1.Close()
		log.Printf("first acquire ava:%d, cur:%d, max:%d",
			driver.Pool.Stat().AvailableConnections, driver.Pool.Stat().CurrentConnections,
			driver.Pool.Stat().MaxConnections)
		log.Printf("%p", conn1)

		conn2, err := driver.Pool.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		defer conn2.Close()
		log.Printf("second acquire ava:%d, cur:%d, max:%d",
			driver.Pool.Stat().AvailableConnections, driver.Pool.Stat().CurrentConnections,
			driver.Pool.Stat().MaxConnections)
		log.Printf("%p", conn2)

		conn3, err := driver.Pool.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		defer conn3.Close()
		log.Printf("third acquire ava:%d, cur:%d, max:%d",
			driver.Pool.Stat().AvailableConnections, driver.Pool.Stat().CurrentConnections,
			driver.Pool.Stat().MaxConnections)
		log.Printf("%p", conn3)
	}
}
