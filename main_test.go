package main

import (
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
)

func testCreateDB(t *testing.T, numConn int) (*DB, func()) {
	cfg := &DBConfig{
		Host:     "localhost",
		User:     "pgtest",
		Pass:     "",
		Database: "pgtest",
		Port:     5432,
		NumConn:  numConn,
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
		if _, err := db.Exec(`TRUNCATE TABLE test`); err != nil {
			t.Fatal(err)
		}
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
	db, cleanup := testCreateDB(t, 3)
	defer cleanup()

	tm, err := selectTime(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tm)
}

func TestInsertValuePlainSQL(t *testing.T) {
	db, cleanup := testCreateDB(t, 3)
	defer cleanup()

	tm := nullTime{
		Valid: false,
	}
	if err := insertValuePlainSQL(db, tm); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM test`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d", n)

	var id int
	var tmt nullTime
	if err := db.QueryRow(`SELECT id, t FROM test`).Scan(&id, &tmt); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d %v", id, tmt)
}

func TestInsertValuePgxSQL(t *testing.T) {
	db, cleanup := testCreateDB(t, 3)
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
	db, cleanup := testCreateDB(t, 4)
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

func TestConnectionPoolWithStdlib(t *testing.T) {
	db, cleanup := testCreateDB(t, 3)
	defer cleanup()

	var n int
	for i := 0; i <= 5; i++ {
		if err := db.QueryRow(checkPidSQL).Scan(&n); err != nil {
			t.Fatal(err)
		}
		t.Logf("pid: %d", n)
	}
}

func TestConnectionPoolWithPgx(t *testing.T) {
	db, cleanup := testCreateDB(t, 10)
	defer cleanup()

	driver := db.Driver().(*stdlib.Driver)
	var n int
	for i := 0; i <= 5; i++ {
		if err := driver.Pool.QueryRow(checkPidSQL).Scan(&n); err != nil {
			t.Fatal(err)
		}
		t.Logf("pid: %d", n)
	}
}

func TestConnectionPoolWithPgxAcquireCon(t *testing.T) {
	db, cleanup := testCreateDB(t, 7)
	defer cleanup()

	driver := db.Driver().(*stdlib.Driver)
	var n int
	for i := 0; i <= 5; i++ {
		con, err := driver.Pool.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		if err := con.QueryRow(checkPidSQL).Scan(&n); err != nil {
			t.Fatal(err)
		}
		t.Logf("pid: %d", n)
		// driver.Pool.Release(con)
	}
}

func TestPreparedStatementWithStdbli(t *testing.T) {
	db, cleanup := testCreateDB(t, 3)
	defer cleanup()

	var tm time.Time
	if err := db.QueryRow("select_row").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	t.Log(tm)
}

func TestPreparedStatementWithPool(t *testing.T) {
	db, cleanup := testCreateDB(t, 3)
	defer cleanup()

	driver := db.Driver().(*stdlib.Driver)
	var tm time.Time
	if err := driver.Pool.QueryRow("select_row").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	t.Log(tm)
}

func TestPreparedStatementWithPgxConn(t *testing.T) {
	db, cleanup := testCreateDB(t, 3)
	defer cleanup()

	driver := db.Driver().(*stdlib.Driver)
	conn, err := driver.Pool.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	var tm time.Time
	if err := conn.QueryRow("select_row").Scan(&tm); err != nil {
		t.Fatal(err)
	}
	t.Log(tm)
}
