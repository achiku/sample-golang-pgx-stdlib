package main

import "testing"

func testCreateDB(t *testing.T) *DB {
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
	return db
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
	db := testCreateDB(t)
	tm, err := selectTime(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tm)
}
