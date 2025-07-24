package hologres

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
)

func init() {
	sql.Register("hologres", &HologresDriver{})
}

type HologresDriver struct {
	driver pq.Driver
}

func (d HologresDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.driver.Open(name)
	if err != nil {
		return nil, err
	}

	if stmt, err := conn.Prepare("set statement_timeout = 500"); err == nil {
		stmt.Exec(nil)
		stmt.Close()
	}
	return conn, err
}

type Hologres struct {
	DSN          string
	DB           *sql.DB
	Name         string
	RegisterTime time.Time
}

var hologresInstances sync.Map

func GetHologres(name string) (*Hologres, error) {
	value, ok := hologresInstances.Load(name)
	if !ok {
		return nil, fmt.Errorf("Hologres not found, name:%s", name)
	}

	hologresInstance, ok := value.(*Hologres)
	if !ok {
		return nil, fmt.Errorf("Hologres not found, name:%s", name)
	}

	return hologresInstance, nil
}
func (m *Hologres) Init() error {
	db, err := sql.Open("hologres", m.DSN)
	if err != nil {
		return err
	}

	db.SetConnMaxLifetime(60 * time.Minute)
	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(100)

	m.DB = db
	err = m.DB.Ping()
	//go m.loopDBStats()
	return err
}

func RegisterHologres(name, dsn string, useCustomAuth bool) {
	value, ok := hologresInstances.Load(name)
	if ok {
		if useCustomAuth {
			return
		}
		hologresInstance, ok2 := value.(*Hologres)
		if ok2 && time.Since(hologresInstance.RegisterTime) < 12*time.Hour {
			return
		}
	}
	m := &Hologres{
		DSN:          dsn,
		Name:         name,
		RegisterTime: time.Now(),
	}
	err := m.Init()
	if err != nil {
		fmt.Printf("event=RegisterHologres\tdsn=%s\tname=%s", dsn, name)
		panic(err)
	}
	hologresInstances.Store(name, m)

}

func RemoveHologres(name string) {
	value, ok := hologresInstances.Load(name)
	if !ok {
		return
	}
	hologres, ok := value.(*Hologres)
	if !ok {
		return
	}

	if hologres.DB != nil {
		hologres.DB.Close()
	}

	hologresInstances.Delete(name)
}
