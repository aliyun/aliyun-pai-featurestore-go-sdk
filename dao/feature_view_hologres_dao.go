package dao

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/hologres"
)

type FeatureViewHologresDao struct {
	db              *sql.DB
	table           string
	primaryKeyField string
	eventTimeField  string
	ttl             int
	mu              sync.RWMutex
	stmtMap         map[uint32]*sql.Stmt
}

func NewFeatureViewHologresDao(config DaoConfig) *FeatureViewHologresDao {
	dao := FeatureViewHologresDao{
		table:           config.HologresTableName,
		primaryKeyField: config.PrimaryKeyField,
		eventTimeField:  config.EventTimeField,
		ttl:             config.TTL,
		stmtMap:         make(map[uint32]*sql.Stmt, 4),
	}
	hologres, err := hologres.GetHologres(config.HologresName)
	if err != nil {
		return nil
	}

	dao.db = hologres.DB
	return &dao
}
func (d *FeatureViewHologresDao) getStmt(key uint32) *sql.Stmt {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stmtMap[key]
}
func (d *FeatureViewHologresDao) GetFeatures(keys []interface{}, selectFields []string) ([]map[string]interface{}, error) {

	selector := make([]string, 0, len(selectFields))
	for _, field := range selectFields {
		selector = append(selector, fmt.Sprintf("\"%s\"", field))
	}
	builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
	builder.Select(selector...)
	builder.From(d.table)
	builder.Where(builder.In(d.primaryKeyField, keys...))
	if d.ttl > 0 {
		t := time.Now().Add(time.Duration(-1 * d.ttl * int(time.Second)))
		builder.Where(builder.GreaterEqualThan(d.eventTimeField, t))
	}

	sql, args := builder.Build()

	stmtKey := crc32.ChecksumIEEE([]byte(sql))
	//stmtKey := Md5(sql)
	stmt := d.getStmt(stmtKey)
	if stmt == nil {
		d.mu.Lock()
		stmt = d.stmtMap[stmtKey]
		if stmt == nil {
			stmt2, err := d.db.Prepare(sql)
			if err != nil {
				d.mu.Unlock()
				return nil, err
			}
			d.stmtMap[stmtKey] = stmt2
			stmt = stmt2
			d.mu.Unlock()
		} else {
			d.mu.Unlock()
		}
	}

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]map[string]interface{}, 0, len(keys))

	columns, _ := rows.ColumnTypes()
	values := ColumnValues(columns)

	for rows.Next() {
		if err := rows.Scan(values...); err == nil {
			properties := make(map[string]interface{}, len(values))
			for i, column := range columns {
				name := column.Name()

				if value := ParseColumnValues(values[i]); value != nil {
					properties[name] = value
				}
			}

			result = append(result, properties)
		}
	}

	return result, nil
}
