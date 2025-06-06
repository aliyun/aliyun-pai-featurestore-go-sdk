package dao

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/api"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/datasource/hologres"
	"github.com/aliyun/aliyun-pai-featurestore-go-sdk/v2/utils"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/huandu/go-sqlbuilder"
)

type FeatureViewHologresDao struct {
	UnimplementedFeatureViewDao
	db              *sql.DB
	table           string
	primaryKeyField string
	eventTimeField  string
	ttl             int
	mu              sync.RWMutex
	stmtMap         map[uint32]*sql.Stmt

	offlineTable string
	onlineTable  string
}

func NewFeatureViewHologresDao(config DaoConfig) *FeatureViewHologresDao {
	dao := FeatureViewHologresDao{
		table:           config.HologresTableName,
		primaryKeyField: config.PrimaryKeyField,
		eventTimeField:  config.EventTimeField,
		ttl:             config.TTL,
		stmtMap:         make(map[uint32]*sql.Stmt, 4),
		offlineTable:    config.HologresOfflineTableName,
		onlineTable:     config.HologresOnlineTableName,
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
	builder.Where(builder.In(fmt.Sprintf("\"%s\"", d.primaryKeyField), keys...))
	if d.ttl > 0 {
		t := time.Now().Add(time.Duration(-1 * d.ttl * int(time.Second)))
		builder.Where(builder.GreaterEqualThan(fmt.Sprintf("\"%s\"", d.eventTimeField), t))
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

type sequenceInfo struct {
	itemId    string
	event     string
	playTime  float64
	timestamp int64
}

func (d *FeatureViewHologresDao) GetUserSequenceFeature(keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
	var selectFields []string
	if sequenceConfig.PlayTimeField == "" {
		selectFields = []string{fmt.Sprintf("\"%s\"", sequenceConfig.ItemIdField), fmt.Sprintf("\"%s\"", sequenceConfig.EventField),
			fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField)}
	} else {
		selectFields = []string{fmt.Sprintf("\"%s\"", sequenceConfig.ItemIdField), fmt.Sprintf("\"%s\"", sequenceConfig.EventField),
			fmt.Sprintf("\"%s\"", sequenceConfig.PlayTimeField), fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField)}
	}
	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	onlineFunc := func(seqEvent string, sequence_events []interface{}, seqLen int, key interface{}) []*sequenceInfo {
		onlineSequences := []*sequenceInfo{}
		builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
		builder.Select(selectFields...)
		builder.From(d.onlineTable)
		where := []string{builder.Equal(fmt.Sprintf("\"%s\"", userIdField), key),
			builder.GreaterThan(fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField), currTime-86400*5)}
		if len(sequence_events) > 1 {
			where = append(where, builder.In(fmt.Sprintf("\"%s\"", sequenceConfig.EventField), sequence_events...))
		} else {
			where = append(where, builder.Equal(fmt.Sprintf("\"%s\"", sequenceConfig.EventField), seqEvent))
		}
		builder.Where(where...)
		builder.Limit(seqLen)
		builder.OrderBy(fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField)).Desc()

		sql, args := builder.Build()
		stmtKey := crc32.ChecksumIEEE([]byte(sql))
		stmt := d.getStmt(stmtKey)
		if stmt == nil {
			d.mu.Lock()
			stmt = d.stmtMap[stmtKey]
			if stmt == nil {
				stmt2, err := d.db.Prepare(sql)
				if err != nil {
					d.mu.Unlock()
					log.Println(err)
					return nil
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
			log.Println(err)
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			seq := new(sequenceInfo)
			var dst []interface{}
			if sequenceConfig.PlayTimeField == "" {
				dst = []interface{}{&seq.itemId, &seq.event, &seq.timestamp}
			} else {
				dst = []interface{}{&seq.itemId, &seq.event, &seq.playTime, &seq.timestamp}
			}
			if err := rows.Scan(dst...); err == nil {
				if seq.event == "" || seq.itemId == "" {
					continue
				}
				if t, exist := sequencePlayTimeMap[seq.event]; exist {
					if seq.playTime <= t {
						continue
					}
				}
				onlineSequences = append(onlineSequences, seq)
			} else {
				log.Println(err)
				return nil
			}
		}

		return onlineSequences
	}

	offlineFunc := func(seqEvent string, sequence_events []interface{}, seqLen int, key interface{}) []*sequenceInfo {
		offlineSequences := []*sequenceInfo{}
		builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
		builder.Select(selectFields...)
		builder.From(d.offlineTable)
		where := []string{builder.Equal(fmt.Sprintf("\"%s\"", userIdField), key)}
		if len(sequence_events) > 1 {
			where = append(where, builder.In(fmt.Sprintf("\"%s\"", sequenceConfig.EventField), sequence_events...))
		} else {
			where = append(where, builder.Equal(fmt.Sprintf("\"%s\"", sequenceConfig.EventField), seqEvent))
		}
		builder.Where(where...)
		builder.Limit(seqLen)
		builder.OrderBy(fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField)).Desc()

		sql, args := builder.Build()
		stmtKey := crc32.ChecksumIEEE([]byte(sql))
		stmt := d.getStmt(stmtKey)
		if stmt == nil {
			d.mu.Lock()
			stmt = d.stmtMap[stmtKey]
			if stmt == nil {
				stmt2, err := d.db.Prepare(sql)
				if err != nil {
					d.mu.Unlock()
					log.Println(err)
					return nil
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
			log.Println(err)
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			seq := new(sequenceInfo)
			var dst []interface{}
			if sequenceConfig.PlayTimeField == "" {
				dst = []interface{}{&seq.itemId, &seq.event, &seq.timestamp}
			} else {
				dst = []interface{}{&seq.itemId, &seq.event, &seq.playTime, &seq.timestamp}
			}
			if err := rows.Scan(dst...); err == nil {
				if seq.event == "" || seq.itemId == "" {
					continue
				}
				if t, exist := sequencePlayTimeMap[seq.event]; exist {
					if seq.playTime <= t {
						continue
					}
				}
				offlineSequences = append(offlineSequences, seq)
			} else {
				log.Println(err)
				return nil
			}
		}

		return offlineSequences

	}

	results := make([]map[string]interface{}, 0, len(keys))
	var outmu sync.Mutex

	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(key interface{}) {
			defer wg.Done()
			properties := make(map[string]interface{})
			var mu sync.Mutex

			var eventWg sync.WaitGroup
			for _, seqConfig := range onlineConfig {
				eventWg.Add(1)
				go func(seqConfig *api.SeqConfig) {
					defer eventWg.Done()
					var onlineSequences []*sequenceInfo
					var offlineSequences []*sequenceInfo

					origin_sequence_events := strings.Split(seqConfig.SeqEvent, "|")
					sequence_events := make([]interface{}, len(origin_sequence_events))
					for i, v := range origin_sequence_events {
						sequence_events[i] = v
					}
					var innerWg sync.WaitGroup
					//get data from online table
					innerWg.Add(1)
					go func(seqEvent string, sequence_events []interface{}, seqLen int, key interface{}) {
						defer innerWg.Done()
						if onlineresult := onlineFunc(seqEvent, sequence_events, seqLen, key); onlineresult != nil {
							onlineSequences = onlineresult
						}
					}(seqConfig.SeqEvent, sequence_events, seqConfig.SeqLen, key)
					//get data from offline table
					innerWg.Add(1)
					go func(seqEvent string, sequence_events []interface{}, seqLen int, key interface{}) {
						defer innerWg.Done()
						if offlineresult := offlineFunc(seqEvent, sequence_events, seqLen, key); offlineresult != nil {
							offlineSequences = offlineresult
						}
					}(seqConfig.SeqEvent, sequence_events, seqConfig.SeqLen, key)
					innerWg.Wait()

					subproperties := makeSequenceFeatures(offlineSequences, onlineSequences, seqConfig, sequenceConfig, currTime)
					mu.Lock()
					defer mu.Unlock()
					for k, value := range subproperties {
						properties[k] = value
					}
				}(seqConfig)
			}
			eventWg.Wait()
			properties[userIdField] = key
			outmu.Lock()
			results = append(results, properties)
			outmu.Unlock()
		}(key)
	}

	wg.Wait()

	return results, nil

}

func (d *FeatureViewHologresDao) GetUserBehaviorFeature(userIds []interface{}, events []interface{}, selectFields []string, sequenceConfig api.FeatureViewSeqConfig) ([]map[string]interface{}, error) {
	selector := make([]string, 0, len(selectFields))
	for _, field := range selectFields {
		selector = append(selector, fmt.Sprintf("\"%s\"", field))
	}
	currTime := time.Now().Unix()
	sequencePlayTimeMap := makePlayTimeMap(sequenceConfig.PlayTimeFilter)

	onlineFunc := func(userId interface{}) []map[string]interface{} {
		builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
		builder.Select(selector...)
		builder.From(d.onlineTable)
		where := []string{builder.Equal(fmt.Sprintf("\"%s\"", d.primaryKeyField), userId),
			builder.GreaterThan(fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField), currTime-86400*5)}
		if len(events) > 0 {
			where = append(where, builder.In(fmt.Sprintf("\"%s\"", sequenceConfig.EventField), events...))
		}
		builder.Where(where...)
		builder.OrderBy(fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField)).Desc()
		sql, args := builder.Build()
		stmtKey := crc32.ChecksumIEEE([]byte(sql))
		stmt := d.getStmt(stmtKey)
		if stmt == nil {
			d.mu.Lock()
			stmt = d.stmtMap[stmtKey]
			if stmt == nil {
				stmt2, err := d.db.Prepare(sql)
				if err != nil {
					d.mu.Unlock()
					log.Println(err)
					return nil
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
			log.Println(err)
			return nil
		}
		defer rows.Close()
		columns, _ := rows.ColumnTypes()
		values := ColumnValues(columns)
		result := make([]map[string]interface{}, 0, len(userIds)*len(events)*50)

		for rows.Next() {
			if err := rows.Scan(values...); err == nil {
				properties := make(map[string]interface{}, len(values))
				for i, column := range columns {
					name := column.Name()
					if value := ParseColumnValues(values[i]); value != nil {
						properties[name] = value
					}
				}
				if t, exist := sequencePlayTimeMap[utils.ToString(properties[sequenceConfig.EventField], "")]; exist {
					if utils.ToFloat(properties[sequenceConfig.PlayTimeField], 0.0) <= t {
						continue
					}
				}
				result = append(result, properties)
			}
		}
		return result
	}
	offlineFunc := func(userId interface{}) []map[string]interface{} {
		builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
		builder.Select(selector...)
		builder.From(d.offlineTable)
		where := []string{builder.Equal(fmt.Sprintf("\"%s\"", d.primaryKeyField), userId)}
		if len(events) > 0 {
			where = append(where, builder.In(fmt.Sprintf("\"%s\"", sequenceConfig.EventField), events...))
		}
		builder.Where(where...)
		builder.OrderBy(fmt.Sprintf("\"%s\"", sequenceConfig.TimestampField)).Desc()
		sql, args := builder.Build()
		stmtKey := crc32.ChecksumIEEE([]byte(sql))
		stmt := d.getStmt(stmtKey)
		if stmt == nil {
			d.mu.Lock()
			stmt = d.stmtMap[stmtKey]
			if stmt == nil {
				stmt2, err := d.db.Prepare(sql)
				if err != nil {
					d.mu.Unlock()
					log.Println(err)
					return nil
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
			log.Println(err)
			return nil
		}
		defer rows.Close()
		columns, _ := rows.ColumnTypes()
		values := ColumnValues(columns)
		result := make([]map[string]interface{}, 0, len(userIds)*len(events)*50)

		for rows.Next() {
			if err := rows.Scan(values...); err == nil {
				properties := make(map[string]interface{}, len(values))
				for i, column := range columns {
					name := column.Name()
					if value := ParseColumnValues(values[i]); value != nil {
						properties[name] = value
					}
				}
				if t, exist := sequencePlayTimeMap[utils.ToString(properties[sequenceConfig.EventField], "")]; exist {
					if utils.ToFloat(properties[sequenceConfig.PlayTimeField], 0.0) <= t {
						continue
					}
				}
				result = append(result, properties)
			}
		}
		return result
	}

	results := make([]map[string]interface{}, 0, len(userIds)*(len(events)+1)*50)
	var outmu sync.Mutex
	var wg sync.WaitGroup
	for _, userId := range userIds {
		wg.Add(1)
		go func(userId interface{}) {
			defer wg.Done()
			var innerWg sync.WaitGroup
			var offlineResult []map[string]interface{}
			var onlineResult []map[string]interface{}
			// offline table
			innerWg.Add(1)
			go func(userId interface{}) {
				defer innerWg.Done()
				offlineResult = offlineFunc(userId)
			}(userId)
			// online table
			innerWg.Add(1)
			go func(userId interface{}) {
				defer innerWg.Done()
				onlineResult = onlineFunc(userId)
			}(userId)
			innerWg.Wait()
			if offlineResult == nil || onlineResult == nil {
				fmt.Println("get user behavior feature failed")
				return
			}
			combinedResult := combineBehaviorFeatures(offlineResult, onlineResult, sequenceConfig.TimestampField)
			outmu.Lock()
			results = append(results, combinedResult...)
			outmu.Unlock()
		}(userId)
	}
	wg.Wait()

	return results, nil
}

type Visitor struct {
	LastNode *ast.BinaryNode
}

func (v *Visitor) Visit(node *ast.Node) {
	switch n := (*node).(type) {
	case *ast.BinaryNode:
		v.LastNode = n
	}
}
func (v *Visitor) ConvertToSql(node *ast.BinaryNode) string {
	if node == nil {
		return ""
	}
	if node.Operator != "&&" && node.Operator != "||" {
		op := node.Operator
		if op == "==" {
			op = "="
		}
		if leftNode, ok := node.Left.(*ast.IdentifierNode); ok {
			return fmt.Sprintf("%s %s '%s'", leftNode, op, strings.ReplaceAll(node.Right.String(), "\"", ""))
		} else {
			return fmt.Sprintf("'%s' %s %s", strings.ReplaceAll(node.Left.String(), "\"", ""), op, node.Right.String())
		}

	} else if node.Operator == "&&" {
		left := v.ConvertToSql(node.Left.(*ast.BinaryNode))
		right := v.ConvertToSql(node.Right.(*ast.BinaryNode))
		return fmt.Sprintf("(%s) and (%s)", left, right)
	} else if node.Operator == "||" {
		left := v.ConvertToSql(node.Left.(*ast.BinaryNode))
		right := v.ConvertToSql(node.Right.(*ast.BinaryNode))
		return fmt.Sprintf("(%s) or (%s)", left, right)
	}
	return ""
}

func (d *FeatureViewHologresDao) RowCount(filterExpr string) int {
	builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
	builder.Select("count(*)")
	builder.From(d.table)
	if filterExpr != "" {
		program, err := expr.Compile(filterExpr)
		if err != nil {
			fmt.Println(err)
			return 0
		}
		node := program.Node()
		visitor := &Visitor{}
		ast.Walk(&node, visitor)

		sqlWhere := visitor.ConvertToSql(visitor.LastNode)
		builder.Where(sqlWhere)
	}

	sql, args := builder.Build()
	fmt.Println("row count sql:", sql)
	var count int
	retry := 3
	for i := 0; i < retry; i++ {
		row := d.db.QueryRow(sql, args...)
		err := row.Scan(&count)
		if i == retry-1 {
			fmt.Println(err)
			return 0
		}
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return count
	}
	return count
}

func (d *FeatureViewHologresDao) RowCountIds(filterExpr string) ([]string, int, error) {
	builder := sqlbuilder.PostgreSQL.NewSelectBuilder()
	builder.Select(d.primaryKeyField)
	builder.From(d.table)
	if filterExpr != "" {
		program, err := expr.Compile(filterExpr)
		if err != nil {
			return nil, 0, err
		}
		node := program.Node()
		visitor := &Visitor{}
		ast.Walk(&node, visitor)

		sqlWhere := visitor.ConvertToSql(visitor.LastNode)
		builder.Where(sqlWhere)
	}

	sql, args := builder.Build()
	fmt.Println("sql:", sql)
	rows, err := d.db.Query(sql, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	ids := make([]string, 0, 1024)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		} else {
			ids = append(ids, id)
		}
	}
	return ids, len(ids), nil
}
