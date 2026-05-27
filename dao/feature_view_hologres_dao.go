package dao

import (
	"context"
	"database/sql"
	"fmt"
	"hash/crc32"
	"log"
	"sort"
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
func (d *FeatureViewHologresDao) GetFeaturesWithContext(ctx context.Context, keys []interface{}, selectFields []string, weight int) ([]map[string]interface{}, error) {

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

	rows, err := stmt.QueryContext(ctx, args...)
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

func (d *FeatureViewHologresDao) GetUserSequenceFeatureWithContext(ctx context.Context, keys []interface{}, userIdField string, sequenceConfig api.FeatureViewSeqConfig, onlineConfig []*api.SeqConfig) ([]map[string]interface{}, error) {
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
		rows, err := stmt.QueryContext(ctx, args...)
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

		rows, err := stmt.QueryContext(ctx, args...)
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

func (d *FeatureViewHologresDao) GetUserBehaviorFeatureWithContext(ctx context.Context, userIds []interface{}, events []interface{}, selectFields []string, sequenceConfig api.FeatureViewSeqConfig) ([]map[string]interface{}, error) {
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
		rows, err := stmt.QueryContext(ctx, args...)
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
		rows, err := stmt.QueryContext(ctx, args...)
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
	LastNode ast.Node
}

func (v *Visitor) Visit(node *ast.Node) {
	v.LastNode = *node
}
func (v *Visitor) ConvertToSql(node ast.Node) string {
	if node == nil {
		return ""
	}
	if unaryNode, ok := node.(*ast.UnaryNode); ok {
		if unaryNode.Operator == "not" {
			inner, ok := unaryNode.Node.(*ast.BinaryNode)
			if ok && inner.Operator == "in" {
				return v.convertInToSql(inner, true)
			}
			innerSql := v.ConvertToSql(unaryNode.Node)
			if innerSql == "" {
				return ""
			}
			return fmt.Sprintf("not (%s)", innerSql)
		}
		return ""
	}
	binaryNode, ok := node.(*ast.BinaryNode)
	if !ok {
		return ""
	}
	if binaryNode.Operator == "in" {
		return v.convertInToSql(binaryNode, false)
	}
	if binaryNode.Operator != "&&" && binaryNode.Operator != "||" {
		op := binaryNode.Operator
		if op == "==" {
			op = "="
		}
		if leftNode, ok := binaryNode.Left.(*ast.IdentifierNode); ok {
			return fmt.Sprintf("%s %s %s", leftNode, op, sqlLiteral(binaryNode.Right))
		} else {
			return fmt.Sprintf("%s %s %s", sqlLiteral(binaryNode.Left), op, binaryNode.Right)
		}

	} else if binaryNode.Operator == "&&" {
		left := v.ConvertToSql(binaryNode.Left)
		right := v.ConvertToSql(binaryNode.Right)
		return fmt.Sprintf("(%s) and (%s)", left, right)
	} else if binaryNode.Operator == "||" {
		left := v.ConvertToSql(binaryNode.Left)
		right := v.ConvertToSql(binaryNode.Right)
		return fmt.Sprintf("(%s) or (%s)", left, right)
	}
	return ""
}

// quoteSQLString 将字符串值转义单引号并包裹单引号，生成合法 SQL 字面量
func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// sqlLiteral 根据 AST 节点类型返回对应的 SQL 字面量（数值不加引号，字符串加引号并转义）
func sqlLiteral(node ast.Node) string {
	switch n := node.(type) {
	case *ast.IntegerNode:
		return fmt.Sprintf("%d", n.Value)
	case *ast.FloatNode:
		return fmt.Sprintf("%g", n.Value)
	case *ast.BoolNode:
		return fmt.Sprintf("%t", n.Value)
	case *ast.StringNode:
		return quoteSQLString(n.Value)
	case *ast.ConstantNode:
		switch v := n.Value.(type) {
		case int:
			return fmt.Sprintf("%d", v)
		case int64:
			return fmt.Sprintf("%d", v)
		case float64:
			return fmt.Sprintf("%g", v)
		case bool:
			return fmt.Sprintf("%t", v)
		case string:
			return quoteSQLString(v)
		default:
			return quoteSQLString(fmt.Sprintf("%v", v))
		}
	default:
		s := strings.ReplaceAll(node.String(), "\"", "")
		return quoteSQLString(s)
	}
}

func (v *Visitor) convertInToSql(node *ast.BinaryNode, negate bool) string {
	leftNode, ok := node.Left.(*ast.IdentifierNode)
	if !ok {
		return ""
	}
	var values []string
	switch right := node.Right.(type) {
	case *ast.ArrayNode:
		for _, elem := range right.Nodes {
			values = append(values, sqlLiteral(elem))
		}
	case *ast.ConstantNode:
		switch m := right.Value.(type) {
		case map[string]struct{}:
			for k := range m {
				values = append(values, quoteSQLString(k))
			}
			sort.Strings(values)
		case map[int]struct{}:
			keys := make([]int, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, k := range keys {
				values = append(values, fmt.Sprintf("%d", k))
			}
		case []any:
			for _, val := range m {
				switch v := val.(type) {
				case string:
					values = append(values, quoteSQLString(v))
				case int:
					values = append(values, fmt.Sprintf("%d", v))
				case float64:
					values = append(values, fmt.Sprintf("%g", v))
				case bool:
					values = append(values, fmt.Sprintf("%t", v))
				default:
					values = append(values, quoteSQLString(fmt.Sprintf("%v", v)))
				}
			}
		default:
			return ""
		}
	default:
		return ""
	}
	op := "in"
	if negate {
		op = "not in"
	}
	return fmt.Sprintf("%s %s (%s)", leftNode, op, strings.Join(values, ", "))
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
