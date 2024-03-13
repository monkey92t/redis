package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type GraphCmdable interface {
	GraphQuery(ctx context.Context, key, query string) *GraphCmd
}

// GraphQuery executes a query on a graph, exec: GRAPH.QUERY key query
func (c cmdable) GraphQuery(ctx context.Context, key, query string) *GraphCmd {
	cmd := NewGraphCmd(ctx, "GRAPH.QUERY", key, query)
	_ = c(ctx, cmd)
	return cmd
}

// ----------------------------------------------------------------------------

type (
	graphDataType int
	graphRowType  int
)

const (
	graphInteger graphDataType = iota + 1 // int (graph int)
	graphNil                              // nil (graph nil/null)
	graphString                           // string (graph string/boolean/double)
)

const (
	graphResultBasic graphRowType = iota + 1 // int/nil/string
	graphResultNode                          // node(3), id +labels +properties
	graphResultEdge                          // edge(5), id + type + src_node + dest_node + properties
)

type GraphResult struct {
	noResult bool         // 是否是非结果集类响应，例如 CREATE 语句
	text     []string     // 响应中的信息描述
	field    []string     // 结果集类响应的字段，理论上字段数量等于rows每行字段数
	rows     [][]graphRow // 结果集列表
	err      error
}

// Message 获取执行描述信息
func (g *GraphResult) Message() []string {
	return g.text
}

// IsResult 是否是结果集的响应，如果是非结果集响应返回 false，例如 CREATE 语句
// 如果是有结果集响应的查询，例如 MATCH ... RETURN x, 无论响应行数是否为0，都返回 true
func (g *GraphResult) IsResult() bool {
	return !g.noResult
}

// Error 读取结果集中遇到的错误
func (g *GraphResult) Error() error {
	return g.err
}

func (g *GraphResult) Field() []string {
	return g.field
}

func (g *GraphResult) Len() int {
	return len(g.rows)
}

// Row 读取一行数据
func (g *GraphResult) Row() map[string]any {
	if g.noResult || len(g.field) == 0 || len(g.rows) == 0 || len(g.rows[0]) == 0 {
		return nil
	}
	row := make(map[string]any, len(g.field))
	for i := 0; i < len(g.field); i++ {
		switch g.rows[0][i].typ {
		case graphResultBasic:
			row[g.field[i]] = g.rows[0][i].basic.String()
		case graphResultNode:
			row[g.field[i]] = g.rows[0][i].node.Map()
		case graphResultEdge:
			row[g.field[i]] = g.rows[0][i].edge.Map()
		}
	}
	return row
}

// RowScan 将结果集的一行扫描到 dest 中，dest 必须是结构体指针
// 如果没有结果集，或非结果集响应，返回 Nil
func (g *GraphResult) RowScan(dest any) error {
	if g.noResult || len(g.field) == 0 || len(g.rows) == 0 || len(g.rows[0]) == 0 {
		return Nil
	}
	v, err := graphStruct(dest)
	if err != nil {
		return err
	}
	return g.scanStruct(v, g.rows[0])
}

// Rows 读取所有数据
func (g *GraphResult) Rows() []map[string]any {
	if g.noResult || len(g.field) == 0 || len(g.rows) == 0 || len(g.rows[0]) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(g.rows))
	for i := 0; i < len(g.rows); i++ {
		row := make(map[string]any, len(g.field))
		for f := 0; f < len(g.field); f++ {
			switch g.rows[i][f].typ {
			case graphResultBasic:
				row[g.field[f]] = g.rows[i][f].basic.String()
			case graphResultNode:
				row[g.field[f]] = g.rows[i][f].node.Map()
			case graphResultEdge:
				row[g.field[f]] = g.rows[i][f].edge.Map()
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// RowsScan 将结果集的所有行扫描到 dest 中，dest 必须是结构体指针
// 如果没有结果集，或非结果集响应，返回 Nil
func (g *GraphResult) RowsScan(dest any) error {
	if g.noResult || len(g.field) == 0 || len(g.rows) == 0 || len(g.rows[0]) == 0 {
		return Nil
	}
	v, err := graphStructSlice(dest)
	if err != nil {
		return err
	}
	return g.scanStructSlice(v)
}

type graphRow struct {
	typ   graphRowType
	basic *GraphData
	node  *GraphNode
	edge  *GraphEdge
}

type GraphData struct {
	typ        graphDataType
	integerVal int64
	stringVal  string
}

func (d GraphData) IsNil() bool {
	return d.typ == graphNil
}

func (d GraphData) String() string {
	switch d.typ {
	case graphInteger:
		return strconv.FormatInt(d.integerVal, 10)
	case graphNil:
		return ""
	case graphString:
		return d.stringVal
	default:
		return ""
	}
}

func (d GraphData) Int() int {
	if d.typ == graphInteger {
		return int(d.integerVal)
	}
	return 0
}

func (d GraphData) Bool() bool {
	switch d.typ {
	case graphInteger:
		return d.integerVal != 0
	case graphNil:
		return false
	case graphString:
		return d.stringVal == "true"
	default:
		return false
	}
}

func (d GraphData) Float64() float64 {
	if d.typ == graphInteger {
		return float64(d.integerVal)
	}
	if d.typ == graphString {
		v, _ := strconv.ParseFloat(d.stringVal, 64)
		return v
	}
	return 0
}

type GraphNode struct {
	ID         int64
	Labels     []string
	Properties map[string]GraphData
}

func (n *GraphNode) Map() map[string]any {
	return map[string]any{
		"id":         n.ID,
		"labels":     n.Labels,
		"properties": n.Properties,
	}
}

type GraphEdge struct {
	ID         int64
	Typ        string
	SrcNode    int64
	DstNode    int64
	Properties map[string]GraphData
}

func (e *GraphEdge) Map() map[string]any {
	return map[string]any{
		"id":         e.ID,
		"type":       e.Typ,
		"srcNode":    e.SrcNode,
		"dstNode":    e.DstNode,
		"properties": e.Properties,
	}
}

// ----------------------------------------------------------------------------

// scan

var (
	graphNodeType = reflect.TypeOf(GraphNode{})
	graphEdgeType = reflect.TypeOf(GraphEdge{})
)

func (g *GraphResult) scanStructSlice(v graphStructValue) error {
	if !v.isSlice {
		return fmt.Errorf("redis.graph.Scan(non-slice %T)", v.value.Type())
	}

	gv := graphStructValue{
		spec: v.spec,
	}
	elem := v.value.Type().Elem()
	for i := 0; i < len(g.rows); i++ {
		var item reflect.Value
		if elem.Kind() == reflect.Ptr {
			item = reflect.New(elem.Elem())
		} else {
			item = reflect.New(elem)
		}
		gv.value = item.Elem()
		if err := g.scanStruct(gv, g.rows[i]); err != nil {
			return err
		}

		if elem.Kind() == reflect.Ptr {
			v.value.Set(reflect.Append(v.value, item))
		} else {
			v.value.Set(reflect.Append(v.value, item.Elem()))
		}
	}
	return nil
}

func (g *GraphResult) scanStruct(v graphStructValue, row []graphRow) error {
	for i := 0; i < len(g.field); i++ {
		key := g.field[i]
		idx, ok := v.spec.m[key]
		if !ok {
			return nil
		}
		field := v.value.Field(idx)
		isPtr := field.Kind() == reflect.Ptr

		if isPtr && field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
		if !isPtr && field.Type().Name() != "" && field.CanAddr() {
			field = field.Addr()
			isPtr = true
		}
		if isPtr {
			field = field.Elem()
		}

		data := row[i]
		if field.Type().Kind() != reflect.Struct && data.typ != graphResultBasic {
			return fmt.Errorf("cannot scan redis.graph.result %v into struct field %s.%s of type %s",
				data, v.value.Type().Name(), key, field.Type())
		}

		switch field.Type().Kind() {
		case reflect.Bool:
			field.SetBool(data.basic.Bool())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetInt(int64(data.basic.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetUint(uint64(data.basic.Int()))
		case reflect.Float32, reflect.Float64:
			field.SetFloat(data.basic.Float64())
		case reflect.String:
			field.SetString(data.basic.String())
		case reflect.Struct:
			// node
			if field.Type() == graphNodeType {
				if data.typ != graphResultNode {
					return errors.New("cannot scan redis.graph.result into struct field, result not graph.node type")
				}
				node := field.Addr().Interface().(*GraphNode)
				node.ID = data.node.ID
				node.Labels = data.node.Labels
				node.Properties = data.node.Properties
			} else if field.Type() == graphEdgeType {
				if data.typ != graphResultEdge {
					return errors.New("cannot scan redis.graph.result into struct field, result not graph.edge type")
				}
				edge := field.Addr().Interface().(*GraphEdge)
				edge.ID = data.edge.ID
				edge.Typ = data.edge.Typ
				edge.SrcNode = data.edge.SrcNode
				edge.DstNode = data.edge.DstNode
				edge.Properties = data.edge.Properties
			} else {
				// miss Anonymous
				return fmt.Errorf("redis.graph.scan unsupported %s, not node,edge type", field.Type())
			}
		default:
			// reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Slice, reflect.Chan,
			// reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.UnsafePointer:
			return fmt.Errorf("redis.Scan(unsupported %s)", field.Type())
		}
	}
	return nil
}

type graphStructValue struct {
	spec    *graphStructSpec
	value   reflect.Value
	isSlice bool
}

type graphStructSpec struct {
	m map[string]int
}

var graphStructMap sync.Map

func graphStructCache(t reflect.Type) *graphStructSpec {
	if v, ok := graphStructMap.Load(t); ok {
		return v.(*graphStructSpec)
	}

	spec := newStructSpec(t, "redis")
	graphStructMap.Store(t, spec)
	return spec
}

func graphStructSlice(dest any) (graphStructValue, error) {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return graphStructValue{}, fmt.Errorf("redis.graph.Scan(non-ptr1 %T)", dest)
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		return graphStructValue{}, fmt.Errorf("redis.graph.Scan(non-slice %T)", dest)
	}
	elem := v.Type().Elem()
	if elem.Kind() == reflect.Ptr {
		elem = v.Type().Elem().Elem()
	}
	if elem.Kind() != reflect.Struct {
		return graphStructValue{}, fmt.Errorf("redis.graph.Scan(slice elem non-struct %T)", dest)
	}

	return graphStructValue{
		spec:    graphStructCache(elem),
		value:   v,
		isSlice: true,
	}, nil
}

func graphStruct(dest any) (graphStructValue, error) {
	v := reflect.ValueOf(dest)

	// The destination to scan into should be a struct pointer.
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return graphStructValue{}, fmt.Errorf("redis.graph.Scan(non-pointer %T)", dest)
	}

	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return graphStructValue{}, fmt.Errorf("redis.graph.Scan(non-struct %T)", dest)
	}

	return graphStructValue{
		spec:  graphStructCache(v.Type()),
		value: v,
	}, nil
}

func newStructSpec(t reflect.Type, fieldTag string) *graphStructSpec {
	numField := t.NumField()
	out := &graphStructSpec{
		m: make(map[string]int, numField),
	}

	for i := 0; i < numField; i++ {
		f := t.Field(i)
		if f.Anonymous {
			at := f.Type
			if at.Kind() == reflect.Pointer {
				at = at.Elem()
			}
			if !f.IsExported() && t.Kind() != reflect.Struct {
				continue
			}
		} else if !f.IsExported() {
			continue
		}

		tag := f.Tag.Get(fieldTag)
		if tag == "" || tag == "-" {
			continue
		}

		tag = strings.Split(tag, ",")[0]
		if tag == "" {
			continue
		}

		// Use the built-in decoder.
		kind := f.Type.Kind()
		if kind == reflect.Pointer {
			kind = f.Type.Elem().Kind()
		}
		out.m[tag] = i
	}

	return out
}

//
//type Graph struct {
//	Nodes             map[string]*GraphNode
//	Edges             []*GraphEdge
//	labels            []string   // List of node labels.
//	relationshipTypes []string   // List of relation types.
//	properties        []string   // List of properties.
//	mutex             sync.Mutex // Lock, used for updating internal state.
//}
//
//// AddNode adds a node to the graph.
//func (g *Graph) AddNode(n *GraphNode) {
//	g.mutex.Lock()
//	g.Nodes[n.Alias] = n
//	g.mutex.Unlock()
//}
//
//// AddEdge adds an edge to the graph.
//func (g *Graph) AddEdge(e *GraphEdge) error {
//	// Verify that the edge has source and destination
//	if e.Source == nil || e.Destination == nil {
//		return fmt.Errorf("redis: both source and destination nodes should be defined")
//	}
//
//	// Verify that the edge's nodes have been previously added to the graph
//	if _, ok := g.Nodes[e.Source.Alias]; !ok {
//		return fmt.Errorf("redis: source node neeeds to be added to the graph first")
//	}
//	if _, ok := g.Nodes[e.Destination.Alias]; !ok {
//		return fmt.Errorf("redis: destination node neeeds to be added to the graph first")
//	}
//
//	g.Edges = append(g.Edges, e)
//	return nil
//}
//
//type GraphNode struct {
//	ID         uint64
//	Labels     []string
//	Alias      string
//	Properties map[string]any
//}
//
//// Encode makes Node satisfy the Stringer interface
//func (n *GraphNode) Encode() string {
//	buff := new(bytes.Buffer)
//	buff.WriteByte('(')
//
//	if n.Alias != "" {
//		buff.WriteString(n.Alias)
//	}
//
//	for _, label := range n.Labels {
//		buff.WriteByte(':')
//		buff.WriteString(label)
//	}
//
//	writeGraphProperties(buff, n.Properties)
//	buff.WriteByte(')')
//	return buff.String()
//}
//
//type GraphEdge struct {
//	ID          uint64
//	Relation    string
//	Source      *GraphNode
//	Destination *GraphNode
//	Properties  map[string]any
//}
//
//// Encode makes Edge satisfy the Stringer interface
//func (e *GraphEdge) Encode() string {
//	buff := new(bytes.Buffer)
//	buff.WriteByte('(')
//	buff.WriteString(e.Source.Alias)
//	buff.WriteByte(')')
//
//	buff.WriteString("-[")
//	if e.Relation != "" {
//		buff.WriteString(e.Relation)
//	}
//	writeGraphProperties(buff, e.Properties)
//	buff.WriteString("]->")
//
//	buff.WriteByte('(')
//	buff.WriteString(e.Destination.Alias)
//	buff.WriteByte(')')
//
//	return buff.String()
//}
//
//func writeGraphProperties(w *bytes.Buffer, p map[string]any) {
//	if len(p) == 0 {
//		return
//	}
//
//	w.WriteByte('{')
//	for k, v := range p {
//		w.WriteString(k)
//		w.WriteByte(':')
//		w.WriteString(graphPropertiesValue(v))
//	}
//	w.WriteByte('}')
//}
//
//func graphPropertiesValue(src any) string {
//	if src == nil {
//		return "null"
//	}
//
//	switch v := src.(type) {
//	case string:
//		return strconv.Quote(v)
//	case int:
//		return strconv.Itoa(v)
//	case int64:
//		return strconv.FormatInt(v, 10)
//	case float64:
//		return strconv.FormatFloat(v, 'f', -1, 64)
//	case bool:
//		return strconv.FormatBool(v)
//	case time.Time:
//		return v.Format(time.RFC3339Nano)
//	case time.Duration:
//		return strconv.FormatInt(v.Nanoseconds(), 10)
//	default:
//		internal.Logger.Printf(context.TODO(), "unrecognized type to convert to string")
//		return ""
//	}
//}
