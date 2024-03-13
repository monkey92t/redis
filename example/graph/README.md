# Redis Graph 使用示例

### 支持的数据类型

##### 普通数据类型

* **Int**: 数字类型，可表示为 golang 中的 int、int8、int16、int32、int64、uint、uint8、uint16、uint32、uint64
* **Float**: 浮点数类型，可表示为 golang 中的 float32、float64
* **String**: 字符串类型，可表示为 golang 中的 string
* **Bool**: 布尔类型，可表示为 golang 中的 bool
* **Nil**: 表示为 golang 中的 nil

**备注**：float、bool 使用 string 类型表示，例如：`"3.1415926"`、`"true"`、`"false"`，在 go-redis 中，认为字符串 `"false"`
  转换为 false，`"true"` 转换为 true， float 存在精度问题(15位)。

##### 特殊数据

**点**： 点包含3个结构，分别是 id、labels、properties，在 go-redis 中，对应结构体 `redis.GraphNode`
**边**： 边包含5个结构，分别是 id、type、src_id、dst_id、properties，在 go-redis 中，对应结构体 `redis.GraphEdge`


### 运行示例

```go
// 初始化客户端对象，和初始化普通的redis客户端一样
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
})

// 执行Query，创建一个节点
query := `CREATE (:pod {id:"x1", name:"pod1", ts: 10240, alive: true, fv: 3.1415926})`
if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
	log.Fatal(err)
}

query = `CREATE (:pod {id:"x2", name:"pod2", ts: 10241, alive: true, fv: 3.1415927})`
if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
    t.Fatal(err)
}

// 创建边
query = `MATCH (p1:pod {id:"x1"}),(p2:pod {id:"x2"}) CREATE (p1)-[:link {src:"pod", dst:"pod", size:1}]->(p2)`
if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
    log.Fatal(err)
}

// 查询，查询所有pod的id属性
query = `MATCH (p:pod) RETURN p.id`
res, err := rdb.GraphQuery(ctx, key, query).Result()
if err != nil {
	log.Fatal(err)
}

// 返回查询过程中遇到的错误，和 rdb.GraphQuery().Err() 是一致的
// return: error
err := res.Error()

// 是否是结果集，比如 CREATE 语句是没有结果集的, MATCH ... RETURN ... 就存在结果集
// return: bool
ok := res.IsResult()

// 获取返回值中的执行信息，比如执行时间等，CREATE 和 MATCH 都存在执行信息
// return: []string
msg := res.Message()

// 获取结果集的行数
// return: int
n := res.Len()

// 获取结果集的字段
// return: []string
field := res.Field()

// 获取结果集的第一行数据，如果是非结果集响应或者结果集为空，返回 nil
// return: map[string]any
row := res.Row()

// 获取所有结果集数据，如果是非结果集响应或者结果集为空，返回 nil
// return: []map[string]any
rows := res.Rows()

// 把第一行数据结果集扫描到 dest 中，dest 必须是结构体指针(*struct)，
// 如果是非结果集响应或者结果集为空，返回 redis.Nil
// return: error
err := res.RowScan(dest any)

// 把所有结果集数据扫描到 dest 中，dest 必须是结构体或结构体指针的切片指针(*[]struct、*[]*struct)
// 如果是非结果集响应或者结果集为空，返回 redis.Nil
// return: error
err := res.RowsScan(dest any)
```

### 结果集的应用

```go
// 查询，查询所有pod的id属性
query := `MATCH (p:pod) RETURN p.id as id`
res, err := rdb.GraphQuery(ctx, key, query).Result()
if err != nil {
	log.Fatal(err)
}

// 读取一行
// print: map[id:x1]
log.Print(res.Row())

row := res.Row()
log.Print(row["id"]) // print: x1

// 读取所有行
// print: [map[id:x1] map[id:x2]]
log.Print(res.Rows())

// 循环输出
rows := res.Rows()
for i := 0; i < len(rows); i++ {
	log.Print(rows[i]["id"]) // x1、x2
}

// ---------------------------------------------------------

// 查询多个字段
query = `MATCH (p:pod) RETURN p.id as id,p.name as name,p.ts as ts,p.alive as alive,p.fv as fv`
res, err = rdb.GraphQuery(ctx, key, query).Result()
if err != nil {
    log.Fatal(err)
}

// 在处理结果集中，除了使用 Row() 或 Rows() 方法获取数据外，
// 还可以使用 RowScan() 或 RowsScan() 方法获取数据，把结果集的数据扫描到结构体中
// tag 标签的定义和 redis 一致，采用 redis:"name" 的方式，
// tag 名称的定义，必须和查询的字段一致，也就是 MATCH 语句中 RETURN 的字段名
type MyNode struct {
    ID    string  `redis:"id"`
    Name  string  `redis:"name"`
    Ts    int     `redis:"ts"`
    Alive bool    `redis:"alive"`
    Fv    float64 `redis:"fv"`
}

// 扫描一行数据
var node MyNode

// 必须传递结构体指针
err = res.RowScan(&node)
if err != nil {
    log.Fatal(err)
}

// {ID:x1 Name:pod1 Ts:10240 Alive:true Fv:3.1415926}
log.Printf("%+v", node)

// 如果是多行数据，需要定义 Slice，成员可以是结构体指针或结构体
// 或 var node []*MyNode
var nodes []MyNode

// 必须传递 Slice 指针
err = res.RowsScan(&nodes)

// [{ID:x1 Name:pod1 Ts:10240 Alive:true Fv:3.1415926}
// {ID:x2 Name:pod2 Ts:10241 Alive:true Fv:3.1415927}]
log.Printf("%+v", nodes)

// 同时，你也可以像正常使用结构体或 Slice 一样使用结果集
log.Print(node.ID) // x1
log.Print(node.Name) // pod1

for i := 0; i < len(nodes); i++ {
	log.Print(nodes[i].ID) // x1、x2
}

// ---------------------------------------------------------

// 查询点和边
query = `MATCH (p:pod) RETURN p limit 2`
res, err = rdb.GraphQuery(ctx, key, query).Result()
if err != nil {
    log.Fatal(err)
}

// 打印查询节点的数据，一般情况下，使用 Row() 或 Rows() 方法获取数据不太适用于 Node 和 Edge，
// 因为 Node 和 Edge 的数据结构比较复杂，可以使用 RowScan() 或 RowsScan() 方法获取数据
rows := res.Rows()
for i := 0; i < len(rows); i++ {
	// [
	//      map
	//     [
	//          p:map[id:0 labels:[pod] properties:map[alive:true fv:3.1415926 id:x1 name:pod1 ts:10240]]
	//     ]
	//      map
	//     [
	//         p:map[id:1 labels:[pod] properties:map[alive:true fv:3.1415927 id:x2 name:pod2 ts:10241]]
	//     ]
	// ]
	log.Print(rows[i]["p"])
}

type MyNode2 struct {
	Node redis.GraphNode `redis:"p"`
}

// or: var ns []*MyNode2
var ns []MyNode2
if err = val.RowsScan(&ns); err != nil {
    t.Fatal(err)
}

// {Nodes:{ID:0 Labels:[pod] Properties:map[alive:true fv:3.1415926 id:x1 name:pod1 ts:10240]}}
// {Nodes:{ID:1 Labels:[pod] Properties:map[alive:true fv:3.1415927 id:x2 name:pod2 ts:10241]}}
log.Printf("%+v", ns)

// 同样，你可以像使用普通结构体一样使用它
for i := 0; i < len(ns); i++ {
	log.Print(ns[i].Node.ID)
    log.Print(ns[i].Node.Labels)
    log.Print(ns[i].Node.Properties)
	
	// 使用点的属性
	log.Print(ns[i].Node.Properties["id"]) // x1、x2
	log.Print(ns[i].Node.Properties["name"]) // pod1、pod2
	
	// 获取具体数据类型的值
	log.Print(ns[i].Node.Properties["id"].String())
	log.Print(ns[i].Node.Properties["fv"].Float64())
	log.Print(ns[i].Node.Properties["alive"].Bool())
}

// ----------------------------------------------------------------

// 查询点和关系
query = `MATCH (p1:pod {id:"x1"})-[r:link]-(p2:pod {id:"x2"}) RETURN p1.id as srcid,p2.id as dstid, r as edge`
val, err := rdb.GraphQuery(ctx, key, query).Result()
if err != nil {
    t.Fatal(err)
}

type MyEdge struct {
    SrcID string          `redis:"srcid"`
    DstID string          `redis:"dstid"`
    Edge  redis.GraphEdge `redis:"edge"`
}

var edge MyEdge
if err = val.RowScan(&edge); err != nil {
    t.Fatal(err)
}

log.Print(edge.SrcID) // x1
log.Print(edge.DstID) // x2
log.Print(edge.Edge.Typ) // link

// 获取边的属性值 size，转换为 int 类型
log.Print(edge.Edge.Properties["size"].Int())
```
