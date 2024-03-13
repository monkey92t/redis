package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type MyEdge struct {
	SrcID string          `redis:"srcid"`
	DstID string          `redis:"dstid"`
	Edge  redis.GraphEdge `redis:"edge"`
}

type MyNode struct {
	Nodes redis.GraphNode `redis:"p"`
}

type MyNode2 struct {
	ID    string  `redis:"id"`
	Name  string  `redis:"name"`
	Ts    int     `redis:"ts"`
	Alive bool    `redis:"alive"`
	Fv    float64 `redis:"fv"`
}

func TestCreate(t *testing.T) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:        "10.254.176.135:6379",
		DialTimeout: 3 * time.Second,
	})
	_ = rdb.FlushDB(ctx).Err()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	key := "g"

	query := `CREATE (:pod {id:"x1", name:"pod1", ts: 10240, alive: true, fv: 3.1415926})`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
		log.Fatal()
	}

	query = `CREATE (:pod {id:"x2", name:"pod2", ts: 10241, alive: true, fv: 3.1415927})`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}

	query = `CREATE (:pod {id:"x3", name:"pod3", ts: 10242, alive: false, fv: 3.1415928})`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}

	query = `CREATE (:pod {id:"x4", name:"pod4", ts: 10243, alive: false, fv: 3.1415929})`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}

	query = `MATCH (p1:pod {id:"x1"}),(p2:pod {id:"x2"}) CREATE (p1)-[:link {src:"pod", dst:"pod", size:1}]->(p2)`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}
	query = `MATCH (p1:pod {id:"x1"}),(p3:pod {id:"x3"}) CREATE (p1)-[:link {src:"pod", dst:"pod", size:2}]->(p3)`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}
	query = `MATCH (p1:pod {id:"x1"}),(p4:pod {id:"x4"}) CREATE (p1)-[:link {src:"pod", dst:"pod", size:3}]->(p4)`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}
	query = `MATCH (p2:pod {id:"x2"}),(p4:pod {id:"x4"}) CREATE (p2)-[:link {src:"pod", dst:"pod", size:1}]->(p4)`
	if err := rdb.GraphQuery(ctx, key, query).Err(); err != nil {
		t.Fatal(err)
	}

	//query = `MATCH (p:pod) RETURN p.id`
	//val, err := rdb.GraphQuery(ctx, key, query).Result()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//log.Println(val.Row())
	////printGraphResult(t, val)

	//query = `MATCH (p:pod) RETURN p.id,p.name,p.ts,p.alive,p.fv limit 2`
	//val, err := rdb.GraphQuery(ctx, key, query).Result()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//printGraphResult(t, val)

	//query = `MATCH (p2:pod {id:"x2"})-[r:link]-(p4:pod {id:"x4"}) RETURN p2,r,p4`
	//val, err := rdb.GraphQuery(ctx, key, query).Result()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//printGraphResult(t, val)

	//query = `MATCH (p2:pod {id:"x2"})-[r:link]-(p4:pod {id:"x4"}) RETURN p2.id as srcid,r as edge,p4.id as dstid`
	//val, err := rdb.GraphQuery(ctx, key, query).Result()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//e := MyEdge{}
	//if err = val.RowScan(&e); err != nil {
	//	t.Fatal(err)
	//}
	//printGraphResult(t, val)
	//t.Logf("%+v", e)
	//t.Logf("%+v", e.Edge.Properties["size"].Int())

	//query = `MATCH (p:pod) return p as nodes`
	//val, err := rdb.GraphQuery(ctx, key, query).Result()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//var ns []*MyNode
	//if err = val.RowsScan(&ns); err != nil {
	//	t.Fatal(err)
	//}
	//printGraphResult(t, val)
	//t.Logf("%+v", ns)

	//query = `MATCH (p:pod) return p.id as id,p.name as name, p.ts as ts, p.alive as alive, p.fv as fv`
	//val, err = rdb.GraphQuery(ctx, key, query).Result()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//var ns []*MyNode2
	//if err = val.RowsScan(&ns); err != nil {
	//	t.Fatal(err)
	//}
	//printGraphResult(t, val)
	//for i := 0; i < len(ns); i++ {
	//	t.Logf("%+v", ns[i])
	//}

	query = `MATCH (p1:pod {id:"x1"})-[r:link]-(p2:pod {id:"x2"}) RETURN p1.id as srcid,p2.id as dstid, r as edge`
	val, err := rdb.GraphQuery(ctx, key, query).Result()
	if err != nil {
		t.Fatal(err)
	}
	var edge MyEdge
	if err = val.RowScan(&edge); err != nil {
		t.Fatal(err)
	}
	log.Printf("%+v", edge.Edge.Properties["size"].Int())
}

func printGraphResult(t *testing.T, r *redis.GraphResult) {
	if r.Error() != nil {
		t.Logf("graph result error - %v", r.Error())
		return
	}
	t.Logf("graph result \n message: %v \n field: %v \n rows length: %d \n rows: %v",
		fmt.Sprint(r.Message()),
		fmt.Sprint(r.Field()),
		r.Len(),
		r.Rows(),
	)
}
