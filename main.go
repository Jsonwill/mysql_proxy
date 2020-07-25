package main

import (
	"fmt"
	"github.com/Jsonwill/mysql_proxy/proxy"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/parser/ast"
	"net"
	"time"
)

func main() {
	fmt.Println("Hi")
	listener, err := net.Listen("tcp4", ":8002")
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		conn, e := listener.Accept()
		if e != nil {
			fmt.Println(e)
			continue
		}
		//go onConn(conn)
		go func(c net.Conn) {
			p := proxy.NewProxy(GetDBs, GetTables, SelectHandler)
			p.DealConn(c)
		}(conn)
	}
}
func GetDBs(sql string, stmt *ast.ShowStmt) (*mysql.Result, error) {
	dbs := GetAllowedDBs()
	vals := make([][]interface{}, 0)
	for _, v := range dbs {
		vals = append(vals, []interface{}{v})
	}
	return proxy.BuildResult([]string{"Database"}, vals)
}

func GetTables(sql string, stmt *ast.ShowStmt) (*mysql.Result, error) {
	dbs := GetAllowedTables()
	vals := make([][]interface{}, 0)
	for _, v := range dbs {
		vals = append(vals, []interface{}{v})
	}
	return proxy.BuildResult([]string{"Table"}, vals)
}

func SelectHandler(sql string, stmt *ast.ShowStmt) (*mysql.Result, error) {
	vals := []float64{2.9, 2.1, 3.2}
	values := make([][]interface{}, 0)
	now := time.Now().Unix()
	for k, v := range vals {
		values = append(values, []interface{}{now - int64(5*60*k), "metric", v})
	}
	return proxy.BuildResult([]string{"time", "metric", "val"}, values)
}

func GetAllowedDBs() []string {
	return []string{"Hive"}
}

func GetAllowedTables() []string {
	return []string{"table1"}
}
