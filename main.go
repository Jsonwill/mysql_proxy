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
	listener, err := net.Listen("tcp4", "0.0.0.0:8002")
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
	dbs := []string{"select-q", "select-w"}
	values := make([][]interface{}, 0)
	for _, db := range dbs {
		values = append(values, []interface{}{db, time.Now().Unix()})
	}
	return proxy.BuildResult([]string{"db", "time"}, values)
}

func GetAllowedDBs() []string {
	return []string{"Hive"}
}

func GetAllowedTables() []string {
	return []string{"table1"}
}
