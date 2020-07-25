package proxy

import (
	"bytes"
	"fmt"
	"github.com/Jsonwill/mysql_proxy/executor"
	"github.com/XiaoMi/Gaea/log"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/parser"
	"github.com/XiaoMi/Gaea/parser/ast"
	"github.com/XiaoMi/Gaea/proxy/plan"
	"github.com/pingcap/errors"
	"net"
	"strings"
)

type SqlHanler func(sql string, stmt *ast.ShowStmt) (*mysql.Result, error)

type Proxy struct {
	showTableHandler    SqlHanler
	showDatabaseHandler SqlHanler
	selectHandler       SqlHanler
}

func NewProxy(showDatabaseHandler SqlHanler, showTableHandler SqlHanler, selectHandler SqlHanler) *Proxy {
	return &Proxy{
		showTableHandler:    showTableHandler,
		showDatabaseHandler: showDatabaseHandler,
		selectHandler:       selectHandler,
	}
}

func (p *Proxy) SetShowTableHandler(h SqlHanler) {
	p.showTableHandler = h
}

func (p *Proxy) SetShowDatabaseHandler(h SqlHanler) {
	p.showDatabaseHandler = h
}

func (p *Proxy) SetSelectHandler(h SqlHanler) {
	p.selectHandler = h
}

func (p *Proxy) DealConn(c net.Conn) error {
	// First build and send the server handshake packet.
	tcpConn := c.(*net.TCPConn)

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	tcpConn.SetNoDelay(true)
	cc := NewClientConn(mysql.NewConn(tcpConn))
	if e := HandleShake(cc); e != nil {
		return e
	}
	for {
		cc.SetSequence(0)
		data, err := cc.ReadEphemeralPacket()
		if err != nil {
			return err
		}

		cmd := data[0]
		data = data[1:]
		fmt.Println("data=", string(data))

		rs := p.ExecuteCommand(cmd, data)
		cc.RecycleReadPacket()
		//rs := executor.CreateOKResponse(0)
		if err = writeResponse(rs, cc); err != nil {
			cc.Close()
			return fmt.Errorf("Session write response error, connId: %d, err: %v\n", cc.GetConnectionID(), err)
		}
		if cmd == mysql.ComQuit {
			cc.Close()
		}
	}
	return nil
}

func HandleShake(c *ClientConn) error {
	if err := c.WriteInitialHandshakeV10(); err != nil {
		clientHost, _, innerErr := net.SplitHostPort(c.RemoteAddr().String())
		if innerErr != nil {
			fmt.Printf("[server] Session parse host(%s) error: %v\n", clientHost, innerErr)
		}
		return err
	}

	info, err := c.ReadHandshakeResponse()
	if err != nil {
		clientHost, _, innerErr := net.SplitHostPort(c.RemoteAddr().String())
		if innerErr != nil {
			fmt.Printf("[server] Session parse host(%s) error: %v\n", clientHost, innerErr)
		}

		return fmt.Errorf("[server] Session readHandshakeResponse error, connId: %d, ip: %s, msg: %s, error: %s",
			c.GetConnectionID(), clientHost, "read Handshake Response error", err.Error())
	}

	if err := handleHandshakeResponse(info, c); err != nil {
		return fmt.Errorf("handleHandshakeResponse error, connId: %d, err: %v", c.GetConnectionID(), err)
	}

	if err := c.WriteOK(mysql.ServerStatusInTrans); err != nil {
		return fmt.Errorf("[server] Session readHandshakeResponse error, connId %d, msg: %s, error: %s",
			c.GetConnectionID(), "write ok fail", err.Error())
	}
	return nil
}

func handleHandshakeResponse(info HandshakeResponseInfo, c *ClientConn) error {
	// check and set user
	user := info.User
	ok := true
	if !ok {
		return mysql.NewDefaultError(mysql.ErrAccessDenied, user, c.RemoteAddr().String(), "Yes")
	}

	// check password
	succPassword := true
	if !succPassword {
		return mysql.NewDefaultError(mysql.ErrAccessDenied, user, c.RemoteAddr().String(), "Yes")
	}

	// handle collation
	collationID := info.CollationID
	collationName, ok := mysql.Collations[mysql.CollationID(collationID)]
	if !ok {
		return mysql.NewError(mysql.ErrInternal, "invalid collation")
	}
	charset, ok := mysql.CollationNameToCharset[collationName]
	if !ok {
		return mysql.NewError(mysql.ErrInternal, "invalid collation")
	}
	log.Trace("set charset = %s", charset)
	return nil
}

func (p *Proxy) ExecuteCommand(cmd byte, data []byte) executor.Response {
	switch cmd {
	case mysql.ComQuit:
		log.Trace("mysql.ComQuit")
		//se.handleRollback()
		// https://dev.mysql.com/doc/internals/en/com-quit.html
		// either a connection close or a OK_Packet, OK_Packet will cause client RST sometimes, but doesn't affect sql execute
		return executor.CreateNoopResponse()
	case mysql.ComQuery: // data type: string[EOF]
		log.Trace("mysql.ComQuery")
		sql := string(data)

		// handle phase
		r, e := p.handleQuery(sql)
		if e != nil {
			return executor.CreateErrorResponse(mysql.ServerStatusAutocommit, e)
		}
		return executor.CreateResultResponse(mysql.ServerStatusAutocommit, r)
	case mysql.ComPing:
		log.Trace("mysql.ComPing")
		return executor.CreateOKResponse(mysql.ServerStatusInTrans)
	case mysql.ComInitDB:
		log.Trace("mysql.ComInitDB")
		//db := string(data)
		// handle phase
		//err := se.handleUseDB(db)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateOKResponse(0)
	case mysql.ComFieldList:
		log.Trace("mysql.ComFieldList")
		//fs, err := se.handleFieldList(data)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		index := bytes.IndexByte(data, 0x00)
		table := string(data[0:index])
		wildcard := string(data[index+1:])
		log.Trace("ComFieldList:", "table=", table, ";wildcard=", wildcard)
		fs := []*mysql.Field{}
		return executor.CreateFieldListResponse(0, fs)
	case mysql.ComStmtPrepare:
		log.Trace("mysql.ComStmtPrepare")
		sql := string(data)
		log.Trace("ComStmtPrepare:sql=", sql)
		//stmt, err := se.handleStmtPrepare(sql)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreatePrepareResponse(0)
	case mysql.ComStmtExecute:
		log.Trace("mysql.ComStmtExecute")
		//values := make([]byte, len(data))
		//copy(values, data)
		//r, err := se.handleStmtExecute(values)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateResultResponse(0, nil)
	case mysql.ComStmtClose: // no response
		log.Trace("mysql.ComStmtClose")
		//if err := se.handleStmtClose(data); err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateNoopResponse()
	case mysql.ComStmtSendLongData: // no response
		log.Trace("mysql.ComStmtSendLongData")
		//values := make([]byte, len(data))
		//copy(values, data)
		//if err := se.handleStmtSendLongData(values); err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateNoopResponse()
	case mysql.ComStmtReset:
		log.Trace("mysql.ComStmtReset")
		//if err := se.handleStmtReset(data); err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateOKResponse(0)
	case mysql.ComSetOption:
		fmt.Println("mysql.ComSetOption")
		return executor.CreateEOFResponse(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Trace("dispatch command failed, error: %s\n", msg)
		return executor.CreateErrorResponse(0, mysql.NewError(mysql.ErrUnknown, msg))
	}
}

func (p *Proxy) handleQuery(sql string) (r *mysql.Result, e error) {
	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号

	stmtType := parser.Preview(sql)
	if canHandleWithoutPlan(stmtType) {
		stmt, err := ParseOneStmt(sql)
		if err != nil {
			e = err
			return
		}
		switch s := stmt.(type) {
		case *ast.ShowStmt:
			return p.handleShow(sql, s)
		case *ast.SetStmt:
		case *ast.BeginStmt:
		case *ast.CommitStmt:
		case *ast.RollbackStmt:
		case *ast.UseStmt:
		default:
			return nil, fmt.Errorf("cannot handle sql without plan, ns: %s, sql: %s", "", sql)
		}
	}
	if stmtType != parser.StmtSelect {
		return nil, fmt.Errorf("not support[%d]", stmtType)
	}
	//reqCtx.Set(util.StmtType, stmtType)
	//
	//dbs := []string{"select-q", "select-w"}
	//values := make([][]interface{}, 0)
	//for _, db := range dbs {
	//	values = append(values, []interface{}{db, time.Now().Unix()})
	//}
	//r, e = BuildResult([]string{"db", "time"}, values)
	if p.selectHandler == nil {
		return BuildResult([]string{"db"}, nil)
	}
	return p.selectHandler(sql, nil)
}
func BuildResult(columns []string, vals [][]interface{}) (*mysql.Result, error) {
	rs, _ := mysql.BuildResultset(nil, columns, vals)
	r := &mysql.Result{
		Status:       mysql.ServerMoreResultsExists,
		AffectedRows: uint64(len(vals)),
		Resultset:    rs,
	}
	e := plan.GenerateSelectResultRowData(r)
	return r, e
}
func ParseOneStmt(sql string) (a ast.StmtNode, e error) {
	stmts, _, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("%d", mysql.ErrSyntax)
	}
	ast.SetFlag(stmts[0])
	return stmts[0], nil
}
func (p *Proxy) handleShow(sql string, stmt *ast.ShowStmt) (*mysql.Result, error) {
	switch stmt.Tp {
	case ast.ShowDatabases:

		if p.showDatabaseHandler == nil {
			dbs := GetAllowedDBs()
			vals := make([][]interface{}, 0)
			for _, v := range dbs {
				vals = append(vals, []interface{}{v})
			}
			return BuildResult([]string{"Database"}, vals)
		}
		return p.showDatabaseHandler(sql, stmt)
	case ast.ShowTables:
		if p.showTableHandler == nil {
			dbs := GetAllowedTables()
			vals := make([][]interface{}, 0)
			for _, v := range dbs {
				vals = append(vals, []interface{}{v})
			}
			return BuildResult([]string{"Tables"}, vals)
		}
		return p.showTableHandler(sql, stmt)
	default:
		return nil, fmt.Errorf("execute sql error, sql: %s, err: not support", sql)
	}
	return nil, nil
}
func GetAllowedDBs() []string {
	return []string{"Hive"}
}

func GetAllowedTables() []string {
	return []string{"table1"}
}

func canHandleWithoutPlan(stmtType int) bool {
	return stmtType == parser.StmtShow ||
		stmtType == parser.StmtSet ||
		stmtType == parser.StmtBegin ||
		stmtType == parser.StmtCommit ||
		stmtType == parser.StmtRollback ||
		stmtType == parser.StmtUse
}

func writeResponse(r executor.Response, c *ClientConn) error {
	switch r.RespType {
	case executor.RespEOF:
		return c.WriteEOFPacketNoneWarning(r.Status)
	case executor.RespResult:
		rs := r.Data.(*mysql.Result)
		if rs == nil {
			return c.WriteOK(r.Status)
		}
		return c.WriteOKResult(r.Status, r.Data.(*mysql.Result))
	//case RespPrepare:
	//stmt := r.Data.(*Stmt)
	//if stmt == nil {
	//	return cc.c.writeOK(r.Status)
	//}
	//return cc.c.writePrepareResponse(r.Status, stmt)
	case executor.RespFieldList:
		rs := r.Data.([]*mysql.Field)
		if rs == nil {
			return c.WriteOK(r.Status)
		}
		return c.WriteFieldList(r.Status, rs)
	case executor.RespError:
		rs := r.Data.(error)
		if rs == nil {
			return c.WriteOK(r.Status)
		}
		err := c.WriteErrorPacket(rs)
		if err != nil {
			return err
		}
		if rs == mysql.ErrBadConn { // 后端连接如果断开, 应该返回通知Session关闭
			return rs
		}
		return nil
	case executor.RespOK:
		return c.WriteOK(r.Status)
	case executor.RespNoop:
		return nil
	default:
		err := fmt.Errorf("invalid response type: %T", r)
		return c.WriteErrorPacket(err)
	}
}
