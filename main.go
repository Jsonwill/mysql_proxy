package main

import (
	"bytes"
	"fmt"
	"github.com/Jsonwill/mysql_proxy/executor"
	"github.com/Jsonwill/mysql_proxy/proxy"
	"github.com/XiaoMi/Gaea/mysql"
	"github.com/XiaoMi/Gaea/util/hack"
	"net"
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
		go onConn(conn)
	}

}

func onConn(c net.Conn) {
	ch := NewConnHandler(c)
	ch.Handshake()
	for {
		ch.c.SetSequence(0)
		data, err := ch.c.ReadEphemeralPacket()
		if err != nil {
			return
		}

		cmd := data[0]
		data = data[1:]
		fmt.Println("data=", string(data))

		ExecuteCommand(cmd, data)
		ch.c.RecycleReadPacket()
		rs := executor.CreateOKResponse(0)
		if err = ch.writeResponse(rs); err != nil {
			fmt.Printf("Session write response error, connId: %d, err: %v\n", ch.c.GetConnectionID(), err)
			ch.c.Close()
			return
		}

		if cmd == mysql.ComQuit {
			ch.c.Close()
		}
	}

}
func (cc *ConnHandler) writeResponse(r executor.Response) error {
	switch r.RespType {
	case executor.RespEOF:
		return cc.c.WriteEOFPacket(r.Status)
	case executor.RespResult:
		rs := r.Data.(*mysql.Result)
		if rs == nil {
			return cc.c.WriteOK(r.Status)
		}
		return cc.c.WriteOKResult(r.Status, r.Data.(*mysql.Result))
	//case RespPrepare:
	//stmt := r.Data.(*Stmt)
	//if stmt == nil {
	//	return cc.c.writeOK(r.Status)
	//}
	//return cc.c.writePrepareResponse(r.Status, stmt)
	case executor.RespFieldList:
		rs := r.Data.([]*mysql.Field)
		if rs == nil {
			return cc.c.WriteOK(r.Status)
		}
		return cc.c.WriteFieldList(r.Status, rs)
	case executor.RespError:
		rs := r.Data.(error)
		if rs == nil {
			return cc.c.WriteOK(r.Status)
		}
		err := cc.c.WriteErrorPacket(rs)
		if err != nil {
			return err
		}
		if rs == mysql.ErrBadConn { // 后端连接如果断开, 应该返回通知Session关闭
			return rs
		}
		return nil
	case executor.RespOK:
		return cc.c.WriteOK(r.Status)
	case executor.RespNoop:
		return nil
	default:
		err := fmt.Errorf("invalid response type: %T", r)
		fmt.Println(err.Error())
		return cc.c.WriteErrorPacket(err)
	}
}
func ExecuteCommand(cmd byte, data []byte) executor.Response {
	switch cmd {
	case mysql.ComQuit:
		fmt.Println("mysql.ComQuit")
		//se.handleRollback()
		// https://dev.mysql.com/doc/internals/en/com-quit.html
		// either a connection close or a OK_Packet, OK_Packet will cause client RST sometimes, but doesn't affect sql execute
		return executor.CreateNoopResponse()
	case mysql.ComQuery: // data type: string[EOF]
		fmt.Println("mysql.ComQuery")
		sql := string(data)
		fmt.Println("sql=", sql)
		// handle phase
		//r, err := se.handleQuery(sql)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		r := &mysql.Resultset{}

		field := &mysql.Field{}
		field.Name = hack.Slice("Database")
		r.Fields = append(r.Fields, field)
		dbs := []string{"q","w"}
		for _, db := range dbs {
			r.Values = append(r.Values, []interface{}{db})
		}

		result := &mysql.Result{
			AffectedRows: uint64(len(dbs)),
			Resultset:    r,
		}
		return executor.CreateResultResponse(mysql.ServerStatusInTrans, result)
	case mysql.ComPing:
		fmt.Println("mysql.ComPing")
		return executor.CreateOKResponse(mysql.ServerStatusInTrans)
	case mysql.ComInitDB:
		fmt.Println("mysql.ComInitDB")
		db := string(data)
		fmt.Println("ComInitDB = ", db)
		// handle phase
		//err := se.handleUseDB(db)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateOKResponse(0)
	case mysql.ComFieldList:
		fmt.Println("mysql.ComFieldList")
		//fs, err := se.handleFieldList(data)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		index := bytes.IndexByte(data, 0x00)
		table := string(data[0:index])
		wildcard := string(data[index+1:])
		fmt.Println("ComFieldList:", "table=", table, ";wildcard=", wildcard)
		fs := []*mysql.Field{}
		return executor.CreateFieldListResponse(0, fs)
	case mysql.ComStmtPrepare:
		fmt.Println("mysql.ComStmtPrepare")
		sql := string(data)
		fmt.Println("ComStmtPrepare:sql=", sql)
		//stmt, err := se.handleStmtPrepare(sql)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreatePrepareResponse(0)
	case mysql.ComStmtExecute:
		fmt.Println("mysql.ComStmtExecute")
		//values := make([]byte, len(data))
		//copy(values, data)
		//r, err := se.handleStmtExecute(values)
		//if err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateResultResponse(0, nil)
	case mysql.ComStmtClose: // no response
		fmt.Println("mysql.ComStmtClose")
		//if err := se.handleStmtClose(data); err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateNoopResponse()
	case mysql.ComStmtSendLongData: // no response
		fmt.Println("mysql.ComStmtSendLongData")
		//values := make([]byte, len(data))
		//copy(values, data)
		//if err := se.handleStmtSendLongData(values); err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateNoopResponse()
	case mysql.ComStmtReset:
		fmt.Println("mysql.ComStmtReset")
		//if err := se.handleStmtReset(data); err != nil {
		//return CreateErrorResponse(se.status, err)
		//}
		return executor.CreateOKResponse(0)
	case mysql.ComSetOption:
		fmt.Println("mysql.ComSetOption")
		return executor.CreateEOFResponse(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		fmt.Printf("dispatch command failed, error: %s\n", msg)
		return executor.CreateErrorResponse(0, mysql.NewError(mysql.ErrUnknown, msg))
	}
}

type ConnHandler struct {
	c *proxy.ClientConn
}

func NewConnHandler(c net.Conn) *ConnHandler {
	// First build and send the server handshake packet.
	tcpConn := c.(*net.TCPConn)

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	tcpConn.SetNoDelay(true)
	cc := proxy.NewClientConn(mysql.NewConn(tcpConn))
	return &ConnHandler{c: cc}
}
func (ch *ConnHandler) Handshake() error {

	if err := ch.c.WriteInitialHandshakeV10(); err != nil {
		clientHost, _, innerErr := net.SplitHostPort(ch.c.RemoteAddr().String())
		if innerErr != nil {
			fmt.Printf("[server] Session parse host(%s) error: %v\n", clientHost, innerErr)
		}
		return err
	}

	info, err := ch.c.ReadHandshakeResponse()
	if err != nil {
		clientHost, _, innerErr := net.SplitHostPort(ch.c.RemoteAddr().String())
		if innerErr != nil {
			fmt.Printf("[server] Session parse host(%s) error: %v\n", clientHost, innerErr)
		}

		fmt.Printf("[server] Session readHandshakeResponse error, connId: %d, ip: %s, msg: %s, error: %s",
			ch.c.GetConnectionID(), clientHost, "read Handshake Response error", err.Error())
		return err
	}

	if err := handleHandshakeResponse(info, ch.c); err != nil {
		fmt.Printf("handleHandshakeResponse error, connId: %d, err: %v", ch.c.GetConnectionID(), err)
		return err
	}

	if err := ch.c.WriteOK(mysql.ServerStatusInTrans); err != nil {
		fmt.Printf("[server] Session readHandshakeResponse error, connId %d, msg: %s, error: %s",
			ch.c.GetConnectionID(), "write ok fail", err.Error())
		return err
	}

	return nil
}

func handleHandshakeResponse(info proxy.HandshakeResponseInfo, c *proxy.ClientConn) error {
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
	fmt.Println("set charset = ", charset)
	return nil
}
