package main

import (
	"fmt"
	"github.com/hashicorp/yamux"
	"net"
)

func main() {
	tcpaddr, _ := net.ResolveTCPAddr("tcp4", ":6789")
	tcplisten, _ := net.ListenTCP("tcp", tcpaddr)
	// Accept a TCP connection  启动TCP层级的监听，等待服务端三次握手连接
	conn, err := tcplisten.Accept()
	if err != nil {
		panic(err)
	}

	// Setup server side of yamux 连接到来之后新建一个会话
	session, err := yamux.Server(conn, nil)
	if err != nil {
		panic(err)
	}

	// Accept a stream  等待客户端新建一个Stream
	stream, err := session.Accept()
	if err != nil {
		panic(err)
	}

	// Listen for a message  从Stream中读取数据
	buf := make([]byte, 4)
	stream.Read(buf)
	fmt.Println(string(buf))
}
