package main

import (
	"fmt"
	"github.com/hashicorp/yamux"
	"net"
)

func main() {
	tcpaddr, _ := net.ResolveTCPAddr("tcp4", ":6789")
	tcplisten, _ := net.ListenTCP("tcp", tcpaddr)
	// Accept a TCP connection
	conn, err := tcplisten.Accept()
	if err != nil {
		panic(err)
	}

	// Setup server side of yamux
	session, err := yamux.Server(conn, nil)
	if err != nil {
		panic(err)
	}

	// Accept a stream
	stream, err := session.Accept()
	if err != nil {
		panic(err)
	}

	// Listen for a message
	buf := make([]byte, 4)
	stream.Read(buf)
	fmt.Println(string(buf))
}
