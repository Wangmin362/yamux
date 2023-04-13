package main

import (
	"github.com/hashicorp/yamux"
	"net"
)

func main() {
	// Get a TCP connection
	conn, err := net.Dial("tcp", ":6789")
	if err != nil {
		panic(err)
	}

	// Setup client side of yamux
	session, err := yamux.Client(conn, nil)
	if err != nil {
		panic(err)
	}

	// Open a new stream
	stream, err := session.Open()
	if err != nil {
		panic(err)
	}

	// Stream implements net.Conn
	stream.Write([]byte("ping"))
}
