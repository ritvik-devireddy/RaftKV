package main

import "net"

type listener struct {
	l    net.Listener
	addr string
}

func netListen() (*listener, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &listener{l: l, addr: l.Addr().String()}, nil
}

func (l *listener) close() { l.l.Close() }
