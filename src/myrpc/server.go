package myrpc

import (
	"context"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type HeartbeatRcvr struct{}

func (*HeartbeatRcvr) Heartbeat(struct{}, *struct{}) error { return nil }

type Server struct {
	listener      net.Listener
	heartbeatRcvr *HeartbeatRcvr
	server        *rpc.Server
	activeConn    map[net.Conn]struct{}
	mu            sync.Mutex
	inShutdown    atomic.Value
}

func (s *Server) setShutdown(v bool) {
	s.inShutdown.Store(v)
}

func (s *Server) isShutdown() bool {
	return s.inShutdown.Load().(bool)
}

func (s *Server) Init(rcvr interface{}) {
	s.heartbeatRcvr = &HeartbeatRcvr{}
	s.activeConn = make(map[net.Conn]struct{})
	s.server = rpc.NewServer()
	s.server.Register(rcvr)
	s.server.Register(s.heartbeatRcvr)
}

func (s *Server) StartServing(network, address string) error {
	var err error
	s.setShutdown(false)
	s.listener, err = net.Listen(network, address)
	if err != nil {
		return err
	}
	go s.Serve()
	return nil
}

func (s *Server) Serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.isShutdown() {
				logrus.Infof("<Serve> shutdown\n")
				return
			}
			logrus.Errorf("<Serve> Accept err: %v\n", err)
			return
		}
		s.mu.Lock()
		s.activeConn[conn] = struct{}{}
		s.mu.Unlock()
		go func() {
			s.server.ServeConn(conn)
			s.mu.Lock()
			delete(s.activeConn, conn)
			s.mu.Unlock()
		}()
	}
}

// Close immediately closes all active connections and net.Listener.
func (s *Server) Close() {
	s.setShutdown(true)
	s.mu.Lock()
	s.listener.Close()
	for conn := range s.activeConn {
		conn.Close()
		delete(s.activeConn, conn)
	}
	s.mu.Unlock()
}

var shutdownPollingInterval = 50 * time.Millisecond

// Shutdown gracefully shuts down the server without interrupting any active
// connection. If the provided context expires before the shutdown completes,
// Shutdown closes all active connections and returns the context's error,
// otherwise it returns any error returned from closing the Listener.
func (s *Server) Shutdown(ctx context.Context) error {
	s.setShutdown(true)
	err := s.listener.Close()

	ticker := time.NewTicker(shutdownPollingInterval)
	defer ticker.Stop()
outer:
	for {
		if len(s.activeConn) == 0 {
			return err
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break outer
		case <-ticker.C:
		}
	}

	s.mu.Lock()
	for conn := range s.activeConn {
		conn.Close()
		delete(s.activeConn, conn)
	}
	s.mu.Unlock()
	return err
}
