package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/christianalexander/kvdb/stores"

	"github.com/christianalexander/kvdb/commands"

	"github.com/sirupsen/logrus"
)

type contextKey struct {
	name string
}

var ctxKeyServer = contextKey{"SERVER"}

func main() {
	logrus.Infoln("Starting KV TCP API")

	ln, err := net.Listen("tcp", ":8888")
	if err != nil {
		logrus.Fatalf("Failed to start listener: %v", err)
	}

	logrus.Infoln("Listening on port 8888")

	store := stores.NewInMemoryStore()

	server{store}.serve(ln.(*net.TCPListener))
}

type server struct {
	store stores.Store
}

func (s server) serve(l net.Listener) error {
	defer l.Close()

	var tempDelay time.Duration
	ctx := context.Background()
	for {
		rw, e := l.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				logrus.Warnf("http: Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0
		c := newConn(rw)
		ctx := context.WithValue(ctx, ctxKeyServer, s)
		go c.serve(ctx)
	}
}

type conn struct {
	nc    net.Conn
	close chan struct{}
	txID  int
}

func newConn(c net.Conn) conn {
	return conn{nc: c, close: make(chan struct{})}
}

func (c conn) serve(ctx context.Context) {
	defer c.nc.Close()

	reader := bufio.NewReaderSize(c.nc, 4<<10)

	for {
		select {
		case <-c.close:
			return
		case <-ctx.Done():
			return
		default:
			l, _, err := reader.ReadLine()
			if err != nil {
				if err != io.EOF {
					logrus.Warnf("Failed to read request: %v", err)
				}
				break
			}

			n, p1, p2, ok := parseCommandLine(string(l))
			if !ok {
				logrus.Warnf("Failed to parse line '%s'", l)
				continue
			}

			cmd, err := c.GetCommand(ctx, n, p1, p2)
			if err != nil {
				logrus.Warnln(err)
				fmt.Fprintf(c.nc, "%v\r\n", err)
				return
			}

			err = cmd.Execute(ctx)
			if err != nil {
				logrus.Warnf("Failed to execute command: %v", err)
				fmt.Fprintf(c.nc, "%v\r\n", err)
				continue
			}
		}
	}
}

func (c conn) GetCommand(ctx context.Context, commandName, p1, p2 string) (commands.Command, error) {
	switch strings.ToUpper(commandName) {
	case "QUIT":
		return commands.NewQuit(func() error {
			close(c.close)
			return nil
		}), nil
	case "SET":
		if p1 == "" || p2 == "" {
			return nil, fmt.Errorf("expected 'SET <key> <value>', got 'SET %s %s'", p1, p2)
		}
		srv := ctx.Value(ctxKeyServer).(server)
		return commands.NewSet(c.nc, srv.store, p1, p2), nil
	case "GET":
		if p1 == "" {
			return nil, fmt.Errorf("expected 'GET <key>', but no key specified")
		}
		srv := ctx.Value(ctxKeyServer).(server)
		return commands.NewGet(c.nc, srv.store, p1), nil
	case "DEL":
		if p1 == "" {
			return nil, fmt.Errorf("expected 'DEL <key>', but no key specified")
		}
		srv := ctx.Value(ctxKeyServer).(server)
		return commands.NewDelete(c.nc, srv.store, p1), nil
	case "BEGIN":
		if c.txID != 0 {
			return nil, fmt.Errorf("cannot begin transaction within an active transaction")
		}
		return commands.NewNoop(), nil
	case "COMMIT":
		if c.txID == 0 {
			return nil, fmt.Errorf("cannot commit without a transaction")
		}
		return commands.NewNoop(), nil
	}

	return nil, fmt.Errorf("invalid command '%s'", commandName)
}

func parseCommandLine(line string) (commandName, p1, p2 string, ok bool) {
	s1 := strings.Index(line, " ")
	s2 := strings.Index(line[s1+1:], " ")
	if s1 < 0 {
		return line, "", "", true
	}
	if s2 < 0 {
		return line[:s1], line[s1+1:], "", true
	}
	s2 += s1 + 1
	return line[:s1], line[s1+1 : s2], line[s2+1:], true
}
