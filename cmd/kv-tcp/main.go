package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/commands"
	"github.com/christianalexander/kvdb/protobuf"
	"github.com/christianalexander/kvdb/stores"
	"github.com/christianalexander/kvdb/stores/serializable"
	"github.com/christianalexander/kvdb/transactors"

	"github.com/sirupsen/logrus"
)

type contextKey struct {
	name string
}

var ctxKeyServer = contextKey{"SERVER"}

var inPath string
var outPath string

func init() {
	flag.StringVar(&inPath, "in", "", "The path to the log input file")
	flag.StringVar(&outPath, "out", "", "The path to the log out file")

	flag.Parse()
}

func main() {
	logrus.Infoln("Starting KV TCP API")

	ln, err := net.Listen("tcp", ":8888")
	defer ln.Close()

	if err != nil {
		logrus.Fatalf("Failed to start listener: %v", err)
	}

	logrus.SetLevel(logrus.DebugLevel)

	logrus.Infoln("Listening on port 8888")

	store := stores.NewInMemoryStore()

	if inPath != "" {
		inFile, err := os.Open(inPath)
		if err != nil {
			logrus.Fatalf("Failed to open inPath file ('%s'): %v", inPath, err)
		}

		reader := protobuf.NewReader(inFile)
		s, err := stores.FromPersistence(context.Background(), reader, store)
		if err != nil {
			logrus.Fatalf("Failed to read from persistence: %v", err)
		}

		store = s
	}

	var writer stores.Writer
	if outPath != "" {
		outFile, err := os.OpenFile(outPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0664)
		if err != nil {
			logrus.Fatalf("Failed to open outPath file ('%s'): %v", outPath, err)
		}

		w := protobuf.NewWriter(outFile)
		writer = w
		store = stores.WithPersistence(writer, store)
	}

	store = serializable.NewTwoPhaseLockStore(store)
	transactor := transactors.New(store, writer)

	s := server{ln.(*net.TCPListener), store, transactor, make(chan bool, 1)}

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stopSignal
		logrus.Infof("Received %v signal\n", sig)
		s.stop()
		os.Exit(0)
	}()

	s.serve()
}

type server struct {
	l          net.Listener
	store      stores.Store
	transactor transactors.Transactor
	quit       chan bool
}

func (s *server) serve() error {
	var tempDelay time.Duration
	ctx := context.Background()

	var wg sync.WaitGroup

	for {
		rw, e := s.l.Accept()
		if e != nil {

			select {
			case <-s.quit:
				wg.Wait()
				s.quit <- true
				return nil
			default:
				//avoids blocking if not stop signal yet
			}

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
		wg.Add(1)
		go c.serve(ctx, &wg)
	}
}

func (s *server) stop() {
	s.quit <- true
	s.l.Close() // unblocks Accept
	logrus.Infoln("Server stopped")
	<-s.quit
}

type conn struct {
	nc    net.Conn
	close chan struct{}
	txID  int64
}

func newConn(c net.Conn) *conn {
	return &conn{nc: c, close: make(chan struct{})}
}

func (c *conn) serve(ctx context.Context, wg *sync.WaitGroup) {
	defer c.nc.Close()
	defer wg.Done()

	reader := bufio.NewReaderSize(c.nc, 4<<10)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-cctx.Done()
		if c.txID != 0 {
			srv := ctx.Value(ctxKeyServer).(server)
			srv.transactor.Rollback(context.WithValue(cctx, stores.ContextKeyTransactionID, c.txID))
		}
	}()

	for {
		select {
		case <-c.close:
			return
		case <-cctx.Done():
			return
		default:
			l, _, err := reader.ReadLine()
			if err != nil {
				if err != io.EOF {
					logrus.Warnf("Failed to read request: %v", err)
				}
				return
			}

			n, p1, p2, ok := parseCommandLine(string(l))
			if !ok {
				logrus.Warnf("Failed to parse line '%s'", l)
				continue
			}

			cmd, err := c.GetCommand(cctx, n, p1, p2)
			if err != nil {
				logrus.Warnln(err)
				fmt.Fprintf(c.nc, "%v\r\n", err)
				return
			}

			srv := ctx.Value(ctxKeyServer).(server)
			srv.transactor.Execute(context.WithValue(cctx, stores.ContextKeyTransactionID, c.txID), cmd)
			if err != nil {
				logrus.Warnf("Failed to execute command: %v", err)
				fmt.Fprintf(c.nc, "%v\r\n", err)
				continue
			}
		}
	}
}

func (c *conn) GetCommand(ctx context.Context, commandName, p1, p2 string) (kvdb.Command, error) {
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
		srv := ctx.Value(ctxKeyServer).(server)
		return commands.NewBegin(c.nc, srv.transactor, func(txID int64) {
			c.txID = txID
		}), nil
	case "COMMIT":
		if c.txID == 0 {
			return nil, fmt.Errorf("cannot commit without a transaction")
		}
		srv := ctx.Value(ctxKeyServer).(server)
		return commands.NewCommit(c.nc, srv.transactor, func(txID int64) {
			c.txID = txID
		}), nil
	case "ROLLBACK":
		if c.txID == 0 {
			return nil, fmt.Errorf("cannot rollback without a transaction")
		}
		srv := ctx.Value(ctxKeyServer).(server)
		return commands.NewRollback(c.nc, srv.transactor, func(txID int64) {
			c.txID = txID
		}), nil
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
