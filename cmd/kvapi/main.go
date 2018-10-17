package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/christianalexander/kvdb/protobuf"

	"github.com/christianalexander/kvdb/cmd/kvapi/handlers"
	"github.com/christianalexander/kvdb/stores"
	"github.com/sirupsen/logrus"

	"github.com/gorilla/mux"
)

var inPath string
var outPath string

func init() {
	flag.StringVar(&inPath, "in", "", "The path to the log input file")
	flag.StringVar(&outPath, "out", "", "The path to the log out file")

	flag.Parse()
}

func main() {
	logrus.Infoln("Starting KV API")

	cctx, cancel := context.WithCancel(context.Background())

	store := stores.NewInMemoryStore()

	if inPath != "" {
		inFile, err := os.Open(inPath)
		if err != nil {
			logrus.Fatalf("Failed to open inPath file ('%s'): %v", inPath, err)
		}

		reader := protobuf.NewReader(inFile)
		s, err := stores.FromPersistence(cctx, reader, store)
		if err != nil {
			logrus.Fatalf("Failed to read from persistence: %v", err)
		}

		store = s
	}

	if outPath != "" {
		outFile, err := os.OpenFile(outPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0664)
		if err != nil {
			logrus.Fatalf("Failed to open outPath file ('%s'): %v", outPath, err)
		}

		writer := protobuf.NewWriter(outFile)
		store = stores.WithPersistence(writer, store)
	}

	r := mux.NewRouter()

	r.Handle("/{Key}", handlers.GetGetHandler(store)).Methods(http.MethodGet)
	r.Handle("/{Key}", handlers.GetSetHandler(store)).Methods(http.MethodPut, http.MethodPost)
	r.Handle("/{Key}", handlers.GetDeleteHandler(store)).Methods(http.MethodDelete)

	srv := http.Server{Handler: r, Addr: ":3001"}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)

	go func() {
		s := <-sig
		logrus.Infof("Signal received: %s", s)
		cancel()

		logrus.Infoln("Shutting down server within one second")
		tctx, cancel := context.WithTimeout(context.Background(), time.Second)
		srv.RegisterOnShutdown(cancel)
		srv.Shutdown(tctx)
	}()

	logrus.Infoln("Listening on port 3001")
	logrus.Fatal(srv.ListenAndServe())
}
