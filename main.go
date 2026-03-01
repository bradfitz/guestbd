package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"

	"tailscale.com/tsweb"
)

var (
	flagListen   = flag.String("listen", ":10809", "NBD listen address")
	flagFile     = flag.String("file", "", "path to the backing file to serve")
	flagPageSize = flag.Int("page-size", 4096, "page size in bytes (must be a power of two)")
	flagMaxMem   = flag.Int64("max-mem", 1<<30, "maximum memory for page cache in bytes")
	flagDebug    = flag.String("debug-addr", ":8080", "debug HTTP listen address")
)

func main() {
	flag.Parse()

	if *flagFile == "" {
		log.Fatal("--file is required")
	}
	if *flagPageSize <= 0 || (*flagPageSize&(*flagPageSize-1)) != 0 {
		log.Fatal("--page-size must be a positive power of two")
	}

	srv := NewServer(*flagFile, *flagPageSize, *flagMaxMem)
	srv.initExpvar()

	// Debug HTTP server with tsweb.
	debugMux := http.NewServeMux()
	tsweb.Debugger(debugMux)
	go func() {
		log.Printf("debug HTTP server listening on %s", *flagDebug)
		if err := http.ListenAndServe(*flagDebug, debugMux); err != nil {
			log.Fatalf("debug HTTP: %v", err)
		}
	}()

	// NBD TCP listener.
	ln, err := net.Listen("tcp", *flagListen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("NBD server listening on %s, serving %s", *flagListen, *flagFile)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("accept: %v", err)
			continue
		}
		go srv.HandleConn(conn)
	}
}
