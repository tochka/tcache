package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tochka/tcached/cache"
	"github.com/tochka/tcached/cache/lru"
	"github.com/tochka/tcached/transport"
)

var (
	maxEntries int
	address    string
)

func init() {
	flag.IntVar(&maxEntries, "max-entries", 0, "maximum entries")
	flag.StringVar(&address, "address", ":30003", "server listening address")
}

func main() {
	flag.Parse()

	var c cache.Cache = lru.NewCache(maxEntries)
	c = &cache.LockCache{
		InnerCache: c,
	}
	srv := transport.NewServer(c, address)

	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-termChan // Blocks here until interrupted
		srv.Shotdown()
	}()

	log.Println("Tcache start listen")
	if err := srv.Listen(); err != nil {
		log.Println(err)
	}
}
