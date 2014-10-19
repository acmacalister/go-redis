package main

import (
	"bufio"
	"fmt"
	"github.com/acmacalister/skittles"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var addr = flag.Int("addr", 6379, "http service address")

const (
	CommandKey = iota
	CommandString
	CommandHash
	CommandList
	CommandSet
	CommandSortedSet
	CommandHyperLogLog
	CommandPubSub
	CommandTransaction
	CommandConnection
	CommandServer
)

// Struct for what type we are storing in our store.
type storeItem struct {
	val         string
	commandType int
}

// Mutex Locked map/dictionary
type store struct {
	dict map[string]storeItem
	lock *sync.RWMutex
}

// Our server's "client" for processing redis commands.
type client struct {
	conn   *net.Conn
	reader *bufio.Reader
	store  *store
}

func main() {
	fmt.Println(skittles.Cyan("Redis Server started..."))
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *addr))
	if err != nil {
		log.Fatal(skittles.BoldRed(err))
	}

	s := store{dict: make(map[string]string), lock: &sync.RWMutex{}}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(skittles.BoldRed(err))
			continue
		}
		c := client{conn: &conn, store: &s}
		go c.process()
	}
}

func (store *store) Get(key string) storeItem {
	store.lock.RLock()
	defer store.lock.RUnlock()

	return store.dict[key]
}

func (store *store) Set(key string, val string) {
	store.lock.Lock()
	defer store.lock.Unlock()

	item := storeItem{val: val, commandType: CommandString}
	store.dict[key] = val
}

func (c *client) process() {
	defer client.conn.Close()
	client.reader = bufio.NewReader(client.conn)
}
