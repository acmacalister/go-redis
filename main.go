package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/textproto"
	"strings"
	"sync"

	"github.com/acmacalister/skittles"
	"github.com/davecheney/profile"
)

var addr = flag.Int("addr", 6379, "http service address")
var textprotoReaderPool sync.Pool

const (
	typeKey = iota
	typeString
	typeHash
	typeList
	typeSet
	typeSortedSet
	typeHyperLogLog
	typePubSub
	typeTransaction
	typeConnection
	typeServer
)

// Struct for what type we are storing in our store.
type storeItem struct {
	val       interface{}
	redisType int
}

// Mutex Locked map/dictionary
type store struct {
	lock *sync.RWMutex
	dict map[string]*storeItem
}

// Our server's "client" for processing redis commands.
type client struct {
	conn   net.Conn
	reader *bufio.Reader
	store  *store
}

func main() {
	defer profile.Start(profile.CPUProfile).Stop()
	fmt.Println(skittles.Cyan("Redis Server started..."))
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *addr))
	if err != nil {
		log.Fatal(skittles.BoldRed(err))
	}

	s := store{dict: make(map[string]*storeItem), lock: new(sync.RWMutex)}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(skittles.BoldRed(err))
			continue
		}
		c := client{conn: conn, store: &s}
		go c.handleConnection()
	}
}

//create a new reader from the pool.
func newTextprotoReader(br *bufio.Reader) *textproto.Reader {
	if v := textprotoReaderPool.Get(); v != nil {
		tr := v.(*textproto.Reader)
		tr.R = br
		return tr
	}
	return textproto.NewReader(br)
}

//put our reader in the pool for reuse.
func putTextprotoReader(r *textproto.Reader) {
	r.R = nil
	textprotoReaderPool.Put(r)
}

// handleConnection processes the tcp connection.
func (c *client) handleConnection() {
	defer c.conn.Close()
	c.reader = bufio.NewReader(c.conn)
	tp := newTextprotoReader(c.reader)
	defer func() {
		putTextprotoReader(tp)
	}()

	for {
		respString := make([]string, 0, 5)
		for {
			s, err := tp.ReadLine()
			if err != nil {
				fmt.Println(err)
				return
			}
			respString = append(respString, s)
			if c.reader.Buffered() == 0 {
				break
			}
		}

		response := c.handleRESPCommand(respString)
		c.conn.Write([]byte(response))
	}
}

func (c *client) handleRESPCommand(values []string) string {
	command := values[2:3]
	switch strings.ToUpper(command[0]) {
	case commandGet:
		val := values[len(values)-1 : len(values)]
		return c.buildRESPResponseString(c.store.Get(val[0]))
	case commandSet:
		key := values[4:5]
		val := values[len(values)-1 : len(values)]
		c.store.Set(key[0], val[0])
		return "+OK\r\n"
	}
	return fmt.Sprintf("-ERR unknown command '%s'\r\n", command)
}

func (c *client) buildRESPResponseString(item *storeItem) string {
	if item == nil {
		return "$-1\r\n"
	}
	switch item.redisType {
	case typeString:
		if item.val == nil {
			return "$-1\r\n"
		}
		str := item.val.(string)
		return fmt.Sprintf("$%d\r\n%s\n\r", len(str), str)
	}

	return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
}
