package daemon

import (
	"io"
	"log"
	"net"
	"net/textproto"
	"regexp"
)

const (
	addr = ":587"
)

var (
	addrRegex = regexp.MustCompile("<(.*)>")
)

type Msg struct {
	From string
	To   []string
	Data []byte
}

type HandlerFunc func(msg *Msg)

var defaultHandle HandlerFunc

func HandleFunc(fn HandlerFunc) {
	defaultHandle = fn
}

func ListenAndServe(addr string) error {
	if addr == "" {
		addr = "localhost:587"
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}

		go handle(textproto.NewConn(c))
	}

}

func handle(c *textproto.Conn) {
	defer c.Close()
	defer func() {
		if r := recover(); r != nil {
			log.Println("Something went wrong:", r)
		}
	}()

	converse(c)
}

func converse(c *textproto.Conn) {
	write(c, "220 At your service")

	var msg Msg

	for {
		s, err := read(c)
		if err == io.EOF {
			return
		}

		log.Println(s)

		cmd := s[:4]

		switch cmd {
		case "EHLO":
			write(c, "250-8BITMIME")
			fallthrough
		case "HELO":
			write(c, "250 I need orders")
		case "MAIL":
			msg.From = addrRegex.FindStringSubmatch(s)[1]
			write(c, "250 In your name")
		case "RCPT":
			addr := addrRegex.FindStringSubmatch(s)[1]
			msg.To = append(msg.To, addr)
			write(c, "250 Defending your honour")
		case "DATA":
			write(c, "354 Give me a quest!")
			data, err := c.ReadDotBytes()
			if err != nil {
				panic(err)
			}
			msg.Data = data

			defaultHandle(&msg)

			write(c, "250 We move")
		case "QUIT":
			write(c, "221 For the king")
		default:
			panic("Unknown command")
		}
	}
}

func write(c *textproto.Conn, msg string) {
	log.Println(msg)
	if err := c.Writer.PrintfLine(msg); err != nil {
		panic(err)
	}
}

func read(c *textproto.Conn) (string, error) {
	s, err := c.ReadLine()
	if err == io.EOF {
		return s, err
	}

	if err != nil {
		panic(err)
	}

	return s, err
}
