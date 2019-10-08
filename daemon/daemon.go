package daemon

import (
	"io"
	"log"
	"net"
	"net/textproto"
	"regexp"
	"runtime/debug"
	"strings"
)

var (
	addrRegex = regexp.MustCompile("<(.+@.+)>")
)

// Msg represents email message
type Msg struct {
	From string
	To   []string
	Data []byte
}

// HandlerFunc handles incoming msg
type HandlerFunc func(msg *Msg)

var defaultHandle HandlerFunc

// HandleFunc sets HandlerFunc
func HandleFunc(fn HandlerFunc) {
	defaultHandle = fn
}

// ListenAndServe starts listening loop
func ListenAndServe(addr string) error {
	if addr == "" {
		addr = ":587"
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
			debug.PrintStack()
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

		log.Println("#", s)

		cmd := strings.ToUpper(s[:4])

		switch cmd {
		case "EHLO":
			write(c, "250-8BITMIME")
			fallthrough

		case "HELO":
			write(c, "250 I need orders")

		case "MAIL":
			msg.From = parseAddr(s)
			if msg.From == "" {
				write(c, "501 Invalid email")
			} else {
				write(c, "250 In your name")
			}

		case "RCPT":
			addr := parseAddr(s)
			if addr == "" {
				write(c, "501 Invalid email")
			} else {
				msg.To = append(msg.To, addr)
				write(c, "250 Defending your honour")
			}

		case "DATA":
			write(c, "354 Give me a quest!")
			data, err := c.ReadDotBytes()
			if err != nil {
				panic(err)
			}
			msg.Data = data

			defaultHandle(&msg)

			write(c, "250 We move")

		case "RSET":
			write(c, "250 OK")

		case "QUIT":
			write(c, "221 For the king")

		default:
			log.Println("Unknown command:", s)
			write(c, "500 Unkown command")
		}
	}
}

func parseAddr(s string) string {
	r := addrRegex.FindStringSubmatch(s)
	if len(r) == 0 {
		return ""
	}

	return r[1]
}

func write(c *textproto.Conn, msg string) {
	log.Println("$", msg)

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
