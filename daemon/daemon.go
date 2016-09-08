package daemon

import (
	"io"
	"log"
	"net"
	"net/textproto"
	"regexp"
	"strings"
)

var (
	addrRegex = regexp.MustCompile("<(.*)>")
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
func ListenAndServe(addr string, verifyOnly bool) error {
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

		if verifyOnly {
			log.Println("Incoming connection from", c.RemoteAddr().String())
		}

		go handle(textproto.NewConn(c), verifyOnly)
	}

}

func handle(c *textproto.Conn, verifyOnly bool) {
	defer c.Close()
	defer func() {
		if r := recover(); r != nil {
			log.Println("Something went wrong:", r)
		}
	}()

	converse(c, verifyOnly)
}

func converse(c *textproto.Conn, verifyOnly bool) {
	write(c, "220 At your service")

	var msg Msg

	for {
		s, err := read(c)
		if err == io.EOF {
			return
		}

		if verifyOnly {
			log.Println("Incoming:", s)
		}

		cmd := strings.ToUpper(s[:4])

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
			if verifyOnly {
				write(c, "502 Verification service only")
				return
			}

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
		}
	}
}

func write(c *textproto.Conn, msg string) {
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
