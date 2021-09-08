package sender

import (
	"crypto"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"strings"

	"scalemail/emailq"
)

type Connection struct {
	addr string
	c    *smtp.Client

	hello string

	signer *Signer
}

func NewConnection(hello string, options ...func(*Connection)) *Connection {
	c := &Connection{hello: hello}

	for _, opt := range options {
		opt(c)
	}

	return c
}

func WithDKIM(domain, selector string, key crypto.Signer) func(*Connection) {
	return func(c *Connection) {
		if key == nil || domain == "" || selector == "" {
			return
		}

		c.signer = NewSigner(domain, selector, key)
	}
}

// Opens an SMTP connection to given host
func (c *Connection) Open(host string) error {
	// find target server, e.g. gmail
	mda, err := findMDA(host)
	if err != nil {
		return fmt.Errorf("failed to look up MX record for %v: %v", host, err)
	}

	// remove trailing dot from the MX record
	addr := strings.TrimSuffix(mda, ".")

	c.c, err = smtp.Dial(addr + ":25") // add port
	if err != nil {
		return fmt.Errorf("failed to dial %v: %v", host, err)
	}

	if c == nil { // no connection let's short circuit
		return fmt.Errorf("connection not made to %v", host)
	}

	return nil
}

// Initialises the conversation with the remote server and negotiates encryption
// This call has to appear before sending any emails
func (c *Connection) Hello() error {
	// start the conversation
	if err := c.c.Hello(c.hello); err != nil {
		return err
	}

	// attempt TLS
	if ok, _ := c.c.Extension("STARTTLS"); ok {
		config := &tls.Config{
			ServerName:         c.addr,
			InsecureSkipVerify: true,
		}
		if err := c.c.StartTLS(config); err != nil {
			return err
		}
	}

	return nil
}

func (c *Connection) Send(msg *emailq.Msg) error {
	if err := c.c.Mail(msg.From); err != nil {
		return err
	}

	for _, addr := range msg.To {
		if err := c.c.Rcpt(addr); err != nil {
			return err
		}
	}

	w, err := c.c.Data()
	if err != nil {
		return err
	}

	// if signer isn't configured or signing fails, send the message without signature
	if c.signer == nil || c.signer.Sign(msg.Data, w) != nil {
		if _, err = w.Write(msg.Data); err != nil {
			return err
		}
	}

	if err = w.Close(); err != nil {
		return err
	}

	return nil
}

// Finalises and closes the connection
func (c *Connection) Quit() error {
	return c.c.Quit()
}

// Find Mail Delivery Agent based on DNS MX record
func findMDA(host string) (string, error) {
	results, err := net.LookupMX(host)
	if err != nil {
		return "", err
	}

	if len(results) == 0 {
		return "", errors.New("no MX records found")
	}

	// todo: support for multiple MX records
	return results[0].Host, nil
}
