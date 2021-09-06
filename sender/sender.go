package sender

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/smtp"

	"scalemail/emailq"

	"github.com/emersion/go-msgauth/dkim"
)

// Sends out emails and manages outgoing connections
type Sender struct {
	q            *emailq.EmailQ
	hello        string
	dkimKey      crypto.Signer
	dkimDomain   string
	dkimSelector string
}

func NewSender(q *emailq.EmailQ, hello string, options ...func(*Sender)) *Sender {
	return &Sender{
		q:     q,
		hello: hello,
	}
}

func WithDKIM(key crypto.Signer, domain, selector string) func(*Sender) {
	return func(s *Sender) {
		s.dkimKey = key
		s.dkimDomain = domain
		s.dkimSelector = selector
	}
}

func (s *Sender) SendMsg(key []byte, msg *emailq.Msg) {
	err := s.send(msg)
	if err == nil {
		err = s.q.RemoveDelivered(key)
		if err != nil {
			log.Println("Error removing delivered:", err)
		}
		return
	}

	log.Println("Sending failed for", msg.To, "message scheduled for retry:", err)

	if msg.Retry == 6 {
		log.Println("Maximum retries reached:", msg.To)
		err = s.q.Kill(key)
		if err != nil {
			log.Println("Error killing msg:", err)
		}
		return
	}

	// schedule for retry
	err = s.q.Retry(key)
	if err != nil {
		log.Println("Error retrying:", err)
	}
}

func (s *Sender) send(msg *emailq.Msg) error {
	conn, addr, err := open(msg.Host)
	if err != nil {
		log.Println("Failed to dial", msg.Host, ":", err)
		return err
	}

	if conn == nil { // no connection let's short circuit
		return nil
	}
	defer conn.Close()

	err = s.sayHello(conn, addr)
	if err != nil {
		log.Println("Failed to init connection to", msg.Host, ":", err)
		return err
	}

	err = s.sendSingle(conn, msg)
	if err != nil {
		return err
	}

	return conn.Quit()
}

func open(host string) (*smtp.Client, string, error) {
	if host == "example.com" {
		log.Println("Skipping test domain:", host)
		return nil, "", nil
	}

	mda, err := findMDA(host)
	if err != nil {
		return nil, "", err
	}

	addr := mda[:len(mda)-1]

	c, err := smtp.Dial(addr + ":25") // remove dot and add port
	return c, addr, err
}

func (s *Sender) sayHello(c *smtp.Client, addr string) error {
	// start the conversation
	if err := c.Hello(s.hello); err != nil {
		return err
	}

	// attempt TLS
	if ok, _ := c.Extension("STARTTLS"); ok {
		config := &tls.Config{
			ServerName:         addr,
			InsecureSkipVerify: true,
		}
		if err := c.StartTLS(config); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sender) sendSingle(c *smtp.Client, msg *emailq.Msg) error {
	if msg.Retry == 0 {
		log.Println("Sending email out to", msg.To)
	} else {
		log.Printf("Retrying (%v) email out to %v\n", msg.Retry, msg.To)
	}

	if err := c.Mail(msg.From); err != nil {
		return err
	}

	for _, addr := range msg.To {
		if err := c.Rcpt(addr); err != nil {
			return err
		}
	}

	w, err := c.Data()
	if err != nil {
		return err
	}

	if s.dkimKey == nil || s.sign(msg.Data, w) != nil {
		if _, err = w.Write(msg.Data); err != nil {
			return err
		}
	}

	if err = w.Close(); err != nil {
		return err
	}

	return nil
}

// Find Mail Delivery Agent based on DNS MX record
func findMDA(host string) (string, error) {
	results, err := net.LookupMX(host)
	if err != nil {
		return "", err
	}

	if len(results) == 0 {
		return "", errors.New("No MX records found")
	}

	// todo: support for multiple MX records
	return results[0].Host, nil
}

func (s *Sender) sign(email []byte, w io.Writer) error {
	defer func() {
		if r := recover(); r != nil {
			log.Println("dkim.Sign panicked:", r)
		}
	}()

	r := bytes.NewReader(email)
	options := &dkim.SignOptions{
		Domain:   s.dkimDomain,
		Selector: s.dkimSelector,
		Signer:   s.dkimKey,
	}

	err := dkim.Sign(w, r, options)
	if err != nil {
		log.Println("Error signing email:", err)
	}

	return err
}
