package main

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/smtp"
	"runtime/debug"
	"strings"
	"time"

	"scalemail/daemon"
	"scalemail/emailq"

	"github.com/emersion/go-msgauth/dkim"
)

const version = "0.9"

var (
	q            *emailq.EmailQ
	localname    string
	dkimKey      string
	dkimDomain   string
	dkimSelector string
	signer       crypto.Signer
	signal       chan struct{}
)

func main() {
	flag.StringVar(&localname, "localname", "localhost", "What server sends out as helo greeting")
	flag.StringVar(&dkimKey, "dkimKey", "", "DKIM Private Key used to sign the emails")
	flag.StringVar(&dkimDomain, "dkimDomain", "", "DKIM Domain")
	flag.StringVar(&dkimSelector, "dkimSelector", "", "DKIM Selector")
	flag.Parse()

	log.Println("Localname:", localname)
	if dkimKey != "" && dkimDomain != "" && dkimSelector != "" {
		var err error
		signer, err = readDKIMKey(dkimKey)
		if err != nil {
			log.Println("Could not parse DKIM Private key, emails will not be signed:", err)
		}
	}

	// open up persistent queue
	var err error
	q, err = emailq.New("emails.db")
	if err != nil {
		log.Panic(err)
	}
	defer q.Close()

	// signals new message just arrived
	signal = make(chan struct{}, 1)

	// wakes up sending goroutine every minute to check queue and run scheduled messages
	t := time.NewTicker(time.Duration(1) * time.Minute)

	go sendLoop(t.C)

	daemon.HandleFunc(handle)

	log.Println("Version:", version)
	log.Println("Listening on :587")
	err = daemon.ListenAndServe(":587")
	if err != nil {
		log.Println("Could not launch daeamon:", err)
	}

	t.Stop()
}

func handle(msg *daemon.Msg) {
	for _, m := range group(msg) {
		err := q.Push(m)
		if err != nil {
			log.Print(err)
			continue
		}
		log.Println("Pushing incoming email", msg.To, ". Queue length", q.Length())
	}

	// wake up sender
	select {
	case signal <- struct{}{}:
	default:
	}
}

// groups messages by host for easier delivery
func group(msg *daemon.Msg) (messages []*emailq.Msg) {
	hostMap := make(map[string][]string)

	for _, to := range msg.To {
		host := strings.Split(to, "@")[1]
		hostMap[host] = append(hostMap[host], to)
	}

	for k, v := range hostMap {
		messages = append(messages, &emailq.Msg{
			From: msg.From,
			Host: k,
			To:   v,
			Data: msg.Data,
		})
	}

	return messages
}

func sendLoop(tick <-chan time.Time) {
	err := q.Recover()
	if err != nil {
		log.Println("Error recovering:", err, debug.Stack())
	}

	for {
		for {
			key, msg, err := q.Pop()
			if err != nil {
				log.Print(err)
			}

			if key == nil {
				break
			}

			go sendMsg(key, msg)
		}

		// wait for signal or tick
		select {
		case <-tick:
		case <-signal:
		}
	}
}

func sendMsg(key []byte, msg *emailq.Msg) {
	if msg.Retry == 0 {
		log.Println("Sending email out to", msg.To)
	} else {
		log.Printf("Retrying (%v) email out to %v\n", msg.Retry, msg.To)
	}

	err := send(msg)
	if err == nil {
		err = q.RemoveDelivered(key)
		if err != nil {
			log.Println("Error removing delivered:", err)
		}
		return
	}

	log.Println("Sending failed for", msg.To, "message scheduled for retry:", err)

	if msg.Retry == 6 {
		log.Println("Maximum retries reached:", msg.To)
		err = q.Kill(key)
		if err != nil {
			log.Println("Error killing msg:", err)
		}
		return
	}

	// schedule for retry
	err = q.Retry(key)
	if err != nil {
		log.Println("Error retrying:", err)
	}
}

func send(msg *emailq.Msg) error {
	if msg.Host == "example.com" {
		log.Println("Skipping test domain:", msg.Host)
		return nil
	}

	mda, err := findMDA(msg.Host)
	if err != nil {
		return err
	}

	host := mda[:len(mda)-1]

	c, err := smtp.Dial(host + ":25") // remove dot and add port
	if err != nil {
		return err
	}
	defer c.Close()

	if err = c.Hello(localname); err != nil {
		return err
	}

	// attempt TLS
	if ok, _ := c.Extension("STARTTLS"); ok {
		config := &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: true,
		}
		if err = c.StartTLS(config); err != nil {
			return err
		}
	}

	if err = c.Mail(msg.From); err != nil {
		return err
	}

	for _, addr := range msg.To {
		if err = c.Rcpt(addr); err != nil {
			return err
		}
	}

	w, err := c.Data()
	if err != nil {
		return err
	}

	if signer == nil || sign(msg.Data, w) != nil {
		if _, err = w.Write(msg.Data); err != nil {
			return err
		}
	}

	if err = w.Close(); err != nil {
		return err
	}

	return c.Quit()
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

func readDKIMKey(filename string) (crypto.Signer, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(buf)
	if block == nil {
		return nil, errors.New("Could not decode PEM file")
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func sign(email []byte, w io.Writer) error {
	defer func() {
		if r := recover(); r != nil {
			log.Println("dkin.Sign panicked:", r)
		}
	}()

	r := bytes.NewReader(email)
	options := &dkim.SignOptions{
		Domain:   dkimDomain,
		Selector: dkimSelector,
		Signer:   signer,
	}

	err := dkim.Sign(w, r, options)
	if err != nil {
		log.Println("Error signing email:", err)
	}

	return err
}
