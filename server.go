package main

import (
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"scalemail/daemon"
	"scalemail/emailq"
	"scalemail/sender"
)

const version = "0.13"

var (
	q         *emailq.EmailQ
	localname string
	signers   = sender.Signers{}
	signal    chan struct{}
)

func main() {
	flag.StringVar(&localname, "localname", "localhost", "What server sends out as helo greeting")
	flag.Func("dkim", "DKIM signing key as domain:selector:keyfile, repeat for each sending domain", addDKIM)
	flag.Parse()

	log.Println("Localname:", localname)

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

	// kick off the sending loop
	go sendLoop(t.C)

	daemon.HandleFunc(handle)

	log.Println("Version:", version)
	log.Println("Listening on :587")

	// kick off listener for incoming connections
	err = daemon.ListenAndServe(":587")
	if err != nil {
		log.Println("Could not launch daeamon:", err)
	}

	t.Stop()
}

func handle(msg *daemon.Msg) {
	for _, m := range split(msg) {
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

// splits messages by host, incoming email to 3 different domains becomes 3 messages
func split(msg *daemon.Msg) (messages []*emailq.Msg) {
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
			keys, messages, err := q.PopBatch(50)
			if err != nil {
				log.Print(err)
			}

			// nothing else to send, awesome!
			if len(keys) == 0 {
				break
			}

			grouped := groupByHost(keys, messages)

			for host, batch := range grouped {
				go sendBatch(host, batch)
			}
		}

		// wait for signal or tick
		select {
		case <-tick:
		case <-signal:
			time.Sleep(3 * time.Second) // grace period
		}
	}
}

type msgWithKey struct {
	key []byte
	msg *emailq.Msg
}

func sendBatch(host string, messages []msgWithKey) {
	c := sender.NewConnection(localname, sender.WithDKIM(signers))

	if host == "example.com" {
		log.Println("Skipping test domain:", host)

		for _, m := range messages {
			handleSuccess(m.key)
		}

		return
	}

	log.Println("Connecting to", host)

	err := c.Open(host)
	if err != nil {
		for _, m := range messages {
			handleError(m.key, m.msg, err)
		}

		return
	}

	defer c.Quit()

	err = c.Hello()
	if err != nil {
		for _, m := range messages {
			handleError(m.key, m.msg, err)
		}

		return
	}

	for _, m := range messages {
		if m.msg.Retry == 0 {
			log.Println("Sending email out to", m.msg.To)
		} else {
			log.Printf("Retrying (%v) email out to %v\n", m.msg.Retry, m.msg.To)
		}

		err = c.Send(m.msg)
		if err != nil {
			handleError(m.key, m.msg, err)
		} else {
			handleSuccess(m.key)
		}
	}
}

func groupByHost(keys [][]byte, messages []*emailq.Msg) map[string][]msgWithKey {
	var result = make(map[string][]msgWithKey)

	for i, key := range keys {
		msg := messages[i]

		result[msg.Host] = append(result[msg.Host], msgWithKey{key: key, msg: msg})
	}

	return result
}

func handleSuccess(key []byte) {
	err := q.RemoveDelivered(key)
	if err != nil {
		log.Println("Error removing delivered:", err)
	}
}

func handleError(key []byte, msg *emailq.Msg, err error) {
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

// parses one -dkim flag value and registers its signer; a bad key is logged and that domain goes unsigned
func addDKIM(value string) error {
	parts := strings.SplitN(value, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return fmt.Errorf("expected domain:selector:keyfile, got %q", value)
	}

	domain, selector, keyfile := strings.ToLower(parts[0]), parts[1], parts[2]

	key, err := readDKIMKey(keyfile)
	if err != nil {
		log.Printf("Could not parse DKIM Private key for %v, its emails will not be signed: %v\n", domain, err)
		return nil
	}

	signers[domain] = sender.NewSigner(domain, selector, key)
	log.Println("DKIM signing enabled for", domain, "selector", selector)

	return nil
}

func readDKIMKey(filename string) (crypto.Signer, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(buf)
	if block == nil {
		return nil, fmt.Errorf("could not decode PEM file %v", filename)
	}

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return key, nil
}
