package main

import (
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"scalemail/daemon"
	"scalemail/emailq"
	"scalemail/sender"
)

const version = "0.10"

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

	// create the sender
	s := sender.NewSender(
		q,
		localname,
		sender.WithDKIM(signer, dkimDomain, dkimSelector),
	)

	// kick off the sending loop
	go sendLoop(s, t.C)

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

func sendLoop(s *sender.Sender, tick <-chan time.Time) {
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

			go s.SendMsg(key, msg)
		}

		// wait for signal or tick
		select {
		case <-tick:
		case <-signal:
		}
	}
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
