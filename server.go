package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/smtp"
	"strings"
	"time"

	"smtp-server/daemon"
	"smtp-server/emailq"
)

var (
	q         *emailq.EmailQ
	localname string
)

func main() {
	flag.StringVar(&localname, "localname", "localhost", "What server sends out as helo greeting")
	flag.Parse()

	// open up persistent queue
	var err error
	q, err = emailq.New("emails.db")
	if err != nil {
		log.Panic(err)
	}
	defer q.Close()

	t := time.NewTicker(time.Duration(1) * time.Minute)

	go sendLoop(t.C)

	daemon.HandleFunc(func(msg *daemon.Msg) {
		handle(msg, t.C)
	})

	daemon.ListenAndServe(":587")
	t.Stop()
}

func handle(msg *daemon.Msg, c <-chan time.Time) {
	for _, m := range group(msg) {
		log.Print("Pushing incoming email")
		err := q.Push(m)

		if err != nil {
			log.Print(err)
		}
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
	// repeat every tick
	for {
		// send all email
		for {
			msg, err := q.Pop()
			if err != nil {
				log.Print(err)
			}

			if msg == nil {
				break
			}

			log.Println("Sending email out to", msg.Host)
			err = send(msg)
			if err != nil {
				log.Print(err)
			}
		}

		<-tick
	}
}

func send(msg *emailq.Msg) error {
	mda, err := findMDA(msg.Host)
	if err != nil {
		return err
	}

	// todo: make sure we're sending matching HELO
	c, err := smtp.Dial(mda)
	if err != nil {
		return err
	}
	defer c.Close()

	if err = c.Hello(localname); err != nil {
		return err
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

	if _, err = w.Write(msg.Data); err != nil {
		return err
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
	h := results[0].Host
	return h[:len(h)-1] + ":25", nil
}
