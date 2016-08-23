package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/smtp"
	"strings"
	"time"

	"github.com/oliverjanik/scalemail/daemon"
	"github.com/oliverjanik/scalemail/emailq"
)

var (
	q         *emailq.EmailQ
	localname string
	signal    chan struct{}
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

	signal = make(chan struct{}, 1)

	t := time.NewTicker(time.Duration(1) * time.Minute)

	go sendLoop(t.C)

	daemon.HandleFunc(func(msg *daemon.Msg) {
		handle(msg, t.C)

		select {
		case signal <- struct{}{}:
		default:
		}
	})

	log.Println("Listening on localhost:587")
	daemon.ListenAndServe("localhost:587")
	t.Stop()
}

func handle(msg *daemon.Msg, c <-chan time.Time) {
	for _, m := range group(msg) {
		err := q.Push(m)
		if err != nil {
			log.Print(err)
			continue
		}
		log.Println("Pushing incoming email. Queue length", q.Length())
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
	// repeat every tick
	for {
		for {
			k, msg, err := q.PopIncoming()
			if err != nil {
				log.Print(err)
			}

			if msg == nil {
				break
			}

			go func(msg *emailq.Msg) {
				log.Println("Sending email out to", msg.Host)
				err = send(msg)
				if err != nil {
					log.Println("Error, requing:", err)
					time.Sleep(1 * time.Minute)
					q.Push(msg)
				}
				q.RemoveDelivered(k)
			}(msg)
		}

		select {
		case <-tick:
		case <-signal:
		}
	}
}

func send(msg *emailq.Msg) error {
	mda, err := findMDA(msg.Host)
	if err != nil {
		return err
	}

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
