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

	go sendLoop(t.C)

	daemon.HandleFunc(handle)

	// run verification daemon
	go daemon.ListenAndServe(":25", true)

	log.Println("Listening on localhost:587")
	daemon.ListenAndServe("localhost:587", false)
	t.Stop()
}

func handle(msg *daemon.Msg) {
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
	err := q.Recover()
	if err != nil {
		log.Println("Error recovering:", err)
	}

	for {
		key, msg, err := q.Pop()
		if err != nil {
			log.Print(err)
		}

		if key != nil {
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

	log.Println("Sending failed, message scheduled for retry:", err)

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
