package main

import (
	"log"
	"net/smtp"
	"time"
)

func main() {
	for {
		log.Println("Sending email")
		go smtp.SendMail("localhost:587", nil, "oliver.janik@gmail.com", []string{"oliver@tiltandco.com"}, []byte("Hi!"))
		time.Sleep(200 * time.Millisecond)
	}
}
