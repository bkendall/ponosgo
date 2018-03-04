package main

import (
	"flag"
	"github.com/bkendall/ponosgo"
	"log"
	"time"
)

var (
	lifetime = flag.Duration("lifetime", 5*time.Second, "lifetime of server")
)

func init() {
	flag.Parse()
}

func main() {
	server, err := ponos.NewServer(
		"amqp://guest:guest@localhost:5672/",
		map[string]func(string){
			"go-sample-queue-one": func(str string) {
				log.Printf("go-sample-queue-one received: %s", str)
			},
			"go-sample-queue-two": func(str string) {
				log.Printf("go-sample-queue-two received: %s", str)
			},
		})
	if err != nil {
		log.Fatal("%s", err)
	}

	server.Consume()

	if *lifetime > 0 {
		log.Printf("running for %s", *lifetime)
		time.Sleep(*lifetime)
	} else {
		log.Printf("running forever")
		select {}
	}

	log.Printf("shutting down")
	if err := server.Shutdown(); err != nil {
		log.Printf("shutting down failed")
	}
}
