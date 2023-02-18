package main

import (
	"fmt"
	"log"
	"order_service/internal/server"
	"os"
	"os/signal"

	"order_service/internal/cache"
	"order_service/internal/repository"
	"order_service/internal/streaming/consumer"
)

func main() {

	repo := repository.New(os.Getenv("connStr"))

	cch := cache.New()
	cch.RecoverCache(repo)

	clusterID := "default"
	clientSub := "client-consumer"

	sub := consumer.New(clusterID, clientSub, "0.0.0.0:4222", repo, cch)
	sub.Subscribe("order")

	srv := server.New(cch)
	srv.Start()
	//sc, _ := stan.Connect(clusterID, clientID, stan.NatsURL("0.0.0.0:4222"))
	//
	//// Simple async subscriber.
	//sub, _ := sc.Subscribe("foo", func(m *stan.Msg) {
	//	fmt.Printf("received message: %s\n", string(m.Data))
	//}, stan.DurableName("my-durable"))
	//
	//// Simple synchronous publisher.
	//// This does not return until an ack has been received from
	//// NATS Streaming.
	//sc.Publish("foo", []byte(uint8(23)))
	//
	//sub.Unsubscribe()
	//sc.Close()

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			fmt.Printf("\nReceived an interrupt, unsubscribing and closing connection...\n\n")
			err := sub.Close()
			if err != nil {
				log.Println(err)
			}
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
