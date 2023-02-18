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

	err := repo.RecoverCache(cch)
	if err != nil {
		log.Fatal(err)
	}

	clusterID := "default"
	clientSub := "client-consumer"

	sub := consumer.New(clusterID, clientSub, os.Getenv("natsURL"), repo, cch)
	err = sub.Subscribe("order")
	if err != nil {
		log.Fatal(err)
	}

	srv := server.New(cch)
	err = srv.Start()
	if err != nil {
		log.Fatal(err)
	}

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
