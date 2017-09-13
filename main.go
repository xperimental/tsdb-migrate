package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/xperimental/tsdb-migrate/config"
)

func main() {
	config, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Error in flags: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	input, err := runInput(ctx, config.InputDirectory)
	if err != nil {
		log.Fatalf("Error starting input: %s", err)
	}

	runLoop(input)

	log.Printf("Shutting down...")
	cancel()
}

func runLoop(input chan string) {
	term := make(chan os.Signal)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	for {
		select {
		case in, ok := <-input:
			if !ok {
				log.Println("input closed.")
				return
			}
			log.Printf("in: %s", in)
		case <-term:
			log.Println("Caught interrupt. Exiting...")

			signal.Reset()
			return
		}
	}
}
