package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	w *kafka.Writer
)

func main() {
	tlsEnabled := false
	w = &kafka.Writer{
		Addr:  kafka.TCP("localhost:9091", "localhost:9092", "localhost:9093"),
		Topic: "shiva",
	}
	transport := &kafka.Transport{
		DialTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second,
		ClientID:    "kafka-reader-golang",
	}
	if tlsEnabled {
		transport.TLS = &tls.Config{}
	}
	defer w.Close()

	exitCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	go accept_input_from_cmd(exitCtx, cancel)

	<-exitCtx.Done()
	fmt.Println("exit signal received")
	time.Sleep(2 * time.Second)
}

func accept_input_from_cmd(ctx context.Context, cancel context.CancelFunc) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Enter msg for kafka")
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "quit" {
			fmt.Println("Exiting the program as 'quit' was entered.")
			cancel()
			break
		}
		err := w.WriteMessages(ctx,
			kafka.Message{
				Value: []byte(input),
			},
		)
		if err != nil {
			fmt.Println("error writing to kafka : ", err.Error())
		}
	}
}
