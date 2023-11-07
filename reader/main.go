package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func getTlsDialter() *kafka.Dialer {
	return &kafka.Dialer{
		TLS: &tls.Config{},
	}
}

func main() {
	tlsEnabled := false
	readerConfig := kafka.ReaderConfig{
		Brokers: []string{"localhost:9091", "localhost:9092", "localhost:9093"},
		GroupID: "cg1",
		Topic:   "shiva",
	}
	if tlsEnabled {
		readerConfig.Dialer = getTlsDialter()
	}
	r := kafka.NewReader(readerConfig)
	defer r.Close()

	exitCtx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT)
	go ReadMessage(exitCtx, r)

	fmt.Println("started reading from kafka")
	<-exitCtx.Done()
	fmt.Println("exit signal received")
	time.Sleep(2 * time.Second)
}

func ReadMessage(ctx context.Context, r *kafka.Reader) {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				fmt.Println("closing the reader")
				break
			}
			fmt.Println("error reading from kafka : ", err.Error())
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
