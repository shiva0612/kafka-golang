package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"
)

var (
	topic     string
	parts     int
	BrokerAdd = []string{"localhost:9091"}
	conn      *kafka.Conn
	partch    []chan kafka.Message
	partwg    *sync.WaitGroup
	kreader   *kafka.Reader
)

// this is processing msgs concurrently but not reading concurrently from kafka
// since groupID is given, only few partition's data is consumed by this consumer => we are creating extra partCh which will not get populated at all
func main() {
	t := flag.String("t", "1", "topic")

	flag.Parse()

	topic = *t
	parts = len(gettingPartitions())
	log.Println("partitions = ", parts)

	kreader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     BrokerAdd,
		Topic:       topic,
		GroupID:     "some",
		StartOffset: kafka.LastOffset,
	})
	// kreader.SetOffset(kafka.LastOffset)
	go create_start_reading_partitions()

	defer kreader.Close()
	go readMessage()

	var exitch = make(chan os.Signal, 1)
	signal.Notify(exitch, syscall.SIGINT)

	select {
	case <-exitch:
		for _, ch := range partch {
			close(ch)
		}
		partwg.Wait()
		log.Println("reading is Done")
		return

	}

}

func readMessage() {
	for {
		msg, err := kreader.ReadMessage(context.Background())
		if err != nil {
			log.Println("error in fetching msgs")
			break
		}

		partch[msg.Partition] <- msg

	}
}

func create_start_reading_partitions() {
	partwg = &sync.WaitGroup{}
	for i := 0; i < parts; i++ {
		partwg.Add(1)
		ch := make(chan kafka.Message, 100)
		partch = append(partch, ch)
		go readPartition(i, partch[i])
	}

}

func readPartition(partition int, ch chan kafka.Message) {
	defer partwg.Done()

	for msg := range ch {
		log.Println(string(msg.Value)) //process msg
		// kreader.CommitMessages(context.Background(), msg) //commit msg
	}
}

func gettingPartitions() []kafka.Partition {
	var err error
	for _, brokeradd := range BrokerAdd {
		conn, err = kafka.Dial("tcp", brokeradd)
		if err != nil {
			log.Println("err while connecting to broker ", brokeradd)
			continue
		}
		break
	}
	if conn == nil {
		log.Fatalln("\nfailed to Dial the broker -> in getPartitions\n")
	}

	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		log.Fatalln("\nerror while getting partitions for topic\n")
	}
	return parts
}
