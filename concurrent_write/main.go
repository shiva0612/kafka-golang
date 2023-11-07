package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/segmentio/kafka-go"
)

var (
	add     = []string{"localhost:9091", "localhost:9092", "localhost:9093"}
	w       *kafka.Writer
	topicwg = &sync.WaitGroup{}

	topicparts  = map[string]int{"1": 3, "2": 3, "3": 3}
	topicch     = map[string]chan kafka.Message{}
	topicpartch = map[string][]chan kafka.Message{}

	bufferPartition = 100
)

type myBal struct {
}

func (mb *myBal) Balance(msg kafka.Message, parts ...int) int {

	switch {
	case msg.Key[1] == 'a':
		return 0
	case msg.Key[1] == 'b':
		return 1
	case msg.Key[1] == 'c':
		return 2
	}
	return 0
}

type CdrRow struct {
	cdr []string
}

func main() {

	myfile := flag.String("f", "kafka_sample.csv", "file name")
	flag.Parse()
	createWriter()
	defer w.Close()

	for topicName, nparts := range topicparts {
		topicch[topicName] = make(chan kafka.Message, bufferPartition)

		for i := 0; i < nparts; i++ {
			partch := make(chan kafka.Message, bufferPartition)
			topicpartch[topicName] = append(topicpartch[topicName], partch)
		}
	}

	//read topic channel
	for topicName, tch := range topicch {
		topicwg.Add(1)
		go readtopicch(topicName, tch)
	}

	cdrowch, err := readCSV(*myfile)
	if err != nil {
		log.Fatalln("error in reading csv")
	}
	for cdrow := range cdrowch {
		msg := getkmsg(cdrow)
		topicch[gettopic(cdrow.cdr)] <- msg
	}

	for _, tch := range topicch {
		close(tch)
	}
	topicwg.Wait()

}

func gettopic(cdr []string) string {
	return cdr[0]
}

func readtopicch(topicName string, ch chan kafka.Message) {

	defer topicwg.Done()

	partwg := &sync.WaitGroup{}

	for i, partch := range topicpartch[topicName] {
		partwg.Add(1)
		go readpartition(partwg, i, partch)
	}

	for msg := range ch {
		p := w.Balancer.Balance(msg, gen0n(topicparts[topicName])...)
		topicpartch[topicName][p] <- msg
	}

	for _, partch := range topicpartch[topicName] {
		close(partch)
	}
	partwg.Wait()
	log.Println("producer Done")

}
func readpartition(partwg *sync.WaitGroup, partition int, kmsgch chan kafka.Message) {

	defer partwg.Done()

	for msg := range kmsgch {
		err := w.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Printf("error in writing msgs to partition %d: %s", partition, err.Error())
			return
		}
		fmt.Printf("%d ", partition)
	}

}

func gen0n(n int) []int {
	parts := []int{}
	for i := 0; i < n; i++ {
		parts = append(parts, i)
	}
	return parts
}

func createWriter() {
	w = &kafka.Writer{
		Addr: kafka.TCP(add...),
		// Topic: topic,
		// Balancer: &kafka.RoundRobin{},
		Balancer:  &myBal{},
		BatchSize: 2,
	}
}

func getkmsg(cdrow CdrRow) kafka.Message {
	msgb, err := json.Marshal(cdrow.cdr)
	if err != nil {
		log.Fatalln("error in marshalling: ", err.Error())
	}
	return kafka.Message{
		Key:   []byte(cdrow.cdr[1]),
		Value: msgb,
		Topic: cdrow.cdr[0],
	}
}

func readCSV(file string) (<-chan CdrRow, error) {

	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("opening file %s ", err)
	}

	ch := make(chan CdrRow, 100)

	cr := csv.NewReader(f)

	go func() {
		for {
			record, err := cr.Read()
			if err == io.EOF {
				close(ch)

				return
			}

			ch <- CdrRow{cdr: record}
		}
	}()

	return ch, nil
}
