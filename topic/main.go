package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

var (
	topic string
	part  int
	rep   int
	// BrokerAdd = []string{"kafka-0:9092", "kafka-1:9092", "kafka-2:9092"}
	// BrokerAdd = []string{"10.144.112.112:9030", "10.144.112.112:9031", "10.144.112.112:9032"}
	BrokerAdd = []string{}
	conn      *kafka.Conn
)

// create 				go run main.go -t <name> --create (or) go run main.go -t <name> --create -p <> -r <>
// describe				go run main.go -t <name>
// alter partitions 	go run main.go -t <name> --alterpart 3
// alter topic config	go run main.go -t <name> --alter

func getbrokers(brokerStr string) []string {
	return strings.Split(brokerStr, ",")
}

func main() {
	fbrokers := flag.String("brokers", "localhost:9091,localhost:9092,localhost:9091", "coma seperated values like h:p,h:p")
	ftopic := flag.String("t", "", "topic")
	fpart := flag.Int("p", 3, "partitions")
	frep := flag.Int("r", 1, "replication")
	fcreate := flag.Bool("create", false, "mention this to create,else ignore")
	fdelete := flag.Bool("delete", false, "mention this to delete,else ignore")
	falterConfig := flag.String("alterconfig", "", "mention this key=value,key=value")
	falterPart := flag.Int("alterpart", 0, "REQUIRED")

	flag.Parse()

	topic = *ftopic
	part = *fpart
	rep = *frep
	create := *fcreate
	alter := *falterConfig
	alterPart := *falterPart
	delete := *fdelete

	BrokerAdd = getbrokers(*fbrokers)
	c := &kafka.Client{
		Addr: kafka.TCP(BrokerAdd...),
	}

	switch {

	case delete:
		deleteTopic(c)
		return

	case create:
		creatingTopics(c)

	//not working
	case alterPart != 0:
		c.CreatePartitions(context.Background(), &kafka.CreatePartitionsRequest{
			Addr: kafka.TCP(BrokerAdd...),
			Topics: []kafka.TopicPartitionsConfig{
				{
					Name:  topic,
					Count: int32(alterPart),
				},
			},
			// ValidateOnly: true,
		})

	//not working
	case alter != "":
		log.Println("in alter ------------")
		alterConfigs := []kafka.AlterConfigRequestConfig{}
		for _, kv := range strings.Split(alter, ",") {
			key, value, _ := strings.Cut(kv, "=")
			alterConfig := kafka.AlterConfigRequestConfig{
				Name:  key,
				Value: value,
			}
			alterConfigs = append(alterConfigs, alterConfig)
		}
		alterReqResource := kafka.AlterConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
			Configs:      alterConfigs,
		}
		alterReqs := kafka.AlterConfigsRequest{
			Addr:         kafka.TCP(BrokerAdd...),
			Resources:    []kafka.AlterConfigRequestResource{alterReqResource},
			ValidateOnly: false, // create topics i guess check
		}
		c.AlterConfigs(context.Background(), &alterReqs)
		log.Println("alter Done ------------------")

	}

	partitions, err := gettingPartitions()
	if err != nil {
		log.Println(err.Error())
		return
	}

	printParts(partitions)

}
func deleteTopic(c *kafka.Client) {
	log.Println("deleting topic: ", topic)
	_, err := c.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{
		Addr:   c.Addr,
		Topics: []string{topic},
	})
	if err != nil {
		log.Println("could not delete the topic: ", topic)
	}
	log.Println("deleted ", topic)

}
func creatingTopics(c *kafka.Client) {

	_, err := c.CreateTopics(
		context.Background(),
		&kafka.CreateTopicsRequest{
			Addr: c.Addr,
			Topics: []kafka.TopicConfig{
				{
					Topic:             topic,
					ReplicationFactor: rep,
					NumPartitions:     part,
				},
			},
		})
	if err != nil {
		log.Fatalln("err while creating topic ", err.Error())
	}
}

func gettingPartitions() ([]kafka.Partition, error) {
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
		msg := fmt.Sprint("failed to connect to any of the brokers in ", BrokerAdd)
		return nil, errors.New(msg)
	}

	var parts []kafka.Partition
	if topic == "" {
		parts, err = conn.ReadPartitions()
	} else {
		parts, err = conn.ReadPartitions(topic)
	}
	if err != nil {
		msg := fmt.Sprint("error while getting partitions for topic: ", topic)
		return nil, errors.New(msg)
	}
	return parts, err
}

func printParts(parts []kafka.Partition) {

	topicsParts := map[string]int{} //topic and number of partitions
	for _, part := range parts {
		fmt.Printf(" part = %d, leader = %d, ", part.ID, part.Leader.ID)
		var rep, isr []int

		for _, broker := range part.Replicas {
			rep = append(rep, broker.ID)
		}
		for _, broker := range part.Isr {
			isr = append(isr, broker.ID)
		}
		fmt.Printf("replicas = %v, isr = %v \n", rep, isr)
		topicsParts[part.Topic] += 1
	}
	log.Println("topic and number of partitions each = ", topicsParts)
}
