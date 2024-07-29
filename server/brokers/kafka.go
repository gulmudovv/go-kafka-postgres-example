package brokers

import (
	"encoding/json"
	"log"

	"github.com/gulmudovv/go-kafka-postgress/server/models"

	"github.com/IBM/sarama"
)

var (
	brokers = []string{"kafka:9092"}

	fromKafkaTopic = "fromKafkaTopic"
	toKafkaTopic   = "toKafkaTopic"
)

func Consumer() {

	comment := new(models.Comment)

	// Создание продюсера Kafka

	producer, err := ConnectProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Создание консьюмера Kafka
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Подписка на партицию  "fromKafkaTopic" в Kafka
	partConsumer, err := consumer.ConsumePartition(fromKafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %v", err)
	}
	defer partConsumer.Close()

	// Горутина для обработки входящих сообщений от Kafka

	for {
		select {
		// Чтение сообщения из Kafka
		case err := <-partConsumer.Errors():
			log.Printf("Consumer errors: %+v\n", err)
		case msg, ok := <-partConsumer.Messages():
			if !ok {
				log.Println("Channel closed, exiting goroutine")
				return
			}
			err := json.Unmarshal(msg.Value, &comment)
			if err != nil {
				log.Printf("Error unmarshaling JSON: %v\n", err)
				continue
			}
			// Oбновления записи в таблице comments
			models.Db.Model(&comment).Updates(models.Comment{Content: comment.Content, Processed: 1})

		}
	}
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(message []byte) error {
	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: toKafkaTopic,
		Value: sarama.StringEncoder(message),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}
