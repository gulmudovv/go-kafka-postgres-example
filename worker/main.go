package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/gulmudovv/go-kafka-postgress/worker/utils"

	"log"

	"github.com/IBM/sarama"
)

var (
	brokers        = []string{"kafka:9092"}
	toKafkaTopic   = "toKafkaTopic"
	fromKafkaTopic = "fromKafkaTopic"
)

// Comment struct
type Comment struct {
	ID        uint   `json:"id"`
	Content   string `json:"content" gorm:"text;not null;default:null"`
	Processed uint   `json:"processed" gorm:"not null;default:0"`
}

func main() {
	comment := new(Comment)

	// Создание продюсера Kafka
	producer, err := sarama.NewSyncProducer(brokers, nil)
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

	// Подписка на партицию "toKafkaTopic" в kafka
	partConsumer, err := consumer.ConsumePartition(toKafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %v", err)
	}
	defer partConsumer.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Канал для получения сигнала с операционной системы
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-partConsumer.Errors():
				log.Printf("Consumer errors: %+v\n", err)

			case msg, ok := <-partConsumer.Messages():
				if !ok {
					log.Println("Channel closed, exiting")
					return
				}
				// Десериализация входящего сообщения из JSON

				err := json.Unmarshal(msg.Value, &comment)
				if err != nil {
					log.Printf("Error unmarshaling JSON: %v\n", err)
					continue
				}
				// Вызываем функцию для обработки комментарий
				res := utils.CensorWord(comment.Content)
				comment.Content = res

				cmtInBytes, err := json.Marshal(comment)
				if err != nil {
					log.Printf("Failed convert body into bytes and send it to kafka: %v", err)
				}

				// Формируем ответное сообщение
				resp := &sarama.ProducerMessage{
					Topic: fromKafkaTopic,
					Value: sarama.StringEncoder(cmtInBytes),
				}
				// Отправляем ответ на сервер kafka
				_, _, err = producer.SendMessage(resp)
				if err != nil {
					log.Printf("Failed to send message to Kafka: %v", err)
				}
			case <-sigchan:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh

}
