package main

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const exchangeName = "logs_topic"

func failOnError(err error, msg string) {
        if err != nil {
                log.Panicf("%s: %s", msg, err)
        }
}


func client(binding_keys []string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for _, key := range binding_keys {
		log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_topic", key)
		err := bindToKey(ch, q, key)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}

func bindToKey(ch *amqp.Channel, q amqp.Queue, key string) error {
	return ch.QueueBind(
		q.Name, // queue name
		key,    // routing key
		exchangeName,
		false,
		nil)
}

func sendMsg(ch *amqp.Channel, bindingKey string, body string, ctx context.Context) error {
	return ch.PublishWithContext(ctx,
		exchangeName,
		bindingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
}
