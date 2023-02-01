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

type Client struct {
	name string
	ch   *amqp.Channel
	q    amqp.Queue
}

func NewClient(conn *amqp.Connection, name string) (*Client, error) {
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &Client{name, ch, q}, nil
}

func (client *Client) Close() error {
	return client.ch.Close()
}

func (client *Client) bindToKey(key string) error {
	return client.ch.QueueBind(
		client.q.Name, // queue name
		key,           // routing key
		exchangeName,
		false,
		nil)
}

func (client *Client) sendMsg(bindingKey string, body string, ctx context.Context) error {
	return client.ch.PublishWithContext(ctx,
		exchangeName,
		bindingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
}

func RunClient(binding_keys []string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	client, err := NewClient(conn, "TODO name")
	failOnError(err, "Couldn't create client")
	defer client.Close()
	for _, key := range binding_keys {
		log.Printf("Binding queue %s to exchange %s with routing key %s",
			client.q.Name, exchangeName, key)
		err := client.bindToKey(key)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := client.ch.Consume(
		client.q.Name, // queue
		"",            // consumer
		true,          // auto ack
		false,         // exclusive
		false,         // no local
		false,         // no wait
		nil,           // args
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
