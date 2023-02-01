package main

import (
	"bufio"
	"context"
	"errors"
	"log"
	"os"

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
	errs chan error
}

func NewClient(conn *amqp.Connection, name string) (*Client, error) {
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

	return &Client{name, ch, q, make(chan error, 1)}, nil
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
			Headers:     map[string]interface{}{"sender": client.name},
		})
}

func RunClient(name string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer ClosePrintErr(conn)
	failOnError(err, "Failed to connect to RabbitMQ")
	client, err := NewClient(conn, name)
	failOnError(err, "Couldn't create client")
	defer ClosePrintErr(client)

	err = client.bindToKey("key1")
	if err != nil {
		log.Fatalln("aoeu", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.readUserInputLoop(ctx)
	go client.printQueueMsgs(ctx)
	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	err = <-client.errs
	log.Fatalln("final err:", err)
}

func (client *Client) readUserInputLoop(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)
	for line, err := ScanLine(scanner); err == nil; line, err = ScanLine(scanner) {
		err := client.sendMsg("key1", line, ctx)
		if err != nil {
			return
		}
	}
}

var ErrNoSender = errors.New("msg header doesn't contain a sender")

func (client *Client) printQueueMsgs(ctx context.Context) {
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

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			sender, ok := msg.Headers["sender"]
			if !ok {
				client.errs <- ErrNoSender
				return
			}
			if sender == client.name {
				continue
			}
			log.Printf("%s: %s", sender, msg.Body)
		case <-ctx.Done():
			return
		}
	}
}
