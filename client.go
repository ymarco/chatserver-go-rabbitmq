package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

const exchangeName = "go_chatserver"

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
		true,   // auto-deleted
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
	log.Printf("Bound to %s\n", key)
	return client.ch.QueueBind(
		client.q.Name, // queue name
		key,           // routing key
		exchangeName,
		false,
		nil)
}
func (client *Client) unbindToKey(key string) error {
	log.Printf("Not bound to %s\n", key)
	return client.ch.QueueUnbind(client.q.Name, key, exchangeName, nil)
}

func (client *Client) sendMsg(bindingKey string, body string, ctx context.Context) error {
	log.Printf("Sending on %s\n", bindingKey)
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
	log.Printf("Connected to %s\n", conn.RemoteAddr())
	client, err := NewClient(conn, name)
	failOnError(err, "Couldn't create client")
	defer ClosePrintErr(client)

	err = client.bindToKey(BindingKeyForGlobalRoom)
	err = client.bindToKey(BindingKeyForPrivateMsg(name))
	failOnError(err, "Couldn't bind")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.readUserInputLoop(ctx)
	go client.printQueueMsgs(ctx)

	err = <-client.errs
	log.Fatalln("final err:", err)
}

func (client *Client) readUserInputLoop(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)
	for line, err := ScanLine(scanner); err == nil; line, err = ScanLine(scanner) {
		err := client.dispatchUserInput(line, ctx)
		if err != nil {
			switch err {
			case ErrUnknownCmd:
				fmt.Println("Unknown cmd")
			default:
				client.errs <- err
				return
			}
		}
	}
}

func (client *Client) dispatchUserInput(line string, ctx context.Context) error {
	if IsCmd(line) {
		cmd, args := UnserializeStrToCmd(line)
		return client.runCmd(cmd, args, ctx)
	} else {
		return client.sendMsg(BindingKeyForGlobalRoom, line, ctx)
	}
}

var ErrWrongNumberOfArgs = errors.New("wrong number of args for command")

func isValidBindingKeyComponent(str string) bool {
	return !strings.ContainsAny(str, ".#*")
}

var ErrClientHasQuit = errors.New("Client has quit")
var ErrInvalidTopicComponent = errors.New("topic components can't contain ., #, *")

func (client *Client) runCmd(cmd Cmd, args []string, ctx context.Context) error {
	switch cmd {
	case CmdLogout:
		return ErrClientHasQuit
	case CmdJoinRoom, CmdLeaveRoom:
		if len(args) != 1 {
			fmt.Printf("Usage: %s ROOM_NAME\n", CmdJoinRoom)
			return ErrWrongNumberOfArgs
		}
		key := args[0]
		if !isValidBindingKeyComponent(key) {
			return ErrInvalidTopicComponent
		}
		switch cmd {
		case CmdJoinRoom:
			return client.bindToKey(BindingKeyForRoom(key))
		case CmdLeaveRoom:
			return client.unbindToKey(BindingKeyForRoom(key))
		}
		panic("unreachable")
	case CmdSend, CmdSendRoom, CmdWhisper:
		key := ""
		body := ""
		switch cmd {
		case CmdSend:
			key = BindingKeyForGlobalRoom
			body = strings.Join(args, " ")
		case CmdWhisper:
			if len(args) < 1 {
				fmt.Printf("Usage: %s USERNAME MSG...", cmd)
				return ErrWrongNumberOfArgs
			}
			username := args[0]
			if !isValidBindingKeyComponent(username) {
				return ErrInvalidTopicComponent
			}
			key = BindingKeyForPrivateMsg(username)
			body = strings.Join(args[1:], " ")
		case CmdSendRoom:
			if len(args) < 1 {
				fmt.Printf("Usage: %s ROOM_NAME MSG...", cmd)
				return ErrWrongNumberOfArgs
			}
			room := args[0]
			if !isValidBindingKeyComponent(key) {
				return ErrInvalidTopicComponent
			}
			key = BindingKeyForRoom(room)
			body = strings.Join(args[1:], " ")
		}
		return client.sendMsg(key, body, ctx)
	case CmdHelp:
		fmt.Println(helpString)
		return nil
	default:
		return ErrUnknownCmd
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
			log.Printf("%s (on %s): %s", sender, msg.RoutingKey, msg.Body)
		case <-ctx.Done():
			return
		}
	}
}
