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
	quit chan struct{}
}

func NewClient(conn *amqp.Connection, name string) (*Client, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic", // type
		false,   // durable
		true,    // auto-deleted
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

	return &Client{name, ch, q, make(chan error, 1), make(chan struct{}, 1)}, nil
}

func (client *Client) Close() error {
	return client.ch.Close()
}

func (client *Client) bindToKey(key BindingKey) error {
	log.Printf("Bound to %s\n", key)
	return client.ch.QueueBind(
		client.q.Name, // queue name
		string(key),   // routing key
		exchangeName,
		false,
		nil)
}
func (client *Client) unbindToKey(key BindingKey) error {
	log.Printf("Not bound to %s\n", key)
	return client.ch.QueueUnbind(client.q.Name, string(key), exchangeName, nil)
}

func (client *Client) sendMsg(key BindingKey, body string, ctx context.Context) error {
	log.Printf("Sending on %s\n", key)
	return client.ch.PublishWithContext(ctx,
		exchangeName,
		string(key),
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
	failOnError(err, "Couldn't bind")
	err = client.bindToKey(BindingKeyForPrivateMsg(name))
	failOnError(err, "Couldn't bind")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.readUserInputLoop(ctx)
	go client.printQueueMsgs(ctx)

	select {
	case err := <-client.errs:
		log.Fatalln("final err:", err)
	case <-client.quit:
	}
}

func (client *Client) readUserInputLoop(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)
	userInput := ReadAsyncIntoChan(scanner)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-userInput:
			if input.Err != nil {
				client.errs <- input.Err
				return
			}

			err := client.dispatchUserInput(input.Val, ctx)
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
}

func (client *Client) dispatchUserInput(input string, ctx context.Context) error {
	if IsCmd(input) {
		cmd, args := UnserializeStrToCmd(input)
		return client.dispatchCmd(cmd, args, ctx)
	} else {
		return client.sendMsg(BindingKeyForGlobalRoom, input, ctx)
	}
}

var ErrWrongNumberOfArgs = errors.New("wrong number of args for command")

func isValidBindingKeyComponent(str string) bool {
	return !strings.ContainsAny(str, ".#*")
}

var ErrInvalidTopicComponent = errors.New("topic components can't contain ., #, *")

func (client *Client) dispatchCmd(cmd Cmd, args []string, ctx context.Context) error {
	switch cmd {
	case CmdLogout:
		client.quit <- struct{}{}
		return nil
	case CmdJoinRoom, CmdLeaveRoom:
		return client.dispatchBindCmd(cmd, args)
	case CmdSend, CmdSendRoom, CmdWhisper:
		return client.dispatchSendCmd(cmd, args, ctx)
	case CmdHelp:
		fmt.Println(helpString)
		return nil
	default:
		return ErrUnknownCmd
	}
}

func (client *Client) dispatchBindCmd(cmd Cmd, args []string) error {
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
}

func (client *Client) dispatchSendCmd(cmd Cmd, args []string, ctx context.Context) error {
	key := BindingKey("")
	body := ""
	switch cmd {
	case CmdSend:
		key = BindingKeyForGlobalRoom
		body = strings.Join(args, " ")
	case CmdWhisper:
		if len(args) < 1 {
			fmt.Printf("Usage: %s USERNAME MSG...\n", cmd)
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
			fmt.Printf("Usage: %s ROOM_NAME MSG...\n", cmd)
			return ErrWrongNumberOfArgs
		}
		room := args[0]
		if !isValidBindingKeyComponent(room) {
			return ErrInvalidTopicComponent
		}
		key = BindingKeyForRoom(room)
		body = strings.Join(args[1:], " ")
	}
	return client.sendMsg(key, body, ctx)
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
