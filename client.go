package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	name             string
	cookie           string
	askForCookie     chan string
	conn             *amqp.Connection
	receiveMsgsQueue amqp.Queue
	errs             chan error
	quit             chan struct{}
}

func NewClient(conn *amqp.Connection, name, cookie string) (*Client, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ClosePrintErr(ch)
	err = ch.ExchangeDeclare(
		msgsExchangeName,
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

	err = ch.ExchangeDeclare(
		cookieExchangeName,
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &Client{name, cookie, make(chan string, 1),
		conn, q, make(chan error, 1), make(chan struct{}, 1)}, nil
}

func (client *Client) ListenToChatMsgsFrom(ch *amqp.Channel, key BindingKey) error {
	log.Printf("Bound to %s\n", key)
	return ch.QueueBind(
		client.receiveMsgsQueue.Name, // queue name
		string(key),                  // routing key
		msgsExchangeName,
		false,
		nil)
}

func (client *Client) DontListenToChatMsgsFrom(ch *amqp.Channel, key BindingKey) error {
	log.Printf("Unbound from %s\n", key)
	return ch.QueueUnbind(client.receiveMsgsQueue.Name, string(key), msgsExchangeName, nil)
}

func (client *Client) sendChatMsg(ch *amqp.Channel, key BindingKey, body string, ctx context.Context) error {
	log.Printf("Sending on %s\n", key)
	return ch.PublishWithContext(ctx,
		msgsExchangeName,
		string(key),
		true,  // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			Headers:     map[string]interface{}{"sender": client.name},
		})
}

const channelReconnectDelay = 1 * time.Second
const connectionReconnectDelay = 5 * time.Second

func RunClient(name, cookie string) {
	for RunClientUntilDisconnected(name, cookie) {
		fmt.Printf("Retrying in %s ...\n", connectionReconnectDelay)
		time.Sleep(connectionReconnectDelay)
	}
}
func RunClientUntilDisconnected(name, cookie string) (shouldReconnect bool) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		if errIsConnectionRefused(err) {
			log.Println(err)
			return true
		}
		log.Fatalln(err)
	}
	defer ClosePrintErr(conn)
	connClosed := conn.NotifyClose(make(chan *amqp.Error))
	log.Printf("Connected to %s\n", conn.RemoteAddr())

	for {
		action := RunClientUntilChannelClosed(name, cookie, conn, connClosed)
		switch action {
		case ReconnectActionShouldOnlyReopenChannel:
			fmt.Printf("Channel closed, retrynig in %s\n", channelReconnectDelay)
			time.Sleep(channelReconnectDelay)
			continue
		case ReconnectActionShouldReopenConnection:
			return true
		case ReconnectActionShouldQuit:
			return false
		}
	}
}

type ReconnectAction int

const (
	ReconnectActionShouldOnlyReopenChannel ReconnectAction = iota
	ReconnectActionShouldReopenConnection
	ReconnectActionShouldQuit
)

func (client *Client) runAsyncAndRouteErrorToChannel(fn func(ctx context.Context) error, ctx context.Context) {
	go func() {
		err := fn(ctx)
		if err != nil {
			client.errs <- err
		}
	}()
}

func RunClientUntilChannelClosed(name, cookie string, conn *amqp.Connection, connClosed chan *amqp.Error) ReconnectAction {
	client, err := NewClient(conn, name, cookie)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.runAsyncAndRouteErrorToChannel(client.readUserInputLoop, ctx)
	client.runAsyncAndRouteErrorToChannel(client.printChatMsgsLoop, ctx)
	client.runAsyncAndRouteErrorToChannel(client.ReplyToIncomingCookieRequestsLoop, ctx)
	client.runAsyncAndRouteErrorToChannel(client.handleOutgoingCookieRequestsLoop, ctx)

	select {
	case err := <-connClosed:
		if err != nil {
			log.Println(err)
		}
		return ReconnectActionShouldReopenConnection
	case err := <-client.errs:
		log.Fatalln("final err:", err)
		return ReconnectActionShouldQuit
	case <-client.quit:
		return ReconnectActionShouldQuit
	}
}

const DispatchUserInputTimeout = 200 * time.Millisecond

func (client *Client) readUserInputLoop(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	go client.printReturendChatMsgsLoop(ch, ctx)
	scanner := bufio.NewScanner(os.Stdin)
	userInput := ReadAsyncIntoChan(scanner)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case input := <-userInput:
			if input.Err != nil {
				if input.Err == io.EOF {
					client.quit <- struct{}{}
					return nil
				}
				return input.Err
			}

			err := client.dispatchUserInput(input.Val, ch, ctx)
			if err != nil {
				switch err {
				case ErrUnknownCmd:
					fmt.Println("Error: unknown command")
				default:
					return err
				}
			}
		}
	}
}

func (client *Client) dispatchUserInput(input string, ch *amqp.Channel, ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, DispatchUserInputTimeout)
	defer cancel()

	if IsCmd(input) {
		cmd, args := UnserializeStrToCmd(input)
		return client.dispatchCmd(ch, cmd, args, ctx)
	} else {
		return client.sendChatMsg(ch, BindingKeyForGlobalRoom, input, ctx)
	}
}

var ErrWrongNumberOfArgs = errors.New("wrong number of args for command")

func isValidBindingKeyComponent(str string) bool {
	return !strings.ContainsAny(str, ".#*")
}

var ErrInvalidTopicComponent = errors.New("topic components can't contain ., #, *")

func (client *Client) dispatchCmd(ch *amqp.Channel, cmd Cmd, args []string, ctx context.Context) error {
	switch cmd {
	case CmdDeleteUser:
		client.quit <- struct{}{}
		client.delete(ch)
		return nil
	case CmdLogout:
		client.quit <- struct{}{}
		return nil
	case CmdJoinRoom, CmdLeaveRoom:
		return client.dispatchBindCmd(ch, cmd, args)
	case CmdSend, CmdSendRoom, CmdWhisper:
		return client.dispatchSendCmd(ch, cmd, args, ctx)
	case CmdHelp:
		fmt.Println(helpString)
		return nil
	case CmdRequestCookie:
		return client.dispatchRequestCookieCmd(args)
	default:
		return ErrUnknownCmd
	}
}

func (client *Client) dispatchRequestCookieCmd(args []string) error {
	if len(args) != 1 {
		fmt.Println("Error: request_cookie needs 1 arg: USERNAME")
		return nil
	}
	username := args[0]
	if !isValidBindingKeyComponent(username) {
		return ErrInvalidTopicComponent
	}
	client.askForCookie <- username
	return nil // handleOutgoingCookieRequestsLoop does its own printing
}

func (client *Client) delete(ch *amqp.Channel) {
	ch.QueueDelete(client.name,
		false, // ifUnused
		false, // ifEmpty
		false, // noWait
	)
}
func (client *Client) dispatchBindCmd(ch *amqp.Channel, cmd Cmd, args []string) error {
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
		return client.ListenToChatMsgsFrom(ch, BindingKeyForRoom(key))
	case CmdLeaveRoom:
		return client.DontListenToChatMsgsFrom(ch, BindingKeyForRoom(key))
	}
	panic("unreachable")
}

func (client *Client) dispatchSendCmd(ch *amqp.Channel, cmd Cmd, args []string, ctx context.Context) error {
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
		client.ListenToChatMsgsFrom(ch, key)

		body = strings.Join(args[1:], " ")
	}
	return client.sendChatMsg(ch, key, body, ctx)
}

var ErrNoSender = errors.New("msg header doesn't contain a sender")

func (client *Client) printChatMsgsLoop(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	msgs, err := ch.Consume(
		client.receiveMsgsQueue.Name, // queue
		client.name,                  // consumer
		true,                         // auto ack
		true,                         // exclusive
		false,                        // no local
		false,                        // no wait
		nil,                          // args
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := ch.Cancel(client.name, true); err != nil {
			log.Println(err)
		}
	}()

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return ErrChannelClosed
			}
			sender, ok := msg.Headers["sender"]
			if !ok {
				return ErrNoSender
			}
			if sender == client.name {
				continue
			}
			log.Printf("%s (on %s): %s", sender, msg.RoutingKey, msg.Body)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (client *Client) printReturendChatMsgsLoop(ch *amqp.Channel, ctx context.Context) error {
	returnedMsgs := ch.NotifyReturn(make(chan amqp.Return))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-returnedMsgs:
			if !ok {
				return ErrChannelClosed
			}
			if msg.Exchange != msgsExchangeName {
				panic("the only messages send on this channel should be chat messages")
			}
			log.Printf("Couldn't send msg on %s: %s\n", msg.RoutingKey, msg.Body)
		}
	}

}
