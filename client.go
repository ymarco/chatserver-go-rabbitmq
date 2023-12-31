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
	conn             *amqp.Connection
	receiveChatMsgsQ amqp.Queue

	repliesToOurCookieRequestQ amqp.Queue

	errs chan error
	quit chan struct{}
}

func NewClient(conn *amqp.Connection, name, cookie string) (*Client, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	defer ClosePrintErr(ch)
	err = ch.ExchangeDeclare(
		msgsExchangeName,
		amqp.ExchangeTopic, // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return nil, err
	}

	receiveChatMsgs, err := ch.QueueDeclare(
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

	repliesToOurCookieRequestQ, err := ch.QueueDeclare(
		(&Client{name: name}).ReplyToAddress(), false, false, true, false, nil)

	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	// we want a large enough buffer so after one error was sent, and we stop
	// pulling from errs, other routines that push to errs won't hang
	errs := make(chan error, 64)

	client := &Client{name, cookie, conn,
		receiveChatMsgs, repliesToOurCookieRequestQ, errs, make(chan struct{}, 1)}
	err = client.ListenToChatMsgsFrom(ch, BindingKeyForGlobalRoom)
	if err != nil {
		return nil, err
	}
	err = client.ListenToChatMsgsFrom(ch, BindingKeyForPrivateMsg(client.name))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (client *Client) ListenToChatMsgsFrom(ch *amqp.Channel, key BindingKey) error {
	log.Printf("Bound to %s\n", key)
	return ch.QueueBind(
		client.receiveChatMsgsQ.Name, // queue name
		string(key),                  // routing key
		msgsExchangeName,
		false,
		nil)
}

func (client *Client) StopListeningToChatMsgsFrom(ch *amqp.Channel, key BindingKey) error {
	log.Printf("Unbound from %s\n", key)
	return ch.QueueUnbind(client.receiveChatMsgsQ.Name, string(key), msgsExchangeName, nil)
}

const SenderHeaderName = "sender"

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
			Headers:     map[string]interface{}{SenderHeaderName: client.name},
		})
}

const channelReconnectDelay = 1 * time.Second
const connectionReconnectDelay = 5 * time.Second

var stdinChan <-chan ReadInput

func RunClient(name, cookie string) {
	stdinChan = ReadAsyncIntoChan(bufio.NewScanner(os.Stdin))
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
		action := RunClientOnConnection(name, cookie, conn, connClosed)
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

func (client *Client) runAsyncAndRouteError(fn func(ctx context.Context) error, ctx context.Context) {
	go func() {
		err := fn(ctx)
		if err != nil {
			client.errs <- err
		}
	}()
}

func RunClientOnConnection(name, cookie string, conn *amqp.Connection, connClosed chan *amqp.Error) ReconnectAction {
	client, err := NewClient(conn, name, cookie)
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.runAsyncAndRouteError(client.executeIncomingUserInput, ctx)
	client.runAsyncAndRouteError(client.printIncomingChatMsgs, ctx)
	client.runAsyncAndRouteError(client.ReplyToIncomingCookieRequests, ctx)

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

type ResultOfCookieRequest struct {
	replies          <-chan amqp.Delivery
	rejectedRequests <-chan amqp.Return
}

func (client *Client) executeIncomingUserInput(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	RPCReplies, err := ch.Consume(client.repliesToOurCookieRequestQ.Name, client.repliesToOurCookieRequestQ.Name, true, true, false, false, nil)
	if err != nil {
		return err
	}

	returnedMsgs := ch.NotifyReturn(make(chan amqp.Return))

	for {
		select {
		case <-ctx.Done():
			return nil
		case input := <-stdinChan:
			if input.Err != nil {
				if input.Err == io.EOF {
					client.quit <- struct{}{}
					return nil
				}
				return input.Err
			}

			err := client.dispatchUserInput(input.Val, ch, ResultOfCookieRequest{RPCReplies, returnedMsgs}, ctx)
			if err != nil {
				switch err {
				case ErrUnknownCmd:
					fmt.Println("Error: unknown command")
				default:
					return err
				}
			}
		case msg, ok := <-returnedMsgs:
			if !ok {
				return ErrChannelClosed
			}
			if msg.Exchange != msgsExchangeName {
				panic("unreachable: the only messages received here should be chat messages")
			}
			log.Printf("Couldn't send msg on %s: %s\n", msg.RoutingKey, msg.Body)
		}
	}
}

func (client *Client) dispatchUserInput(input string, ch *amqp.Channel, rpcChannels ResultOfCookieRequest, ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, DispatchUserInputTimeout)
	defer cancel()

	if IsCmd(input) {
		cmd, args := DeserializeStrToCmd(input)
		return client.dispatchCmd(ch, cmd, args, rpcChannels, ctx)
	} else {
		return client.sendChatMsg(ch, BindingKeyForGlobalRoom, input, ctx)
	}
}

var ErrWrongNumberOfArgs = errors.New("wrong number of args for command")

func isValidBindingKeyComponent(str string) bool {
	return !strings.ContainsAny(str, ".#*")
}

var ErrInvalidTopicComponent = errors.New("topic components can't contain ., #, *")

func (client *Client) dispatchCmd(ch *amqp.Channel, cmd Cmd, args []string, rpcChannels ResultOfCookieRequest, ctx context.Context) error {
	switch cmd {
	case CmdDeleteUser:
		client.quit <- struct{}{}
		return client.delete(ch)
	case CmdLogout:
		client.quit <- struct{}{}
		return nil
	case CmdJoinRoom, CmdLeaveRoom:
		return client.dispatchRoomJoinOrLeaveCmd(ch, cmd, args)
	case CmdSend, CmdSendRoom, CmdWhisper:
		return client.dispatchSendCmd(ch, cmd, args, ctx)
	case CmdHelp:
		fmt.Println(helpString)
		return nil
	case CmdRequestCookie:
		return client.dispatchRequestCookieCmd(ch, args, rpcChannels, ctx)
	default:
		return ErrUnknownCmd
	}
}

func (client *Client) dispatchRequestCookieCmd(ch *amqp.Channel, args []string, rpcChannels ResultOfCookieRequest, ctx context.Context) error {
	if len(args) != 1 {
		fmt.Println("Error: request_cookie needs 1 arg: USERNAME")
		return nil
	}
	username := args[0]
	if !isValidBindingKeyComponent(username) {
		return ErrInvalidTopicComponent
	}
	return client.requestCookie(ch, username, rpcChannels, ctx)
}

func (client *Client) delete(ch *amqp.Channel) error {
	_, err := ch.QueueDelete(client.name,
		false, // ifUnused
		false, // ifEmpty
		false, // noWait
	)
	return err
}
func (client *Client) dispatchRoomJoinOrLeaveCmd(ch *amqp.Channel, cmd Cmd, args []string) error {
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
		return client.StopListeningToChatMsgsFrom(ch, BindingKeyForRoom(key))
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
		err := client.ListenToChatMsgsFrom(ch, key)
		if err != nil {
			return err
		}

		body = strings.Join(args[1:], " ")
	}
	return client.sendChatMsg(ch, key, body, ctx)
}

var ErrNoSender = errors.New("msg header doesn't contain a sender")

func (client *Client) printIncomingChatMsgs(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	msgs, err := ch.Consume(
		client.receiveChatMsgsQ.Name, // queue
		client.name,                  // consumer
		true,                         // auto ack
		true,                         // exclusive
		false,                        // this flag is unsupported
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
			sender, ok := msg.Headers[SenderHeaderName]
			if !ok {
				return ErrNoSender
			}
			if sender == client.name {
				continue
			}
			log.Printf("%s (on %s): %s", sender, msg.RoutingKey, msg.Body)
		case <-ctx.Done():
			return nil
		}
	}
}
