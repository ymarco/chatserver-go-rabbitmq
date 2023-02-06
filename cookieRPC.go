package main

import (
	"context"
	"errors"
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (client *Client) GetSenderRPCQueueName() string {
	return client.name + "_cookieRPCSender"
}

func (client *Client) GetReplyToAddress() string {
	return client.name + "_cookieRPCReplyTo"
}
func (client *Client) GetListenerRPCQueueName() string {
	return client.name + "_cookieRPCListener"
}

func (client *Client) ReplyToIncomingCookieRequestsLoop(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	q, err := ch.QueueDeclare(
		client.GetListenerRPCQueueName(), // name
		false,                            // durable
		false,                            // auto-delete
		true,                             // exclusive
		false,                            // no-wait
		nil,                              // args
	)
	if err != nil {
		return err
	}
	defer ch.QueueDelete(q.Name, false, false, false)
	err = ch.QueueBind(
		q.Name,      // queue name
		client.name, // routing key
		cookieExchangeName,
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(q.Name, "",
		true,  // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}
	go printOurRejectedRepliesLoop(ch, ctx)
	return client.replyToCookieRequestsLoop(ch, msgs, ctx)
}
func (client *Client) replyToCookieRequestsLoop(ch *amqp.Channel, msgs <-chan amqp.Delivery, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-msgs:
			if !ok {
				return ErrChannelClosed
			}
			err := client.replyToCookieRPCRequest(ch, delivery, ctx)
			if err != nil {
				return err
			}
		}
	}
}

func printOurRejectedRepliesLoop(ch *amqp.Channel, ctx context.Context) {
	returnedMsgs := ch.NotifyReturn(make(chan amqp.Return, 1))
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-returnedMsgs:
			if !ok {
				return
			}
			log.Printf("Couldn't reply with our cookie to %s\n", msg.ReplyTo)
		}
	}
}

func (client *Client) replyToCookieRPCRequest(ch *amqp.Channel, delivery amqp.Delivery, ctx context.Context) error {
	log.Printf("Replying with our cookie to %s\n", delivery.ReplyTo)
	return ch.PublishWithContext(
		ctx,
		cookieExchangeName,
		delivery.ReplyTo, // routing key
		true,             // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: delivery.CorrelationId,
			Body:          []byte(client.cookie),
		},
	)
}

func (client *Client) handleOutgoingCookieRequestsLoop(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	repliesQueue, err := ch.QueueDeclare(
		client.GetSenderRPCQueueName(), // name
		false,                          // durable
		false,                          // auto-delete
		true,                           // exclusive
		false,                          // no-wait
		nil,                            // args
	)
	if err != nil {
		return err
	}
	defer ch.QueueDelete(repliesQueue.Name, false, false, false)
	err = ch.QueueBind(
		repliesQueue.Name,          // queue name
		client.GetReplyToAddress(), // routing key
		cookieExchangeName,
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		return err
	}
	replies, err := ch.Consume(repliesQueue.Name, "",
		true,  // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}
	go printOurRejectedRequestsLoop(ch, ctx)
	return client.sendCookieRequestsLoop(ch, ctx, replies)
}

func (client *Client) sendCookieRequestsLoop(ch *amqp.Channel, ctx context.Context, replies <-chan amqp.Delivery) error {
	for {
		username := ""
		select {
		case <-ctx.Done():
			return ctx.Err()
		case username = <-client.askForCookie:
		}

		id, err := client.requestCookie(ch, username, ctx)
		if err != nil {
			return err
		}
		cookie, err := client.expectReply(replies, id, ctx)
		if err != nil {
			return err
		}
		log.Printf("%s's cookie is %s\n", username, cookie)
	}
}
func printOurRejectedRequestsLoop(ch *amqp.Channel, ctx context.Context) {
	returnedMsgs := ch.NotifyReturn(make(chan amqp.Return, 1))
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-returnedMsgs:
			if !ok {
				return
			}
			log.Printf("Couldn't request cookie from %s\n", msg.RoutingKey)
		}
	}
}

var globalIdInt int64 = 0

func getGlobalId() string {
	return strconv.FormatInt(globalIdInt, 10)
}

func (client *Client) requestCookie(ch *amqp.Channel, user string, ctx context.Context) (correlationID string, err error) {
	correlationID = getGlobalId()
	err = ch.PublishWithContext(ctx,
		cookieExchangeName, // exchange
		user,               // routing key
		true,               // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: correlationID,
			ReplyTo:       client.GetReplyToAddress(),
			Body:          nil,
		})
	if err != nil {
		return "", err
	}

	return correlationID, nil
}

var ErrChannelClosed = errors.New("channel closed")
var ErrUnexpectedCorrelationId = errors.New("channel closed")

func (client *Client) expectReply(msgs <-chan amqp.Delivery, id string, ctx context.Context) (cookie string, err error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case delivery, ok := <-msgs:
		if !ok {
			return "", ErrChannelClosed
		}
		if delivery.CorrelationId == id {
			return string(delivery.Body), nil
		} else {
			return "", ErrUnexpectedCorrelationId
		}
	}
}
