package main

import (
	"context"
	"errors"
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (client *Client) RepliesQueueName() string {
	return client.name + "_cookieRPCSender"
}

func (client *Client) ReplyToAddress() string {
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
	defer func() {
		_, err := ch.QueueDelete(q.Name, false, false, false)
		if err != nil {
			log.Println(err)
		}
	}()
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
	return client.replyToCookieRequestsLoop(ch, msgs, ctx)
}
func (client *Client) replyToCookieRequestsLoop(ch *amqp.Channel, msgs <-chan amqp.Delivery, ctx context.Context) error {
	returnedMsgs := ch.NotifyReturn(make(chan amqp.Return, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-msgs:
			if !ok {
				return ErrChannelClosed
			}
			err := client.replyToCookieRPCRequest(ch, delivery, ctx)
			if err != nil {
				return err
			}
		case msg, ok := <-returnedMsgs:
			if !ok {
				return ErrChannelClosed
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
		client.RepliesQueueName(), // name
		false,                     // durable
		false,                     // auto-delete
		true,                      // exclusive
		false,                     // no-wait
		nil,                       // args
	)
	if err != nil {
		return err
	}
	defer func() {
		_, err := ch.QueueDelete(repliesQueue.Name, false, false, false)
		if err != nil {
			log.Println("couldn't close replies queue", err)
		}
	}()
	err = ch.QueueBind(
		repliesQueue.Name,       // queue name
		client.ReplyToAddress(), // routing key
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
	return client.sendCookieRequestsLoop(ch, ctx, replies)
}

func (client *Client) sendCookieRequestsLoop(ch *amqp.Channel, ctx context.Context, replies <-chan amqp.Delivery) error {
	returnedMsgs := ch.NotifyReturn(make(chan amqp.Return, 1))
	for {
		targetUsername := ""
		select {
		case <-ctx.Done():
			return nil
		case targetUsername = <-client.requestACookieFromUsername:
		}

		id, err := client.requestCookie(ch, targetUsername, ctx)
		if err != nil {
			return err
		}
		cookie, err := client.expectReply(replies, returnedMsgs, id, ctx)
		if err != nil {
			if err == ErrMsgWasReturned {
				log.Println(err)
				continue
			}
			return err
		}
		log.Printf("%s's cookie is %s\n", targetUsername, cookie)
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
			ReplyTo:       client.ReplyToAddress(),
			Body:          nil,
		})
	if err != nil {
		return "", err
	}

	return correlationID, nil
}

var ErrChannelClosed = errors.New("channel closed")
var ErrMsgWasReturned = errors.New("message didn't find a destination and was returned")

func (client *Client) expectReply(msgs <-chan amqp.Delivery, returnedMsgs <-chan amqp.Return, id string, ctx context.Context) (cookie string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case returnedMsg, ok := <-returnedMsgs:
			if !ok {
				return "", ErrChannelClosed
			}
			if returnedMsg.CorrelationId == id {
				return "", ErrMsgWasReturned
			} else {
				continue
			}
		case delivery, ok := <-msgs:
			if !ok {
				return "", ErrChannelClosed
			}
			if delivery.CorrelationId == id {
				return string(delivery.Body), nil
			} else {
				continue // leftover message, should not error (see rmq docs)
			}
		}
	}
}
