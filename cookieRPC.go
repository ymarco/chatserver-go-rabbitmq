package main

import (
	"context"
	"errors"
	"log"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (client *Client) ReplyToAddress() string {
	return client.name + "_replyToAddress"
}

func (client *Client) RPCListenerQueueName() string {
	return client.name + "_cookieRPCListener"
}

func (client *Client) getIncomingMsgs(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare(
		client.RPCListenerQueueName(), // name
		false,                         // durable
		false,                         // auto-delete
		true,                          // exclusive
		false,                         // no-wait
		nil,                           // args
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(q.Name, "",
		true,  // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

func (client *Client) ReplyToIncomingCookieRequests(ctx context.Context) error {
	ch, err := client.conn.Channel()
	if err != nil {
		return err
	}
	defer ClosePrintErr(ch)

	msgs, err := client.getIncomingMsgs(ch)
	if err != nil {
		return err
	}
	return client.replyToCookieRequestsFromChan(ch, msgs, ctx)
}
func (client *Client) replyToCookieRequestsFromChan(ch *amqp.Channel, msgs <-chan amqp.Delivery, ctx context.Context) error {
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
		delivery.Exchange,
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

func (client *Client) requestCookie(ch *amqp.Channel, targetUsername string, rpcChannels ResultOfCookieRequest, ctx context.Context) error {
	id, err := client.sendCookieRequest(ch, targetUsername, ctx)
	if err != nil {
		return err
	}
	cookie, err := client.expectReply(id, rpcChannels, ctx)
	if err != nil {
		if err == ErrMsgWasReturned {
			log.Println(err)
			return nil
		}
		return err
	}
	log.Printf("%s's cookie is %s\n", targetUsername, cookie)
	return nil
}

var globalIdInt int64 = 0

func GenerateRequestId() string {
	globalIdInt++
	return strconv.FormatInt(globalIdInt, 10)
}

func (client *Client) sendCookieRequest(ch *amqp.Channel, user string, ctx context.Context) (correlationID string, err error) {
	correlationID = GenerateRequestId()
	err = ch.PublishWithContext(ctx,
		amqp.DefaultExchange,
		(&Client{name: user}).RPCListenerQueueName(), // routing key
		true,                                         // mandatory
		false,                                        // immediate
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

func (client *Client) expectReply(id string, rpcChannels ResultOfCookieRequest, ctx context.Context) (cookie string, err error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case returnedMsg, ok := <-rpcChannels.rejectedRequests:
			if !ok {
				return "", ErrChannelClosed
			}
			if returnedMsg.CorrelationId == id {
				return "", ErrMsgWasReturned
			}
		case delivery, ok := <-rpcChannels.replies:
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
