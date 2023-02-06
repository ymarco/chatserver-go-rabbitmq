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

func (client *Client) GetListenerRPCQueueName() string {
	return client.name + "_cookieRPCListener"
}

func (client *Client) getRPCListenerBindingKey() string {
	return client.name + "_cookieRPCListener"
}

func (client *Client) handleIncomingCookieRequestsLoop(ch *amqp.Channel, ctx context.Context) {
	q, err := ch.QueueDeclare(
		client.GetListenerRPCQueueName(), // name
		false,                            // durable
		false,                            // auto-delete
		true,                             // exclusive
		false,                            // no-wait
		nil,                              // args
	)
	if err != nil {
		client.errs <- err
		return
	}
	defer ch.QueueDelete(q.Name, false, false, false)
	err = ch.QueueBind(
		q.Name,                            // queue name
		client.getRPCListenerBindingKey(), // routing key
		cookieExchangeName,
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		client.errs <- err
		return
	}
	msgs, err := ch.Consume(q.Name, "",
		true,  // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		client.errs <- err
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-msgs:
			if !ok {
				return
			}
			err := client.replyToCookieRPCRequest(ch, delivery, ctx)
			if err != nil {
				client.errs <- err
				return
			}
		}
	}
}

func (client *Client) replyToCookieRPCRequest(ch *amqp.Channel, delivery amqp.Delivery, ctx context.Context) error {
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

func (client *Client) handleOutgoingCookieRequestsLoop(ch *amqp.Channel, ctx context.Context) {
	q, err := ch.QueueDeclare(
		client.GetSenderRPCQueueName(), // name
		false,                          // durable
		false,                          // auto-delete
		true,                           // exclusive
		false,                          // no-wait
		nil,                            // args
	)
	if err != nil {
		client.errs <- err
		return
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
		client.errs <- err
		return
	}
	msgs, err := ch.Consume(q.Name, "",
		true,  // auto-ack
		true,  // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		client.errs <- err
		return
	}

	for {
		username := ""
		select {
		case <-ctx.Done():
			return
		case username = <-client.askForCookie:
		}
		id, err := client.requestCookie(ch, username, ctx)
		if err != nil {
			client.errs <- err
			return
		}
		cookie, err := client.expectResponse(msgs, id, ctx)
		if err != nil {
			client.errs <- err
			return
		}
		log.Printf("%s's cookie is %s\n", username, cookie)
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
			ReplyTo:       client.name,
			Body:          nil,
		})
	if err != nil {
		return "", err
	}

	return correlationID, nil
}

var ErrChannelClosed = errors.New("channel closed")
var ErrUnexpectedCorrelationId = errors.New("channel closed")

func (client *Client) expectResponse(msgs <-chan amqp.Delivery, id string, ctx context.Context) (cookie string, err error) {
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
