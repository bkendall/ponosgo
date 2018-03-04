package ponos

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"reflect"
)

type Server struct {
	connection          *amqp.Connection
	channel             *amqp.Channel
	done                chan error
	handlersByQueueName map[string]func(string)
}

func NewServer(amqpURI string, tasks map[string]func(string)) (*Server, error) {
	server := &Server{
		connection:          nil,
		channel:             nil,
		done:                make(chan error),
		handlersByQueueName: tasks,
	}

	var err error

	log.Printf("connecting %q", amqpURI)
	server.connection, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	log.Printf("creating channel")
	server.channel, err = server.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("creating queues")
	for queueName, _ := range server.handlersByQueueName {
		log.Printf("creating queue: %s", queueName)
		_, err := server.channel.QueueDeclare(
			queueName, // name of the queue
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // noWait
			nil,       // arguments
		)
		if err != nil {
			return nil, fmt.Errorf("Queue Declare: %s", err)
		}
	}

	return server, nil
}

func (server Server) Consume() error {
	var channels []chan error

	log.Printf("setting up %d handlers", len(server.handlersByQueueName))
	for queueName, handler := range server.handlersByQueueName {
		log.Printf("hander setup: %s", queueName)
		deliveries, err := server.channel.Consume(
			queueName,           // name
			"ponos::"+queueName, // consumerTag
			false, // noAck
			false, // exclusive
			false, // noLocal
			false, // noWait
			nil,   // arguments
		)
		if err != nil {
			return fmt.Errorf("Consume %s: %s", queueName, err)
		}

		channelClose := make(chan error)
		channels = append(channels, channelClose)
		go handle(queueName, deliveries, handler, channelClose)
	}

	log.Printf("channels stuff length: %d", len(channels))
	cases := make([]reflect.SelectCase, len(channels))
	for i, channelDone := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(channelDone),
		}
	}
	go func() {
		reflect.Select(cases)
		server.done <- nil
	}()

	log.Printf("Consuming started?")
	return nil
}

func (server Server) Shutdown() error {
	for queueName, _ := range server.handlersByQueueName {
		if err := server.channel.Cancel("ponos-go::"+queueName, true); err != nil {
			return fmt.Errorf("Consumer cancel failed: %s", err)
		}
	}

	if err := server.connection.Close(); err != nil {
		return fmt.Errorf("Connection close failed: %s", err)
	}

	defer log.Printf("AMQP shutdown OK.")

	return <-server.done
}

func handle(
	queueName string,
	deliveries <-chan amqp.Delivery,
	handler func(string),
	done chan error) {
	for d := range deliveries {
		log.Printf("got %dB delivery: [%v] %q", len(d.Body), d.DeliveryTag, d.Body)
		handler(string(d.Body))
		d.Ack(false)
	}
	log.Printf("handle: %s delveries channel closed :(", queueName)
	done <- nil
}
