package minimsg

import (
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

// MiniMessage defines the interface for connecting to broker & consuming messages.
type MiniMessage interface {
	ConnectToBroker(AmqpConfig *AmqpConfig) error
	PublishToQueue(QueueName string, body []byte) error
	SubscribeToQueue(QueueName string, ConsumerName string, out chan amqp.Delivery) error
	CloseChannel()
	Close()
}

// MiniMessage implementation contains pointer to the amqp connection
type AmqpClient struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	consumerName string
}

// Configuration struct for connecting to broker
type AmqpConfig struct {
	Username string
	Password string
	Host     string
	Port     int
}

// ConnectToBroker takes an AmqpConfig, initializes a connection to the broker, and returns nil if successful.
func (a *AmqpClient) ConnectToBroker(config *AmqpConfig) error {
	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%d/", config.Username, config.Password, config.Host, config.Port)
	var err error
	a.conn, err = amqp.Dial(connectionString)
	if err != nil {
		return fmt.Errorf("Cannot connect to broker: %s", err.Error())
	}
	return nil
}

// PublishToQueue publishes a message to a Queue
func (a *AmqpClient) PublishToQueue(QueueName string, body []byte) error {
	if a.conn == nil {
		return errors.New("Cannot publish message to queue, connection is not initialized.")
	}
	channel, err := a.conn.Channel()
	if err != nil {
		return fmt.Errorf("Error getting channel: %v", err)
	}
	defer channel.Close()

	err = channel.Publish(
		"",        // exchange
		QueueName, // routing key
		false,     // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		})
	if err != nil {
		return fmt.Errorf("Error publishing message: %v", err)
	}
	return nil

}

// SubscribeToQueue takes a QueueName, Consumer name and a handler function that handles incoming messages from the AMQP broker.
func (a *AmqpClient) SubscribeToQueue(QueueName string, ConsumerName string, out chan amqp.Delivery) error {
	a.consumerName = ConsumerName
	var err error
	a.channel, err = a.conn.Channel()
	if err != nil {
		return fmt.Errorf("Error getting channel: %v", err)
	}
	queue, err := a.channel.QueueDeclare(
		QueueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	messages, err := a.channel.Consume(
		queue.Name,   // queue
		ConsumerName, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return err
	}
	go doConsume(messages, out)
	return nil
}

// Close closes the connection to the AMQP broker
func (a *AmqpClient) Close() {
	if a.conn != nil {
		a.conn.Close()
	}
}
func (a *AmqpClient) CloseChannel() {
	if a.channel != nil {
		a.channel.Close()
	}
}
func doConsume(d <-chan amqp.Delivery, out chan amqp.Delivery) {
	for msg := range d {
		out <- msg
	}
}
