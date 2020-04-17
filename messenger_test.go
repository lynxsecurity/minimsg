package minimsg

import (
	"testing"

	"github.com/streadway/amqp"
)

func TestMessenger(t *testing.T) {
	m := &AmqpClient{}
	config := AmqpConfig{Username: "", Password: "", Host: "rabbitmq.example.com", Port: 5672}
	err := m.ConnectToBroker(&config)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"Domain":"http://www.example.com"}`)
	err = m.PublishToQueue("theQueue", body)
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan amqp.Delivery)
	err = m.SubscribeToQueue("theQueue", "test_consumer", out)
	if err != nil {
		t.Fatal(err)
	}
	m.Close()
}
