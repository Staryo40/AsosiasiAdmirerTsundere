//go:build integration

package api

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func testBroker(t *testing.T) (*amqp.Channel, *url.URL) {
	t.Helper()
	address := os.Getenv("BNPB_TEST_AMQP_URL")
	if address == "" {
		t.Skip("set BNPB_TEST_AMQP_URL to run broker integration tests")
	}
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return channel, parsed
}

// Runs against identities created by broker/provision.sh, proving its permissions are enough and no wider.
func TestProvisionedIdentitiesCanOnlyPublishAndConsumeFieldReports(t *testing.T) {
	apiAddress, intakeAddress := os.Getenv("BNPB_TEST_API_AMQP_URL"), os.Getenv("BNPB_TEST_INTAKE_AMQP_URL")
	if apiAddress == "" || intakeAddress == "" {
		t.Skip("set BNPB_TEST_API_AMQP_URL and BNPB_TEST_INTAKE_AMQP_URL to provisioned identities")
	}
	apiURL, _ := url.Parse(apiAddress)
	password, _ := apiURL.User.Password()
	publisher := newBrokerPublisher(apiURL.User.Username(), password, apiURL.Host)
	t.Cleanup(publisher.Close)
	ctx, cancel := context.WithTimeout(context.Background(), fieldReportPublishBy)
	defer cancel()
	if err := publisher.Publish(ctx, amqp.Publishing{DeliveryMode: amqp.Persistent, MessageId: "fr-provisioned", Body: []byte(`{"probe":true}`)}); err != nil {
		t.Fatalf("the API identity must be able to publish: %v", err)
	}

	connection, err := amqp.Dial(intakeAddress)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channel.QueueDeclare(fieldReportQueue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("field-intake must be able to declare its queue: %v", err)
	}
	deliveries, err := channel.Consume(fieldReportQueue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case delivery := <-deliveries:
		if delivery.MessageId != "fr-provisioned" {
			t.Fatalf("unexpected message %q", delivery.MessageId)
		}
		_ = delivery.Ack(false)
	case <-time.After(5 * time.Second):
		t.Fatal("field-intake did not receive the report")
	}

	other, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.QueueDeclare("hazard.events.dashboard", true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err == nil {
		t.Fatal("field-intake must not touch hazard event queues")
	}
}

func TestBrokerPublisherConfirmsReportsIntoTheQueue(t *testing.T) {
	admin, broker := testBroker(t)
	_, _ = admin.QueueDelete(fieldReportQueue, false, false, false)
	if _, err := admin.QueueDeclare(fieldReportQueue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatal(err)
	}
	password, _ := broker.User.Password()
	publisher := newBrokerPublisher(broker.User.Username(), password, broker.Host)
	t.Cleanup(publisher.Close)

	var group sync.WaitGroup
	errs := make(chan error, 20)
	for index := range 20 {
		group.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), fieldReportPublishBy)
			defer cancel()
			errs <- publisher.Publish(ctx, amqp.Publishing{DeliveryMode: amqp.Persistent, MessageId: "fr-" + strconv.Itoa(index), Body: []byte("{}")})
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("confirmed publish failed: %v", err)
		}
	}
	queue, err := admin.QueueDeclarePassive(fieldReportQueue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	if err != nil || queue.Messages != 20 {
		t.Fatalf("queue holds %d messages, err=%v", queue.Messages, err)
	}

	// Without the queue a mandatory publish comes back unrouted, so the API cannot claim 202.
	if _, err := admin.QueueDelete(fieldReportQueue, false, false, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), fieldReportPublishBy)
	defer cancel()
	if err := publisher.Publish(ctx, amqp.Publishing{Body: []byte("{}")}); err == nil {
		t.Fatal("publishing to a missing queue must fail")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := newBrokerPublisher("nobody", "wrong", broker.Host).Publish(ctx, amqp.Publishing{Body: []byte("{}")}); err == nil {
		t.Fatal("publishing with bad credentials must fail")
	}
}
