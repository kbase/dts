package transfers

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// This small publish/subscribe subsystem allows other DTS packages to subscribe to a feed that
// publishes messages related to the transfer of files and metadata.

// A message sent to a subscriber relating to a transfer
type Message struct {
	Description    string
	TransferId     uuid.UUID
	TransferStatus TransferStatus
	Time           time.Time
}

// A subscription that identifies the subscriber and provides it with a channel on which to receive
// messages
type Subscription struct {
	Id      int
	Channel <-chan Message
}

// Subscribes the caller to the transfer message feed, returning a channel with the given buffer
// size on which messages are queued.
func Subscribe(bufferSize int) Subscription {
	messageBroker_.Mutex.Lock()
	subscriberId := len(messageBroker_.Subscriptions)
	messageBroker_.Subscriptions = append(messageBroker_.Subscriptions, make(chan Message, bufferSize))
	sub := Subscription{
		Id:      subscriberId,
		Channel: messageBroker_.Subscriptions[subscriberId],
	}
	messageBroker_.Mutex.Unlock()
	return sub
}

// Unsubscribes the caller from its messages.
func Unsubscribe(sub Subscription) error {
	messageBroker_.Mutex.Lock()
	if sub.Id < 0 || sub.Id >= len(messageBroker_.Subscriptions) {
		return fmt.Errorf("invalid subscription ID: %d", sub.Id)
	}
	close(messageBroker_.Subscriptions[sub.Id])
	messageBroker_.Subscriptions = slices.Delete(messageBroker_.Subscriptions, sub.Id, sub.Id)
	messageBroker_.Mutex.Unlock()
	return nil
}

// Publishes the given message to all subscribers.
func publish(message Message) error {
	if message.TransferStatus.Code < TransferStatusUnknown || message.TransferStatus.Code > TransferStatusFailed {
		return errors.New("invalid transfer status for message")
	}
	messageBroker_.Mutex.Lock()
	for _, channel := range messageBroker_.Subscriptions {
		channel <- message
	}
	messageBroker_.Mutex.Unlock()
	return nil
}

// Unsubscribes all subscribers.
func unsubscribeAll() error {
	messageBroker_.Mutex.Lock()
	for _, channel := range messageBroker_.Subscriptions {
		close(channel)
	}
	messageBroker_.Subscriptions = nil
	messageBroker_.Mutex.Unlock()
	return nil
}

var messageBroker_ struct {
	Mutex         sync.Mutex
	Subscriptions []chan Message
}
