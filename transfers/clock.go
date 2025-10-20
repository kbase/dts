package transfers

import (
	"sync"
	"time"

	"github.com/kbase/dts/config"
)

// this singleton sends a pulse to transfer-related goroutines at the configured poll interval
var clock clockType

type clockType struct {
	Initialized    bool
	Mutex          sync.Mutex
	NumSubscribers int
	Pulses         []chan struct{}
	Tick           time.Duration
}

// subscribes the caller to the clock, returning a new channel on which the pulse is sent
func (c *clockType) Subscribe() chan struct{} {
	c.Mutex.Lock()
	c.NumSubscribers++
	if len(c.Pulses) < c.NumSubscribers {
		c.Pulses = append(c.Pulses, make(chan struct{}))
	} else {
		c.Pulses[c.NumSubscribers-1] = make(chan struct{})
	}
	if !c.Initialized {
		c.Tick = time.Duration(config.Service.PollInterval) * time.Millisecond
		c.Initialized = true
		go c.process()
	}
	c.Mutex.Unlock()
	return c.Pulses[len(c.Pulses)-1]
}

// unsubscribes the caller from the clock
func (c *clockType) Unsubscribe() {
	c.Mutex.Lock()
	c.NumSubscribers--
	close(c.Pulses[c.NumSubscribers])
	c.Mutex.Unlock()
}

func (c *clockType) process() {
	c.Mutex.Lock()
	tick := c.Tick
	c.Mutex.Unlock()

	for {
		time.Sleep(tick)
		c.Mutex.Lock()
		if c.NumSubscribers == 0 {
			c.Mutex.Unlock()
			break
		}
		c.Mutex.Unlock()
		for i := range c.NumSubscribers {
			c.Pulses[i] <- struct{}{}
		}
	}
}
