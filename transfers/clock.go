package transfers

import (
	"sync"
	"time"

	"github.com/kbase/dts/config"
)

// This singleton sends a pulse to transfer-related goroutines at the configured poll interval. It's
// managed entirely by calls to its Subscribe and Unsubscribe methods. DO NOT ATTEMPT TO ACCESS ANY
// OTHER DATA.
var clock clockState

type clockState struct {
	Running        bool
	Mutex          sync.Mutex
	NumSubscribers int
	Pulse          chan struct{}
	Tick           time.Duration
}

// Subscribes the caller to the clock, returning the channel on which the pulse is sent. This
// channel is closed when all the subscribers have unsubscribed.
func (c *clockState) Subscribe() chan struct{} {
	c.Mutex.Lock()
	if !c.Running {
		c.Tick = time.Duration(config.Service.PollInterval) * time.Millisecond
		go c.process()
		c.Running = true
	}
	if c.NumSubscribers == 0 {
		c.Pulse = make(chan struct{}, 32)
	}
	c.NumSubscribers++
	c.Mutex.Unlock()
	return c.Pulse
}

// unsubscribes the caller from the clock
func (c *clockState) Unsubscribe() {
	c.Mutex.Lock()
	c.NumSubscribers--
	if c.NumSubscribers == 0 {
		close(c.Pulse)
	}
	c.Running = false
	c.Mutex.Unlock()
}

//----------------------------------------------------
// everything past here runs in the clock's goroutine
//----------------------------------------------------

func (c *clockState) process() {
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
		for range c.NumSubscribers {
			c.Pulse <- struct{}{}
		}
	}
}
