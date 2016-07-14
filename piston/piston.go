package piston

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
)

var Drained = errors.New("Drained")

type Input struct {
	ID string
	P  Processor
}

type Output struct {
	ID string
	V  interface{}
	E  error
}

type Processor interface {
	Process() (interface{}, error)
}

type Crank struct {
	running atomic.Value
	W       int
	in      chan Input
	out     chan Output
	done    chan struct{}
	drained chan struct{}
}

const defaultGoRoutines = 20

func (c *Crank) Start() error {
	if c.isRunning() {
		return errors.New("Crank: already running")
	}
	c.running.Store(true)
	log.Println("Crank: running")

	c.in = make(chan Input)
	c.out = make(chan Output)
	c.done = make(chan struct{})
	c.drained = make(chan struct{})

	if c.W < 1 {
		c.W = defaultGoRoutines
	}

	var wg sync.WaitGroup
	wg.Add(c.W)
	for w := 0; w < c.W; w++ {
		go func() {
			digest(c.done, c.in, c.out)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		log.Println("Crank: digest finished")
		close(c.out)
		close(c.drained)
	}()

	return nil
}

// Close should be called by the client when they are ready to
// stop sending messages. Further calls to Push will result in an error
// until the crank has been started again.
func (c *Crank) Close() {
	if !c.isRunning() {
		log.Println("Close: Crank not running")
		return
	}
	log.Println("Crank: closing")
	close(c.in)
	log.Println("Crank: closed")
	<-c.drained
	close(c.done)
	c.running.Store(false)
}

func (c *Crank) Halt() {
	if !c.isRunning() {
		log.Println("Halt: Crank not running")
		return
	}
	log.Println("Crank: halting")
	close(c.done)
	log.Println("Crank: stopped")
	<-c.drained
	c.running.Store(false)
}

func (c *Crank) Push(i Input) error {
	if !c.isRunning() {
		return errors.New("Push: Crank not running")
	}
	select {
	case <-c.done:
		return errors.New("Push: Crank has been stopped")
	case c.in <- i:
		return nil
	}
}

func (c *Crank) Pull() (Output, error) {
	select {
	case <-c.done:
		return Output{}, errors.New("Pull: Crank has been stopped")
	case o, ok := <-c.out:
		if !ok {
			return Output{}, Drained
		} else {
			return o, nil
		}
	}
}

func digest(done <-chan struct{}, in <-chan Input, out chan<- Output) {
	for {
		select {
		case <-done:
			return
		case i, ok := <-in:
			if !ok {
				return
			} else {
				v, err := i.P.Process()
				select {
				case out <- Output{i.ID, v, err}:
				case <-done:
					return
				}
			}
		}
	}
}

func (c *Crank) isRunning() bool {
	v := c.running.Load()
	if running, found := v.(bool); !found {
		return false
	} else {
		return running
	}
}
