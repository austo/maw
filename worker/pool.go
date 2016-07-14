package worker

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
)

var Drained = errors.New("Drained")

type Job interface {
	Process() (interface{}, error)
}

type task struct {
	id  string
	job Job
}

type Result struct {
	ID string
	V  interface{}
	E  error
}

type Pool struct {
	running atomic.Value
	W       int
	lastId  int32
	in      chan task
	out     chan Result
	done    chan struct{}
	drained chan struct{}
}

const defaultGoRoutines = 20

func (c *Pool) Open() error {
	if c.isRunning() {
		return errors.New("Pool: already running")
	}
	c.running.Store(true)
	// log.Println("Pool: running")

	c.in = make(chan task)
	c.out = make(chan Result)
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
		// log.Println("Pool: digest finished")
		close(c.out)
		close(c.drained)
	}()

	return nil
}

// Close should be called by the client when they are ready to
// stop sending messages. Further calls to Sub will result in an error
// until the Pool has been opened again. Both Close and Kill will block
// until the Pool has drained its existing work.
func (c *Pool) Close() {
	if !c.isRunning() {
		// log.Println("Close: Pool not running")
		return
	}
	c.running.Store(false)
	// log.Println("Pool: closing")
	close(c.in)
	// log.Println("Pool: closed")
	<-c.drained
	close(c.done)
}

// Kill can be called to forcibly stop the Pool.
// Existing calls to Sub will be processed but new ones
// will result in an error.
func (c *Pool) Kill() {
	if !c.isRunning() {
		// log.Println("Kill: Pool not running")
		return
	}
	c.running.Store(false)
	// log.Println("Pool: killing workers")
	close(c.done)
	// log.Println("Pool: draining")
	<-c.drained
	// log.Println("Pool: killed")
}

func (c *Pool) Sub(j Job) (string, error) {
	if !c.isRunning() {
		return "", errors.New("Sub: Pool closed")
	}
	id := c.nextId()
	select {
	case <-c.done:
		return "", errors.New("Sub: Pool has been closed")
	case c.in <- task{id, j}:
		return id, nil
	}
}

func (c *Pool) Recv() (Result, error) {
	select {
	case o, ok := <-c.out:
		if !ok {
			return Result{}, Drained
		} else {
			return o, nil
		}
	}
}

func digest(done <-chan struct{}, in <-chan task, out chan<- Result) {
	for {
		select {
		case <-done:
			return
		case t, ok := <-in:
			if !ok {
				return
			} else {
				v, err := t.job.Process()
				select {
				case out <- Result{t.id, v, err}:
				case <-done:
					return
				}
			}
		}
	}
}

func (c *Pool) isRunning() bool {
	v := c.running.Load()
	if running, found := v.(bool); !found {
		return false
	} else {
		return running
	}
}

func (c *Pool) nextId() string {
	id := atomic.AddInt32(&(c.lastId), 1)
	return strconv.Itoa(int(id))
}
