package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/austo/maw/model"
	"github.com/austo/maw/worker"
)

/* What if you made this a complete piece of software?
 * With:
 * Multiple inputs: ftp, http, gRCP
 * Clean startup/shutdown service for the processing of Bar objects
 * Structured logging with exposed metrics
 *	(n<4 logging channels - system data, metrics, errors)
 * Swagger HTTP API documentation
 *
 */

func main() {
	log.Println("Starting main routine...")

	var pool worker.Pool
	if err := pool.Open(); err != nil {
		log.Fatal(err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT)
	var wg sync.WaitGroup
	go func() {
		s := <-interrupt
		wg.Add(1)
		defer wg.Done()
		log.Printf("Recieved %v signal. Killing pool...\n", s)
		pool.Kill()
	}()

	go func() {
		defer pool.Close()
		for v := range fetchItems() {
			if id, err := pool.Sub(v); err != nil {
				log.Println(err)
				return
			} else {
				log.Printf("Submitted %s\n", id)
			}
			// time.Sleep(100 * time.Millisecond)
		}
	}()

	for {
		if o, err := pool.Recv(); err != nil {
			if err != worker.Drained {
				log.Println(err)
			}
			break
		} else {
			fmt.Printf("%v\n", o)
		}
	}
	wg.Wait()
	// panic("Show me the stacks")
}

// This code is so bad - do I do anything that's not quick and dirty?

func fetchItems() <-chan model.Bar {
	c := make(chan model.Bar)

	go func() {
		defer close(c)

		var t int
		fmt.Scanf("%d\n", &t)

		r := bufio.NewReader(os.Stdin)

		for i := 0; i < t; i++ {
			b := model.Bar{}
			nk, _, _ := r.ReadLine()
			p := strings.Split(string(nk), " ")
			n, _ := strconv.Atoi(p[0])
			k, _ := strconv.Atoi(p[1])
			b.N = n
			b.K = k

			for j := 0; j < b.N; j++ {
				line, _, _ := r.ReadLine()
				pp := strings.Split(string(line), " ")
				var a []int

				for jj, s := range pp {
					if jj == 0 {
						continue
					}
					x, _ := strconv.Atoi(s)
					a = append(a, x)
				}
				b.A = append(b.A, a)
			}
			b.Sort()
			for j := 0; j < 100000; j++ {
				c <- b
			}
		}
	}()
	return c
}
