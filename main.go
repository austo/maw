package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/austo/challenge/piston"
)

type TestCase struct {
	N int
	K int
	A [][]int
}

// This code is so bad - do I do anything that's not quick and dirty?

/* What if you made this a complete piece of software?
 * With:
 * Multiple inputs: ftp, http, gRCP
 * Clean startup/shutdown service for the processing of Bar objects
 * Structured logging with exposed metrics
 *	(n<4 logging channels - one for system data, one for )
 * Swagger HTTP API documentation
 *
 */

func main() {
	log.Println("Starting main routine...")

	var crank piston.Crank
	if err := crank.Start(); err != nil {
		log.Fatal(err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT)
	var wg sync.WaitGroup
	go func() {
		s := <-interrupt
		wg.Add(1)
		defer wg.Done()
		log.Printf("Recieved %v signal. Halting crank\n", s)
		crank.Halt()
		// panic("Show me the stacks")
	}()

	go func() {
		defer crank.Close()
		i := 0
		for v := range fetchTestCases() {
			if err := crank.Push(piston.Input{strconv.Itoa(i), v}); err != nil {
				log.Println(err)
				return
			}
			i++
			time.Sleep(100 * time.Millisecond)
		}
	}()

	for {
		if o, err := crank.Pull(); err != nil {
			if err != piston.Drained {
				log.Println(err)
			}
			break
		} else {
			fmt.Println(o)
		}
	}
	wg.Wait()
}

func fetchTestCases() <-chan TestCase {
	c := make(chan TestCase)

	go func() {
		defer close(c)

		var t int
		fmt.Scanf("%d\n", &t)

		r := bufio.NewReader(os.Stdin)

		for i := 0; i < t; i++ {
			tc := TestCase{}
			nk, _, _ := r.ReadLine()
			p := strings.Split(string(nk), " ")
			n, _ := strconv.Atoi(p[0])
			k, _ := strconv.Atoi(p[1])
			tc.N = n
			tc.K = k

			for j := 0; j < tc.N; j++ {
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
				tc.A = append(tc.A, a)
			}
			tc.sort()
			c <- tc
		}
	}()
	return c
}

func (tc TestCase) Process() (interface{}, error) {
	possibilities := map[int]bool{}
	for i, a := range tc.A {
		for _, v := range a {
			if !possibilities[v] {
				if tc.check(v, i) {
					possibilities[v] = true
				}
			}
		}
	}
	return len(possibilities), nil
}

// http://stackoverflow.com/questions/17927746/explain-example-of-giving-k-th-largest-number-of-numbers-from-each-of-n-given-se
func (tc TestCase) check(v, gpos int) bool {
	min, max := 0, 0
	for i, a := range tc.A {
		if i == gpos { // current array (have already selected)
			continue
		}
		if a[0] <= v { // smaller or equal number increases minimum position
			min++
		}
		if a[len(a)-1] < v { // larger number decreases maximum position
			max++
		}
	}

	if min >= (tc.K-1) && max <= (tc.K-1) {
		return true
	} else {
		return false
	}
}

func (tc *TestCase) sort() {
	for i, _ := range tc.A {
		sort.Ints(tc.A[i])
	}
}
