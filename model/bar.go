package model

import "sort"

type Bar struct {
	N int
	K int
	A [][]int
}

func (b Bar) Process() (interface{}, error) {
	// b.sort()
	possibilities := map[int]bool{}
	for i, a := range b.A {
		for _, v := range a {
			if !possibilities[v] {
				if b.check(v, i) {
					possibilities[v] = true
				}
			}
		}
	}
	return len(possibilities), nil
}

// http://stackoverflow.com/questions/17927746/explain-example-of-giving-k-th-largest-number-of-numbers-from-each-of-n-given-se
func (b Bar) check(v, gpos int) bool {
	min, max := 0, 0
	for i, a := range b.A {
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

	if min >= (b.K-1) && max <= (b.K-1) {
		return true
	} else {
		return false
	}
}

func (b *Bar) Sort() {
	for i, _ := range b.A {
		sort.Ints(b.A[i])
	}
}
