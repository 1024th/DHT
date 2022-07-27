// Throttle works like sync.WaitGroup, except that it has a limit on the number
// of concurrent workers.
//
// (*Throttle).Add can only add 1 to the WaitGroup counter, and
// if the number of concurrent workers reaches maxWorkers set by NewThrottle,
// Add will block until one of the workers finishes and call (*Throttle).Done.
//
// Internally, it uses buffered channels to achieve this.
package throttler

import (
	"sync"
)

type Throttler struct {
	ch chan struct{}
	wg sync.WaitGroup
}

// (*Throttle).Add adds 1 to the WaitGroup counter, and
// if the number of concurrent workers reaches the limit (maxWorkers),
// Add will block until one of the workers finishes and call (*Throttle).Done.
func (t *Throttler) Add() {
	t.ch <- struct{}{}
	t.wg.Add(1)
}

func (t *Throttler) Done() {
	<-t.ch
	t.wg.Done()
}

func (t *Throttler) Wait() {
	t.wg.Wait()
}

func NewThrottler(maxWorkers uint) *Throttler {
	t := new(Throttler)
	t.ch = make(chan struct{}, maxWorkers)
	return t
}
