package relay

import (
	// "fmt" // Remove unused import
	"sync"
	"testing"
	"time"
)

func TestSendChanWithNoReceiver(t *testing.T) {

	var wg sync.WaitGroup
	send := make(chan int, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			i := <-send
			t.Logf("received: %v", i) // Use t.Logf
			if i == 3 {
				t.Logf("receiving routine exit") // Use t.Logf
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		i := 0
		for {
			select {
			case send <- i:
				t.Logf("send: %v", i) // Use t.Logf
				i++
				time.Sleep(1 * time.Second)
			default:
				t.Logf("send buffer is full") // Use t.Logf
				return
			}
		}
	}()

	wg.Wait()
}
