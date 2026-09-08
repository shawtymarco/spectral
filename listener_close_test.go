package spectral

import (
	"context"
	"github.com/cooldogedev/spectral/internal/frame"
	"net"
	"sync"
	"testing"
	"time"
)

func TestListenerCloseReleasesItsUDPSocket(t *testing.T) {
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.conn.LocalAddr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("closed listener retained socket: %v", err)
	}
	_ = rebound.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := l.Accept(ctx); err == nil {
		t.Fatal("closed listener accepted a connection")
	}
}

func TestConcurrentPeerCleanupAndListenerClose(t *testing.T) {
	for iteration := 0; iteration < 10; iteration++ {
		l, err := Listen("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var clients []Connection
		for i := 0; i < 8; i++ {
			client, err := Dial(ctx, l.conn.LocalAddr().String())
			if err != nil {
				cancel()
				_ = l.Close()
				t.Fatal(err)
			}
			clients = append(clients, client)
			if _, err := l.Accept(ctx); err != nil {
				cancel()
				_ = l.Close()
				t.Fatal(err)
			}
		}
		var workers sync.WaitGroup
		start := make(chan struct{})
		for _, client := range clients {
			workers.Add(1)
			go func(c Connection) {
				defer workers.Done()
				<-start
				_ = c.CloseWithError(frame.ConnectionCloseGraceful, "test close")
			}(client)
		}
		workers.Add(1)
		go func() { defer workers.Done(); <-start; _ = l.Close() }()
		close(start)
		workers.Wait()
		cancel()
		l.connectionsMu.Lock()
		remaining := len(l.connections)
		l.connectionsMu.Unlock()
		if remaining != 0 {
			t.Fatalf("closed listener retained %d peers", remaining)
		}
	}
}
