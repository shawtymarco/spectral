package spectral

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/cooldogedev/spectral/internal/frame"
)

func TestCancelledOpenClosesStreamAcceptedAfterCancellation(t *testing.T) {
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, l.conn.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(frame.ConnectionCloseGraceful, "test complete")
	peer, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server := peer.(*ServerConnection)
	opening, cancelOpen := context.WithCancel(ctx)
	opened := make(chan error, 1)
	go func() {
		stream, err := client.OpenStream(opening)
		if stream != nil {
			_ = stream.Close()
		}
		opened <- err
	}()
	// Hold the request until the initiating caller has abandoned it, matching
	// a disconnect racing a proxy fallback. The shared peer stays connected.
	var request *frame.StreamRequest
	select {
	case request = <-server.streamRequests:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancelOpen()
	if err := <-opened; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled open returned %v", err)
	}
	server.streamRequests <- request
	orphan, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer orphan.Close()
	read := make(chan error, 1)
	go func() { _, err := orphan.Read(make([]byte, 1)); read <- err }()
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("cancelled stream remained readable")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled open left a silent server stream on the shared connection")
	}
	// Cancelling one stream must still permit unrelated players on the peer.
	next := make(chan *Stream, 1)
	nextErr := make(chan error, 1)
	go func() { stream, err := client.OpenStream(ctx); next <- stream; nextErr <- err }()
	remote, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	local := <-next
	if err := <-nextErr; err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if _, err := local.Write([]byte("ready")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(remote, got); err != nil || string(got) != "ready" {
		t.Fatalf("subsequent stream: %q, %v", got, err)
	}
}

func TestAcceptStreamStopsWhenPeerCloses(t *testing.T) {
	peerCtx, closePeer := context.WithCancelCause(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &ServerConnection{connection: &connection{ctx: peerCtx}, streamRequests: make(chan *frame.StreamRequest)}
	done := make(chan error, 1)
	go func() { _, err := s.AcceptStream(ctx); done <- err }()
	want := errors.New("peer disconnected")
	closePeer(want)
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("closed peer returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream accept outlived its peer connection")
	}
}
