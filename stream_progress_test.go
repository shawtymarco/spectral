package spectral

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cooldogedev/spectral/internal"
	"github.com/cooldogedev/spectral/internal/log"
)

func TestReliableStreamRecoversAfterRepeatedLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newStream(1, ctx, newSendQueue(), func() {}, func() {}, log.NopLogger{})
	queue := newRetransmissionQueue()
	now, rto := time.Now(), time.Millisecond
	queue.add(now, 1, []byte("first"))
	// Later stream bytes have arrived, but must wait for the missing prefix.
	stream.receive(1, []byte("second"))
	for attempt := 0; attempt < 5; attempt++ {
		now = now.Add(rto)
		payload, _ := queue.shift(now, rto)
		if len(payload) == 0 {
			t.Fatalf("retry %d abandoned an unacknowledged stream prefix", attempt+1)
		}
		if attempt == 4 {
			stream.receive(0, payload)
		}
	}
	if queue.remove(1) == nil {
		t.Fatal("late acknowledgement cannot retire the retransmission")
	}
	got := make([]byte, len("firstsecond"))
	if n := stream.read(got); n != len(got) || string(got) != "firstsecond" {
		t.Fatalf("stream did not recover its ordered bytes: %q (%d)", got, n)
	}
	if !queue.next(rto).IsZero() {
		t.Fatal("acknowledged packet kept retransmitting")
	}
}

func TestStreamReadReleasesQueuedFramesWithoutNewNetworkTraffic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newStream(1, ctx, newSendQueue(), func() {}, func() {}, log.NopLogger{})
	stream.buffer = internal.NewRingBuffer[byte](8)
	stream.receive(0, []byte("12345678"))
	stream.receive(2, []byte("tail"))
	stream.receive(1, []byte("next"))
	for _, want := range [][]byte{[]byte("12345678"), []byte("nexttail")} {
		got := make([]byte, len(want))
		if n := stream.read(got); n != len(want) || !bytes.Equal(got, want) {
			t.Fatalf("buffer drain required another packet: got %q (%d), want %q", got, n, want)
		}
	}
}
