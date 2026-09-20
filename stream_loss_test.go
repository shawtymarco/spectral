package spectral

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cooldogedev/spectral/internal/frame"
)

// Drop the original server packet and its first three retries. The peer must
// recover the byte stream without waiting for unrelated application traffic.
func TestSharedPeerRecoversStreamAfterFourDroppedDatagrams(t *testing.T) {
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	serverAddr := listener.conn.LocalAddr().(*net.UDPAddr)
	var dropped atomic.Int32
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		var clientAddr *net.UDPAddr
		var lostSequence uint32
		buffer := make([]byte, 65536)
		for {
			n, from, err := relay.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			target := serverAddr
			if from.String() == serverAddr.String() {
				target = clientAddr
				if n >= 20 {
					sequence := binary.LittleEndian.Uint32(buffer[12:16])
					if lostSequence == 0 && binary.LittleEndian.Uint32(buffer[16:20]) == frame.IDStreamData {
						lostSequence = sequence
					}
					if sequence == lostSequence && lostSequence != 0 && dropped.Load() < 4 {
						dropped.Add(1)
						continue
					}
				}
			} else {
				clientAddr = from
			}
			if target != nil {
				_, _ = relay.WriteToUDP(buffer[:n], target)
			}
		}
	}()
	defer func() { _ = relay.Close(); <-relayDone }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := Dial(ctx, relay.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(frame.ConnectionCloseGraceful, "test complete")
	server, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := func() (*Stream, *Stream) {
		t.Helper()
		type result struct {
			stream *Stream
			err    error
		}
		ready := make(chan result, 1)
		go func() { s, err := client.OpenStream(ctx); ready <- result{s, err} }()
		remote, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = remote.Close() })
		local := <-ready
		if local.err != nil {
			t.Fatal(local.err)
		}
		t.Cleanup(func() { _ = local.stream.Close() })
		return local.stream, remote
	}
	local, remote := open()
	if _, err := remote.Write([]byte("bootstrap")); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() {
		got := make([]byte, len("bootstrap"))
		_, err := io.ReadFull(local, got)
		if err == nil && string(got) != "bootstrap" {
			err = io.ErrUnexpectedEOF
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("bootstrap stalled after %d dropped datagrams: %v", dropped.Load(), ctx.Err())
	}
	if dropped.Load() != 4 {
		t.Fatalf("dropped %d datagrams, want 4", dropped.Load())
	}
	// Reusing this peer for another player must still work after recovery.
	nextLocal, nextRemote := open()
	if _, err := nextLocal.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	go func() { _, err := io.ReadFull(nextRemote, make([]byte, 4)); read <- err }()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("recovered peer blocked the next player")
	}
}
