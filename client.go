package spectral

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/cooldogedev/spectral/internal/frame"
	"github.com/cooldogedev/spectral/internal/log"
	"github.com/cooldogedev/spectral/internal/protocol"
)

type ClientConnection struct {
	*connection
	response        chan *frame.ConnectionResponse
	streamResponses map[protocol.StreamID]chan *frame.StreamResponse
	streamID        protocol.StreamID
	mu              sync.RWMutex
}

func newClientConnection(conn *udpConn, peerAddr *net.UDPAddr, ctx context.Context) *ClientConnection {
	c := &ClientConnection{
		connection:      newConnection(conn, peerAddr, -1, ctx, log.PerspectiveClient),
		response:        make(chan *frame.ConnectionResponse, 1),
		streamResponses: make(map[protocol.StreamID]chan *frame.StreamResponse),
	}
	c.connection.handler = c.handle
	return c
}

func (c *ClientConnection) OpenStream(ctx context.Context) (*Stream, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := context.Cause(c.ctx); err != nil {
		return nil, err
	}
	ch := make(chan *frame.StreamResponse, 1)
	c.mu.Lock()
	streamID := c.streamID
	c.streamID++
	c.streamResponses[streamID] = ch
	c.mu.Unlock()
	opened := false
	defer func() {
		c.mu.Lock()
		delete(c.streamResponses, streamID)
		c.mu.Unlock()
		if !opened {
			// The server may have accepted this request before the caller left.
			// Closing only the caller's wait leaves a silent remote stream alive.
			_ = c.writeControl(&frame.StreamClose{StreamID: streamID}, true)
		}
	}()

	c.logger.Log("stream_open_request", "streamID", streamID)
	if err := c.writeControl(&frame.StreamRequest{StreamID: streamID}, true); err != nil {
		return nil, err
	}

	select {
	case <-c.ctx.Done():
		return nil, context.Cause(c.ctx)
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case response := <-ch:
		if response.Response == frame.StreamResponseFailed {
			c.logger.Log("stream_open_fail", "streamID", streamID)
			return nil, errors.New("failed to open stream")
		}
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if err := context.Cause(c.ctx); err != nil {
			return nil, err
		}

		stream, err := c.createStream(streamID)
		if err != nil {
			return nil, err
		}
		c.logger.Log("stream_open_success", "streamID", streamID)
		opened = true
		return stream, nil
	}
}

func (c *ClientConnection) handle(fr frame.Frame) (err error) {
	switch fr := fr.(type) {
	case *frame.ConnectionResponse:
		select {
		case c.response <- fr:
		default:
		}
	case *frame.StreamResponse:
		c.mu.RLock()
		ch, ok := c.streamResponses[fr.StreamID]
		c.mu.RUnlock()
		if ok {
			select {
			case ch <- fr:
			default:
			}
		} else {
			c.logger.Log("stream_response_unknown", "streamID", fr.StreamID)
			if fr.Response == frame.StreamResponseSuccess && c.streams.get(fr.StreamID) == nil {
				// Cancellation may have reached the server before AcceptStream
				// created the stream. A late success must close that orphan too.
				return c.writeControl(&frame.StreamClose{StreamID: fr.StreamID}, true)
			}
		}
	}
	return
}
