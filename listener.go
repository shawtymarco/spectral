package spectral

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/cooldogedev/spectral/internal/frame"
	"github.com/cooldogedev/spectral/internal/protocol"
)

type Listener struct {
	conn                *udpConn
	connections         map[protocol.ConnectionID]*ServerConnection
	connectionsMu       sync.Mutex
	connectionID        protocol.ConnectionID
	incomingConnections chan *ServerConnection
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	once                sync.Once
}

func newListener(conn *udpConn) *Listener {
	listener := &Listener{
		conn:                conn,
		connections:         make(map[protocol.ConnectionID]*ServerConnection),
		incomingConnections: make(chan *ServerConnection, 100),
	}
	listener.ctx, listener.cancelFunc = context.WithCancel(context.Background())
	go conn.Read(func(dgram *datagram) (err error) {
		defer dgram.reset()
		connectionID, sequenceID, frames, err := frame.Unpack(dgram.b)
		if err != nil {
			return nil
		}

		listener.connectionsMu.Lock()
		if listener.ctx.Err() != nil {
			listener.connectionsMu.Unlock()
			return context.Cause(listener.ctx)
		}
		c, ok := listener.connections[connectionID]
		created := false
		if !ok && slices.ContainsFunc(frames, func(fr frame.Frame) bool { return fr.ID() == frame.IDConnectionRequest }) {
			c = newServerConnection(conn, dgram.peerAddr, listener.connectionID, listener.ctx)
			c.logger.Log("connection_accepted", "addr", dgram.peerAddr.String())
			listener.connections[listener.connectionID] = c
			listener.connectionID++
			created = true
			go func() {
				<-c.ctx.Done()
				listener.connectionsMu.Lock()
				id := protocol.ConnectionID(c.connectionID.Load())
				if listener.connections[id] == c {
					delete(listener.connections, id)
				}
				listener.connectionsMu.Unlock()
			}()
		}
		listener.connectionsMu.Unlock()

		if c == nil {
			return
		}
		if created {
			select {
			case listener.incomingConnections <- c:
			case <-listener.ctx.Done():
				return context.Cause(listener.ctx)
			case <-c.ctx.Done():
				return nil
			}
		}

		select {
		case <-listener.ctx.Done():
			return context.Cause(listener.ctx)
		case <-c.ctx.Done():
		case c.packets <- &receivedPacket{sequenceID, frames, time.Now()}:
		}
		return
	})
	return listener
}

func Listen(address string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	c, err := newUDPConn(conn, true)
	if err != nil {
		return nil, err
	}
	return newListener(c), nil
}

func (l *Listener) Accept(ctx context.Context) (Connection, error) {
	select {
	case <-l.ctx.Done():
		return nil, errors.New("listener closed")
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case conn := <-l.incomingConnections:
		if l.ctx.Err() != nil || conn.ctx.Err() != nil {
			return nil, errors.New("listener or connection closed")
		}
		return conn, nil
	}
}

func (l *Listener) Close() (err error) {
	l.once.Do(func() {
		l.cancelFunc()
		l.connectionsMu.Lock()
		connections := make([]*ServerConnection, 0, len(l.connections))
		for _, conn := range l.connections {
			connections = append(connections, conn)
		}
		clear(l.connections)
		l.connectionsMu.Unlock()
		for _, conn := range connections {
			_ = conn.CloseWithError(frame.ConnectionCloseGraceful, "closed listener")
		}
		// Per-connection Close deliberately keeps a shared listener socket
		// open. Only the listener owns and closes the actual UDP socket.
		err = l.conn.conn.Close()
	})
	return
}
