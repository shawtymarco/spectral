# Spectral

## Maintenance fork

This fork retains the upstream module path and MIT license. Its baseline is
`cooldogedev/spectral` v0.0.5 (`6216af5e67dc623f50953077b52293592e17e944`).
The maintenance changes synchronize listener shutdown, release the listener-owned
UDP socket, and make shutdown/backpressure safe for in-flight channel producers.
The UDP frame format and stream protocol are unchanged. Examples have separate
client/server packages so the complete repository can be validated.

Validation: `go test ./...`, `go vet ./...`, and `go test -race ./...`.
Consumers must pin an immutable fork revision using a Go module replacement.

Reliable datagrams remain in the retransmission queue until acknowledged or the
peer is closed. A fixed retry count must not silently discard ordered stream
bytes: the resulting sequence gap and unreleased flight accounting can stall
existing and subsequent streams on the shared peer. Retransmissions retain the
existing RTO/congestion pacing and connection inactivity cleanup.

Reading a full stream buffer also resumes queued ordered frames immediately;
progress must not depend on another datagram arriving. Regression tests force
four consecutive UDP losses, verify recovery and reuse of the shared peer, and
drain a saturated receiver without further network traffic.

## CRITICAL: cancelled stream opens must release the remote stream

Cancellation can race server acceptance while a shared connection still carries
other sessions. An abandoned `OpenStream` must send `StreamClose`, and a late
successful response must close the remote stream again if no local stream exists.
Otherwise a consumer waiting for its first application packet can wait forever.
`AcceptStream` also stops when the peer connection closes, even if its caller
supplied a background context. The cancellation regression delays acceptance
until after the caller has left, checks that the remote read ends, and verifies
that another stream on the same connection still works. Consumers must separately
bound application handshakes and must not perform them in a shared accept loop.

**Spectral** is a blazingly fast, lightweight, and powerful network engine designed for real-time, low-latency applications such as gaming, streaming, and other interactive services. Built on top of UDP, Spectral ensures high performance while maintaining reliability through advanced networking concepts.

## Core Concepts

- **Streams**: Spectral supports streams, enabling multiple data channels over a single connection. This allows for efficient data handling and avoids head-of-line blocking.
- **Reliability**: Despite being built on top of the connectionless UDP protocol, Spectral incorporates mechanisms for guaranteed packet delivery.
- **Stream-level Ordering**: Spectral ensures that data within a stream is delivered in the correct order, optimizing application performance where packet sequence matters.
- **Packet Pacing**: The engine manages transmission timing for efficient bandwidth use and reduced network congestion.
- **Congestion Control**: Spectral dynamically adjusts its transmission rate to adapt to varying network conditions, ensuring smooth data flow and minimal packet loss.
- **Retransmission**: Lost or dropped packets are intelligently detected and retransmitted, providing robustness in unreliable networks.

These features make Spectral ideal for scenarios requiring fast, reliable, and scalable communication.

## Examples

Explore the [example](example) directory to learn how to integrate Spectral into your project.

## Implementations

Spectral is implemented in the following languages:

- **Go**: [Spectral Go](https://github.com/cooldogedev/spectral)
- **PHP**: [Spectral PHP](https://github.com/cooldogedev/spectral-php)

Additional language implementations are under development to expand its reach across different platforms.

## Projects Using Spectral

| Project    | Description                                                                                 | Stars |
|------------|---------------------------------------------------------------------------------------------|-------|
| [Spectrum](https://github.com/cooldogedev/spectrum) | A fast and lightweight proxy for Minecraft: Bedrock Edition, leveraging Spectral for enhanced performance. | [![Stars](https://img.shields.io/github/stars/cooldogedev/spectrum?style=flat-square)](https://github.com/cooldogedev/spectrum) |
