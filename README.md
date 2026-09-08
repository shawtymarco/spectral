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
