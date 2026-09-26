//! Verifies that a close frame reaches the peer, and that a server flushes its close reply
//! before it reports `ConnectionClosed`.

use std::io::{self, Cursor, Read, Write};
use tungstenite::{
    protocol::{Role, WebSocketConfig},
    Error, Message, WebSocket,
};

/// A masked close frame without a payload, as a client sends it.
const CLIENT_CLOSE: [u8; 6] = [0x88, 0x80, 0, 0, 0, 0];
/// The unmasked close frame the server replies with.
const SERVER_CLOSE_REPLY: [u8; 2] = [0x88, 0x00];

/// Keeps written bytes until `flush`, like `BufWriter` or a TLS stream.
/// Reads `CLIENT_CLOSE`, then EOF.
struct BufferUntilFlush {
    incoming: Cursor<Vec<u8>>,
    buffered: Vec<u8>,
    /// Bytes that were flushed, i.e. sent to the peer.
    wire: Vec<u8>,
    /// Number of `flush` calls that return `WouldBlock` before one succeeds.
    blocked_flushes: usize,
}

impl BufferUntilFlush {
    fn new(blocked_flushes: usize) -> Self {
        Self {
            incoming: Cursor::new(CLIENT_CLOSE.to_vec()),
            buffered: Vec::new(),
            wire: Vec::new(),
            blocked_flushes,
        }
    }
}

impl Read for BufferUntilFlush {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.incoming.read(buf)
    }
}

impl Write for BufferUntilFlush {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffered.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.blocked_flushes > 0 {
            self.blocked_flushes -= 1;
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.wire.append(&mut self.buffered);
        Ok(())
    }
}

fn server_after_client_close(blocked_flushes: usize) -> WebSocket<BufferUntilFlush> {
    let mut ws =
        WebSocket::from_raw_socket(BufferUntilFlush::new(blocked_flushes), Role::Server, None);
    assert_eq!(ws.read().unwrap(), Message::Close(None));
    ws
}

/// Header and payload of the binary frame that fills the write buffer.
const BINARY_FRAME_LEN: usize = 16;

/// A server whose write buffer holds one binary frame and has no room for a close frame.
fn server_with_full_write_buffer() -> WebSocket<BufferUntilFlush> {
    let config = WebSocketConfig::default()
        .write_buffer_size(BINARY_FRAME_LEN)
        .max_write_buffer_size(BINARY_FRAME_LEN + 1);
    let mut ws = WebSocket::from_raw_socket(BufferUntilFlush::new(0), Role::Server, Some(config));
    ws.write(Message::binary(vec![7; BINARY_FRAME_LEN - 2])).unwrap();
    assert!(ws.get_ref().buffered.is_empty());
    ws
}

fn assert_binary_then_close(wire: &[u8]) {
    assert_eq!(wire.len(), BINARY_FRAME_LEN + SERVER_CLOSE_REPLY.len());
    assert_eq!(wire[BINARY_FRAME_LEN..], SERVER_CLOSE_REPLY);
}

fn assert_would_block(result: tungstenite::Result<impl std::fmt::Debug>) {
    match result {
        Err(Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {}
        other => panic!("expected WouldBlock, got {other:?}"),
    }
}

#[test]
fn server_flushes_close_reply_before_connection_closed() {
    let mut ws = server_after_client_close(0);

    assert!(matches!(ws.read(), Err(Error::ConnectionClosed)));
    assert_eq!(ws.get_ref().wire, SERVER_CLOSE_REPLY);
}

#[test]
fn server_retries_blocked_close_flush_on_next_read() {
    let mut ws = server_after_client_close(1);

    assert_would_block(ws.flush());
    assert!(ws.get_ref().wire.is_empty());

    assert!(matches!(ws.read(), Err(Error::ConnectionClosed)));
    assert_eq!(ws.get_ref().wire, SERVER_CLOSE_REPLY);
}

#[test]
fn server_keeps_close_reply_when_flush_blocks_before_peer_eof() {
    let mut ws = server_after_client_close(1);

    // The peer already sent EOF, but the reply is not sent yet: keep the connection.
    assert_would_block(ws.read());
    assert!(ws.get_ref().wire.is_empty());

    assert!(matches!(ws.read(), Err(Error::ConnectionClosed)));
    assert_eq!(ws.get_ref().wire, SERVER_CLOSE_REPLY);
}

#[test]
fn server_sends_close_reply_when_write_buffer_is_full() {
    let mut ws = server_with_full_write_buffer();
    assert_eq!(ws.read().unwrap(), Message::Close(None));

    assert!(matches!(ws.read(), Err(Error::ConnectionClosed)));
    assert_binary_then_close(&ws.get_ref().wire);
}

#[test]
fn server_sends_close_reply_larger_than_max_write_buffer_size() {
    // The smallest limit the config allows. A close frame has at least 2 bytes.
    let config = WebSocketConfig::default().write_buffer_size(0).max_write_buffer_size(1);
    let mut ws = WebSocket::from_raw_socket(BufferUntilFlush::new(0), Role::Server, Some(config));
    assert_eq!(ws.read().unwrap(), Message::Close(None));

    assert!(matches!(ws.read(), Err(Error::ConnectionClosed)));
    assert_eq!(ws.get_ref().wire, SERVER_CLOSE_REPLY);
}

#[test]
fn close_queues_close_frame_when_write_buffer_is_full() {
    let mut ws = server_with_full_write_buffer();

    ws.close(None).unwrap();
    assert_binary_then_close(&ws.get_ref().wire);
}
