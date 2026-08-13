//! Framed Unix-domain-socket transport.
//!
//! Wraps a [`tokio::net::UnixStream`] in read/write halves framed by
//! [`LfCodec`]: one JSON value per frame in each direction. The write half is
//! driven through [`crate::transport::outbound::BoundedSerialOutboundWriter`]
//! for FIFO ordering and backpressure.

use std::io;
use std::path::Path;

use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::protocol::LfCodec;

pub type FrameRead = FramedRead<tokio::io::ReadHalf<UnixStream>, LfCodec>;
pub type FrameWrite = FramedWrite<tokio::io::WriteHalf<UnixStream>, LfCodec>;

/// Read + write frame halves over a single Unix socket.
pub struct FramedConn {
    pub read: FrameRead,
    pub write: FrameWrite,
}

impl FramedConn {
    /// Split `stream` into a framed read half and a framed write half.
    pub fn new(stream: UnixStream) -> Self {
        let (rh, wh) = tokio::io::split(stream);
        Self {
            read: FramedRead::new(rh, LfCodec),
            write: FramedWrite::new(wh, LfCodec),
        }
    }
}

/// Bind a listening socket at `path`, removing any stale socket file first.
pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixListener> {
    let _ = std::fs::remove_file(path.as_ref());
    UnixListener::bind(path)
}

/// Connect to a listening socket at `path`.
pub async fn connect(path: impl AsRef<Path>) -> io::Result<FramedConn> {
    let stream = UnixStream::connect(path).await?;
    Ok(FramedConn::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::MAX_FRAME_BYTES;
    use futures::{sink::SinkExt, stream::StreamExt};
    use serde_json::json;

    #[tokio::test]
    async fn echo_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("h.sock");
        let listener = bind(&sock).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = FramedConn::new(stream);
            let v = conn.read.next().await.unwrap().unwrap();
            conn.write.send(v).await.unwrap();
        });
        let mut client = connect(&sock).await.unwrap();
        client.write.send(json!({"hello": "world"})).await.unwrap();
        let got = client.read.next().await.unwrap().unwrap();
        assert_eq!(got, json!({"hello": "world"}));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn oversize_frame_errors_the_read_stream() {
        // A raw line over the 96KiB wire cap fails the codec (FrameTooLarge),
        // surfacing as an error on the read stream. The writer runs concurrently
        // so the socket buffer can drain past the line length without blocking.
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let big = format!("{}\n", "x".repeat(MAX_FRAME_BYTES + 10));
        let mut writer = tokio::net::UnixStream::from_std(a).unwrap();
        let payload = big.clone();
        let writer_task = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            writer.write_all(payload.as_bytes()).await.unwrap();
            writer.shutdown().await.ok();
        });
        let mut conn = FramedConn::new(tokio::net::UnixStream::from_std(b).unwrap());
        let res = conn.read.next().await;
        assert!(matches!(res, Some(Err(_))), "oversize should error, got {res:?}");
        writer_task.await.unwrap();
    }
}
