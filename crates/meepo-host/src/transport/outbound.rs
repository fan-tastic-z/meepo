//! Bounded serial outbound writer — per-connection FIFO with backpressure.
//!
//! Frames are enqueued in order and written to the sink one at a time (strict
//! FIFO). Each [`enqueue`](BoundedSerialOutboundWriter::enqueue) returns a
//! `flushed` oneshot that resolves once that frame has been handed to the
//! sink. Overflow beyond [`MAX_QUEUED_FRAMES`] or [`MAX_QUEUED_BYTES`] fails
//! fast (the caller should `await` flushed to avoid runaway queuing); a sink
//! error marks the writer closed.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::sink::SinkExt;
use futures::Sink;
use serde_json::Value;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;

/// Maximum frames buffered in the outbound queue.
pub const MAX_QUEUED_FRAMES: usize = 64;
/// Maximum bytes buffered in the outbound queue.
pub const MAX_QUEUED_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum OutboundQueueError {
    #[error("frame limit reached ({queued}/{cap})")]
    FrameLimit { queued: usize, cap: usize },
    #[error("byte limit reached ({queued}/{cap})")]
    ByteLimit { queued: usize, cap: usize },
    #[error("connection closed")]
    Closed,
}

struct Item {
    frame: Value,
    flushed: Option<oneshot::Sender<()>>,
    size: usize,
}

struct State {
    items: VecDeque<Item>,
    queued_bytes: usize,
    closed: bool,
}

/// Serializes outbound frames FIFO with bounded buffering. Generic over the
/// sink so it backs real framed writes (`FrameWrite`) and test sinks alike.
pub struct BoundedSerialOutboundWriter<S>
where
    S: Sink<Value> + Unpin + Send + 'static,
    S::Error: Send + Debug + 'static,
{
    state: Arc<Mutex<State>>,
    notify: Arc<Notify>,
    _writer: JoinHandle<()>,
    // The drain task owns the sink; mark S used without imposing its auto-traits.
    _sink: PhantomData<fn() -> S>,
}

impl<S> BoundedSerialOutboundWriter<S>
where
    S: Sink<Value> + Unpin + Send + 'static,
    S::Error: Send + Debug + 'static,
{
    /// Spawn the drain task over `sink`. The task owns the sink for its lifetime.
    pub fn new(sink: S) -> Self {
        let state = Arc::new(Mutex::new(State {
            items: VecDeque::new(),
            queued_bytes: 0,
            closed: false,
        }));
        let notify = Arc::new(Notify::new());
        let task_state = state.clone();
        let task_notify = notify.clone();

        let _writer = tokio::spawn(async move {
            let mut sink = sink;
            loop {
                // Pop the head under the lock; dequeue bytes the moment a frame
                // leaves the queue for the sink.
                let popped = {
                    let mut s = task_state.lock().await;
                    match s.items.pop_front() {
                        Some(it) => {
                            s.queued_bytes = s.queued_bytes.saturating_sub(it.size);
                            Some(it)
                        }
                        None => None,
                    }
                };
                let item = match popped {
                    Some(it) => it,
                    None => {
                        // Nothing queued: exit if closed, else wait for work.
                        if task_state.lock().await.closed {
                            break;
                        }
                        task_notify.notified().await;
                        continue;
                    }
                };
                let Item { frame, flushed, size: _ } = item;
                // A sink error is terminal: the connection is gone.
                if sink.send(frame).await.is_err() {
                    task_state.lock().await.closed = true;
                    break;
                }
                if let Some(tx) = flushed {
                    let _ = tx.send(());
                }
            }
            task_state.lock().await.closed = true;
        });

        Self { state, notify, _writer, _sink: PhantomData }
    }

    /// Enqueue one frame. Returns the `flushed` receiver that resolves once the
    /// frame is handed to the sink. Fails fast on overflow or if closed.
    pub async fn enqueue(&self, frame: Value) -> Result<oneshot::Receiver<()>, OutboundQueueError> {
        let size = frame_bytes(&frame);
        let (tx, rx) = oneshot::channel();
        let mut s = self.state.lock().await;
        if s.closed {
            return Err(OutboundQueueError::Closed);
        }
        if s.items.len() >= MAX_QUEUED_FRAMES {
            return Err(OutboundQueueError::FrameLimit { queued: s.items.len(), cap: MAX_QUEUED_FRAMES });
        }
        if s.queued_bytes + size > MAX_QUEUED_BYTES {
            return Err(OutboundQueueError::ByteLimit { queued: s.queued_bytes, cap: MAX_QUEUED_BYTES });
        }
        s.items.push_back(Item { frame, flushed: Some(tx), size });
        s.queued_bytes += size;
        drop(s);
        self.notify.notify_one();
        Ok(rx)
    }

    /// Whether a sink error or explicit close has shut the writer down.
    pub async fn is_closed(&self) -> bool {
        self.state.lock().await.closed
    }

    /// Mark the writer closed. The drain task finishes what is queued then exits.
    pub async fn close(&self) {
        self.state.lock().await.closed = true;
        self.notify.notify_one();
    }
}

fn frame_bytes(v: &Value) -> usize {
    serde_json::to_vec(v).map(|b| b.len() + 1).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A sink whose `poll_ready` never resolves — the drain task stalls on the
    /// first frame so the queue deterministically accumulates.
    struct StalledSink;

    impl Sink<Value> for StalledSink {
        type Error = std::io::Error;
        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
        fn start_send(self: Pin<&mut Self>, _item: Value) -> Result<(), Self::Error> {
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn enforces_frame_limit_when_sink_stalled() {
        let writer = BoundedSerialOutboundWriter::new(StalledSink);
        let mut accepted = 0usize;
        loop {
            match writer.enqueue(json!({"i": accepted})).await {
                Ok(_) => accepted += 1,
                Err(OutboundQueueError::FrameLimit { .. }) | Err(OutboundQueueError::ByteLimit { .. }) => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
            assert!(accepted <= MAX_QUEUED_FRAMES + 1, "overflowed too late: {accepted}");
        }
        // Stalled sink ⇒ at most one frame in flight + the queue cap accepted.
        assert!(accepted <= MAX_QUEUED_FRAMES + 1, "accepted {accepted}");
    }

    #[tokio::test]
    async fn fifo_order_and_flushed_signal() {
        use crate::transport::framed::FramedConn;
        use futures::StreamExt;
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let server = FramedConn::new(tokio::net::UnixStream::from_std(a).unwrap());
        let mut client = FramedConn::new(tokio::net::UnixStream::from_std(b).unwrap());
        let writer = BoundedSerialOutboundWriter::new(server.write);

        let mut receipts = Vec::new();
        for i in 0..5u32 {
            receipts.push(writer.enqueue(json!({"i": i})).await.unwrap());
        }
        let mut got = Vec::new();
        for _ in 0..5 {
            let v = client.read.next().await.unwrap().unwrap();
            got.push(v);
        }
        assert_eq!(
            got,
            (0..5u32).map(|i| json!({"i": i})).collect::<Vec<_>>(),
            "frames must arrive in FIFO order"
        );
        for r in receipts {
            r.await.expect("flushed signal must resolve once written");
        }
    }

    #[tokio::test]
    async fn enqueue_after_close_is_closed() {
        let writer = BoundedSerialOutboundWriter::new(StalledSink);
        writer.close().await;
        match writer.enqueue(json!({})).await {
            Err(OutboundQueueError::Closed) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
    }
}
