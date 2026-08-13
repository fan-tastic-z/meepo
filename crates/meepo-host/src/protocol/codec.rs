//! Line-framed NDJSON codec.
//!
//! One JSON object per frame, delimited by a single LF (`0x0a`). Frames are
//! UTF-8 (fatal on invalid bytes), capped at [`MAX_FRAME_BYTES`] (delimiter
//! included), and a trailing CR (`0x0d`) before the LF is tolerated and
//! stripped. Any framing fault is fatal — the caller MUST close the
//! connection. The codec is JSON-value-level; envelope `kind` routing is done
//! upstream by [`crate::protocol::envelope`].
//!
//! [`CodecError`] unifies framing faults ([`ProtocolError`], fatal) with
//! `io::Error` (transport); tokio-util's codec traits require `Error:
//! From<io::Error>` on both sides.

use bytes::{Buf, BufMut, BytesMut};
use serde_json::Value;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use super::errors::ProtocolError;
use super::versions::MAX_FRAME_BYTES;

/// Codec-layer error: a fatal framing fault, or a transport `io::Error`.
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("{0}")]
    Protocol(#[from] ProtocolError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Encode/decode JSON values as LF-delimited UTF-8 frames.
#[derive(Debug, Clone, Copy, Default)]
pub struct LfCodec;

impl Decoder for LfCodec {
    type Item = Value;
    type Error = CodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Find the next frame boundary (LF).
        let lf = match buf.iter().position(|&b| b == b'\n') {
            Some(i) => i,
            None => {
                // No complete frame yet — reject runaway accumulation early.
                if buf.len() > MAX_FRAME_BYTES {
                    return Err(ProtocolError::FrameTooLarge.into());
                }
                return Ok(None);
            }
        };

        // Reject empty payloads: bare LF, or CR+LF.
        if lf == 0 || (lf == 1 && buf[0] == b'\r') {
            return Err(ProtocolError::InvalidFrame.into());
        }
        // Cap includes the delimiter.
        if lf + 1 > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge.into());
        }

        // Slice the line (excluding LF), strip an optional trailing CR.
        let mut end = lf;
        if buf[end - 1] == b'\r' {
            end -= 1;
        }
        let line = &buf[..end];
        // Fatal UTF-8 validation, distinct from JSON errors.
        let s = std::str::from_utf8(line).map_err(|_| ProtocolError::InvalidUtf8)?;
        let value: Value =
            serde_json::from_str(s).map_err(|e| ProtocolError::InvalidJson(e.to_string()))?;
        buf.advance(lf + 1);
        Ok(Some(value))
    }
}

impl Encoder<Value> for LfCodec {
    type Error = CodecError;

    fn encode(&mut self, item: Value, buf: &mut BytesMut) -> Result<(), Self::Error> {
        // serde_json escapes control characters (including LF) inside strings,
        // so a serialized object never contains a raw LF — one object per line
        // holds. Reject frames that would exceed the cap (delimiter included).
        let json =
            serde_json::to_vec(&item).map_err(|e| ProtocolError::InvalidJson(e.to_string()))?;
        if json.len() + 1 > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge.into());
        }
        buf.put_slice(&json);
        buf.put_u8(b'\n');
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encode(item: Value) -> BytesMut {
        let mut buf = BytesMut::new();
        LfCodec.encode(item, &mut buf).expect("encode");
        buf
    }

    fn decode_one(buf: &mut BytesMut) -> Value {
        LfCodec.decode(buf).expect("decode").expect("had a frame")
    }

    /// Extract the [`ProtocolError`] from a decode result, asserting it is one.
    fn protocol_err(buf: &mut BytesMut) -> ProtocolError {
        match LfCodec.decode(buf) {
            Err(CodecError::Protocol(p)) => p,
            Err(other) => panic!("expected Protocol error, got {other:?}"),
            Ok(other) => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_a_frame() {
        let v = json!({"requestId": "r1", "operation": "host.status", "input": {}});
        let mut buf = encode(v.clone());
        let got = decode_one(&mut buf);
        assert_eq!(got, v);
        assert!(LfCodec.decode(&mut buf).unwrap().is_none(), "no second frame");
    }

    #[test]
    fn strips_trailing_cr() {
        let mut buf = BytesMut::new();
        buf.put_slice(br#"{"ok":true}"#);
        buf.put_u8(b'\r');
        buf.put_u8(b'\n');
        let got = decode_one(&mut buf);
        assert_eq!(got, json!({"ok": true}));
    }

    #[test]
    fn rejects_empty_frame() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'\n');
        assert_eq!(protocol_err(&mut buf), ProtocolError::InvalidFrame);
        let mut buf = BytesMut::new();
        buf.put_slice(b"\r\n");
        assert_eq!(protocol_err(&mut buf), ProtocolError::InvalidFrame);
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut buf = BytesMut::new();
        buf.put_slice(b"{\"a\":\"");
        buf.put_u8(0xff); // invalid utf-8 byte
        buf.put_slice(b"\"}\n");
        assert_eq!(protocol_err(&mut buf), ProtocolError::InvalidUtf8);
    }

    #[test]
    fn rejects_invalid_json() {
        let mut buf = BytesMut::new();
        buf.put_slice(b"{not json}\n");
        match protocol_err(&mut buf) {
            ProtocolError::InvalidJson(_) => {}
            other => panic!("expected InvalidJson, got {other:?}"),
        }
    }

    #[test]
    fn rejects_frame_over_cap_without_lf() {
        // Accumulate > MAX_FRAME_BYTES with no LF → fatal FrameTooLarge.
        let mut buf = BytesMut::new();
        buf.resize(MAX_FRAME_BYTES + 1, b' ');
        assert_eq!(protocol_err(&mut buf), ProtocolError::FrameTooLarge);
    }

    #[test]
    fn rejects_completed_frame_over_cap() {
        let mut buf = BytesMut::new();
        buf.resize(MAX_FRAME_BYTES, b' ');
        buf.put_u8(b'\n'); // delimiter pushes total over the cap
        assert_eq!(protocol_err(&mut buf), ProtocolError::FrameTooLarge);
    }

    #[test]
    fn yields_none_for_partial_frame_under_cap() {
        let mut buf = BytesMut::new();
        buf.put_slice(b"{\"a\":1"); // no LF yet, under cap
        assert!(LfCodec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn encode_rejects_oversized_value() {
        let huge = Value::String("x".repeat(MAX_FRAME_BYTES));
        let mut buf = BytesMut::new();
        assert!(LfCodec.encode(huge, &mut buf).is_err());
    }
}
