//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! The CAN FD link wire format.
//!
//! This module deliberately contains no I/O and no platform types, so every
//! rule below is a unit test that runs anywhere, with no `vcan0` and no root.
//!
//! The format is fixed by the zenoh-pico implementation it interoperates with
//! (`link/transport/can.h` and `can_codec.c`), and is not ours to change
//! unilaterally. Pico also has a classic CAN mode with 8-byte frames; this link
//! is CAN FD only, and drops classic frames it hears.
//!
//! # Identifier
//!
//! One 29-bit extended identifier layout serves unicast and multicast. CAN
//! arbitrates on the identifier -- lowest wins -- so what goes in the high bits
//! is what the bus treats as most urgent, and the low bits are what a
//! controller's hardware filter selects on.
//!
//! ```text
//!   28..26  priority   zenoh priority (0 = control ... 7 = background)
//!   25..21  key id     the sender's ExprId for the topic, 0 = none
//!   20..16  reserved   zero
//!   15..8   destination node, 0xFF = every node (multicast)
//!    7..0   source node
//! ```
//!
//! # Segmentation
//!
//! A datagram of up to [`MTU`] bytes is far more than one frame, and is
//! segmented here the way Cyphal/CAN does it: the **last** byte of every frame
//! is a tail byte, and there is no flow control and no round trip.
//!
//! ```text
//!   bit 7    start of transfer
//!   bit 6    end of transfer
//!   bit 5    toggle: 1 on the first frame, alternating
//!   bit 4..0 transfer id
//! ```
//!
//! The stream carried is `length prefix | datagram | zero padding | CRC`. The
//! prefix is one byte below 128 and otherwise two (low seven bits first, bit 7
//! meaning "another byte follows"), because CAN FD frame lengths are quantised
//! (0..=8, 12, 16, 20, 24, 32, 48, 64) and the last frame is padded. A transfer
//! that needs more than one frame ends with a CRC-16/CCITT-FALSE over
//! everything before it, padding included, most significant byte first. A
//! datagram that fits a single frame has no CRC.

use core::fmt;

use zenoh_protocol::transport::BatchSize;

pub(crate) const SRC_SHIFT: u32 = 0;
pub(crate) const DST_SHIFT: u32 = 8;
pub(crate) const KEY_SHIFT: u32 = 21;
pub(crate) const PRIO_SHIFT: u32 = 26;

pub(crate) const NODE_MASK: u32 = 0xFF;
/// The destination of multicast frames.
pub(crate) const NODE_BROADCAST: u8 = 0xFF;
const KEY_MASK: u32 = 0x1F;
const PRIO_MASK: u32 = 0x07;

/// The highest node id a link may use: 0xFF is the broadcast address.
pub(crate) const NODE_MAX: u8 = 0xFE;

/// zenoh's `Priority::Data`, which is what pico sends everything at.
pub(crate) const PRIO_DEFAULT: u8 = 5;

pub(crate) const TAIL_SOT: u8 = 0x80;
pub(crate) const TAIL_EOT: u8 = 0x40;
pub(crate) const TAIL_TOGGLE: u8 = 0x20;
pub(crate) const TAIL_TID_MASK: u8 = 0x1F;

/// `CANFD_MAX_DLEN`, the biggest frame there is.
pub(crate) const FD_MAX_DLEN: usize = 64;

/// The largest datagram. Every peer must agree on it.
pub(crate) const MTU: BatchSize = 4095;

/// The most a transfer can occupy on the wire: length prefix, datagram, padding
/// and CRC.
const MAX_TRANSFER: usize = MTU as usize + 2 + 16 + 2;

/// How many transfers one receiver reassembles at once.
///
/// A transfer is keyed by (priority, key, source), so one node's urgent message
/// can arrive in the middle of its own bulk one. A unicast link hears one node;
/// a multicast one hears everybody, so it gets more.
pub(crate) const RX_SESSIONS_UNICAST: usize = 4;
pub(crate) const RX_SESSIONS_MULTICAST: usize = 16;

/// `sizeof(struct canfd_frame)`, which is the read and write size the kernel
/// uses for CAN FD. A 16-byte read is a classic frame and is not ours.
pub(crate) const CANFD_MTU_WIRE: usize = 72;

/// `CANFD_BRS` -- use the fast data phase.
pub(crate) const CANFD_BRS: u8 = 0x01;
/// `CANFD_FDF` -- this is a CAN FD frame, as opposed to a classic one. Newer
/// kernels want it on every frame written with a `canfd_frame`.
pub(crate) const CANFD_FDF: u8 = 0x04;

/// `CAN_EFF_FLAG`, which marks an identifier as 29-bit.
pub(crate) const CAN_EFF_FLAG: u32 = 0x8000_0000;
pub(crate) const CAN_EFF_MASK: u32 = 0x1FFF_FFFF;
const CAN_RTR_FLAG: u32 = 0x4000_0000;
const CAN_ERR_FLAG: u32 = 0x2000_0000;

/// The representable CAN FD frame lengths above 8.
const FD_DLC_STEPS: [usize; 7] = [12, 16, 20, 24, 32, 48, 64];

/// The identifier a frame goes out under.
pub(crate) const fn make_id(prio: u8, key: u8, dst: u8, src: u8) -> u32 {
    ((prio as u32 & PRIO_MASK) << PRIO_SHIFT)
        | ((key as u32 & KEY_MASK) << KEY_SHIFT)
        | ((dst as u32 & NODE_MASK) << DST_SHIFT)
        | ((src as u32 & NODE_MASK) << SRC_SHIFT)
}

pub(crate) const fn id_src(id: u32) -> u8 {
    ((id >> SRC_SHIFT) & NODE_MASK) as u8
}

#[cfg(test)]
pub(crate) const fn id_dst(id: u32) -> u8 {
    ((id >> DST_SHIFT) & NODE_MASK) as u8
}

/// What a hardware filter must match to receive frames addressed to `dst`:
/// the identifier, and the mask that selects the destination field (and the
/// extended-identifier flag, so standard frames never match).
pub(crate) const fn filter_for(dst: u8) -> (u32, u32) {
    (
        ((dst as u32) << DST_SHIFT) | CAN_EFF_FLAG,
        (NODE_MASK << DST_SHIFT) | CAN_EFF_FLAG,
    )
}

/// The identifier with the destination removed, which names a transfer: the
/// frames of one datagram share it, whoever they were addressed to.
const fn transfer_key(id: u32) -> u32 {
    id & !(NODE_MASK << DST_SHIFT)
}

/// `struct canfd_frame` from `<linux/can.h>`.
///
/// Declared here rather than taken from `libc` because `libc`'s version keeps
/// its reserved fields private, so it cannot be constructed. The layout is
/// asserted against `libc`'s in `sys.rs` on Linux.
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Frame {
    pub(crate) can_id: u32,
    pub(crate) len: u8,
    pub(crate) flags: u8,
    pub(crate) res0: u8,
    pub(crate) res1: u8,
    pub(crate) data: [u8; FD_MAX_DLEN],
}

impl Frame {
    pub(crate) const fn zeroed() -> Self {
        Frame {
            can_id: 0,
            len: 0,
            flags: 0,
            res0: 0,
            res1: 0,
            data: [0u8; FD_MAX_DLEN],
        }
    }

    /// The frame as the kernel writes and reads it.
    ///
    /// `Frame` is `repr(C)` with no interior padding -- 4 + 1 + 1 + 1 + 1 + 64
    /// is exactly 72, and 72 is a multiple of the 8-byte alignment -- so every
    /// byte of the struct is an initialised field byte.
    pub(crate) fn as_wire_bytes(&self) -> &[u8] {
        // SAFETY: `Frame` is `repr(C)` and padding-free, so it is valid to read
        // as a byte slice.
        unsafe { core::slice::from_raw_parts(self as *const Frame as *const u8, CANFD_MTU_WIRE) }
    }

    /// The frame's payload, tail byte included.
    pub(crate) fn payload(&self) -> &[u8] {
        &self.data[..(self.len as usize).min(FD_MAX_DLEN)]
    }

    /// The tail byte, if there is a payload at all.
    pub(crate) fn tail(&self) -> Option<u8> {
        self.payload().last().copied()
    }

    /// Whether this frame begins a transfer.
    pub(crate) fn is_start(&self) -> bool {
        self.tail().is_some_and(|t| t & TAIL_SOT != 0)
    }
}

/// Refusal reasons for [`encode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxError {
    /// The datagram is bigger than [`MTU`]. zenoh's transport never hands the
    /// link more than that, so this is a bug at the call site.
    TooLarge { len: usize },
}

impl fmt::Display for TxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TxError::TooLarge { len } => {
                write!(
                    f,
                    "datagram of {len} bytes exceeds the CAN link MTU of {MTU}"
                )
            }
        }
    }
}

/// CRC-16/CCITT-FALSE, one byte at a time: initial value `0xFFFF`, polynomial
/// `0x1021`, no reflection.
fn crc16(mut crc: u16, b: u8) -> u16 {
    crc ^= (b as u16) << 8;
    for _ in 0..8 {
        crc = if crc & 0x8000 != 0 {
            (crc << 1) ^ 0x1021
        } else {
            crc << 1
        };
    }
    crc
}

/// The smallest legal frame length that holds `n` bytes.
fn frame_len(n: usize) -> usize {
    if n <= 8 {
        return n;
    }
    FD_DLC_STEPS
        .iter()
        .copied()
        .find(|&step| n <= step)
        .unwrap_or(FD_MAX_DLEN)
}

/// The length prefix: one byte below 128, otherwise two.
fn put_len(len: usize) -> ([u8; 2], usize) {
    if len < 0x80 {
        ([len as u8, 0], 1)
    } else {
        ([0x80 | (len & 0x7F) as u8, (len >> 7) as u8], 2)
    }
}

/// Returns the length and the number of bytes it took, or `None` if malformed.
fn get_len(src: &[u8]) -> Option<(usize, usize)> {
    let first = *src.first()?;
    if first & 0x80 == 0 {
        return Some((first as usize, 1));
    }
    let second = *src.get(1)?;
    if second >= 0x80 {
        return None;
    }
    Some((((first & 0x7F) as usize) | ((second as usize) << 7), 2))
}

/// Segment `datagram` into the frames that carry it.
///
/// `id` is the 29-bit identifier every frame goes out under and `tid` the
/// transfer id, of which only the low 5 bits are used. Every frame is marked
/// as an extended-identifier one, and asks for the fast data phase.
pub(crate) fn encode(datagram: &[u8], id: u32, tid: u8) -> Result<Vec<Frame>, TxError> {
    if datagram.len() > MTU as usize {
        return Err(TxError::TooLarge {
            len: datagram.len(),
        });
    }

    let (prefix, plen) = put_len(datagram.len());
    let total = plen + datagram.len();
    let cap = FD_MAX_DLEN - 1;
    let tid = tid & TAIL_TID_MASK;

    // The stream is prefix | datagram | padding | crc, and the crc exists only
    // when it spans several frames. Padding is zeros, sized so that the last
    // frame is a legal length.
    let (pad, crc_len) = if total < FD_MAX_DLEN {
        (frame_len(total + 1) - (total + 1), 0)
    } else {
        let l0 = total + 2;
        let last = l0 - (l0.div_ceil(cap) - 1) * cap;
        (frame_len(last + 1) - (last + 1), 2)
    };
    let body = total + pad;
    let stream = body + crc_len;

    let byte_at = |p: usize, crc: u16| -> u8 {
        if p < plen {
            prefix[p]
        } else if p < total {
            datagram[p - plen]
        } else if p < body {
            0
        } else if p == body {
            (crc >> 8) as u8
        } else {
            (crc & 0xFF) as u8
        }
    };

    let mut crc = 0xFFFFu16;
    if crc_len != 0 {
        for p in 0..body {
            crc = crc16(crc, byte_at(p, 0));
        }
    }

    let mut frames = Vec::with_capacity(stream.div_ceil(cap));
    let mut toggle = TAIL_TOGGLE;
    let mut pos = 0;
    while pos < stream {
        let n = (stream - pos).min(cap);
        let mut frame = Frame::zeroed();
        frame.can_id = id | CAN_EFF_FLAG;
        for i in 0..n {
            frame.data[i] = byte_at(pos + i, crc);
        }
        let mut tail = toggle | tid;
        if pos == 0 {
            tail |= TAIL_SOT;
        }
        if pos + n == stream {
            tail |= TAIL_EOT;
        }
        frame.data[n] = tail;
        frame.len = (n + 1) as u8;
        frame.flags = CANFD_BRS | CANFD_FDF;
        frames.push(frame);

        pos += n;
        toggle ^= TAIL_TOGGLE;
    }
    Ok(frames)
}

/// Take the datagram out of a finished stream, or `None` if it does not hold
/// one that fits `out`.
fn deliver(stream: &[u8], out: &mut [u8]) -> Option<usize> {
    let (dlen, plen) = get_len(stream)?;
    if dlen > stream.len() - plen || dlen > out.len() {
        return None;
    }
    out[..dlen].copy_from_slice(&stream[plen..plen + dlen]);
    Some(dlen)
}

#[derive(Debug)]
struct Session {
    key: u32,
    /// For evicting the stalest slot.
    age: u64,
    tid: u8,
    /// The toggle bit the next frame must carry.
    toggle: u8,
    buf: Vec<u8>,
}

/// Reassembles the datagrams in the frames a receiver hears.
#[derive(Debug)]
pub(crate) struct Reassembler {
    max_sessions: usize,
    sessions: Vec<Session>,
    clock: u64,
}

impl Reassembler {
    pub(crate) fn new(max_sessions: usize) -> Self {
        Reassembler {
            max_sessions,
            sessions: Vec::new(),
            clock: 0,
        }
    }

    fn position(&self, key: u32) -> Option<usize> {
        self.sessions.iter().position(|s| s.key == key)
    }

    /// Feed one received frame. Returns the datagram's length once `out` holds
    /// it, or `None` when more frames are needed or the frame was dropped as
    /// stale or corrupt.
    ///
    /// The rules are applied in the same order as the zenoh-pico receiver, so
    /// the two implementations drop the same frames for the same reasons.
    pub(crate) fn decode(&mut self, id: u32, payload: &[u8], out: &mut [u8]) -> Option<usize> {
        let (&tail, data) = payload.split_last()?;
        let sot = tail & TAIL_SOT != 0;
        let eot = tail & TAIL_EOT != 0;
        let toggle = tail & TAIL_TOGGLE;
        let tid = tail & TAIL_TID_MASK;
        let key = transfer_key(id);

        if sot && eot {
            // A single frame. A transfer in flight under the same key is
            // abandoned.
            if let Some(i) = self.position(key) {
                self.sessions.swap_remove(i);
            }
            return if toggle != 0 {
                deliver(data, out)
            } else {
                None
            };
        }

        let index = if sot {
            if toggle == 0 {
                return None;
            }
            // A start replaces the transfer under its key, else takes a free
            // slot, else evicts the stalest one.
            let index = match self.position(key) {
                Some(i) => i,
                None if self.sessions.len() < self.max_sessions => {
                    self.sessions.push(Session {
                        key,
                        age: 0,
                        tid,
                        toggle: TAIL_TOGGLE,
                        buf: Vec::new(),
                    });
                    self.sessions.len() - 1
                }
                None => self
                    .sessions
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, s)| s.age)
                    .map(|(i, _)| i)?,
            };
            let s = &mut self.sessions[index];
            s.key = key;
            s.tid = tid;
            s.toggle = TAIL_TOGGLE;
            s.buf.clear();
            index
        } else {
            match self.position(key) {
                Some(i) if self.sessions[i].tid == tid && self.sessions[i].toggle == toggle => i,
                // A frame went missing, or this is the tail of something we
                // never saw the start of. Drop the transfer; the sender's next
                // one starts clean.
                Some(i) => {
                    self.sessions.swap_remove(i);
                    return None;
                }
                None => return None,
            }
        };

        self.clock += 1;
        let s = &mut self.sessions[index];
        if s.buf.len() + data.len() > MAX_TRANSFER {
            self.sessions.swap_remove(index);
            return None;
        }
        s.buf.extend_from_slice(data);
        s.toggle ^= TAIL_TOGGLE;
        s.age = self.clock;

        if !eot {
            return None;
        }
        let s = self.sessions.swap_remove(index);
        if s.buf.len() < 3 {
            return None;
        }
        let crc = s.buf.iter().fold(0xFFFFu16, |crc, &b| crc16(crc, b));
        if crc != 0 {
            tracing::debug!("CAN: transfer from node {} failed its CRC", id_src(id));
            return None;
        }
        deliver(&s.buf[..s.buf.len() - 2], out)
    }
}

/// Why a frame read from the socket was not handed to the link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RxDrop {
    /// Not a `canfd_frame`: a classic frame from some other device on the bus,
    /// a runt, or an error frame.
    NotAFrame { nread: usize },
    /// A standard-identifier, remote or error frame. Not ours.
    NotExtended,
    /// Our own transmission, heard back on a loopback-enabled interface.
    OwnFrame,
    /// A frame whose payload length is not a legal CAN FD length, or is empty.
    BadLength,
}

impl fmt::Display for RxDrop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RxDrop::NotAFrame { nread } => {
                write!(f, "read of {nread} bytes is not a CAN FD frame")
            }
            RxDrop::NotExtended => write!(f, "not an extended-identifier data frame"),
            RxDrop::OwnFrame => write!(f, "own transmission"),
            RxDrop::BadLength => write!(f, "empty or oversized payload"),
        }
    }
}

/// Decide whether a frame read from the socket is one a link may use. On
/// success, its identifier has the flag bits removed.
pub(crate) fn accept(frame: &Frame, nread: usize, own: u8) -> Result<u32, RxDrop> {
    // Only CAN FD. A 16-byte read is a classic frame, which on a shared bus is
    // some other device's traffic and never ours.
    if nread != CANFD_MTU_WIRE {
        return Err(RxDrop::NotAFrame { nread });
    }
    let id = frame.can_id;
    if id & CAN_EFF_FLAG == 0 || id & (CAN_RTR_FLAG | CAN_ERR_FLAG) != 0 {
        return Err(RxDrop::NotExtended);
    }
    let id = id & CAN_EFF_MASK;
    if id_src(id) == own {
        return Err(RxDrop::OwnFrame);
    }
    if frame.len == 0 || frame.len as usize > FD_MAX_DLEN {
        return Err(RxDrop::BadLength);
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn datagram(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 + 1) as u8).collect()
    }

    fn roundtrip(n: usize) {
        let d = datagram(n);
        let id = make_id(3, 0, 0xFF, 0x12);
        let frames = encode(&d, id, 9).unwrap();
        let mut rx = Reassembler::new(4);
        let mut out = vec![0u8; MTU as usize];
        let mut got = None;
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.can_id, id | CAN_EFF_FLAG);
            assert!(
                f.len as usize <= 8 || FD_DLC_STEPS.contains(&(f.len as usize)),
                "illegal frame length {} for a {n}-byte datagram",
                f.len
            );
            let r = rx.decode(f.can_id, f.payload(), &mut out);
            if i + 1 == frames.len() {
                got = r;
            } else {
                assert_eq!(r, None, "a datagram of {n} bytes finished early");
            }
        }
        assert_eq!(got, Some(n), "{n} bytes");
        assert_eq!(&out[..n], &d[..]);
    }

    #[test]
    fn every_size_round_trips() {
        for n in 0..=MTU as usize {
            roundtrip(n);
        }
    }

    #[test]
    fn small_datagrams_are_one_frame_without_a_crc() {
        let f = encode(&datagram(10), 0, 0).unwrap();
        assert_eq!(f.len(), 1);
        // prefix + 10 bytes + tail = 12, a legal length, so no padding
        assert_eq!(f[0].len, 12);
        assert_eq!(f[0].data[0], 10);
        assert_eq!(f[0].data[11], TAIL_SOT | TAIL_EOT | TAIL_TOGGLE);
    }

    #[test]
    fn padding_makes_the_last_frame_a_legal_length() {
        // prefix + 9 bytes + tail = 11, which is not a CAN FD length
        let f = encode(&datagram(9), 0, 0).unwrap();
        assert_eq!(f[0].len, 12);
        assert_eq!(f[0].data[10], 0, "padding is zeros");
    }

    #[test]
    fn a_long_prefix_is_two_bytes_low_seven_bits_first() {
        let f = encode(&datagram(300), 0, 0).unwrap();
        assert_eq!(f[0].data[0], 0x80 | (300 & 0x7F) as u8);
        assert_eq!(f[0].data[1], (300 >> 7) as u8);
    }

    #[test]
    fn the_toggle_alternates_and_the_ends_are_marked() {
        let f = encode(&datagram(400), 0, 0x1F).unwrap();
        assert!(f.len() > 2);
        for (i, frame) in f.iter().enumerate() {
            let tail = frame.tail().unwrap();
            assert_eq!(tail & TAIL_TID_MASK, 0x1F);
            assert_eq!(tail & TAIL_TOGGLE != 0, i % 2 == 0, "frame {i}");
            assert_eq!(tail & TAIL_SOT != 0, i == 0);
            assert_eq!(tail & TAIL_EOT != 0, i + 1 == f.len());
        }
    }

    #[test]
    fn the_transfer_id_wraps_at_five_bits() {
        let f = encode(&datagram(5), 0, 0x23).unwrap();
        assert_eq!(f[0].tail().unwrap() & TAIL_TID_MASK, 0x03);
    }

    #[test]
    fn a_datagram_over_the_mtu_is_refused() {
        assert_eq!(
            encode(&datagram(MTU as usize + 1), 0, 0),
            Err(TxError::TooLarge {
                len: MTU as usize + 1
            })
        );
    }

    /// Without the flag the kernel sends a standard frame with the identifier
    /// cut to 11 bits, and nothing zenoh-pico filters on would match.
    #[test]
    fn every_frame_is_an_extended_identifier_frame() {
        for f in encode(&datagram(500), make_id(5, 0, 2, 1), 0).unwrap() {
            assert_ne!(f.can_id & CAN_EFF_FLAG, 0);
            assert_eq!(f.can_id & CAN_EFF_MASK, make_id(5, 0, 2, 1));
        }
    }

    #[test]
    fn every_frame_asks_for_the_fast_data_phase() {
        for f in encode(&datagram(500), 0, 0).unwrap() {
            assert_eq!(f.flags, CANFD_BRS | CANFD_FDF);
        }
    }

    /// The CRC covers the padding too, and a flipped bit anywhere fails it.
    #[test]
    fn a_corrupt_transfer_is_dropped() {
        let d = datagram(300);
        let mut frames = encode(&d, 0x100, 0).unwrap();
        frames[2].data[5] ^= 0x04;
        let mut rx = Reassembler::new(4);
        let mut out = vec![0u8; MTU as usize];
        for f in &frames {
            assert_eq!(rx.decode(f.can_id, f.payload(), &mut out), None);
        }
    }

    #[test]
    fn a_lost_frame_drops_the_transfer_and_the_next_one_is_fine() {
        let id = make_id(5, 0, 1, 2);
        let mut frames = encode(&datagram(300), id, 0).unwrap();
        frames.remove(2);
        let mut rx = Reassembler::new(4);
        let mut out = vec![0u8; MTU as usize];
        for f in &frames {
            assert_eq!(rx.decode(f.can_id, f.payload(), &mut out), None);
        }
        for f in encode(&datagram(300), id, 1).unwrap() {
            let r = rx.decode(f.can_id, f.payload(), &mut out);
            if f.tail().unwrap() & TAIL_EOT != 0 {
                assert_eq!(r, Some(300));
            }
        }
    }

    #[test]
    fn the_tail_of_a_transfer_never_seen_is_ignored() {
        let frames = encode(&datagram(300), 0x100, 0).unwrap();
        let mut rx = Reassembler::new(4);
        let mut out = vec![0u8; MTU as usize];
        for f in &frames[1..] {
            assert_eq!(rx.decode(f.can_id, f.payload(), &mut out), None);
        }
    }

    /// An urgent message from a node can arrive in the middle of its own bulk
    /// one, and they are told apart by priority.
    #[test]
    fn transfers_interleave_when_their_identifiers_differ() {
        let bulk_id = make_id(6, 0, 0xFF, 1);
        let urgent_id = make_id(1, 0, 0xFF, 1);
        let bulk = encode(&datagram(400), bulk_id, 0).unwrap();
        let urgent = encode(&datagram(200), urgent_id, 0).unwrap();
        let mut rx = Reassembler::new(4);
        let mut out = vec![0u8; MTU as usize];
        let mut done = Vec::new();
        let mut a = bulk.iter();
        let mut b = urgent.iter();
        loop {
            let (fa, fb) = (a.next(), b.next());
            if fa.is_none() && fb.is_none() {
                break;
            }
            for f in fa.into_iter().chain(fb) {
                if let Some(n) = rx.decode(f.can_id, f.payload(), &mut out) {
                    done.push((f.can_id, n));
                }
            }
        }
        assert!(done.contains(&(bulk_id | CAN_EFF_FLAG, 400)));
        assert!(done.contains(&(urgent_id | CAN_EFF_FLAG, 200)));
    }

    /// The same node and priority, addressed differently, are one transfer to
    /// the receiver: destination is not part of the key.
    #[test]
    fn the_destination_is_not_part_of_the_transfer_key() {
        assert_eq!(
            transfer_key(make_id(5, 0, 0x01, 7)),
            transfer_key(make_id(5, 0, 0xFF, 7))
        );
        assert_ne!(
            transfer_key(make_id(5, 0, 0x01, 7)),
            transfer_key(make_id(5, 0, 0x01, 8))
        );
    }

    #[test]
    fn the_stalest_session_is_evicted_when_there_are_none_free() {
        let mut rx = Reassembler::new(2);
        let mut out = vec![0u8; MTU as usize];
        let starts: Vec<_> = (1..=3u8)
            .map(|src| encode(&datagram(300), make_id(5, 0, 0xFF, src), 0).unwrap())
            .collect();
        rx.decode(starts[0][0].can_id, starts[0][0].payload(), &mut out);
        rx.decode(starts[1][0].can_id, starts[1][0].payload(), &mut out);
        // Node 3 takes node 1's slot, the stalest.
        rx.decode(starts[2][0].can_id, starts[2][0].payload(), &mut out);
        assert!(rx.position(transfer_key(starts[0][0].can_id)).is_none());
        assert!(rx.position(transfer_key(starts[1][0].can_id)).is_some());
        assert!(rx.position(transfer_key(starts[2][0].can_id)).is_some());
    }

    #[test]
    fn a_datagram_bigger_than_the_buffer_is_dropped_not_truncated() {
        let frames = encode(&datagram(100), 0x100, 0).unwrap();
        let mut rx = Reassembler::new(4);
        let mut out = vec![0u8; 50];
        for f in &frames {
            assert_eq!(rx.decode(f.can_id, f.payload(), &mut out), None);
        }
    }

    #[test]
    fn the_identifier_layout() {
        let id = make_id(5, 17, 0x42, 0x07);
        assert_eq!(id, (5 << 26) | (17 << 21) | (0x42 << 8) | 0x07);
        assert_eq!(id_src(id), 0x07);
        assert_eq!(id_dst(id), 0x42);
        assert!(id <= CAN_EFF_MASK);
    }

    /// A lower identifier wins arbitration, so an urgent message from any node
    /// beats a bulk one from any other.
    #[test]
    fn priority_dominates_arbitration() {
        assert!(make_id(1, 0, 0xFF, 200) < make_id(5, 0, 0xFF, 1));
    }

    #[test]
    fn the_hardware_filter_selects_the_destination_and_extended_frames() {
        let (id, mask) = filter_for(0x42);
        assert_eq!(id, (0x42 << 8) | CAN_EFF_FLAG);
        assert_eq!(mask, (0xFF << 8) | CAN_EFF_FLAG);
        // Frame to 0x42, from anyone, at any priority
        assert_eq!((make_id(3, 9, 0x42, 5) | CAN_EFF_FLAG) & mask, id);
        // Frame to someone else
        assert_ne!((make_id(3, 9, 0x43, 5) | CAN_EFF_FLAG) & mask, id);
        // A standard frame with the same bits
        assert_ne!(((0x42 << 8) & CAN_EFF_MASK) & mask, id);
    }

    fn frame_with(id: u32, len: u8) -> Frame {
        let mut f = Frame::zeroed();
        f.can_id = id;
        f.len = len;
        f
    }

    #[test]
    fn classic_frames_are_not_ours() {
        let f = frame_with(make_id(5, 0, 1, 2) | CAN_EFF_FLAG, 8);
        assert_eq!(accept(&f, 16, 1), Err(RxDrop::NotAFrame { nread: 16 }));
    }

    #[test]
    fn standard_remote_and_error_frames_are_not_ours() {
        for id in [
            0x123,
            make_id(5, 0, 1, 2) | CAN_EFF_FLAG | CAN_RTR_FLAG,
            CAN_ERR_FLAG | CAN_EFF_FLAG,
        ] {
            let f = frame_with(id, 8);
            assert_eq!(
                accept(&f, CANFD_MTU_WIRE, 1),
                Err(RxDrop::NotExtended),
                "{id:#x}"
            );
        }
    }

    #[test]
    fn our_own_frames_are_skipped() {
        let f = frame_with(make_id(5, 0, 0xFF, 7) | CAN_EFF_FLAG, 8);
        assert_eq!(accept(&f, CANFD_MTU_WIRE, 7), Err(RxDrop::OwnFrame));
        assert_eq!(accept(&f, CANFD_MTU_WIRE, 8), Ok(make_id(5, 0, 0xFF, 7)));
    }

    #[test]
    fn empty_and_oversized_payloads_are_dropped() {
        let id = make_id(5, 0, 0xFF, 7) | CAN_EFF_FLAG;
        assert_eq!(
            accept(&frame_with(id, 0), CANFD_MTU_WIRE, 1),
            Err(RxDrop::BadLength)
        );
        assert_eq!(
            accept(&frame_with(id, 65), CANFD_MTU_WIRE, 1),
            Err(RxDrop::BadLength)
        );
    }
}
