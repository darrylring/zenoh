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

//! The Linux SocketCAN binding.
//!
//! This is also the binding that talks to a virtual `vcan0`, which is what
//! makes the link testable with no hardware:
//!
//! ```sh
//! sudo modprobe vcan
//! sudo ip link add dev vcan0 type vcan
//! sudo ip link set up vcan0
//! ```

use std::{
    ffi::CString,
    io, mem,
    os::fd::{AsRawFd, RawFd},
    time::{Duration, Instant},
};

use tokio::{io::unix::AsyncFd, sync::Mutex};
use zenoh_result::{bail, zerror, ZResult};

use crate::{
    frame::{self, Frame, RxDrop},
    CanEndpoint,
};

/// How long a datagram may wait for room in the controller's transmit queue
/// before it is dropped.
const TX_QUEUE_PATIENCE: Duration = Duration::from_millis(250);
/// How long to wait between tries when the transmit queue is full.
const TX_QUEUE_RETRY: Duration = Duration::from_micros(500);

/// An owned CAN socket descriptor.
struct RawCan {
    fd: RawFd,
}

impl AsRawFd for RawCan {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for RawCan {
    fn drop(&mut self) {
        // SAFETY: `fd` is owned by this value and closed exactly once.
        unsafe { libc::close(self.fd) };
    }
}

/// Ask for a receive buffer of `requested` bytes and report what was granted.
///
/// The kernel clamps the request to `net.core.rmem_max` without telling anyone,
/// and then reports back double what it stored, so the only honest thing to do
/// is read the value back and say when it fell short.
/// Why a classic interface is refused, and the way out.
fn classic_refusal(device: &str) -> String {
    format!(
        "This link is CAN FD only. Enable FD on the interface (`ip link set {device} type can \
         bitrate 500000 dbitrate 2000000 fd on`; a vcan: `ip link set {device} mtu 72`). A \
         PCAN-USB (not FD) cannot do it: that adapter is classic CAN only."
    )
}

/// The interface's MTU: `CANFD_MTU` (72) once FD is on, `CAN_MTU` (16) otherwise.
fn interface_mtu(raw: &RawCan, name: &CString) -> io::Result<libc::c_int> {
    // SAFETY: an all-zero `ifreq` is a valid value of this plain C struct.
    let mut ifr: libc::ifreq = unsafe { mem::zeroed() };
    let bytes = name.as_bytes();
    if bytes.len() >= ifr.ifr_name.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interface name too long",
        ));
    }
    for (dst, src) in ifr.ifr_name.iter_mut().zip(bytes) {
        *dst = *src as libc::c_char;
    }
    // SAFETY: `ifr` is a valid `ifreq` whose name is NUL-terminated (zeroed
    // beyond `bytes`), and it outlives the call.
    let rc = unsafe { libc::ioctl(raw.fd, libc::SIOCGIFMTU, &mut ifr) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: SIOCGIFMTU wrote the `ifru_mtu` member of the union.
    Ok(unsafe { ifr.ifr_ifru.ifru_mtu })
}

fn set_rcvbuf(raw: &RawCan, requested: u32, device: &str) -> ZResult<()> {
    let value = requested as libc::c_int;
    // SAFETY: `value` outlives the call and its length is its own size.
    let rc = unsafe {
        libc::setsockopt(
            raw.fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &value as *const libc::c_int as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        let e = io::Error::last_os_error();
        bail!("CAN: setting the receive buffer on {device:?} to {requested} bytes failed: {e}");
    }

    let mut granted: libc::c_int = 0;
    let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `granted` and `len` are valid for the sizes given.
    let rc = unsafe {
        libc::getsockopt(
            raw.fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut granted as *mut libc::c_int as *mut libc::c_void,
            &mut len,
        )
    };
    if rc < 0 {
        // Not fatal: the buffer was set, we simply cannot report on it.
        tracing::debug!("CAN: could not read back the receive buffer on {device:?}");
        return Ok(());
    }

    // The kernel reports twice what it stored, the second half being its own
    // bookkeeping allowance.
    let effective = (granted as u32) / 2;
    if effective < requested {
        tracing::warn!(
            "CAN: asked {device:?} for a {requested}-byte receive buffer but got {effective};              net.core.rmem_max is the ceiling. Raise it with              `sysctl -w net.core.rmem_max={requested}` if bursts are being dropped."
        );
    } else {
        tracing::debug!("CAN: receive buffer on {device:?} is {effective} bytes");
    }
    Ok(())
}

pub(crate) struct CanSocket {
    io: AsyncFd<RawCan>,
    /// This node, which is the source of every frame it sends.
    node: u8,
    /// The transfer id of the next datagram. Held while a datagram is sent,
    /// so that two are never interleaved.
    tid: Mutex<u8>,
}

impl CanSocket {
    /// Opens the interface, hearing only frames addressed to `rx_dst`: this node
    /// for a unicast link, everybody ([`frame::NODE_BROADCAST`]) for a multicast
    /// one.
    pub(crate) fn open(ep: &CanEndpoint, rx_dst: u8) -> ZResult<CanSocket> {
        // Bit rates are set out of band on Linux (`ip link set can0 type can
        // bitrate ...`) and a virtual interface has none at all, so `bitrate`
        // and `dbitrate` are advisory here. CAN FD versus classic is the
        // interface's, read from its MTU below.
        let fd = unsafe {
            libc::socket(
                libc::PF_CAN,
                libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                libc::CAN_RAW,
            )
        };
        if fd < 0 {
            let e = io::Error::last_os_error();
            bail!("CAN: socket(PF_CAN, SOCK_RAW, CAN_RAW) failed: {e}");
        }
        // From here on `raw` owns the descriptor, so every early return closes it.
        let raw = RawCan { fd };

        let name = CString::new(ep.device.as_str())
            .map_err(|e| zerror!("CAN: interface name {:?} is not usable: {e}", ep.device))?;
        // SAFETY: `name` is a valid NUL-terminated string for the duration of
        // the call.
        let ifindex = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if ifindex == 0 {
            let e = io::Error::last_os_error();
            bail!(
                "CAN: no such interface {:?}: {e}. On Linux a virtual bus is created with \
                 `sudo ip link add dev {} type vcan && sudo ip link set up {}`",
                ep.device,
                ep.device,
                ep.device
            );
        }

        // Have the kernel drop everything not addressed to us, so the CPU
        // never sees it. The flag in the mask leaves out standard-identifier
        // frames, which are someone else's.
        let (filter_id, filter_mask) = frame::filter_for(rx_dst);
        let filter = libc::can_filter {
            can_id: filter_id,
            can_mask: filter_mask,
        };
        // SAFETY: `filter` outlives the call and its length is its own size.
        let rc = unsafe {
            libc::setsockopt(
                raw.fd,
                libc::SOL_CAN_RAW,
                libc::CAN_RAW_FILTER,
                &filter as *const libc::can_filter as *const libc::c_void,
                mem::size_of::<libc::can_filter>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            let e = io::Error::last_os_error();
            bail!(
                "CAN: setting the receive filter on {:?} failed: {e}",
                ep.device
            );
        }

        // CAN FD is required, not preferred. This link does not
        // send or hear classic frames.
        //
        // `CAN_RAW_FD_FRAMES` alone does NOT detect one: the kernel accepts the
        // option on any CAN_RAW socket and checks the device only at send,
        // where an FD-sized write to a classic interface fails with EINVAL. So
        // the option is set for the FD frames this link writes, and the
        // interface's MTU is what decides.
        let enable: libc::c_int = 1;
        // SAFETY: `enable` outlives the call and its length is its own size.
        let rc = unsafe {
            libc::setsockopt(
                raw.fd,
                libc::SOL_CAN_RAW,
                libc::CAN_RAW_FD_FRAMES,
                &enable as *const libc::c_int as *const libc::c_void,
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            let e = io::Error::last_os_error();
            bail!(
                "CAN: interface {:?} refused CAN FD frames ({e}). {}",
                ep.device,
                classic_refusal(&ep.device)
            );
        }
        let if_mtu = interface_mtu(&raw, &name)
            .map_err(|e| zerror!("CAN: reading the MTU of {:?} failed: {e}", ep.device))?;
        if if_mtu != frame::CANFD_MTU_WIRE as libc::c_int {
            bail!(
                "CAN: interface {:?} is classic CAN (MTU {if_mtu}, not {}). {}",
                ep.device,
                frame::CANFD_MTU_WIRE,
                classic_refusal(&ep.device)
            );
        }

        // Frames that arrive faster than the link drains them are dropped by the
        // kernel, silently, before the link ever sees them. A real bus cannot
        // outrun the reader -- 2 Mbit/s of CAN FD is under 2 800 frames per
        // second -- but a virtual interface has no bit rate at all, and a burst
        // over `vcan` will overrun the default buffer. Measured: a 4 KiB
        // payload, which is 71 frames, lost 31% of messages on the default
        // buffer and none of them on 8 MiB.
        if let Some(requested) = ep.so_rcvbuf {
            set_rcvbuf(&raw, requested, &ep.device)?;
        }

        let mut addr: libc::sockaddr_can = unsafe { mem::zeroed() };
        addr.can_family = libc::AF_CAN as libc::sa_family_t;
        addr.can_ifindex = ifindex as libc::c_int;
        // SAFETY: `addr` is a fully initialised `sockaddr_can` and the length
        // passed is its own size.
        let rc = unsafe {
            libc::bind(
                raw.fd,
                &addr as *const libc::sockaddr_can as *const libc::sockaddr,
                mem::size_of::<libc::sockaddr_can>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            let e = io::Error::last_os_error();
            bail!("CAN: binding to {:?} failed: {e}", ep.device);
        }

        let io = AsyncFd::new(raw).map_err(|e| {
            zerror!(
                "CAN: registering {:?} with the runtime failed: {e}",
                ep.device
            )
        })?;

        Ok(CanSocket {
            io,
            node: ep.id,
            tid: Mutex::new(0),
        })
    }

    /// This node's id, the source of every frame it sends.
    pub(crate) fn node(&self) -> u8 {
        self.node
    }

    /// Write one frame, waiting for the socket to be writable.
    async fn write_frame(&self, f: &Frame) -> ZResult<()> {
        let bytes = f.as_wire_bytes();
        let deadline = Instant::now() + TX_QUEUE_PATIENCE;

        loop {
            let mut guard = self
                .io
                .writable()
                .await
                .map_err(|e| zerror!("CAN: waiting to write failed: {e}"))?;

            let attempt = guard.try_io(|inner| {
                // SAFETY: `bytes` is a valid readable region of its own length.
                let n = unsafe {
                    libc::write(
                        inner.as_raw_fd(),
                        bytes.as_ptr() as *const libc::c_void,
                        bytes.len(),
                    )
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            });

            match attempt {
                // The readiness was stale; wait for it again.
                Err(_would_block) => continue,
                Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => continue,
                Ok(Err(e)) if e.raw_os_error() == Some(libc::ENOBUFS) => {
                    // The controller's transmit queue is full, which on a bus
                    // means frames are going out as fast as the bus takes them
                    // (or arbitration is being lost to higher priority
                    // traffic). A datagram is several frames, so dropping the
                    // frame would waste the ones already sent: wait for room,
                    // for a while. Raising `txqueuelen` on the interface helps.
                    if Instant::now() >= deadline {
                        return Err(zerror!("CAN: the transmit queue stayed full").into());
                    }
                    tokio::time::sleep(TX_QUEUE_RETRY).await;
                }
                Ok(Err(e)) => bail!("CAN: write failed: {e}"),
                Ok(Ok(n)) if n == bytes.len() => return Ok(()),
                Ok(Ok(n)) => bail!("CAN: short write of {n} bytes, expected {}", bytes.len()),
            }
        }
    }

    /// Write one datagram of up to [`frame::MTU`] bytes, in as many frames as it
    /// takes, to `dst`. `priority` is zenoh's own numbering, `Control` at 0
    /// through `Background` at 7, and goes in the identifier.
    ///
    /// Returns the number of datagram bytes accepted. A datagram that cannot be
    /// sent because the bus is saturated is dropped, which is what a best-effort
    /// link does; it is reported as sent so that it does not fail the transport
    /// over a transient condition.
    pub(crate) async fn send(&self, datagram: &[u8], dst: u8, priority: u8) -> ZResult<usize> {
        self.send_with_id(datagram, frame::make_id(priority, 0, dst, self.node))
            .await
    }

    /// [`send`](Self::send) under an identifier of the caller's making, as the
    /// control messages need.
    pub(crate) async fn send_with_id(&self, datagram: &[u8], id: u32) -> ZResult<usize> {
        let dst = frame::id_dst(id);

        // One transfer at a time. The frames of two datagrams under the same
        // identifier would interleave and neither would reassemble, so the
        // lock is held for the whole of one. It also hands out the transfer id.
        let mut tid = self.tid.lock().await;
        let frames = frame::encode(datagram, id, *tid).map_err(|e| zerror!("CAN: {e}"))?;
        *tid = tid.wrapping_add(1) & frame::TAIL_TID_MASK;

        tracing::trace!(
            "CAN tx {} bytes in {} frames to node {dst}",
            datagram.len(),
            frames.len()
        );
        for f in &frames {
            if let Err(e) = self.write_frame(f).await {
                tracing::debug!(
                    "CAN: dropping a {}-byte datagram, its frames did not all go: {e}",
                    datagram.len()
                );
                break;
            }
        }
        Ok(datagram.len())
    }

    /// Read one frame this node may use, and its identifier. Frames this node
    /// sent itself, and ones that are not CAN FD data frames with an extended
    /// identifier, are skipped rather than returned.
    pub(crate) async fn recv(&self) -> ZResult<(u32, Frame)> {
        loop {
            let mut guard = self
                .io
                .readable()
                .await
                .map_err(|e| zerror!("CAN: waiting to read failed: {e}"))?;

            let mut f = Frame::zeroed();
            let attempt = guard.try_io(|inner| {
                // SAFETY: `f` is a valid writable region of `size_of::<Frame>()`
                // bytes, which is the largest frame the kernel can return.
                let n = unsafe {
                    libc::read(
                        inner.as_raw_fd(),
                        &mut f as *mut Frame as *mut libc::c_void,
                        mem::size_of::<Frame>(),
                    )
                };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            });

            let nread = match attempt {
                Err(_would_block) => continue,
                Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => continue,
                Ok(Err(e)) => bail!("CAN: read failed: {e}"),
                Ok(Ok(n)) => n,
            };

            // A raw CAN socket has no end of stream, so a zero-length read is
            // the descriptor going away. Treating it as a malformed frame would
            // spin: the fd stays readable and the loop would never block.
            if nread == 0 {
                bail!("CAN: the socket reported end of file");
            }

            match frame::accept(&f, nread, self.node) {
                Ok(id) => return Ok((id, f)),
                // Expected on any shared bus, and on a loopback-enabled
                // interface every transmission comes back to us.
                Err(RxDrop::OwnFrame) | Err(RxDrop::NotAFrame { .. }) => continue,
                // A malformed frame is worth a line, but not worth failing the
                // link over: the bus is shared and anyone may put anything on it.
                Err(drop) => {
                    tracing::trace!("CAN: dropping a frame with id {:#x}: {drop}", f.can_id);
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Frame` stands in for `libc::canfd_frame`, so it has to be the same
    /// shape.
    #[test]
    fn the_frame_matches_the_kernels() {
        assert_eq!(mem::size_of::<Frame>(), mem::size_of::<libc::canfd_frame>());
        assert_eq!(mem::size_of::<Frame>(), frame::CANFD_MTU_WIRE);
        assert_eq!(
            mem::align_of::<Frame>(),
            mem::align_of::<libc::canfd_frame>()
        );
        assert_eq!(libc::CAN_EFF_FLAG, frame::CAN_EFF_FLAG);
        assert_eq!(libc::CAN_EFF_MASK, frame::CAN_EFF_MASK);
        assert_eq!(libc::CANFD_BRS as u8, frame::CANFD_BRS);
    }
}
