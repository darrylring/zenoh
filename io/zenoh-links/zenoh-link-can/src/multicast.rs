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

use async_trait::async_trait;
use zenoh_link_commons::{LinkManagerMulticastTrait, LinkMulticast};
use zenoh_protocol::core::EndPoint;
use zenoh_result::ZResult;

/// The multicast side of the link: every frame goes to all nodes and every
/// node's frames are heard. There is no connect side and no accept.
#[derive(Debug, Default)]
pub struct LinkManagerMulticastCan;

#[async_trait]
impl LinkManagerMulticastTrait for LinkManagerMulticastCan {
    async fn new_link(&self, endpoint: &EndPoint) -> ZResult<LinkMulticast> {
        // Parse and validate on every platform, so a bad endpoint is reported
        // as a bad endpoint rather than as a missing platform.
        let ep = crate::CanEndpoint::parse(endpoint)?;
        new_link_inner(ep).await
    }
}

#[cfg(target_os = "linux")]
async fn new_link_inner(ep: crate::CanEndpoint) -> ZResult<LinkMulticast> {
    let link = imp::LinkMulticastCan::new(ep)?;
    Ok(LinkMulticast(std::sync::Arc::new(link)))
}

#[cfg(not(target_os = "linux"))]
async fn new_link_inner(ep: crate::CanEndpoint) -> ZResult<LinkMulticast> {
    zenoh_result::bail!(
        "CAN links need SocketCAN, which is a Linux kernel interface; \
         cannot open {:?} on this platform",
        ep.device
    )
}

#[cfg(target_os = "linux")]
mod imp {
    use std::{borrow::Cow, fmt, sync::Mutex};

    use async_trait::async_trait;
    use zenoh_core::zlock;
    use zenoh_link_commons::{LinkAuthId, LinkMulticastTrait};
    use zenoh_protocol::{
        core::{Locator, Priority},
        transport::BatchSize,
    };
    use zenoh_result::ZResult;

    use crate::{
        frame::{self, Reassembler},
        sys::CanSocket,
        CanEndpoint,
    };

    pub(super) struct LinkMulticastCan {
        socket: CanSocket,
        /// This node's own address on the bus.
        src_locator: Locator,
        /// Everybody, which is where a multicast link sends.
        dst_locator: Locator,
        endpoint: CanEndpoint,
        rx: Mutex<Reassembler>,
    }

    impl LinkMulticastCan {
        pub(super) fn new(endpoint: CanEndpoint) -> ZResult<LinkMulticastCan> {
            let socket = CanSocket::open(&endpoint, frame::NODE_BROADCAST)?;
            let src_locator = endpoint.node_locator(endpoint.id);
            let dst_locator = endpoint.broadcast_locator();

            tracing::debug!(
                "CAN multicast link on {:?}: node {}, MTU {}",
                endpoint.device,
                endpoint.id,
                frame::MTU
            );

            Ok(LinkMulticastCan {
                socket,
                src_locator,
                dst_locator,
                endpoint,
                rx: Mutex::new(Reassembler::new(frame::RX_SESSIONS_MULTICAST)),
            })
        }
    }

    #[async_trait]
    impl LinkMulticastTrait for LinkMulticastCan {
        fn get_mtu(&self) -> BatchSize {
            frame::MTU
        }

        fn get_src(&self) -> &Locator {
            &self.src_locator
        }

        fn get_dst(&self) -> &Locator {
            &self.dst_locator
        }

        fn get_auth_id(&self) -> &LinkAuthId {
            &LinkAuthId::Can
        }

        /// CAN is reliable at frame level -- CRC, ACK slot, automatic
        /// retransmission -- but not end to end: controller buffers overrun and
        /// a bus-off condition drops everything.
        fn is_reliable(&self) -> bool {
            crate::IS_RELIABLE
        }

        async fn write(&self, buffer: &[u8]) -> ZResult<usize> {
            self.socket
                .send(buffer, frame::NODE_BROADCAST, frame::PRIO_DEFAULT)
                .await
        }

        /// A datagram link writes one datagram or fails; there is no partial
        /// write to loop over. zenoh's transport never hands the link more than
        /// its MTU, because the transmission pipeline clamps to it and
        /// fragments above it.
        async fn write_all(&self, buffer: &[u8]) -> ZResult<()> {
            self.write(buffer).await.map(|_| ())
        }

        /// A CAN identifier **is** the bus priority, so this is the one place
        /// the batch's priority belongs on the wire.
        async fn write_all_with_priority(&self, buffer: &[u8], priority: Priority) -> ZResult<()> {
            self.socket
                .send(buffer, frame::NODE_BROADCAST, priority as u8)
                .await
                .map(|_| ())
        }

        async fn read<'a>(&'a self, buffer: &mut [u8]) -> ZResult<(usize, Cow<'a, Locator>)> {
            loop {
                let (id, f) = self.socket.recv().await?;
                let n = zlock!(self.rx).decode(id, f.payload(), buffer);
                if let Some(n) = n {
                    let sender = frame::id_src(id);
                    return Ok((n, Cow::Owned(self.endpoint.node_locator(sender))));
                }
            }
        }

        async fn close(&self) -> ZResult<()> {
            // The socket is closed when the link is dropped; a CAN bus has no
            // group to leave and no connection to shut down.
            tracing::trace!("Closing CAN link: {self}");
            Ok(())
        }
    }

    impl fmt::Display for LinkMulticastCan {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{} => {}", self.src_locator, self.dst_locator)
        }
    }

    impl fmt::Debug for LinkMulticastCan {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Can")
                .field("device", &self.endpoint.device)
                .field("node", &self.socket.node())
                .field("mtu", &frame::MTU)
                .finish()
        }
    }
}
