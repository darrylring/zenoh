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
use zenoh_link_commons::{
    ConstructibleLinkManagerUnicast, LinkManagerUnicastTrait, LinkUnicast, NewLinkChannelSender,
};
use zenoh_protocol::core::{EndPoint, Locator};
use zenoh_result::ZResult;

/// The unicast side of the link: a link talks to one node, `peer`.
///
/// There is no connection on a CAN bus, so connecting to a locator is only
/// opening the interface and addressing every frame to `peer`. Listening is
/// opening it to hear every frame addressed to this node, and making a link of
/// every node that starts talking, the way a UDP listener makes one of every
/// address.
pub struct LinkManagerUnicastCan {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    manager: NewLinkChannelSender,
    #[cfg(target_os = "linux")]
    listeners: imp::Listeners,
}

impl std::fmt::Debug for LinkManagerUnicastCan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkManagerUnicastCan")
            .field("manager", &self.manager)
            .finish_non_exhaustive()
    }
}

impl LinkManagerUnicastCan {
    pub fn new(manager: NewLinkChannelSender) -> Self {
        Self {
            manager,
            #[cfg(target_os = "linux")]
            listeners: imp::Listeners::default(),
        }
    }
}

impl ConstructibleLinkManagerUnicast<()> for LinkManagerUnicastCan {
    fn new(new_link_sender: NewLinkChannelSender, _: ()) -> ZResult<Self> {
        Ok(Self::new(new_link_sender))
    }
}

#[cfg(target_os = "linux")]
#[async_trait]
impl LinkManagerUnicastTrait for LinkManagerUnicastCan {
    async fn new_link(&self, endpoint: EndPoint) -> ZResult<LinkUnicast> {
        // Parse and validate on every platform, so a bad endpoint is reported
        // as a bad endpoint rather than as a missing platform.
        let ep = crate::CanEndpoint::parse(&endpoint)?;
        imp::new_link(ep)
    }

    async fn new_listener(&self, endpoint: EndPoint) -> ZResult<Locator> {
        let ep = crate::CanEndpoint::parse(&endpoint)?;
        self.listeners.add(endpoint, ep, self.manager.clone()).await
    }

    async fn del_listener(&self, endpoint: &EndPoint) -> ZResult<()> {
        let ep = crate::CanEndpoint::parse(endpoint)?;
        self.listeners.del(&ep).await
    }

    async fn get_listeners(&self) -> Vec<EndPoint> {
        self.listeners.endpoints()
    }

    async fn get_locators(&self) -> Vec<Locator> {
        self.listeners.locators()
    }

    async fn get_locators_noloopback(&self) -> Vec<Locator> {
        self.listeners.locators()
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait]
impl LinkManagerUnicastTrait for LinkManagerUnicastCan {
    async fn new_link(&self, endpoint: EndPoint) -> ZResult<LinkUnicast> {
        let ep = crate::CanEndpoint::parse(&endpoint)?;
        no_platform(&ep)
    }

    async fn new_listener(&self, endpoint: EndPoint) -> ZResult<Locator> {
        let ep = crate::CanEndpoint::parse(&endpoint)?;
        no_platform(&ep)
    }

    async fn del_listener(&self, endpoint: &EndPoint) -> ZResult<()> {
        let ep = crate::CanEndpoint::parse(endpoint)?;
        no_platform(&ep)
    }

    async fn get_listeners(&self) -> Vec<EndPoint> {
        vec![]
    }

    async fn get_locators(&self) -> Vec<Locator> {
        vec![]
    }

    async fn get_locators_noloopback(&self) -> Vec<Locator> {
        vec![]
    }
}

#[cfg(not(target_os = "linux"))]
fn no_platform<T>(ep: &crate::CanEndpoint) -> ZResult<T> {
    zenoh_result::bail!(
        "CAN links need SocketCAN, which is a Linux kernel interface; \
         cannot open {:?} on this platform",
        ep.device
    )
}

#[cfg(target_os = "linux")]
mod imp {
    use std::{
        collections::HashMap,
        fmt,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    #[cfg(feature = "uring")]
    use std::os::fd::RawFd;

    use async_trait::async_trait;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use zenoh_core::zlock;
    use zenoh_link_commons::{LinkAuthId, LinkUnicast, LinkUnicastTrait, NewLinkChannelSender};
    use zenoh_protocol::{
        core::{EndPoint, Locator, Priority},
        transport::BatchSize,
    };
    use zenoh_result::{bail, zerror, ZResult};

    use crate::{
        frame::{self, Frame, Reassembler},
        ids::{Allocator, ID_TTL},
        sys::CanSocket,
        CanEndpoint,
    };

    /// Frames a listener holds for one node's link, waiting to be read. A
    /// full-size datagram is about 70 of them.
    const INBOX_FRAMES: usize = 1024;

    /// How long the accept task waits after the socket fails, before trying it
    /// again.
    const ACCEPT_THROTTLE: Duration = Duration::from_millis(100);

    type Inbox = flume::Sender<(u32, Frame)>;
    type Peers = Arc<Mutex<HashMap<u8, Inbox>>>;

    /// Where a link's frames come from.
    enum Source {
        /// A link opened by connecting, which owns an interface of its own and
        /// reads it directly, keeping only its peer's frames.
        Socket,
        /// A link a listener made, which is handed its peer's frames.
        Inbox {
            frames: flume::Receiver<(u32, Frame)>,
            peers: Peers,
        },
    }

    pub(crate) struct LinkUnicastCan {
        socket: Arc<CanSocket>,
        peer: u8,
        src_locator: Locator,
        dst_locator: Locator,
        device: String,
        source: Source,
        rx: Mutex<Reassembler>,
    }

    impl LinkUnicastCan {
        fn new(socket: Arc<CanSocket>, ep: &CanEndpoint, peer: u8, source: Source) -> Self {
            LinkUnicastCan {
                socket,
                peer,
                src_locator: ep.node_locator(ep.id),
                dst_locator: ep.node_locator(peer),
                device: ep.device.clone(),
                source,
                rx: Mutex::new(Reassembler::new(frame::RX_SESSIONS_UNICAST)),
            }
        }

        async fn next_frame(&self) -> ZResult<(u32, Frame)> {
            match &self.source {
                Source::Socket => loop {
                    let (id, f) = self.socket.recv().await?;
                    // Someone else wrote to this node
                    if frame::id_src(id) == self.peer {
                        return Ok((id, f));
                    }
                },
                Source::Inbox { frames, .. } => frames
                    .recv_async()
                    .await
                    .map_err(|_| zerror!("CAN link to node {} is closed", self.peer).into()),
            }
        }
    }

    /// Opens an interface, and a link on it that talks to the endpoint's peer.
    pub(super) fn new_link(ep: CanEndpoint) -> ZResult<LinkUnicast> {
        let peer = ep.require_peer()?;
        let socket = Arc::new(CanSocket::open(&ep, ep.id)?);
        tracing::debug!(
            "CAN unicast link on {:?}: node {} to node {peer}",
            ep.device,
            ep.id
        );
        let link = LinkUnicastCan::new(socket, &ep, peer, Source::Socket);
        Ok(LinkUnicast::from(
            Arc::new(link) as Arc<dyn LinkUnicastTrait>
        ))
    }

    #[async_trait]
    impl LinkUnicastTrait for LinkUnicastCan {
        async fn close(&self) -> ZResult<()> {
            tracing::trace!("Closing CAN link: {self}");
            if let Source::Inbox { peers, .. } = &self.source {
                // Stop the listener making frames for this node. The next
                // transfer that starts from it makes a new link.
                zlock!(peers).remove(&self.peer);
            }
            Ok(())
        }

        async fn write(&self, buffer: &[u8], priority: Option<Priority>) -> ZResult<usize> {
            let priority = priority.map_or(frame::PRIO_DEFAULT, |p| p as u8);
            self.socket.send(buffer, self.peer, priority).await
        }

        /// A datagram link writes one datagram or fails; there is no partial
        /// write to loop over.
        async fn write_all(&self, buffer: &[u8], priority: Option<Priority>) -> ZResult<()> {
            self.write(buffer, priority).await.map(|_| ())
        }

        async fn read(&self, buffer: &mut [u8], _priority: Option<Priority>) -> ZResult<usize> {
            loop {
                let (id, f) = self.next_frame().await?;
                let n = zlock!(self.rx).decode(id, f.payload(), buffer);
                tracing::trace!("CAN rx frame id {id:#x} len {} -> {n:?}", f.len);
                if let Some(n) = n {
                    return Ok(n);
                }
            }
        }

        async fn read_exact(&self, buffer: &mut [u8], priority: Option<Priority>) -> ZResult<()> {
            let mut read = 0;
            while read < buffer.len() {
                read += self.read(&mut buffer[read..], priority).await?;
            }
            Ok(())
        }

        fn get_src(&self) -> &Locator {
            &self.src_locator
        }

        fn get_dst(&self) -> &Locator {
            &self.dst_locator
        }

        fn get_mtu(&self) -> BatchSize {
            frame::MTU
        }

        fn get_interface_names(&self) -> Vec<String> {
            vec![self.device.clone()]
        }

        fn is_reliable(&self) -> bool {
            crate::IS_RELIABLE
        }

        fn is_streamed(&self) -> bool {
            false
        }

        fn get_auth_id(&self) -> &LinkAuthId {
            &LinkAuthId::Can
        }

        #[cfg(feature = "uring")]
        fn get_fd(&self) -> ZResult<RawFd> {
            bail!("FD unavailable for a CAN link")
        }
    }

    impl fmt::Display for LinkUnicastCan {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{} => {}", self.src_locator, self.dst_locator)
        }
    }

    impl fmt::Debug for LinkUnicastCan {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Can")
                .field("device", &self.device)
                .field("node", &self.socket.node())
                .field("peer", &self.peer)
                .finish()
        }
    }

    struct Listener {
        endpoint: EndPoint,
        token: CancellationToken,
        handle: JoinHandle<ZResult<()>>,
    }

    /// The listeners of one manager, by the interface and node they hear.
    #[derive(Default)]
    pub(super) struct Listeners {
        listeners: Arc<Mutex<HashMap<(String, u8), Listener>>>,
    }

    impl Listeners {
        pub(super) async fn add(
            &self,
            endpoint: EndPoint,
            ep: CanEndpoint,
            manager: NewLinkChannelSender,
        ) -> ZResult<Locator> {
            let key = (ep.device.clone(), ep.id);
            if zlock!(self.listeners).contains_key(&key) {
                bail!("CAN node {} is already listening on {:?}", ep.id, ep.device);
            }

            let socket = Arc::new(CanSocket::open(&ep, ep.id)?);
            let token = CancellationToken::new();
            let locator = endpoint.to_locator();

            tracing::debug!(
                "Ready to accept CAN connections on {:?} as node {}",
                ep.device,
                ep.id
            );
            let task = {
                let token = token.clone();
                let listeners = self.listeners.clone();
                let key = key.clone();
                async move {
                    let res = accept_task(socket, ep, token, manager).await;
                    zlock!(listeners).remove(&key);
                    res
                }
            };
            let handle = zenoh_runtime::ZRuntime::Acceptor.spawn(task);

            zlock!(self.listeners).insert(
                key,
                Listener {
                    endpoint,
                    token,
                    handle,
                },
            );
            Ok(locator)
        }

        pub(super) async fn del(&self, ep: &CanEndpoint) -> ZResult<()> {
            let listener = zlock!(self.listeners)
                .remove(&(ep.device.clone(), ep.id))
                .ok_or_else(|| {
                    zerror!(
                        "Can not delete the listener because it has not been found: node {} on {:?}",
                        ep.id,
                        ep.device
                    )
                })?;
            listener.token.cancel();
            listener.handle.await?
        }

        pub(super) fn endpoints(&self) -> Vec<EndPoint> {
            zlock!(self.listeners)
                .values()
                .map(|l| l.endpoint.clone())
                .collect()
        }

        pub(super) fn locators(&self) -> Vec<Locator> {
            zlock!(self.listeners)
                .values()
                .map(|l| l.endpoint.to_locator())
                .collect()
        }
    }

    /// Answers a node that is asking for an id, and ignores anything else that
    /// is not data.
    async fn answer_control(
        socket: &CanSocket,
        ids: &mut Allocator,
        asks: &mut Reassembler,
        id: u32,
        f: &Frame,
    ) {
        if frame::id_kind(id) != frame::KIND_ID_REQUEST
            || frame::id_src(id) != frame::NODE_UNASSIGNED
        {
            return;
        }
        let mut buf = [0u8; frame::UID_LEN + 1];
        let Some(frame::UID_LEN) = asks.decode(id, f.payload(), &mut buf) else {
            return;
        };
        let mut uid = [0u8; frame::UID_LEN];
        uid.copy_from_slice(&buf[..frame::UID_LEN]);

        let Some(given) = ids.assign(uid, Instant::now()) else {
            tracing::warn!("CAN: every node id is in use, cannot give one to {uid:02x?}");
            return;
        };
        tracing::debug!("CAN: node {given} for {uid:02x?}");

        let mut reply = [0u8; frame::UID_LEN + 1];
        reply[..frame::UID_LEN].copy_from_slice(&uid);
        reply[frame::UID_LEN] = given;
        let reply_id = frame::make_ctl_id(
            frame::KIND_ID_ASSIGN,
            frame::uid_key(&uid),
            frame::NODE_UNASSIGNED,
            socket.node(),
        );
        if let Err(e) = socket.send_with_id(&reply, reply_id).await {
            tracing::debug!("CAN: could not send node {given}'s id: {e}");
        }
    }

    /// Hears every frame addressed to this node and sorts them into a link per
    /// sender, making one when a transfer starts from a node that has none.
    pub(super) async fn accept_task(
        socket: Arc<CanSocket>,
        ep: CanEndpoint,
        token: CancellationToken,
        manager: NewLinkChannelSender,
    ) -> ZResult<()> {
        let peers: Peers = Arc::new(Mutex::new(HashMap::new()));
        // Nodes with no id ask for one, and this is what gives it
        let mut ids = Allocator::new(ep.id, ID_TTL);
        let mut asks = Reassembler::new(frame::RX_SESSIONS_UNICAST);

        loop {
            let (id, f) = tokio::select! {
                _ = token.cancelled() => break,
                r = socket.recv() => match r {
                    Ok(frame) => frame,
                    Err(e) => {
                        tracing::warn!("{e}");
                        tokio::time::sleep(ACCEPT_THROTTLE).await;
                        continue;
                    }
                },
            };

            let src = frame::id_src(id);

            if frame::id_kind(id) != frame::KIND_DATA {
                answer_control(&socket, &mut ids, &mut asks, id, &f).await;
                continue;
            }
            // A node that has no id has nothing to say but a request for one
            if src == frame::NODE_UNASSIGNED {
                continue;
            }
            ids.observe(src, Instant::now());

            let mut frame = Some((id, f));
            while let Some(item) = frame.take() {
                let inbox = zlock!(peers).get(&src).cloned();
                match inbox {
                    Some(inbox) => match inbox.try_send(item) {
                        Ok(()) => {}
                        Err(flume::TrySendError::Full(_)) => {
                            tracing::debug!(
                                "CAN: node {src}'s link is not keeping up, dropping a frame"
                            );
                        }
                        // The link went away without closing
                        Err(flume::TrySendError::Disconnected(item)) => {
                            zlock!(peers).remove(&src);
                            frame = Some(item);
                        }
                    },
                    // Only a start makes a link: a frame from the middle of a
                    // transfer is the tail of something that is gone, and would
                    // make a transport out of nothing.
                    None if item.1.is_start() => {
                        let (tx, rx) = flume::bounded(INBOX_FRAMES);
                        zlock!(peers).insert(src, tx.clone());
                        tracing::debug!("Accepted CAN connection from node {src}");
                        let link = LinkUnicastCan::new(
                            socket.clone(),
                            &ep,
                            src,
                            Source::Inbox {
                                frames: rx,
                                peers: peers.clone(),
                            },
                        );
                        let _ = tx.try_send(item);
                        if let Err(e) = manager
                            .send_async(LinkUnicast::from(
                                Arc::new(link) as Arc<dyn LinkUnicastTrait>
                            ))
                            .await
                        {
                            tracing::error!("{}-{}: {}", file!(), line!(), e);
                        }
                    }
                    None => {}
                }
            }
        }

        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod id_tests {
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    use super::imp::accept_task;
    use crate::{frame, sys::CanSocket, CanEndpoint};

    const DEVICE: &str = "vcan0";

    fn vcan_present() -> bool {
        // The interfaces of this process's network namespace, which `unshare -rn` changes
        let present = std::fs::read_to_string("/proc/net/dev").is_ok_and(|devs| {
            devs.lines()
                .any(|l| l.trim_start().starts_with(&format!("{DEVICE}:")))
        });
        if !present {
            println!("SKIPPING: no `{DEVICE}` interface. Create one with `ci/vcan-setup.sh`.");
        }
        present
    }

    fn endpoint(id: u8) -> CanEndpoint {
        CanEndpoint {
            device: DEVICE.into(),
            bitrate: 500_000,
            dbitrate: 2_000_000,
            id,
            peer: None,
            so_rcvbuf: Some(8 << 20),
        }
    }

    /// Asks `router` for an id as a node with this UID does: from the
    /// unassigned address, hearing what is addressed to it.
    async fn ask(uid: [u8; frame::UID_LEN], router: u8) -> Option<u8> {
        let socket = CanSocket::open(&endpoint(frame::NODE_UNASSIGNED), frame::NODE_UNASSIGNED)
            .expect("open");
        let id = frame::make_ctl_id(
            frame::KIND_ID_REQUEST,
            frame::uid_key(&uid),
            router,
            frame::NODE_UNASSIGNED,
        );
        socket.send_with_id(&uid, id).await.expect("send");

        let mut rx = frame::Reassembler::new(frame::RX_SESSIONS_UNICAST);
        let heard = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let (id, f) = socket.recv().await.expect("recv");
                if frame::id_kind(id) != frame::KIND_ID_ASSIGN {
                    continue;
                }
                let mut out = [0u8; frame::UID_LEN + 1];
                if rx.decode(id, f.payload(), &mut out) == Some(out.len()) && out[..12] == uid {
                    return out[frame::UID_LEN];
                }
            }
        })
        .await;
        heard.ok()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs a vcan0 interface; see the module docs of the transport tests"]
    async fn a_listener_gives_ids_to_nodes_that_ask() {
        if !vcan_present() {
            return;
        }

        let router = 1;
        let ep = endpoint(router);
        let socket = std::sync::Arc::new(CanSocket::open(&ep, router).expect("open"));
        let token = CancellationToken::new();
        let (tx, _links) = flume::unbounded();
        tokio::spawn(accept_task(socket, ep, token.clone(), tx));

        let a = ask([1; frame::UID_LEN], router).await.expect("an id for a");
        let b = ask([2; frame::UID_LEN], router).await.expect("an id for b");
        assert_ne!(a, b);
        for id in [a, b] {
            assert!(id <= frame::NODE_MAX && id != router, "{id}");
        }
        // Asking again, with the same UID, gets the same id
        assert_eq!(ask([1; frame::UID_LEN], router).await, Some(a));

        token.cancel();
    }
}
