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

//! WARNING
//!
//! This crate is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)
//!
//! A CAN FD link, interoperating with the zenoh-pico link of the same wire
//! format (see [`frame`] for it).
//!
//! A CAN bus is a broadcast medium: every node hears every frame and filters by
//! identifier, and the link builds both kinds of link on that.
//!
//! * A **unicast** link -- the default -- talks to one node, `peer`. Every frame
//!   goes to that node and it hears only that node. Connecting to a locator
//!   makes one; listening on one accepts every node that sends to it, a link
//!   each, the way a UDP listener does.
//! * A **multicast** link, selected by `?mode=multicast` on the locator, sends
//!   every frame to all nodes and hears all of them. It is what zenoh-pico
//!   opens when it listens on a locator.
//!
//! The wire format is the same for both and only the destination differs.
//!
//! A device that has no node id asks for one. A listening unicast link answers:
//! see [`ids`] and the README.
//!
//! Frames are segmented and reassembled by the link itself, so it is a
//! **datagram** link with a 4095-byte MTU, and zenoh's own fragmentation never
//! runs on it. CAN FD is required: classic CAN is not supported.

mod frame;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod ids;
mod multicast;
#[cfg(target_os = "linux")]
mod sys;
mod unicast;

use std::str::FromStr;

use async_trait::async_trait;
pub use multicast::LinkManagerMulticastCan;
pub use unicast::LinkManagerUnicastCan;
use zenoh_link_commons::{LocatorInspector, TCP_SO_RCV_BUF};
use zenoh_protocol::{
    core::{EndPoint, Locator, Metadata, Reliability},
    transport::BatchSize,
};
use zenoh_result::{bail, ZResult};

pub const CAN_LOCATOR_PREFIX: &str = "can";

/// Reliable at frame level -- CRC, ACK slot, automatic retransmission -- but not
/// end to end: controller buffers overrun and a bus-off condition drops
/// everything. zenoh's own reliability covers that.
const IS_RELIABLE: bool = false;

/// The MTU of the link: the largest datagram it carries.
pub const CAN_MAX_MTU: BatchSize = frame::MTU;

/// `IFNAMSIZ`, including the terminator.
const IFNAMSIZ: usize = 16;

/// Endpoint configuration keys.
///
/// ```text
/// can/<device>#bitrate=500000;dbitrate=2000000;id=1;peer=2
/// ```
///
/// * `device` -- the CAN interface name, e.g. `can0` or `vcan0`
/// * `bitrate` -- arbitration-phase bit rate
/// * `dbitrate` -- CAN FD data-phase bit rate. Must be non-zero: this link is
///   CAN FD only
/// * `id` -- **this** node, 0..=253. It is the source in every identifier it
///   sends, and node ids must be unique on the bus
/// * `peer` -- 0..=253, the one node a unicast link talks to. Required to
///   connect, and not used to listen
///
/// On Linux the bit rates are advisory: rates are set out of band with
/// `ip link set can0 type can bitrate ... fd on`, and a virtual interface has
/// none at all. They are still validated, because `dbitrate=0` selects classic
/// CAN in zenoh-pico and is a misconfiguration here.
///
/// The locator itself, before the `#`, takes one key:
///
/// * `mode` -- `unicast`, the default, or `multicast`
///
/// It has to live there because zenoh decides what kind of link a locator is
/// from the locator alone, and the configuration after `#` is not part of it.
///
/// # Identifier value is bus priority
///
/// The identifier is the bus priority as well as the address: the lowest wins
/// arbitration, and zenoh's own priority sits in its high bits, so an urgent
/// message beats a bulk one on the wire regardless of which node sent it.
pub mod config {
    pub const BITRATE: &str = "bitrate";
    pub const DBITRATE: &str = "dbitrate";
    pub const ID: &str = "id";
    pub const PEER: &str = "peer";
    /// Receive buffer size in bytes, spelled as the TCP and UDP links spell it.
    ///
    /// Absent, the kernel default applies. Raising it matters only when frames
    /// can arrive faster than the link drains them, which a real bus cannot do
    /// -- 2 Mbit/s of CAN FD is under 2 800 frames per second -- but a virtual
    /// interface can, because it has no bit rate at all. See the README.
    pub use zenoh_link_commons::TCP_SO_RCV_BUF as SO_RCVBUF;

    /// The locator metadata key naming the kind of link.
    pub const MODE: &str = "mode";
    pub const MODE_UNICAST: &str = "unicast";
    pub const MODE_MULTICAST: &str = "multicast";
}

pub const DEFAULT_BITRATE: u32 = 500_000;
pub const DEFAULT_DBITRATE: u32 = 2_000_000;
pub const DEFAULT_ID: u8 = 1;

#[derive(Debug, Default, Clone, Copy)]
pub struct CanLocatorInspector;

#[async_trait]
impl LocatorInspector for CanLocatorInspector {
    fn protocol(&self) -> &str {
        CAN_LOCATOR_PREFIX
    }

    /// Whether the locator asks for `mode=multicast`. Otherwise it is unicast,
    /// which is what zenoh-pico opens when it connects to a locator.
    async fn is_multicast(&self, locator: &Locator) -> ZResult<bool> {
        match locator.metadata().get(config::MODE) {
            None | Some(config::MODE_UNICAST) => Ok(false),
            Some(config::MODE_MULTICAST) => Ok(true),
            Some(other) => bail!(
                "unknown CAN link mode {other:?} in {locator}; expected `{}` or `{}`",
                config::MODE_UNICAST,
                config::MODE_MULTICAST
            ),
        }
    }

    fn is_reliable(&self, locator: &Locator) -> ZResult<bool> {
        if let Some(reliability) = locator
            .metadata()
            .get(Metadata::RELIABILITY)
            .map(Reliability::from_str)
            .transpose()?
        {
            Ok(reliability == Reliability::Reliable)
        } else {
            Ok(IS_RELIABLE)
        }
    }
}

/// A parsed and validated CAN endpoint.
///
/// Parsing and validation happen on every platform, so a malformed endpoint is
/// reported as a malformed endpoint rather than as a missing platform. Only the
/// fields the SocketCAN binding consumes are otherwise unused off Linux.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct CanEndpoint {
    pub(crate) device: String,
    pub(crate) bitrate: u32,
    pub(crate) dbitrate: u32,
    /// This node.
    pub(crate) id: u8,
    /// The node a unicast link talks to.
    pub(crate) peer: Option<u8>,
    /// `None` leaves the kernel default in place.
    pub(crate) so_rcvbuf: Option<u32>,
}

/// Parse an unsigned integer, accepting decimal and `0x`-prefixed hex.
///
/// Node ids are conventionally written in either and bit rates in decimal, and
/// forcing one notation on both would make every endpoint harder to read.
///
/// Unlike the zenoh-pico parser, a malformed value is an error rather than a
/// silent fall back to the default: a mistyped node id is an addressing
/// misconfiguration, and those must not be quiet.
fn parse_u32(key: &str, s: &str) -> ZResult<u32> {
    let t = s.trim();
    let parsed = match t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        Some(hex) => u32::from_str_radix(hex, 16),
        None => t.parse::<u32>(),
    };
    parsed.map_err(|e| {
        zenoh_result::zerror!("invalid `{key}` value {s:?} on a CAN locator: {e}").into()
    })
}

fn get_u32(endpoint: &EndPoint, key: &str, default: u32) -> ZResult<u32> {
    match endpoint.config().get(key) {
        Some(v) => parse_u32(key, v),
        None => Ok(default),
    }
}

fn get_opt_u32(endpoint: &EndPoint, key: &str) -> ZResult<Option<u32>> {
    endpoint
        .config()
        .get(key)
        .map(|v| parse_u32(key, v))
        .transpose()
}

/// A node id: 0..=253, since 0xFF is everybody and 0xFE is a node without an id.
fn node(key: &str, value: u32) -> ZResult<u8> {
    match u8::try_from(value) {
        Ok(n) if n <= frame::NODE_MAX => Ok(n),
        _ => bail!(
            "CAN `{key}` is {value}, outside the node range 0..={}: {:#x} is the broadcast \
             address and {:#x} is a node that has no id yet",
            frame::NODE_MAX,
            frame::NODE_BROADCAST,
            frame::NODE_UNASSIGNED
        ),
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl CanEndpoint {
    pub(crate) fn parse(endpoint: &EndPoint) -> ZResult<CanEndpoint> {
        if endpoint.protocol().as_str() != CAN_LOCATOR_PREFIX {
            bail!(
                "not a CAN locator: {} (expected protocol `{CAN_LOCATOR_PREFIX}`)",
                endpoint
            );
        }

        let device = endpoint.address().as_str().to_owned();
        // The address is the interface name. Empty is meaningless, and an
        // over-long one would silently truncate into a different interface, so
        // both are rejected here rather than at open, where the error would be
        // less obvious.
        if device.is_empty() {
            bail!("a CAN locator needs an interface name: {endpoint}");
        }
        if device.len() >= IFNAMSIZ {
            bail!(
                "CAN interface name {device:?} is {} bytes; the kernel allows at most {}",
                device.len(),
                IFNAMSIZ - 1
            );
        }

        let id = node(
            config::ID,
            get_u32(endpoint, config::ID, DEFAULT_ID as u32)?,
        )?;
        let peer = get_opt_u32(endpoint, config::PEER)?
            .map(|p| node(config::PEER, p))
            .transpose()?;

        let ep = CanEndpoint {
            device,
            bitrate: get_u32(endpoint, config::BITRATE, DEFAULT_BITRATE)?,
            dbitrate: get_u32(endpoint, config::DBITRATE, DEFAULT_DBITRATE)?,
            id,
            peer,
            so_rcvbuf: get_opt_u32(endpoint, TCP_SO_RCV_BUF)?,
        };
        ep.validate()?;
        Ok(ep)
    }

    fn validate(&self) -> ZResult<()> {
        // This link is CAN FD only. In zenoh-pico `dbitrate=0` selects classic
        // CAN, whose 8-byte frames this link neither sends nor hears, so a
        // locator asking for it would open a link that could never talk to
        // anyone. Refusing beats a hang.
        if self.dbitrate == 0 {
            bail!(
                "CAN `{}` is 0, which selects classic CAN in zenoh-pico. This link is CAN FD                  only: set a data-phase bit rate, e.g. `{}=2000000`",
                config::DBITRATE,
                config::DBITRATE
            );
        }

        if self.peer == Some(self.id) {
            bail!(
                "CAN `{}` is this node's own id {}: a link cannot talk to itself",
                config::PEER,
                self.id
            );
        }

        Ok(())
    }

    /// The node a unicast link talks to, or an error that says so.
    pub(crate) fn require_peer(&self) -> ZResult<u8> {
        match self.peer {
            Some(peer) => Ok(peer),
            None => bail!(
                "a CAN unicast link needs `{}=<node>` to say who it talks to; to talk to \
                 everybody use `?{}={}` on the locator",
                config::PEER,
                config::MODE,
                config::MODE_MULTICAST
            ),
        }
    }

    /// The locator naming a node on this bus. Derived locally from the frame
    /// identifier and never transmitted.
    ///
    /// The id goes in the locator's **metadata**, not its config: a `Locator`
    /// is an `EndPoint` with the config truncated away (`From<EndPoint> for
    /// Locator`), so a config-borne id would be silently dropped and every node
    /// would look alike.
    pub(crate) fn node_locator(&self, node: u8) -> Locator {
        Locator::new(
            CAN_LOCATOR_PREFIX,
            &self.device,
            format!("{}={}", config::ID, node),
        )
        .expect("a CAN node locator is always well formed")
    }

    /// The locator naming everybody on this bus, the destination of a
    /// multicast link.
    pub(crate) fn broadcast_locator(&self) -> Locator {
        self.node_locator(frame::NODE_BROADCAST)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(s: &str) -> EndPoint {
        EndPoint::from_str(s).unwrap()
    }

    #[test]
    fn defaults_apply_when_keys_are_absent() {
        let c = CanEndpoint::parse(&ep("can/vcan0")).unwrap();
        assert_eq!(c.device, "vcan0");
        assert_eq!(c.bitrate, DEFAULT_BITRATE);
        assert_eq!(c.dbitrate, DEFAULT_DBITRATE);
        assert_eq!(c.id, DEFAULT_ID);
        assert_eq!(c.peer, None);
    }

    #[test]
    fn the_full_grammar_parses() {
        let c = CanEndpoint::parse(&ep(
            "can/can0#bitrate=500000;dbitrate=2000000;id=0x0A;peer=2",
        ))
        .unwrap();
        assert_eq!(c.device, "can0");
        assert_eq!(c.id, 10);
        assert_eq!(c.peer, Some(2));
    }

    #[test]
    fn decimal_and_hex_are_both_accepted() {
        assert_eq!(CanEndpoint::parse(&ep("can/vcan0#id=17")).unwrap().id, 0x11);
        assert_eq!(
            CanEndpoint::parse(&ep("can/vcan0#id=0x11")).unwrap().id,
            0x11
        );
    }

    #[test]
    fn the_receive_buffer_defaults_to_the_kernels() {
        assert_eq!(
            CanEndpoint::parse(&ep("can/vcan0")).unwrap().so_rcvbuf,
            None
        );
        assert_eq!(
            CanEndpoint::parse(&ep("can/vcan0#so_rcvbuf=8388608"))
                .unwrap()
                .so_rcvbuf,
            Some(8 * 1024 * 1024)
        );
        assert!(CanEndpoint::parse(&ep("can/vcan0#so_rcvbuf=lots")).is_err());
    }

    /// Classic CAN is refused, and the message says why rather than leaving
    /// someone to wonder why nothing arrives.
    #[test]
    fn a_zero_dbitrate_is_refused() {
        let e = CanEndpoint::parse(&ep("can/vcan0#dbitrate=0")).unwrap_err();
        assert!(e.to_string().contains("CAN FD"), "{e}");
    }

    #[test]
    fn a_malformed_value_is_an_error_not_a_default() {
        let e = CanEndpoint::parse(&ep("can/vcan0#id=0x1g")).unwrap_err();
        assert!(e.to_string().contains("invalid `id` value"), "{e}");
    }

    #[test]
    fn an_over_long_interface_name_is_refused() {
        let e = CanEndpoint::parse(&ep("can/vcan0123456789abcdef")).unwrap_err();
        assert!(e.to_string().contains("at most 15"), "{e}");
    }

    #[test]
    fn the_broadcast_and_unassigned_addresses_are_not_nodes() {
        for s in [
            "can/vcan0#id=255",
            "can/vcan0#id=254",
            "can/vcan0#peer=254",
            "can/vcan0#id=0x100",
            "can/vcan0#peer=255",
        ] {
            let e = CanEndpoint::parse(&ep(s)).unwrap_err();
            assert!(e.to_string().contains("0..=253"), "{s}: {e}");
        }
        assert!(CanEndpoint::parse(&ep("can/vcan0#id=253;peer=0")).is_ok());
    }

    #[test]
    fn a_node_cannot_be_its_own_peer() {
        let e = CanEndpoint::parse(&ep("can/vcan0#id=3;peer=3")).unwrap_err();
        assert!(e.to_string().contains("talk to itself"), "{e}");
    }

    #[test]
    fn a_unicast_link_without_a_peer_says_what_to_do() {
        let c = CanEndpoint::parse(&ep("can/vcan0#id=3")).unwrap();
        let e = c.require_peer().unwrap_err();
        assert!(e.to_string().contains("peer="), "{e}");
        assert!(e.to_string().contains("mode=multicast"), "{e}");
        let c = CanEndpoint::parse(&ep("can/vcan0#id=3;peer=4")).unwrap();
        assert_eq!(c.require_peer().unwrap(), 4);
    }

    #[test]
    fn a_node_locator_carries_the_node() {
        let c = CanEndpoint::parse(&ep("can/vcan0")).unwrap();
        let l = c.node_locator(0x11);
        assert_eq!(l.protocol().as_str(), CAN_LOCATOR_PREFIX);
        assert_eq!(l.address().as_str(), "vcan0");
        assert_eq!(l.metadata().get(config::ID), Some("17"));
    }

    /// Links are told apart by comparing these locators, so two nodes must
    /// never produce the same one.
    #[test]
    fn node_locators_distinguish_nodes() {
        let c = CanEndpoint::parse(&ep("can/vcan0")).unwrap();
        assert_ne!(c.node_locator(1), c.node_locator(2));
        assert_ne!(c.node_locator(1), c.broadcast_locator());
    }

    /// A `Locator` is an `EndPoint` with the config truncated, which is why the
    /// node lives in the metadata. This pins that reasoning: had it gone in the
    /// config, it would vanish here.
    #[test]
    fn a_config_borne_node_would_be_lost() {
        let e = ep("can/vcan0#id=17");
        assert_eq!(e.config().get(config::ID), Some("17"));
        assert_eq!(e.to_locator().metadata().get(config::ID), None);
    }

    #[tokio::test]
    async fn the_inspector_reads_the_mode_from_the_locator() {
        let i = CanLocatorInspector;
        assert_eq!(i.protocol(), "can");
        let unicast = Locator::from_str("can/vcan0").unwrap();
        let explicit = Locator::from_str("can/vcan0?mode=unicast").unwrap();
        let multicast = Locator::from_str("can/vcan0?mode=multicast").unwrap();
        assert!(!i.is_multicast(&unicast).await.unwrap());
        assert!(!i.is_multicast(&explicit).await.unwrap());
        assert!(i.is_multicast(&multicast).await.unwrap());
        assert!(!i.is_reliable(&unicast).unwrap());
    }

    /// The mode is the locator's, so the configuration after `#` cannot change
    /// it, and an endpoint with both reads as the locator says.
    #[tokio::test]
    async fn the_mode_survives_the_endpoint_becoming_a_locator() {
        let e = ep("can/vcan0?mode=multicast#id=3");
        assert!(CanLocatorInspector
            .is_multicast(&e.to_locator())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn an_unknown_mode_is_refused() {
        let l = Locator::from_str("can/vcan0?mode=anycast").unwrap();
        let e = CanLocatorInspector.is_multicast(&l).await.unwrap_err();
        assert!(e.to_string().contains("unknown CAN link mode"), "{e}");
    }
}
