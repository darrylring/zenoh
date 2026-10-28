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

//! Handing out node ids.
//!
//! A node on a CAN bus needs an id that no other node has, and a device has
//! nothing to take one from but its unique id, which is far too long. So a
//! device without an id asks for one, from the node it talks to, and this is
//! what answers.
//!
//! The ids it hands out are only for the link. Who a device is, is for the
//! protocol above to say, so an id need not be the same one next time. A device
//! that asks again, with the same UID, gets the id it was given only while that
//! id has not been used: the request was repeated because the answer was lost.
//! Once the device has sent anything from it, a request is a device that has
//! started again, and it gets a new id, because the old one still has a link
//! bound to it here. That link would take the new session's first frames, and
//! the session would never start.
//!
//! No node is told that an id has been given up. An id is free again when
//! nothing has been heard from it for a while, whoever it was given to, which
//! also keeps a node whose id was set by hand from being handed out.
//!
//! This holds no clock and does no I/O, so it can be tested without a bus.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::frame::{NODE_MAX, UID_LEN};

/// How long an id is held with nothing heard from it. A node that is up is
/// heard from at least as often as the transport's keep-alive, a few seconds.
pub(crate) const ID_TTL: Duration = Duration::from_secs(30);

pub(crate) type Uid = [u8; UID_LEN];

pub(crate) struct Allocator {
    /// The node doing the allocating, which no one else may have.
    own: u8,
    ttl: Duration,
    /// When each id was last heard from, or given out.
    seen: [Option<Instant>; NODE_MAX as usize + 1],
    /// Whether anything has been heard from each id since it was given out.
    used: [bool; NODE_MAX as usize + 1],
    /// Who each given id went to.
    uids: HashMap<Uid, u8>,
}

impl Allocator {
    pub(crate) fn new(own: u8, ttl: Duration) -> Self {
        Allocator {
            own,
            ttl,
            seen: [None; NODE_MAX as usize + 1],
            used: [false; NODE_MAX as usize + 1],
            uids: HashMap::new(),
        }
    }

    /// A frame came from `src`, so that id is in use.
    pub(crate) fn observe(&mut self, src: u8, now: Instant) {
        if let Some(slot) = self.seen.get_mut(src as usize) {
            *slot = Some(now);
            self.used[src as usize] = true;
        }
    }

    /// The id for the node with this UID: the one it was given, if nothing has
    /// been heard from it yet, or else the lowest that is free. None when they
    /// are all in use.
    pub(crate) fn assign(&mut self, uid: Uid, now: Instant) -> Option<u8> {
        self.expire(now);
        if let Some(&id) = self.uids.get(&uid) {
            if !self.used[id as usize] {
                self.seen[id as usize] = Some(now);
                return Some(id);
            }
            // It has started again. Its old id stays held until it has been quiet for a while
            self.uids.remove(&uid);
        }
        let id = (0..=NODE_MAX).find(|&id| id != self.own && self.seen[id as usize].is_none())?;
        self.seen[id as usize] = Some(now);
        self.used[id as usize] = false;
        self.uids.insert(uid, id);
        Some(id)
    }

    fn expire(&mut self, now: Instant) {
        for slot in self.seen.iter_mut() {
            if slot.is_some_and(|t| now.saturating_duration_since(t) >= self.ttl) {
                *slot = None;
            }
        }
        let seen = &self.seen;
        self.uids.retain(|_, id| seen[*id as usize].is_some());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(10);

    fn uid(n: u8) -> Uid {
        [n; UID_LEN]
    }

    #[test]
    fn ids_are_given_lowest_first_and_never_the_allocators_own() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        assert_eq!(a.assign(uid(10), now), Some(0));
        // 1 is the allocator's
        assert_eq!(a.assign(uid(11), now), Some(2));
        assert_eq!(a.assign(uid(12), now), Some(3));
    }

    #[test]
    fn asking_again_before_using_the_id_gets_the_same_one() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        let first = a.assign(uid(10), now);
        a.assign(uid(11), now);
        // The answer was lost, so the request is repeated
        assert_eq!(a.assign(uid(10), now), first);
    }

    #[test]
    fn a_node_that_has_started_again_gets_a_new_id() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        let first = a.assign(uid(10), now).unwrap();
        // It used its id, then stopped and started again, with the same UID
        a.observe(first, now);
        let second = a.assign(uid(10), now).unwrap();
        assert_ne!(first, second, "the old id still has a link here");
        // And the same again
        a.observe(second, now);
        let third = a.assign(uid(10), now).unwrap();
        assert!(third != first && third != second);
    }

    #[test]
    fn the_old_id_of_a_node_that_started_again_is_free_when_it_has_been_quiet() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        let first = a.assign(uid(10), now).unwrap();
        a.observe(first, now);
        let second = a.assign(uid(10), now).unwrap();
        a.observe(second, now + TTL / 2);
        // Only the second is kept up
        let later = now + TTL;
        assert_eq!(a.assign(uid(11), later), Some(first));
    }

    #[test]
    fn an_id_heard_from_is_not_given_out() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        // A node whose id was set by hand
        a.observe(0, now);
        a.observe(2, now);
        assert_eq!(a.assign(uid(10), now), Some(3));
    }

    #[test]
    fn an_id_nobody_has_been_heard_from_on_is_free_again() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        assert_eq!(a.assign(uid(10), now), Some(0));
        let later = now + TTL;
        // A new node takes it, and the first, who asks again, gets another
        assert_eq!(a.assign(uid(11), later), Some(0));
        assert_eq!(a.assign(uid(10), later), Some(2));
    }

    #[test]
    fn hearing_from_an_id_keeps_it() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        assert_eq!(a.assign(uid(10), now), Some(0));
        let mut t = now;
        for _ in 0..5 {
            t += TTL / 2;
            a.observe(0, t);
        }
        // Still held, so not given to someone else
        assert_eq!(a.assign(uid(11), t), Some(2));
    }

    #[test]
    fn none_when_every_id_is_in_use() {
        let now = Instant::now();
        let mut a = Allocator::new(1, TTL);
        for n in 0..=NODE_MAX {
            if n != 1 {
                assert!(a.assign(uid(n), now).is_some(), "{n}");
            }
        }
        // A newcomer, not one of those that already have an id
        let mut newcomer = uid(0);
        newcomer[0] = 0xFF;
        assert_eq!(a.assign(newcomer, now), None);
    }

    #[test]
    fn ids_outside_the_node_range_are_not_given() {
        let now = Instant::now();
        let mut a = Allocator::new(0, TTL);
        let mut last = 0;
        for n in 0..=255u8 {
            if let Some(id) = a.assign(uid(n), now) {
                last = last.max(id);
            }
        }
        assert_eq!(last, NODE_MAX);
    }
}
