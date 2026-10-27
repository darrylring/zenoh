# WARNING

This crate is intended for Zenoh's internal use.
It is not guaranteed that the API will remain unchanged in any version, including patch updates.
It is highly recommended to depend solely on the zenoh and zenoh-ext crates and to utilize their public APIs.

- [Click here for Zenoh's main repository](https://github.com/eclipse-zenoh/zenoh)
- [Click here for Zenoh's documentation](https://zenoh.io)

## The CAN link

A CAN FD transport, behind the `transport_can` feature. Linux only: it is built on SocketCAN.

It speaks the same wire format as zenoh-pico's CAN link (`link/transport/can.h` and `can_codec.c`), so the two talk to each other over one bus with no special configuration.

**CAN FD is required.** zenoh-pico also has a classic CAN mode with 8-byte frames; this link neither sends nor hears them. It refuses an interface that is not FD at open, and `dbitrate=0` at parse. An adapter that is classic-only -- a PCAN-USB, as opposed to a PCAN-USB FD -- cannot be used.

### Link kinds

The link is a **datagram** link with a 4095-byte MTU. It segments a datagram into CAN FD frames and reassembles it on receipt itself (see below), so zenoh's own fragmentation never runs on it.

There are two kinds of link, and they use the same frames:

| | unicast (default) | multicast |
| --- | --- | --- |
| Locator | `can/can0#id=1;peer=2` | `can/can0?mode=multicast#id=1` |
| Sends to | `peer` | every node |
| Hears | only `peer` | everybody |
| Connect | opens a link to `peer` | |
| Listen | accepts every node that sends to it, a link each, as a UDP listener does | one link that hears everybody |
| zenoh-pico | what it opens when it **connects** to a locator | what it opens when it **listens** on one |

zenoh-pico picks the kind by whether it connects or listens. zenoh does not: it decides what kind of link a locator is from the locator alone, before `#`, so the kind is the locator's `mode`. To accept a pico client, a router listens on a unicast locator and the client connects to it:

```sh
# the router, node 1
zenohd -l can/can0#id=1

# a zenoh-pico client, node 2, which talks to node 1
z_pub -m client -e 'can/can0#id=2;peer=1' ...
```

A pico peer that **listens** on `can/can0#id=2` is a multicast link, and talks to a Rust peer on `can/can0?mode=multicast#id=1`. Multicast peers must agree on the batch size, which zenoh-pico takes from `Z_BATCH_MULTICAST_SIZE` (2048 by default) and advertises as is, and which this link advertises as the smaller of zenoh's configured batch size and 4095. Make them equal: build pico with a `BATCH_MULTICAST_SIZE` that matches, or set `transport/link/tx/batch_size` here.

### Endpoints

```text
can/<device>#bitrate=500000;dbitrate=2000000;id=1;peer=2
can/<device>?mode=multicast#id=1
```

| key | meaning |
| --- | --- |
| `device` | the CAN interface name, such as `can0` or `vcan0` |
| `bitrate` | arbitration-phase bit rate |
| `dbitrate` | CAN FD data-phase bit rate. Must be non-zero |
| `id` | **this** node, 0..=254, the source in every identifier it sends. Unique on the bus |
| `peer` | 0..=254, the one node a unicast link talks to. Required to connect |
| `so_rcvbuf` | receive buffer in bytes. Absent, the kernel default applies |
| `mode` | on the locator, before `#`: `unicast` (default) or `multicast` |

On Linux the bit rates are advisory: rates are set out of band, and a virtual interface has none at all. They are still validated. For a real adapter:

```sh
sudo ip link set can0 down
sudo ip link set can0 type can bitrate 125000 dbitrate 125000 fd on
sudo ip link set can0 txqueuelen 1000
sudo ip link set can0 up
```

`txqueuelen` matters because a 4 KiB datagram is about 66 frames and goes out in a burst. When the controller's queue is full the link waits for room, for a quarter of a second, rather than send part of a datagram. A datagram that still does not fit is dropped, and zenoh's own reliability covers it.

### Wire format

The identifier is 29 bits, and carries the bus priority as well as the address:

```text
  28..26  priority   zenoh priority (0 = control ... 7 = background)
  25..21  key id     the sender's ExprId for the topic, 0 = none (not filled in yet)
  20..16  reserved   zero
  15..8   destination node, 0xFF = every node
   7..0   source node
```

A **lower identifier wins arbitration**, and the priority is in the high bits, so an urgent message beats a bulk one on the wire regardless of which node sent it. The controller's hardware filter selects on the destination, so a node's CPU never sees frames for others.

The last byte of every frame is a tail byte -- start, end, a toggle that alternates, and a 5-bit transfer id -- and the transfer is `length prefix | datagram | zero padding | CRC-16/CCITT-FALSE`. The CRC is there only when a datagram needs more than one frame. [`frame.rs`](src/frame.rs) documents it and unit-tests it, with no bus.

Frames of different transfers interleave on a bus, so a receiver keeps a transfer per (priority, key, source). A frame that is lost, or out of order, drops its transfer and the sender's next one starts clean.

### `so_rcvbuf`, and when it matters

Frames arriving faster than the link drains them are dropped by the kernel, silently, before the link sees them. A real bus cannot outrun the reader: 2 Mbit/s of CAN FD is under 2 800 frames per second. A **virtual** interface can, because it has no bit rate at all, so a test over `vcan0` that sends bursts wants `so_rcvbuf=8388608`.

The kernel clamps the request to `net.core.rmem_max` without saying so, so the link reads the value back and warns when it fell short.

### Testing without hardware

The link runs against a virtual bus, which needs no CAN controller:

```sh
ci/vcan-setup.sh              # create and bring up vcan0, prompting for sudo
ci/vcan-setup.sh --status     # report, changing nothing
ci/vcan-setup.sh --down       # tear it down again
```

or, with no root, inside a user and network namespace of your own:

```sh
unshare -rn sh -c 'ip link add dev vcan0 type vcan && ip link set vcan0 mtu 72 && ip link set vcan0 up && exec <your test>'
```

`mtu 72` is what makes a vcan an FD interface.
