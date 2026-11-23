//! libp2p connection limits for the gossip swarm.

use libp2p::core::transport::PortUse;
use libp2p::core::{ConnectedPoint, Endpoint};
use libp2p::swarm::behaviour::ConnectionEstablished;
use libp2p::swarm::{
    dummy, ConnectionClosed, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler,
    THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::task::{Context, Poll};

pub const MAX_ESTABLISHED_INCOMING: u32 = 100;
pub const MAX_ESTABLISHED_OUTGOING: u32 = 100;
/// A peer needs one inbound connection; a second covers a reconnect racing a
/// stale connection's close.
pub const MAX_ESTABLISHED_INCOMING_PER_PEER: usize = 2;

/// Connection limits for the gossip swarm.
///
/// Wraps `libp2p_connection_limits` for the incoming and outgoing totals and
/// adds a per-peer cap on *incoming* connections only. The upstream per-peer
/// cap counts both directions, so two nodes that dial each other at the same
/// time can each fill the cap with their own dials and deny every connection
/// the other makes, leaving them disconnected.
///
/// Direct peers (validators) bypass every limit. Without that, other peers can
/// fill the incoming slots and a validator whose link drops is denied on every
/// redial until some unrelated connection closes.
pub struct ConnectionLimits {
    totals: libp2p_connection_limits::Behaviour,
    direct_peers: HashSet<PeerId>,
    inbound_per_peer: HashMap<PeerId, HashSet<ConnectionId>>,
}

/// Denial cause when a peer already holds its share of incoming connections.
#[derive(Debug, Clone, Copy)]
pub struct InboundPerPeerExceeded {
    pub limit: usize,
}

impl fmt::Display for InboundPerPeerExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "connection limit exceeded: at most {} incoming connections per peer are allowed",
            self.limit
        )
    }
}

impl std::error::Error for InboundPerPeerExceeded {}

/// Short, fixed label for why a connection was denied, for metric tags.
pub fn denial_reason(cause: &ConnectionDenied) -> &'static str {
    if cause.downcast_ref::<InboundPerPeerExceeded>().is_some() {
        "incoming_per_peer"
    } else if cause
        .downcast_ref::<libp2p_connection_limits::Exceeded>()
        .is_some()
    {
        "total"
    } else {
        "other"
    }
}

impl ConnectionLimits {
    pub fn new(direct_peers: impl IntoIterator<Item = PeerId>) -> Self {
        let mut totals = libp2p_connection_limits::Behaviour::new(
            libp2p_connection_limits::ConnectionLimits::default()
                .with_max_established_incoming(Some(MAX_ESTABLISHED_INCOMING))
                .with_max_established_outgoing(Some(MAX_ESTABLISHED_OUTGOING)),
        );
        let direct_peers: HashSet<PeerId> = direct_peers.into_iter().collect();
        for peer in &direct_peers {
            totals.bypass_peer_id(peer);
        }
        Self {
            totals,
            direct_peers,
            inbound_per_peer: HashMap::new(),
        }
    }
}

impl NetworkBehaviour for ConnectionLimits {
    type ConnectionHandler = dummy::ConnectionHandler;
    type ToSwarm = Infallible;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.totals
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.totals.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )?;
        let inbound = self.inbound_per_peer.get(&peer).map_or(0, HashSet::len);
        if !self.direct_peers.contains(&peer) && inbound >= MAX_ESTABLISHED_INCOMING_PER_PEER {
            return Err(ConnectionDenied::new(InboundPerPeerExceeded {
                limit: MAX_ESTABLISHED_INCOMING_PER_PEER,
            }));
        }
        Ok(dummy::ConnectionHandler)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.totals.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.totals.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint: ConnectedPoint::Listener { .. },
                ..
            }) => {
                self.inbound_per_peer
                    .entry(peer_id)
                    .or_default()
                    .insert(connection_id);
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                ..
            }) => {
                if let Some(connections) = self.inbound_per_peer.get_mut(&peer_id) {
                    connections.remove(&connection_id);
                    if connections.is_empty() {
                        self.inbound_per_peer.remove(&peer_id);
                    }
                }
            }
            _ => {}
        }
        self.totals.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self,
        _peer: PeerId,
        _connection: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {}
    }

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Harness {
        limits: ConnectionLimits,
        next_id: usize,
        local: Multiaddr,
        remote: Multiaddr,
    }

    impl Harness {
        fn new(direct_peers: &[PeerId]) -> Self {
            Self {
                limits: ConnectionLimits::new(direct_peers.iter().copied()),
                next_id: 0,
                local: "/ip4/127.0.0.1/udp/3382/quic-v1".parse().unwrap(),
                remote: "/ip4/10.0.0.1/udp/50000/quic-v1".parse().unwrap(),
            }
        }

        /// Run an inbound connection from `peer` through the limit check and,
        /// if admitted, report it established. Returns whether it was admitted.
        fn inbound(&mut self, peer: PeerId) -> bool {
            let id = ConnectionId::new_unchecked(self.next_id);
            self.next_id += 1;
            if self
                .limits
                .handle_pending_inbound_connection(id, &self.local, &self.remote)
                .is_err()
            {
                return false;
            }
            if self
                .limits
                .handle_established_inbound_connection(id, peer, &self.local, &self.remote)
                .is_err()
            {
                return false;
            }
            let endpoint = ConnectedPoint::Listener {
                local_addr: self.local.clone(),
                send_back_addr: self.remote.clone(),
            };
            self.limits
                .on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
                    peer_id: peer,
                    connection_id: id,
                    endpoint: &endpoint,
                    failed_addresses: &[],
                    other_established: 0,
                }));
            true
        }

        /// Run an outbound connection to `peer` through the limit check and,
        /// if admitted, report it established. Returns whether it was admitted.
        fn outbound(&mut self, peer: PeerId) -> bool {
            let id = ConnectionId::new_unchecked(self.next_id);
            self.next_id += 1;
            if self
                .limits
                .handle_pending_outbound_connection(id, Some(peer), &[], Endpoint::Dialer)
                .is_err()
            {
                return false;
            }
            if self
                .limits
                .handle_established_outbound_connection(
                    id,
                    peer,
                    &self.remote,
                    Endpoint::Dialer,
                    PortUse::Reuse,
                )
                .is_err()
            {
                return false;
            }
            let endpoint = ConnectedPoint::Dialer {
                address: self.remote.clone(),
                role_override: Endpoint::Dialer,
                port_use: PortUse::Reuse,
            };
            self.limits
                .on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
                    peer_id: peer,
                    connection_id: id,
                    endpoint: &endpoint,
                    failed_addresses: &[],
                    other_established: 0,
                }));
            true
        }

        fn fill_inbound_slots(&mut self) {
            for _ in 0..MAX_ESTABLISHED_INCOMING {
                assert!(self.inbound(PeerId::random()));
            }
        }
    }

    #[test]
    fn direct_peer_admitted_when_inbound_slots_are_full() {
        let validator = PeerId::random();
        let mut h = Harness::new(&[validator]);
        h.fill_inbound_slots();

        assert!(!h.inbound(PeerId::random()), "other peers are still capped");
        assert!(h.inbound(validator), "a validator is never locked out");
    }

    #[test]
    fn one_peer_cannot_hold_more_than_two_inbound_connections() {
        let mut h = Harness::new(&[]);
        let peer = PeerId::random();

        let admitted = (0..10).filter(|_| h.inbound(peer)).count();

        assert_eq!(admitted, 2);
        assert!(h.inbound(PeerId::random()), "other peers still get slots");
    }

    #[test]
    fn direct_peer_is_not_capped_per_peer() {
        let validator = PeerId::random();
        let mut h = Harness::new(&[validator]);

        let admitted = (0..4).filter(|_| h.inbound(validator)).count();

        assert_eq!(admitted, 4);
    }

    #[test]
    fn simultaneous_dials_do_not_deny_each_other() {
        let mut h = Harness::new(&[]);
        let peer = PeerId::random();

        assert!(h.outbound(peer));
        assert!(h.outbound(peer));

        assert!(
            h.inbound(peer),
            "our own dials must not use up the peer's inbound allowance"
        );
    }
}
