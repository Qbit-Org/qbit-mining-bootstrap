//! The fault injector: each fault a scenario can inject, and how it heals.
//!
//! | Fault | Injection | Heal |
//! | --- | --- | --- |
//! | `FrontendKill9` | SIGKILL the node's frontend | start it again, wait until ready |
//! | `FrontendFreeze` | SIGSTOP it: sockets and locks held, nothing answered | SIGCONT |
//! | `PostgresKill` | SIGKILL the postmaster and every backend | start it: crash recovery |
//! | `PostgresFreeze` | SIGSTOP the postmaster and every backend | SIGCONT |
//! | `NetworkDrop` | cut every link to and from the node (balancer routes and checks, both peer pulls, the 3.0 writer and replication links) and turn its `qbitd`'s networking off | reopen them, reconnect its `qbitd` |
//! | `LinkCut` | cut only the links between the two databases | reopen them |
//!
//! A cut is a reset or a blackhole ([`LinkState`]); see `relay.rs`.
//! Restoring an older base backup and replacing a disk are scenario steps
//! (they need the node down and a recovery procedure), in `scenarios.rs`.

use crate::{frontend::Node, relay::LinkState, sim::Sim};
use anyhow::Result;
use serde::Serialize;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "fault", content = "node", rename_all = "kebab-case")]
pub enum Fault {
    FrontendKill9(Node),
    FrontendFreeze(Node),
    PostgresKill(Node),
    PostgresFreeze(Node),
    NetworkDrop(Node, LinkState),
    LinkCut(LinkState),
}

impl Fault {
    pub fn describe(&self) -> String {
        match self {
            Fault::FrontendKill9(node) => format!("kill -9 of node {node:?}'s frontend"),
            Fault::FrontendFreeze(node) => format!("SIGSTOP of node {node:?}'s frontend"),
            Fault::PostgresKill(node) => format!("kill -9 of node {node:?}'s PostgreSQL"),
            Fault::PostgresFreeze(node) => format!("SIGSTOP of node {node:?}'s PostgreSQL"),
            Fault::NetworkDrop(node, state) => {
                format!("node {node:?}'s network dropped ({state:?})")
            }
            Fault::LinkCut(state) => format!("the link between the databases cut ({state:?})"),
        }
    }

    /// The node the fault takes out of service, if any.
    pub fn victim(&self) -> Option<Node> {
        match self {
            Fault::FrontendKill9(node)
            | Fault::FrontendFreeze(node)
            | Fault::PostgresKill(node)
            | Fault::PostgresFreeze(node)
            | Fault::NetworkDrop(node, _) => Some(*node),
            Fault::LinkCut(_) => None,
        }
    }
}

/// How long a healed frontend may take to report ready: a restart migrates
/// nothing new, connects, and builds its first work.
const READY_LIMIT: Duration = Duration::from_secs(120);

impl Sim {
    /// Inject `fault` now, and stamp it on the timeline. Returns the run
    /// clock's time of the injection.
    pub async fn inject(&mut self, fault: Fault) -> Result<u64> {
        let at = self.clock.now_ms();
        match fault {
            Fault::FrontendKill9(node) => self.frontend_mut(node).kill9()?,
            Fault::FrontendFreeze(node) => self.frontend_mut(node).freeze()?,
            Fault::PostgresKill(node) => self.pg_mut(node).kill9()?,
            Fault::PostgresFreeze(node) => self.pg_mut(node).freeze()?,
            Fault::NetworkDrop(node, state) => {
                for link in self.links.of_node(node) {
                    self.links.set(&link, state)?;
                }
                self.chain.isolate(crate::sim::qbitd_of(node)).await?;
            }
            Fault::LinkCut(state) => {
                for link in self.links.between_nodes() {
                    self.links.set(&link, state)?;
                }
            }
        }
        self.mark(&format!("injected: {}", fault.describe()));
        Ok(at)
    }

    /// Undo `fault` and wait until what it broke serves again.
    pub async fn heal(&mut self, fault: Fault) -> Result<u64> {
        match fault {
            Fault::FrontendKill9(node) => {
                self.frontend_mut(node).start()?;
                let took = self.frontend(node).wait_ready(READY_LIMIT).await?;
                self.mark(&format!(
                    "node {node:?}'s frontend ready {:.1} s after its restart",
                    took.as_secs_f64()
                ));
            }
            Fault::FrontendFreeze(node) => self.frontend_mut(node).thaw()?,
            Fault::PostgresKill(node) => self.pg_mut(node).start()?,
            Fault::PostgresFreeze(node) => self.pg_mut(node).thaw()?,
            Fault::NetworkDrop(node, _) => {
                for link in self.links.of_node(node) {
                    self.links.set(&link, LinkState::Open)?;
                }
                self.chain.rejoin(crate::sim::qbitd_of(node)).await?;
            }
            Fault::LinkCut(_) => {
                for link in self.links.between_nodes() {
                    self.links.set(&link, LinkState::Open)?;
                }
            }
        }
        let at = self.clock.now_ms();
        self.mark(&format!("healed: {}", fault.describe()));
        Ok(at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fault_names_its_victim() {
        assert_eq!(Fault::FrontendKill9(Node::A).victim(), Some(Node::A));
        assert_eq!(
            Fault::NetworkDrop(Node::B, LinkState::Blackholed).victim(),
            Some(Node::B)
        );
        assert_eq!(Fault::LinkCut(LinkState::Reset).victim(), None);
        assert_eq!(
            serde_json::to_value(Fault::PostgresKill(Node::B)).unwrap(),
            serde_json::json!({"fault": "postgres-kill", "node": "B"})
        );
    }
}
