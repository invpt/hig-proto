use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    time::SystemTime,
};

use crate::{
    actor::{Address, Version},
    expr::{Action, Expr, Name, Upgrade},
    value::Value,
};

#[derive(Clone)]
pub enum Message {
    // messages sent by the system itself
    Unreachable {
        message: Box<Message>,
    },

    // propagation
    Propagate {
        sender: Address,
        value: StampedValue,
    },

    // transaction - initial lock request
    Lock {
        txid: TxId,
        kind: LockKind,
    },
    LockGranted {
        txid: TxId,
        address: Address,
        roots: BasisStamp,
        ancestor_vars: HashSet<Address>,
    },

    // transaction - messages available to shared and exclusive locks
    Read {
        txid: TxId,
        roots: BasisStamp,
    },
    ReadResult {
        txid: TxId,
        address: Address,
        value: Value,
    },
    UpdateSubscriptions {
        txid: TxId,
        changes: HashMap<Address, bool>,
    },

    // transaction - messages available to exclusive locks
    Write {
        txid: TxId,
        value: Value,
    },
    Reconfigure {
        txid: TxId,
        configuration: NodeConfiguration,
    },
    Retire {
        txid: TxId,
    },

    // transaction - messages related to ending the lock
    Preempt {
        txid: TxId,
    },
    Abort {
        txid: TxId,
    },
    Release {
        txid: TxId,
        roots: BasisStamp,
    },

    // messages sent/received by managers
    Do {
        action: Action,
    },
    Upgrade {
        upgrade: Upgrade,
    },
    Directory {
        state: DirectoryState,
    },
}

#[derive(Clone)]
pub struct StampedValue {
    pub value: Value,
    pub basis: BasisStamp,
}

#[derive(Clone)]
pub struct BasisStamp {
    pub root_iterations: HashMap<Address, Iteration>,
}

impl BasisStamp {
    pub fn empty() -> BasisStamp {
        BasisStamp {
            root_iterations: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root_iterations.is_empty()
    }

    pub fn latest(&self, address: &Address) -> Iteration {
        self.root_iterations
            .get(address)
            .copied()
            .unwrap_or(Iteration(0))
    }

    pub fn add(&mut self, address: Address, iteration: Iteration) {
        let entry = self.root_iterations.entry(address).or_insert(iteration);
        *entry = (*entry).max(iteration);
    }

    pub fn merge_from(&mut self, other: &BasisStamp) {
        for (address, version) in &other.root_iterations {
            match self.root_iterations.entry(address.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(*version);
                }
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() = (*entry.get()).max(*version);
                }
            }
        }
    }

    pub fn prec_eq_wrt_roots(&self, other: &BasisStamp, roots: &HashSet<Address>) -> bool {
        for root in roots {
            if self.latest(root) > other.latest(root) {
                return false;
            }
        }

        true
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Iteration(usize);

impl Iteration {
    #[must_use]
    pub fn increment(self) -> Iteration {
        Iteration(self.0 + 1)
    }
}

#[derive(Clone)]
pub enum NodeConfiguration {
    Variable {
        value: StampedValue,
    },
    Definition {
        expr: Expr,
        inputs: HashMap<Address, InputConfiguration>,
    },
}

#[derive(Clone)]
pub struct InputConfiguration {
    pub roots: HashSet<Address>,
    pub value: StampedValue,
}

#[derive(Clone)]
pub struct DirectoryState {
    pub managers: HashMap<Address, bool>,

    // Multi-value register semantics:
    // If multiple nodes are assigned the same name concurrently, the directory will store all of them.
    pub nodes: HashMap<Name, HashMap<Address, Option<Version>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxId {
    pub priority: TxPriority,
    pub timestamp: Timestamp,
    pub address: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxPriority {
    High = 0,
    Low = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    epoch_micros: u64,
}

pub struct MonotonicTimestampGenerator {
    latest: Timestamp,
}

impl MonotonicTimestampGenerator {
    pub fn new() -> MonotonicTimestampGenerator {
        MonotonicTimestampGenerator {
            latest: Timestamp { epoch_micros: 0 },
        }
    }

    pub fn generate_timestamp(&mut self) -> Timestamp {
        #[cfg(not(target_arch = "wasm32"))]
        let epoch_micros = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        #[cfg(target_arch = "wasm32")]
        compile_error!("Wasm support has not yet been implemented.");

        if epoch_micros > self.latest.epoch_micros {
            self.latest = Timestamp { epoch_micros };
        } else {
            self.latest.epoch_micros += 1;
        }

        self.latest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockKind {
    Shared = 0,
    Exclusive = 1,
}
