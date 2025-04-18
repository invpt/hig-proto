use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    time::SystemTime,
};

use crate::{
    actor::{Address, Version},
    expr::{Action, Expr, Name, Type, Upgrade, Value},
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
        version: Version,
        node_kind: NodeKind,
        type_: Type,
    },

    // transaction - messages available to shared and exclusive locks
    Read {
        txid: TxId,
        basis: BasisStamp,
    },
    ReadResult {
        txid: TxId,
        address: Address,
        value: StampedValue,
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
        basis: BasisStamp,
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
pub enum NodeKind {
    Variable {
        iteration: Iteration,
    },
    Definition {
        ancestors: HashMap<Address, Ancestor>,
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
        match self.root_iterations.entry(address) {
            Entry::Vacant(entry) => {
                entry.insert(iteration);
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = (*entry.get()).max(iteration);
            }
        }
    }

    pub fn merge_from(&mut self, other: &BasisStamp) {
        for (address, iteration) in &other.root_iterations {
            self.add(address.clone(), *iteration);
        }
    }

    pub fn prec_eq_wrt_ancestors(
        &self,
        other: &BasisStamp,
        ancestors: &HashMap<Address, Ancestor>,
    ) -> bool {
        for ancestor in ancestors.keys() {
            if self.latest(ancestor) > other.latest(ancestor) {
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
        expr: Expr<Address>,
        inputs: HashMap<Address, InputConfiguration>,
    },
}

#[derive(Clone)]
pub struct InputConfiguration {
    pub ancestors: HashMap<Address, Ancestor>,
    pub value: StampedValue,
}

#[derive(Clone)]
pub struct Ancestor {
    pub is_root: bool,
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
