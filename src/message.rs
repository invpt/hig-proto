use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    time::SystemTime,
};

use crate::{
    actor::{Address, Version},
    expr::{Action, Expr, Name, Type, Upgrade, Value},
    node::{Import, ReactiveAddress, ReactiveId},
};

#[derive(Clone)]
pub enum Message {
    // messages sent by the system itself
    Unreachable {
        message: Box<Message>,
    },

    // propagation
    Propagate {
        sender: ReactiveAddress,
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
    },

    // transaction - messages available to shared and exclusive locks
    Read {
        txid: TxId,
        reactive: ReactiveId,
        basis: BasisStamp,
    },
    ReadResult {
        txid: TxId,
        reactive: ReactiveAddress,
        value: StampedValue,
    },

    // transaction - messages available to exclusive locks
    Write {
        txid: TxId,
        reactive: ReactiveId,
        value: Value,
    },
    ReadConfiguration {
        txid: TxId,
    },
    ReadConfigurationResult {
        imports: HashMap<ReactiveAddress, Import>,
    },
    Configure {
        txid: TxId,
        imports: HashMap<ReactiveAddress, Option<ImportConfiguration>>,
        reactives: HashMap<ReactiveId, Option<ReactiveConfiguration>>,
        exports: HashMap<ReactiveId, HashSet<Address>>,
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
    PrepareCommit {
        txid: TxId,
    },
    CommitPrepared {
        txid: TxId,
        basis: BasisStamp,
    },
    Commit {
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
pub struct ImportConfiguration {
    pub roots: HashSet<ReactiveAddress>,
}

#[derive(Clone)]
pub struct StampedValue {
    pub value: Value,
    pub basis: BasisStamp,
}

#[derive(Clone)]
pub struct BasisStamp {
    pub roots: HashMap<ReactiveAddress, Iteration>,
}

impl BasisStamp {
    pub fn empty() -> BasisStamp {
        BasisStamp {
            roots: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    pub fn latest(&self, address: &ReactiveAddress) -> Iteration {
        self.roots.get(address).copied().unwrap_or(Iteration(0))
    }

    pub fn add(&mut self, address: ReactiveAddress, iteration: Iteration) {
        match self.roots.entry(address) {
            Entry::Vacant(entry) => {
                entry.insert(iteration);
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = (*entry.get()).max(iteration);
            }
        }
    }

    pub fn merge_from(&mut self, other: &BasisStamp) {
        for (address, iteration) in &other.roots {
            self.add(address.clone(), *iteration);
        }
    }

    pub fn clear(&mut self) {
        self.roots.clear();
    }

    pub fn prec_eq_wrt_roots(&self, other: &BasisStamp, roots: &HashSet<ReactiveAddress>) -> bool {
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
    pub const ZERO: Iteration = Iteration(0);

    #[must_use]
    pub fn increment(self) -> Iteration {
        Iteration(self.0 + 1)
    }
}

#[derive(Clone)]
pub enum ReactiveConfiguration {
    Variable { value: StampedValue },
    Definition { expr: Expr<ReactiveAddress> },
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

#[derive(Debug, Clone, Copy)]
pub enum LockKind {
    Shared,
    Exclusive,
}
