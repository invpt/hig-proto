use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

use crate::{
    actor::Address,
    expr::{Action, Name, Upgrade},
    value::Value,
};

#[derive(Clone)]
pub enum Message {
    // messages sent by the system itself
    Unreachable {
        message: Box<Message>,
    },

    // propagation
    Update {
        sender: Address,
        value: Value,
        predecessors: HashMap<TxId, TxMeta>,
    },

    // mutation - initial lock request
    Lock {
        txid: TxId,
        kind: LockKind,
        predecessors: HashSet<TxId>,
    },
    LockRejected {
        txid: TxId,
        needs_predecessors_from_inputs: HashSet<Address>,
    },
    LockGranted {
        txid: TxId,
        predecessors: HashSet<TxId>,
    },

    // mutation - messages available to shared and exclusive locks
    SubscriptionUpdate {
        txid: TxId,
        subscriber: Address,
        subscribe: bool,
    },
    Read {
        txid: TxId,
    },
    ReadValue {
        txid: TxId,
        value: Value,
        predecessors: HashMap<TxId, TxMeta>,
    },

    // mutation - messages available to exclusive locks
    Write {
        txid: TxId,
        value: Value,
    },
    Retire {
        txid: TxId,
    },

    // mutation - messages related to ending the lock
    Preempt {
        txid: TxId,
    },
    Abort {
        txid: TxId,
    },
    Release {
        txid: TxId,
        predecessors: HashMap<TxId, TxMeta>,
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
pub struct DirectoryState {
    pub managers: HashMap<Address, bool>,
    pub nodes: HashMap<Name, DirectoryEntry>,
}

#[derive(Clone)]
pub struct DirectoryEntry {
    pub txid: TxId,
    pub address: Address,
    pub deleted: bool,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxId {
    pub kind: TxKind,
    pub timestamp: Timestamp,
    pub address: Address,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxKind {
    Code = 0,
    Data = 1,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Clone, PartialEq, Eq)]
pub struct TxMeta {
    pub affected: HashSet<Address>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum LockKind {
    Shared = 0,
    Exclusive = 1,
}
