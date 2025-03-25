use std::{
    collections::{HashMap, HashSet},
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
        value: Value,
        predecessors: HashMap<TxId, TxMeta>,
    },

    // transaction - initial lock request
    Lock {
        txid: TxId,
        kind: LockKind,
    },
    LockGranted {
        txid: TxId,
        address: Address,
        completed: HashMap<TxId, TxMeta>,
        ancestor_vars: HashSet<Address>,
    },

    // transaction - messages available to shared and exclusive locks
    ReadValue {
        txid: TxId,
        predecessors: HashSet<TxId>,
    },
    ReadValueResult {
        txid: TxId,
        address: Address,
        value: Value,
        predecessors: HashMap<TxId, TxMeta>,
    },
    UpdateSubscriptions {
        txid: TxId,
        changes: HashMap<Address, bool>,
    },

    // transaction - messages available to exclusive locks
    WriteValue {
        txid: TxId,
        value: Value,
    },
    UpdateConfiguration {
        txid: TxId,
        configuration: ConfigurationUpdate,
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
pub struct InputMetadata {
    pub entries: HashMap<Address, InputMetadataEntry>,
}

#[derive(Clone)]
pub struct InputMetadataEntry {
    pub ancestor_variables: HashSet<Address>,
    pub current_value: Value,
}

#[derive(Clone)]
pub enum ConfigurationUpdate {
    Variable { value: Value },
    Definition { inputs: InputMetadata, expr: Expr },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxMeta {
    pub affected: HashSet<Address>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockKind {
    Shared = 0,
    Exclusive = 1,
}
