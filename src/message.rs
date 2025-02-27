use std::{
    collections::{HashMap, HashSet},
    sync::atomic::AtomicU64,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    actor::Address,
    expr::{Action, Upgrade},
    value::Value,
};

#[derive(Clone)]
pub enum Message {
    Unreachable {
        message: Box<Message>,
    },
    Update {
        value: Value,
        predecessors: HashMap<TxId, TxMeta>,
    },
    Lock {
        txid: TxId,
        kind: LockKind,
        predecessors: HashSet<TxId>,
    },
    LockGranted {
        txid: TxId,
        predecessors: HashSet<TxId>,
    },
    SubscriptionUpdate {
        txid: TxId,
        subscriber: Address,
        subscribe: bool,
    },
    Read {
        txid: TxId,
    },
    Value {
        txid: TxId,
        value: Value,
        predecessors: HashMap<TxId, TxMeta>,
    },
    Write {
        txid: TxId,
        value: Value,
    },
    Retire {
        txid: TxId,
    },
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

    // manager-only
    Do {
        action: Action,
    },
    Upgrade {
        upgrade: Upgrade,
    },
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    Shared,
    Exclusive,
}
