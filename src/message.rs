use std::{
    collections::{HashMap, HashSet},
    time::{Instant, UNIX_EPOCH},
};

use crate::{router::Address, value::Value};

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
    epoch_micros: u128,
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
