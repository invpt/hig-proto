use std::collections::{HashMap, HashSet};

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
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxId {}

#[derive(Clone, PartialEq, Eq)]
pub struct TxMeta {
    pub affected: HashSet<Address>,
}
