use std::collections::HashMap;

use crate::{router::Address, value::Value};

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

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TxMeta {
    pub affected: Box<[Address]>,
}

// [(t, w), (t1, w1)]

// (d, P)
