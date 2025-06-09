use std::collections::HashMap;

use crate::{
    actor::{Address, Version},
    expr::{Action, Upgrade},
    message::TxId,
    node::ReactiveId,
};

pub struct Transaction {
    kind: TransactionKind,
    state: TransactionState,
}

pub enum TransactionKind {
    Action(Action),
    Upgrade(Upgrade),
}

struct TransactionState {
    id: TxId,
    pending_locks: HashMap<Address, ExpectedVersions>,
    locks: HashMap<Address, Lock>,
}

type ExpectedVersions = HashMap<ReactiveId, Version>;

struct Lock {}

impl Transaction {
    pub fn new(id: TxId, kind: TransactionKind) -> Transaction {
        Transaction {
            kind: kind,
            state: TransactionState::new(id),
        }
    }
}

impl TransactionState {
    fn new(id: TxId) -> TransactionState {
        TransactionState {
            id,
            pending_locks: HashMap::new(),
            locks: HashMap::new(),
        }
    }
}
