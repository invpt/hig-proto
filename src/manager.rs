use std::collections::HashMap;

use crate::{
    message::{LockKind, Message, TxId},
    router::{Actor, Address, Context},
    transaction::Transaction,
    value::Value,
};

pub struct Manager {
    transactions: HashMap<TxId, ActiveTransaction>,
}

struct ActiveTransaction {
    transaction: Transaction,
    locks: HashMap<Address, LockKind>,
    state: State,
}

enum State {
    New,
    Read(Address, ReadState),
    Write(Address, Value, WriteState),
}

enum ReadState {}

enum WriteState {}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            transactions: HashMap::new(),
        }
    }
}

impl Actor for Manager {
    fn handle(&mut self, sender: Address, message: Message, ctx: Context) {}
}
