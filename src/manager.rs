use std::collections::HashMap;

use crate::{
    actor::{Actor, Address, Context},
    expr::Action,
    message::{LockKind, Message, TxId},
    value::Value,
};

pub struct Manager {
    transactions: HashMap<TxId, ActiveTransaction>,
}

struct ActiveTransaction {
    action: Action,
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
