use std::collections::{HashMap, HashSet};

use crate::{
    actor::{Actor, Address, Context},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext},
        Action, Expr,
    },
    message::{LockKind, Message, MonotonicTimestampGenerator, TxId, TxKind},
    value::Value,
};

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    transactions: HashMap<TxId, ActiveTransaction>,
}

struct ActiveTransaction {
    action: Action,
    will_write: HashSet<Address>,
    locks: HashMap<Address, Lock>,
}

struct Lock {
    kind: LockKind,
    address: Address,
    value: Option<Value>,
    state: LockState,
}

enum LockState {
    Requested,
    Held,
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            timestamp_generator: MonotonicTimestampGenerator::new(),
            transactions: HashMap::new(),
        }
    }
}

impl Actor for Manager {
    fn handle(&mut self, _sender: Address, message: Message, ctx: Context) {
        match message {
            Message::Do { action } => {
                let txid = TxId {
                    kind: TxKind::Data,
                    timestamp: self.timestamp_generator.generate_timestamp(),
                    address: ctx.me().clone(),
                };

                self.transactions.insert(
                    txid,
                    ActiveTransaction {
                        action,
                        will_write: HashSet::new(),
                        locks: HashMap::new(),
                    },
                );
            }
            _ => todo!(),
        }
    }
}

impl ExprEvalContext<Address> for ActiveTransaction {
    fn read(&mut self, address: &Address) -> Option<Value> {
        todo!()
    }
}

impl ActionEvalContext<Address> for ActiveTransaction {
    fn write(&mut self, address: &Address, value: Option<Value>) {
        if let Some(value) = value {
            todo!();
        } else {
            self.will_write.insert(address.clone());
        }
    }
}
