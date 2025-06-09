use std::collections::HashMap;

use transaction::Transaction;

use crate::{
    actor::{Actor, Context},
    message::{Message, MonotonicTimestampGenerator, TxId},
};

mod transaction;

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    transactions: HashMap<TxId, Transaction>,
}

impl Actor for Manager {
    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            _ => todo!(),
        }
    }
}
