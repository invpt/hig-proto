use std::collections::HashMap;

use transaction::Transaction;

use crate::{
    actor::{Actor, Context},
    expr::TransactionKind,
    message::{Message, MonotonicTimestampGenerator, TxId, TxPriority},
};

mod transaction;

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    transactions: HashMap<TxId, Transaction>,
}

impl Manager {
    fn eval(&mut self, txid: &TxId, ctx: &Context) {}
}

impl Actor for Manager {
    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            Message::Do { transaction } => {
                let txid = TxId {
                    priority: match transaction {
                        TransactionKind::Action(_) => TxPriority::Low,
                        TransactionKind::Upgrade(_) => TxPriority::High,
                    },
                    timestamp: self.timestamp_generator.generate_timestamp(),
                    address: ctx.me().clone(),
                };

                let tx = self
                    .transactions
                    .entry(txid.clone())
                    .insert_entry(Transaction::new(txid, transaction))
                    .into_mut();

                tx.init(&ctx);
            }
            Message::LockGranted {
                txid,
                address,
                reactives,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("Lock granted for unknown transaction");

                tx.lock_granted(address, reactives, &ctx);
            }
            Message::ReturnedConfiguration {
                txid,
                address,
                imports,
                exports,
                subscriptions,
                roots,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("Lock granted for unknown transaction");

                tx.returned_configuration(address, imports, exports, subscriptions, roots, &ctx);
            }
            _ => todo!(),
        }
    }
}
