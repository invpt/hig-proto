use std::collections::HashMap;

use transaction::Transaction;

use crate::{
    actor::{Actor, Context},
    expr::Upgrade,
    message::{Message, MonotonicTimestampGenerator, TxId, TxPriority},
};

mod graph;
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
            Message::Do {
                action,
                upgrade,
                expected_versions,
            } => {
                let txid = TxId {
                    priority: if let Upgrade::Nil = upgrade {
                        TxPriority::Low
                    } else {
                        TxPriority::High
                    },
                    timestamp: self.timestamp_generator.generate_timestamp(),
                    address: ctx.me().clone(),
                };

                self.transactions.insert(
                    txid.clone(),
                    Transaction::new(txid, action, upgrade, expected_versions, &ctx),
                );
            }
            Message::LockGranted {
                txid,
                service,
                reactives,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("Lock granted for unknown transaction");

                tx.lock_granted(service, reactives, &ctx);
            }
            Message::ReturnedConfiguration {
                txid,
                service,
                imports,
                inputs: reactives,
                exports,
                next_id,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("Returned configuration for unknown transaction");

                tx.returned_configuration(service, imports, reactives, exports, next_id, &ctx);
            }
            Message::ReturnedValue {
                txid,
                reactive,
                value,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("Returned value for unknown transaction");

                tx.returned_value(reactive, value, &ctx);
            }
            _ => todo!(),
        }
    }
}
