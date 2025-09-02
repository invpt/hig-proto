use std::collections::HashMap;

use transaction::Transaction;

use crate::{
    actor::{Actor, Context},
    expr::Upgrade,
    message::{Message, MonotonicTimestampGenerator, TxId, TxPriority},
};

mod transaction;

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    transactions: HashMap<TxId, Transaction>,
}

impl Manager {
    fn eval(&mut self, txid: &TxId, ctx: &Context) {
        self.transactions
            .get_mut(txid)
            .expect("missing transaction")
            .eval(ctx);
    }
}

impl Actor for Manager {
    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            Message::Do {
                action,
                upgrade,
                expected_versions,
            } => {
                let timestamp = self.timestamp_generator.generate_timestamp();
                let txid = TxId {
                    priority: if !matches!(upgrade, Upgrade::Nil) {
                        TxPriority::High
                    } else {
                        TxPriority::Low
                    },
                    timestamp,
                    address: ctx.me().clone(),
                };

                self.transactions.insert(
                    txid.clone(),
                    Transaction::new(txid.clone(), action, upgrade, expected_versions),
                );

                self.eval(&txid, &ctx);
            }
            Message::LockGranted {
                txid,
                address,
                reactives,
            } => {
                self.transactions
                    .get_mut(&txid)
                    .expect("missing transaction")
                    .lock_granted(address, reactives, &ctx);
            }
            Message::ReadResult {
                txid,
                reactive,
                value,
            } => {
                self.transactions
                    .get_mut(&txid)
                    .expect("missing transaction")
                    .read_result(reactive, value, &ctx);
            }
            Message::ReadConfigurationResult {
                txid,
                service,
                imports,
            } => {
                self.transactions
                    .get_mut(&txid)
                    .expect("missing transaction")
                    .read_configuration_result(service, imports, &ctx);
            }
            _ => todo!(),
        }
    }
}
