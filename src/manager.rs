use std::collections::{HashMap, HashSet};

use crate::{
    actor::{Actor, Address, Context},
    expr::{Action, Upgrade},
    message::{Message, MonotonicTimestampGenerator, TxId, TxPriority},
};

mod directory;
mod transaction;

use directory::Directory;
use transaction::{Transaction, TransactionKind};

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    node_inputs: HashMap<Address, HashSet<Address>>,
    transactions: HashMap<TxId, Transaction>,
    directory: Directory,
}

impl Manager {
    pub fn new(seed_peers: impl Iterator<Item = Address>) -> Manager {
        Manager {
            timestamp_generator: MonotonicTimestampGenerator::new(),
            node_inputs: HashMap::new(),
            transactions: HashMap::new(),
            directory: Directory::new(seed_peers),
        }
    }

    fn do_action(&mut self, action: Action, ctx: &Context) {
        let txid = TxId {
            priority: TxPriority::Low,
            timestamp: self.timestamp_generator.generate_timestamp(),
            address: ctx.me().clone(),
        };

        let tx = Transaction::new(txid.clone(), TransactionKind::Action(action));

        self.transactions.insert(txid.clone(), tx);

        self.eval_tx(&txid, &ctx);
    }

    fn do_upgrade(&mut self, upgrade: Upgrade, ctx: &Context) {
        let txid = TxId {
            priority: TxPriority::High,
            timestamp: self.timestamp_generator.generate_timestamp(),
            address: ctx.me().clone(),
        };

        let tx = Transaction::new(txid.clone(), TransactionKind::Upgrade(upgrade));

        self.transactions.insert(txid.clone(), tx);

        self.eval_tx(&txid, &ctx);
    }

    fn eval_tx(&mut self, txid: &TxId, ctx: &Context) {
        let is_done = self
            .transactions
            .get_mut(txid)
            .expect("attempted to evaluate nonexistent transaction")
            .eval(&self.directory, ctx);

        if is_done {
            self.transactions.remove(txid);
        }
    }
}

impl Actor for Manager {
    // fn init(&mut self, ctx: Context) {
    //     self.directory.init(&ctx);
    // }

    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            Message::Directory { state } => self.directory.merge_and_update(state, &ctx),
            Message::Do { action } => self.do_action(action, &ctx),
            Message::Upgrade { upgrade } => self.do_upgrade(upgrade, &ctx),
            Message::LockGranted {
                txid,
                address,
                node_kind,
                version,
            } => {
                let tx = self.transactions.get_mut(&txid).unwrap();

                tx.lock_granted(address, version, node_kind);

                self.eval_tx(&txid, &ctx);
            }
            Message::ReadResult {
                txid,
                address,
                value,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("received message for unknown transaction");

                tx.read_result(address, value);

                self.eval_tx(&txid, &ctx);
            }
            _ => todo!(),
        }
    }
}
