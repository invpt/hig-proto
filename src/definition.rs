use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    actor::{Actor, Address, Context},
    expr::Expr,
    lock::{Lock, LockEvent},
    message::{Message, TxId, TxMeta},
    value::Value,
};

pub struct Definition {
    lock: Lock<SharedLockState, ExclusiveLockState>,
    replicas: HashMap<Address, Value>,
    ancestor_variable_to_inputs: HashMap<Address, Vec<Address>>,
    expr: Expr,
    value: Value,
    subscribers: HashSet<Address>,
    applied_transactions: HashSet<TxId>,
    transactions: HashMap<TxId, PendingTransaction>,
    updates: HashMap<ReceivedUpdateId, ReceivedUpdate>,
    counter: usize,
}

struct PendingTransaction {
    updates: HashMap<Address, Option<ReceivedUpdateId>>,
    predecessors: HashSet<TxId>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ReceivedUpdateId(usize);

pub struct InputMetadata {
    pub ancestor_variables: Box<[Address]>,
    pub current_value: Value,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct UpdateLink {
    address: Address,
    txid: TxId,
}

struct ReceivedUpdate {
    address: Address,
    value: Value,
}

impl Definition {
    fn new(
        inputs: HashMap<Address, InputMetadata>,
        expr: Expr,
        subscribers: HashSet<Address>,
    ) -> Definition {
        let mut ancestor_variable_to_inputs = HashMap::<Address, Vec<Address>>::new();
        for (input, meta) in &inputs {
            for ancestor in &meta.ancestor_variables {
                match ancestor_variable_to_inputs.entry(ancestor.clone()) {
                    Entry::Occupied(mut e) => {
                        e.get_mut().push(input.clone());
                    }
                    Entry::Vacant(e) => {
                        e.insert(Vec::from([input.clone()]));
                    }
                }
            }
        }

        let mut replicas = inputs
            .into_iter()
            .map(|(a, i)| (a, i.current_value))
            .collect();

        let mut expr_copy = expr.clone();
        expr_copy.eval(&mut replicas);
        let Expr::Value(value) = expr_copy else {
            panic!("def expression did not fully evaluate")
        };

        Definition {
            lock: Lock::new(),
            replicas,
            ancestor_variable_to_inputs,
            subscribers,
            expr,
            value,
            transactions: HashMap::new(),
            updates: HashMap::new(),
            applied_transactions: HashSet::new(),
            counter: 0,
        }
    }

    fn find_batch(&mut self) -> HashSet<TxId> {
        let mut batch = HashSet::<TxId>::new();

        let mut scratch = HashSet::<TxId>::new();
        let mut memo = HashMap::<TxId, bool>::new();
        for txid in self.transactions.keys().cloned() {
            if self.batch_dfs(txid, &mut scratch, &mut memo) {
                for batch_txid in scratch.iter().cloned() {
                    batch.insert(batch_txid);
                }
            }

            scratch.clear();
        }

        batch
    }

    fn batch_dfs(
        &self,
        txid: TxId,
        batch: &mut HashSet<TxId>,
        memo: &mut HashMap<TxId, bool>,
    ) -> bool {
        if let Some(result) = memo.get(&txid) {
            return *result;
        }

        batch.insert(txid.clone());

        let tx = &self.transactions[&txid];

        let complete = tx.updates.values().all(Option::is_some);
        if !complete {
            memo.insert(txid, false);
            return false;
        }

        for pred in &tx.predecessors {
            if !self.batch_dfs(pred.clone(), batch, memo) {
                memo.insert(txid, false);
                return false;
            }
        }

        memo.insert(txid, true);

        true
    }

    fn apply_batch(&mut self, batch: HashSet<TxId>, ctx: Context) {
        let mut update_ids = batch
            .iter()
            .cloned()
            .flat_map(|txid| self.transactions[&txid].updates.values())
            .map(|id| id.expect("incomplete transaction found in batch"))
            .collect::<Vec<ReceivedUpdateId>>();

        // collected into vec above so that we can sort
        update_ids.sort();

        for id in update_ids {
            let update = self.updates.remove(&id).expect("could not locate update");
            self.replicas.insert(update.address, update.value);
        }

        let mut inputs = HashMap::<Address, Value>::new();
        for (address, value) in &self.replicas {
            inputs.insert(address.clone(), value.clone());
        }

        let mut expr_copy = self.expr.clone();
        expr_copy.eval(&mut self.replicas);
        let Expr::Value(value) = expr_copy else {
            panic!("def expression did not fully evaluate")
        };

        let message = Message::Update {
            sender: ctx.me().clone(),
            value,
            predecessors: batch
                .iter()
                .cloned()
                .map(|txid| {
                    (
                        txid.clone(),
                        TxMeta {
                            affected: self
                                .transactions
                                .remove(&txid)
                                .expect("could not locate transaction")
                                .updates
                                .keys()
                                .cloned()
                                .collect(),
                        },
                    )
                })
                .collect::<HashMap<TxId, TxMeta>>(),
        };

        for sub in &self.subscribers {
            ctx.send(sub, message.clone());
        }

        for txid in batch {
            self.transactions.remove(&txid);
            self.applied_transactions.insert(txid);
        }
    }

    fn new_pending_update_id(&mut self) -> ReceivedUpdateId {
        let id = self.counter;
        self.counter += 1;
        ReceivedUpdateId(id)
    }
}

impl Actor for Definition {
    fn handle(&mut self, message: Message, ctx: Context) {
        let message = 'unhandled: {
            match self.lock.handle(
                message,
                &ctx,
                &self.ancestor_variable_to_inputs.keys().cloned().collect(),
                &self.applied_transactions,
            ) {
                LockEvent::Unhandled(message) => break 'unhandled message,
                LockEvent::Queued { .. } => (),
                LockEvent::Aborted { .. } => (),
                LockEvent::Released { data, .. } => {
                    for (subscriber, subscribe) in data.shared.subscription_updates {
                        if subscribe {
                            self.subscribers.insert(subscriber);
                        } else {
                            self.subscribers.remove(&subscriber);
                        }
                    }

                    if let Some(exclusive_data) = data.exclusive {
                        match exclusive_data {
                            ExclusiveLockState::Normal => (),
                            ExclusiveLockState::Retire => {
                                ctx.retire();
                            }
                        }
                    }
                }
            }

            return;
        };

        match message {
            Message::Update {
                sender,
                value,
                predecessors,
            } => {
                let id = self.new_pending_update_id();
                self.updates.insert(
                    id,
                    ReceivedUpdate {
                        address: sender.clone(),
                        value,
                    },
                );

                for (txid, meta) in &predecessors {
                    match self.transactions.entry(txid.clone()) {
                        Entry::Vacant(e) => {
                            let affected_inputs = meta
                                .affected
                                .iter()
                                .flat_map(|v| self.ancestor_variable_to_inputs.get(v))
                                .flat_map(|v| v)
                                .cloned()
                                .map(|addr| (addr, None));

                            let mut updates = HashMap::from_iter(affected_inputs);
                            updates.insert(sender.clone(), Some(id));

                            let predecessors = HashSet::from_iter(predecessors.keys().cloned());

                            e.insert(PendingTransaction {
                                updates,
                                predecessors,
                            });
                        }
                        Entry::Occupied(mut entry) => {
                            let txn = entry.get_mut();
                            if let Some(slot @ None) = txn.updates.get_mut(&sender) {
                                *slot = Some(id);
                                for pred in predecessors.keys().cloned() {
                                    txn.predecessors.insert(pred);
                                }
                            }
                        }
                    }
                }

                let batch = self.find_batch();
                self.apply_batch(batch, ctx);
            }
            Message::SubscriptionUpdate {
                txid,
                subscriber,
                subscribe,
            } => {
                let Some(state) = self.lock.shared_lock_mut(&txid) else {
                    panic!("requested subscription update without shared lock")
                };

                state.subscription_updates.push((subscriber, subscribe));
            }
            Message::Retire { txid } => {
                let Some(state) = self.lock.exclusive_lock_mut(&txid) else {
                    panic!("requested retirement without exclusive lock")
                };

                *state = ExclusiveLockState::Retire;
            }
            _ => todo!(),
        }
    }
}

#[derive(Default)]
struct SharedLockState {
    subscription_updates: Vec<(Address, bool)>,
}

#[derive(Default)]
enum ExclusiveLockState {
    #[default]
    Normal,
    Retire,
}
