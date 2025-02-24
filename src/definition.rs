use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    lock::{Lock, LockData, LockEvent},
    message::{Message, TxId, TxMeta},
    router::{Actor, Address, Context},
    value::Value,
};

pub struct Definition {
    lock: Lock<SharedLockState, ()>,
    replicas: HashMap<Address, Option<Value>>,
    ancestor_variable_to_inputs: HashMap<Address, Vec<Address>>,
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
    fn new(inputs: HashMap<Address, InputMetadata>, subscribers: HashSet<Address>) -> Definition {
        let mut ancestor_variable_to_inputs = HashMap::<Address, Vec<Address>>::new();
        for (input, ancestors) in &inputs {
            for ancestor in &ancestors.ancestor_variables {
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

        Definition {
            lock: Lock::new(),
            replicas: inputs.keys().map(|a| (a.clone(), None)).collect(),
            ancestor_variable_to_inputs,
            subscribers,
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
            self.replicas.insert(update.address, Some(update.value));
        }

        let replicas_complete = self.replicas.values().all(Option::is_some);
        if !replicas_complete {
            return;
        }

        let mut inputs = HashMap::<Address, Value>::new();
        for (address, value) in &self.replicas {
            let Some(value) = value else {
                // if one of our inputs is still missing a value, we can't send an update
                return;
            };

            inputs.insert(address.clone(), value.clone());
        }

        let message = Message::Update {
            value: Value::Definition {
                address: ctx.me().clone(),
                inputs,
            },
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
            ctx.send(sub.clone(), message.clone());
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
    fn handle(&mut self, sender: Address, message: Message, ctx: Context) {
        let message = 'unhandled: {
            match self.lock.handle(message, &ctx, &self.applied_transactions) {
                LockEvent::Unhandled(message) => break 'unhandled message,
                LockEvent::Queued { .. } => (),
                LockEvent::Aborted { .. } => (),
                LockEvent::Released { data, .. } => {
                    let state = match data {
                        LockData::Shared(state) => state,
                        LockData::Exclusive(state, _) => state,
                    };

                    for (subscriber, subscribe) in state.subscription_updates {
                        if subscribe {
                            self.subscribers.insert(subscriber);
                        } else {
                            self.subscribers.remove(&subscriber);
                        }
                    }
                }
            }

            return;
        };

        match message {
            Message::Update {
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
            _ => todo!(),
        }
    }
}

#[derive(Default)]
struct SharedLockState {
    subscription_updates: Vec<(Address, bool)>,
}
