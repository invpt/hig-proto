use std::collections::{HashMap, HashSet};

use crate::{
    actor::{Actor, Address, Context},
    lock::{Lock, LockEvent},
    message::{Message, TxId, TxMeta},
    value::Value,
};

pub struct Variable {
    lock: Lock<SharedLockState, ExclusiveLockState>,
    applied_transactions: HashMap<TxId, TxMeta>,
    subscribers: HashSet<Address>,
    value: Value,
}

impl Variable {
    pub fn new(value: Value, subscribers: HashSet<Address>) -> Variable {
        Variable {
            lock: Lock::new(),
            applied_transactions: HashMap::new(),
            subscribers,
            value,
        }
    }
}

impl Actor for Variable {
    fn handle(&mut self, message: Message, ctx: Context) {
        let message = 'unhandled: {
            match self.lock.handle(
                message,
                &ctx,
                &HashSet::new(),
                // TODO: make this more efficient
                &self.applied_transactions.keys().cloned().collect(),
            ) {
                LockEvent::Unhandled(message) => break 'unhandled message,
                LockEvent::Queued { .. } => (),
                LockEvent::Rejected { .. } => (),
                LockEvent::Aborted { .. } => (),
                LockEvent::Released {
                    data, predecessors, ..
                } => {
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
                            ExclusiveLockState::Update(new_value) => {
                                self.value = new_value;

                                for (txid, meta) in predecessors {
                                    self.applied_transactions.insert(txid, meta);
                                }

                                let message = Message::Update {
                                    sender: ctx.me().clone(),
                                    value: self.value.clone(),
                                    predecessors: self.applied_transactions.clone(),
                                };

                                for address in &self.subscribers {
                                    ctx.send(&address, message.clone());
                                }
                            }
                            ExclusiveLockState::Retire => ctx.retire(),
                        }
                    }
                }
            }

            return;
        };

        match message {
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
            Message::Read { txid } => {
                if self.lock.shared_lock(&txid).is_none() {
                    panic!("requested read without shared lock")
                }

                ctx.send(
                    &txid.address,
                    Message::ReadValue {
                        txid: txid.clone(),
                        value: self.value.clone(),
                        predecessors: self.applied_transactions.clone(),
                    },
                );
            }
            Message::Write { txid, value } => {
                let Some(state) = self.lock.exclusive_lock_mut(&txid) else {
                    panic!("requested write without exclusive lock")
                };

                if let ExclusiveLockState::Retire = state {
                    panic!("requested write after retirement request")
                }

                *state = ExclusiveLockState::Update(value);
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
    Update(Value),
    Retire,
}
