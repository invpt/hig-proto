use std::{
    collections::{btree_map, BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use crate::{
    actor::{Actor, Address, Context},
    expr::{eval::ExprEvalContext, Expr},
    message::{LockKind, Message, TxId, TxMeta},
    value::Value,
};

pub struct Node {
    queue: BTreeMap<TxId, LockKind>,
    held: HeldLocks,
    serviced: HashMap<TxId, TxMeta>,
    preempting: HashSet<TxId>,

    inputs: InputMetadata,
    updates: HashMap<TxId, Update>,
    expr: Expr,

    cached_value: Option<Value>,

    subscribers: HashSet<Address>,
}

#[derive(Clone)]
pub struct InputMetadataEntry {
    pub ancestor_variables: HashSet<Address>,
    pub current_value: Value,
}

pub struct InputMetadata {
    pub entries: HashMap<Address, InputMetadataEntry>,
}

enum Update {
    Pending {
        values: HashMap<Address, Option<Arc<Value>>>,
        predecessors: HashSet<TxId>,
    },
    Applied,
}

enum HeldLocks {
    None,
    Shared(BTreeMap<TxId, SubscriptionUpdates>),
    Exclusive(TxId, SubscriptionUpdates, CodeUpdates),
}

type SubscriptionUpdates = Vec<(Address, bool)>;

enum CodeUpdates {
    None,
    Update(Expr, InputMetadata),
    Retire,
}

impl Node {
    fn handle_lock_released(
        &mut self,
        predecessors: HashMap<TxId, TxMeta>,
        subscription_updates: SubscriptionUpdates,
        code_updates: CodeUpdates,
        ctx: Context,
    ) {
        for (subscriber, subscribe) in subscription_updates {
            if subscribe {
                self.subscribers.insert(subscriber);
            } else {
                self.subscribers.remove(&subscriber);
            }
        }

        match code_updates {
            CodeUpdates::None => {}
            CodeUpdates::Retire => ctx.retire(),
            CodeUpdates::Update(expr, inputs) => {
                self.expr = expr;
                self.inputs = inputs;

                let value = if self.inputs.entries.is_empty() {
                    // optimization when we have no inputs: eval expr directly
                    self.expr.eval(&mut self.inputs);
                    self.cached_value = None;
                    let Expr::Value(value) = &self.expr else {
                        panic!("expr did not successfully evaluate")
                    };
                    value.clone()
                } else {
                    let mut copy = self.expr.clone();
                    copy.eval(&mut self.inputs);
                    let Expr::Value(value) = copy else {
                        panic!("expr did not successfully evaluate")
                    };
                    self.cached_value = Some(value.clone());
                    value
                };

                for (txid, meta) in predecessors {
                    self.serviced.insert(txid, meta);
                }

                let message = Message::Update {
                    sender: ctx.me().clone(),
                    value,
                    predecessors: self.serviced.clone(),
                };

                for address in &self.subscribers {
                    ctx.send(&address, message.clone());
                }
            }
        }
    }

    fn process_queue(&mut self, ctx: &Context) {
        let mut granted = Vec::new();

        for (txid, kind) in self.queue.iter() {
            match &mut self.held {
                // if no locks are held, we can grant this queued lock unconditionally
                held @ HeldLocks::None => match kind {
                    LockKind::Shared => {
                        *held = HeldLocks::Shared(BTreeMap::from([(
                            txid.clone(),
                            SubscriptionUpdates::new(),
                        )]));
                    }
                    LockKind::Exclusive => {
                        *held = HeldLocks::Exclusive(
                            txid.clone(),
                            SubscriptionUpdates::new(),
                            CodeUpdates::None,
                        );
                    }
                },

                // if shared locks are held, we can grant only shared locks
                HeldLocks::Shared(held) => match kind {
                    LockKind::Shared => {
                        held.insert(txid.clone(), SubscriptionUpdates::new());
                    }
                    LockKind::Exclusive => {
                        // request preemption of all held shared locks younger than the queued
                        // exclusive lock
                        for shared_txid in held.keys().rev() {
                            if shared_txid < txid {
                                break;
                            }

                            Self::preempt(shared_txid, &mut self.preempting, ctx);
                        }

                        break;
                    }
                },

                // if an exclusive lock is held, we can grant no locks
                HeldLocks::Exclusive(held_txid, _, _) => {
                    // request preemption of the exclusive lock if it is younger than the queued lock
                    if txid < held_txid {
                        Self::preempt(held_txid, &mut self.preempting, ctx);
                    }

                    break;
                }
            }

            // if control flow reaches here, the lock has now been granted
            granted.push(txid.clone());
        }

        let ancestor_vars = self
            .inputs
            .entries
            .values()
            .flat_map(|i| i.ancestor_variables.iter())
            .cloned()
            .collect::<HashSet<_>>();

        for txid in granted {
            self.queue.remove(&txid);
            ctx.send(
                &txid.address,
                Message::LockGranted {
                    txid: txid.clone(),
                    address: ctx.me().clone(),
                    completed: self.serviced.clone(),
                    ancestor_vars: ancestor_vars.clone(),
                },
            );
        }
    }

    // cannot take &mut self, must take ref to preemptions, because we might need to ref other parts
    // of self while calling this function
    fn preempt(txid: &TxId, preempted: &mut HashSet<TxId>, ctx: &Context) {
        if !preempted.contains(txid) {
            ctx.send(&txid.address, Message::Preempt { txid: txid.clone() });
            preempted.insert(txid.clone());
        }
    }
}

impl Actor for Node {
    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            Message::Lock { txid, kind } => {
                let btree_map::Entry::Vacant(e) = self.queue.entry(txid.clone()) else {
                    panic!("lock was double-requested")
                };

                e.insert(kind);
            }
            Message::Abort { txid } => match std::mem::replace(&mut self.held, HeldLocks::None) {
                HeldLocks::None => panic!("abort of unheld lock requested"),
                HeldLocks::Shared(mut held) => {
                    let data = held.remove(&txid);

                    if held.len() != 0 {
                        // restore the remaining held shared locks
                        self.held = HeldLocks::Shared(held);
                    }

                    if data.is_none() {
                        panic!("abort of unheld lock requested")
                    }
                }
                HeldLocks::Exclusive(held_txid, shared_data, exclusive_data) => {
                    if held_txid != txid {
                        // restore the unmatched exclusive lock
                        self.held = HeldLocks::Exclusive(held_txid, shared_data, exclusive_data);

                        panic!("abort of unheld lock requested")
                    }
                }
            },
            Message::Release { txid, predecessors } => {
                match std::mem::replace(&mut self.held, HeldLocks::None) {
                    HeldLocks::None => panic!("release of unheld lock requested"),
                    HeldLocks::Shared(mut held) => {
                        let data = held.remove(&txid);

                        if held.len() != 0 {
                            // restore the remaining held shared locks
                            self.held = HeldLocks::Shared(held);
                        }

                        if let Some(data) = data {
                            self.handle_lock_released(predecessors, data, CodeUpdates::None, ctx);
                        } else {
                            panic!("release of unheld lock requested")
                        }
                    }
                    HeldLocks::Exclusive(held_txid, shared_data, exclusive_data) => {
                        if held_txid == txid {
                            self.handle_lock_released(
                                predecessors,
                                shared_data,
                                exclusive_data,
                                ctx,
                            );
                        } else {
                            // restore the unmatched exclusive lock
                            self.held =
                                HeldLocks::Exclusive(held_txid, shared_data, exclusive_data);

                            panic!("release of unheld lock requested")
                        }
                    }
                }
            }
            _ => todo!(),
        }
    }
}

impl ExprEvalContext for InputMetadata {
    fn read(&mut self, address: &Address) -> Option<&Value> {
        self.entries.get(address).map(|entry| &entry.current_value)
    }
}
