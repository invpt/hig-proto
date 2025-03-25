use std::{
    collections::{btree_map, BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use crate::{
    actor::{Actor, Address, Context},
    expr::{eval::ExprEvalContext, Expr},
    message::{ConfigurationUpdate, InputMetadata, LockKind, Message, TxId, TxMeta},
    value::Value,
};

pub struct Node {
    queue: BTreeMap<TxId, LockKind>,
    held: HeldLocks,

    definition: Option<Definition>,

    value: Value,

    predecessors: HashMap<TxId, TxMeta>,

    subscribers: HashSet<Address>,
}

struct Definition {
    inputs: InputMetadata,
    updates: HashMap<TxId, Update>,
    expr: Expr,
}

struct Update {
    values: HashMap<Address, Option<Arc<Value>>>,
    predecessors: HashSet<TxId>,
}

enum HeldLocks {
    None,
    Shared(BTreeMap<TxId, SharedLockState>),
    Exclusive(TxId, SharedLockState, ExclusiveLockState),
}

enum Preemption {
    None,
    Sent,
}

impl HeldLocks {
    fn exclusive(&self, txid: &TxId) -> Option<&ExclusiveLockState> {
        match self {
            HeldLocks::Exclusive(held_txid, _, exclusive_data) => {
                if held_txid == txid {
                    Some(exclusive_data)
                } else {
                    None
                }
            }
            HeldLocks::None | HeldLocks::Shared(_) => None,
        }
    }

    fn exclusive_mut(&mut self, txid: &TxId) -> Option<&mut ExclusiveLockState> {
        match self {
            HeldLocks::Exclusive(held_txid, _, exclusive_data) => {
                if held_txid == txid {
                    Some(exclusive_data)
                } else {
                    None
                }
            }
            HeldLocks::None | HeldLocks::Shared(_) => None,
        }
    }

    fn shared(&self, txid: &TxId) -> Option<&SharedLockState> {
        match self {
            HeldLocks::Shared(held) => held.get(txid),
            HeldLocks::Exclusive(held_txid, shared_data, _) => {
                if held_txid == txid {
                    Some(shared_data)
                } else {
                    None
                }
            }
            HeldLocks::None => None,
        }
    }

    fn shared_mut(&mut self, txid: &TxId) -> Option<&mut SharedLockState> {
        match self {
            HeldLocks::Shared(held) => held.get_mut(txid),
            HeldLocks::Exclusive(held_txid, shared_data, _) => {
                if held_txid == txid {
                    Some(shared_data)
                } else {
                    None
                }
            }
            HeldLocks::None => None,
        }
    }
}

#[derive(Default)]
struct SharedLockState {
    preempting: bool,
    subscription_updates: HashMap<Address, bool>,
}

impl SharedLockState {
    fn preempt(&mut self, txid: &TxId, ctx: &Context) {
        if !self.preempting {
            self.preempting = true;
            ctx.send(&txid.address, Message::Preempt { txid: txid.clone() });
        }
    }
}

enum ExclusiveLockState {
    Unchanged,
    Write(Value),
    Update(ConfigurationUpdate),
    Retire,
}

impl Node {
    fn handle_lock_released<'a>(
        &mut self,
        predecessors: HashMap<TxId, TxMeta>,
        shared_state: SharedLockState,
        exclusive_state: ExclusiveLockState,
        ctx: Context<'a>,
    ) -> Option<Context<'a>> {
        let ctx = match exclusive_state {
            ExclusiveLockState::Unchanged => Some(ctx),
            ExclusiveLockState::Retire => {
                ctx.retire();
                None
            }
            ExclusiveLockState::Write(value) => {
                let None = self.definition else {
                    panic!("attempting to write a definition")
                };

                self.value = value;

                let message = Message::Propagate {
                    sender: ctx.me().clone(),
                    value: self.value.clone(),
                    predecessors: self.predecessors.clone(),
                };

                for address in &self.subscribers {
                    ctx.send(&address, message.clone());
                }

                Some(ctx)
            }
            ExclusiveLockState::Update(update) => {
                match update {
                    ConfigurationUpdate::Variable { value } => {
                        self.definition = None;
                        self.value = value;
                    }
                    ConfigurationUpdate::Definition { inputs, expr } => {
                        let definition = match &mut self.definition {
                            None => {
                                self.definition = Some(Definition {
                                    inputs,
                                    updates: HashMap::new(),
                                    expr,
                                });
                                self.definition.as_mut().unwrap()
                            }
                            Some(existing) => {
                                existing.inputs.entries.extend(inputs.entries);
                                existing.expr = expr;
                                existing
                            }
                        };

                        let mut expr = definition.expr.clone();
                        expr.eval(&mut definition.inputs);
                        let Expr::Value(value) = expr else {
                            panic!("expr did not successfully evaluate")
                        };

                        self.value = value;
                    }
                }

                for (txid, meta) in predecessors {
                    self.predecessors.insert(txid, meta);
                }

                Some(ctx)
            }
        };

        for (subscriber, subscribe) in shared_state.subscription_updates {
            if subscribe {
                self.subscribers.insert(subscriber);
            } else {
                self.subscribers.remove(&subscriber);
            }
        }

        ctx
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
                            SharedLockState::default(),
                        )]));
                    }
                    LockKind::Exclusive => {
                        *held = HeldLocks::Exclusive(
                            txid.clone(),
                            SharedLockState::default(),
                            ExclusiveLockState::Unchanged,
                        );
                    }
                },

                // if shared locks are held, we can grant only shared locks
                HeldLocks::Shared(held) => match kind {
                    LockKind::Shared => {
                        held.insert(txid.clone(), SharedLockState::default());
                    }
                    LockKind::Exclusive => {
                        // request preemption of all held shared locks younger than the queued
                        // exclusive lock
                        for (shared_txid, shared_state) in held.iter_mut().rev() {
                            if shared_txid < txid {
                                break;
                            }

                            shared_state.preempt(shared_txid, ctx);
                        }

                        break;
                    }
                },

                // if an exclusive lock is held, we can grant no locks
                HeldLocks::Exclusive(held_txid, shared_state, _) => {
                    // request preemption of the exclusive lock if it is younger than the queued lock
                    if txid < held_txid {
                        shared_state.preempt(txid, ctx);
                    }

                    break;
                }
            }

            // if control flow reaches here, the lock has now been granted
            granted.push(txid.clone());
        }

        let ancestor_vars = self
            .definition
            .iter()
            .flat_map(|d| {
                d.inputs
                    .entries
                    .values()
                    .flat_map(|i| i.ancestor_variables.iter())
                    .cloned()
            })
            .collect::<HashSet<_>>();

        for txid in granted {
            self.queue.remove(&txid);
            ctx.send(
                &txid.address,
                Message::LockGranted {
                    txid: txid.clone(),
                    address: ctx.me().clone(),
                    completed: self.predecessors.clone(),
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
    fn handle(&mut self, message: Message, mut ctx: Context) {
        match message {
            Message::Lock { txid, kind } => {
                let btree_map::Entry::Vacant(e) = self.queue.entry(txid.clone()) else {
                    panic!("lock was double-requested")
                };

                e.insert(kind);

                self.process_queue(&ctx);
            }
            Message::Abort { txid } => {
                match std::mem::replace(&mut self.held, HeldLocks::None) {
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
                            self.held =
                                HeldLocks::Exclusive(held_txid, shared_data, exclusive_data);

                            panic!("abort of unheld lock requested")
                        }
                    }
                }

                self.process_queue(&ctx);
            }
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
                            if let Some(returned) = self.handle_lock_released(
                                predecessors,
                                data,
                                ExclusiveLockState::Unchanged,
                                ctx,
                            ) {
                                ctx = returned;
                            } else {
                                return;
                            }
                        } else {
                            panic!("release of unheld lock requested")
                        }
                    }
                    HeldLocks::Exclusive(held_txid, shared_data, exclusive_data) => {
                        if held_txid == txid {
                            if let Some(returned) = self.handle_lock_released(
                                predecessors,
                                shared_data,
                                exclusive_data,
                                ctx,
                            ) {
                                ctx = returned;
                            } else {
                                return;
                            }
                        } else {
                            // restore the unmatched exclusive lock
                            self.held =
                                HeldLocks::Exclusive(held_txid, shared_data, exclusive_data);

                            panic!("release of unheld lock requested")
                        }
                    }
                }

                self.process_queue(&ctx);
            }
            Message::UpdateSubscriptions { txid, changes } => {
                let Some(shared_state) = self.held.shared_mut(&txid) else {
                    panic!("requested subscription update without shared lock")
                };

                shared_state.subscription_updates.extend(changes);
            }
            Message::ReadValue { txid, predecessors } => {
                if self.held.shared(&txid).is_none() {
                    panic!("requested read without shared lock")
                }

                if !predecessors.is_empty() {
                    todo!("read node with predecessors")
                }

                ctx.send(
                    &txid.address,
                    Message::ReadValueResult {
                        txid: txid.clone(),
                        address: ctx.me().clone(),
                        value: self.value.clone(),
                        predecessors: self.predecessors.clone(),
                    },
                );
            }
            Message::UpdateConfiguration {
                txid,
                configuration,
            } => {
                let Some(exclusive) = self.held.exclusive_mut(&txid) else {
                    panic!("requested update configuration without exclusive lock")
                };

                match exclusive {
                    ExclusiveLockState::Unchanged
                    | ExclusiveLockState::Write(..)
                    | ExclusiveLockState::Update(..) => {
                        *exclusive = ExclusiveLockState::Update(configuration);
                    }
                    ExclusiveLockState::Retire => {
                        panic!("attempted to update configuration after retire")
                    }
                }
            }
            Message::WriteValue { txid, value } => {
                let Some(exclusive) = self.held.exclusive_mut(&txid) else {
                    panic!("requested write without exclusive lock")
                };

                let None = self.definition else {
                    panic!("requested to write value on definition")
                };

                match exclusive {
                    ExclusiveLockState::Unchanged | ExclusiveLockState::Write(..) => {
                        *exclusive = ExclusiveLockState::Write(value);
                    }
                    ExclusiveLockState::Update(ConfigurationUpdate::Variable {
                        value: current_value,
                    }) => {
                        *current_value = value;
                    }
                    ExclusiveLockState::Update(ConfigurationUpdate::Definition { .. })
                    | ExclusiveLockState::Retire => {
                        panic!("attempted to write value on definition or after retire")
                    }
                }
            }
            Message::Retire { txid } => {
                let Some(exclusive) = self.held.exclusive_mut(&txid) else {
                    panic!("requested retirement without exclusive lock")
                };

                *exclusive = ExclusiveLockState::Retire;
            }
            Message::Propagate {
                sender,
                value,
                predecessors,
            } => {
                todo!("propagate {sender:?}, {value:?}, {predecessors:?}")
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
