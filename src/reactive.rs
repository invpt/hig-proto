mod definition;
mod held_locks;

use std::collections::{btree_map, BTreeMap, HashSet};

use definition::Definition;
use held_locks::{ExclusiveLockState, HeldLocks, Read, SharedLockState};

use crate::{
    actor::{Actor, Address, Context, Version},
    message::{
        BasisStamp, Iteration, LockKind, Message, NodeKind, ReactiveConfiguration, StampedValue,
        TxId,
    },
};

pub struct Reactive {
    /// The set of locks waiting to be granted, ordered by transaction ID and hence from oldest to
    /// youngest, thanks to the use of BTreeMap. The ordering makes it easy to pick the highest-
    /// priority transaction for Wound-Wait.
    queued: BTreeMap<TxId, LockKind>,

    /// The set of locks currently held.
    held: HeldLocks,

    /// When present, the definition of a node automatically updates the value based on values
    /// propagated by other nodes.
    definition: Option<Definition>,

    /// The current value held by the node. On nodes with a definition, the value is updated whenever
    /// the definition applies a batch. On nodes without a definition, the value is updated by Write
    /// messages during a transaction. The value is also updated whenever a node is reconfigured.
    value: Option<StampedValue>,

    /// `reads` contains the transactions that have read the current value since it was set.
    reads: BasisStamp,

    /// The set of addresses to whom `Propagate` messages are sent whenever the value is changed.
    subscribers: HashSet<Address>,

    version: Version,
    iteration: Iteration,
}

impl Reactive {
    pub fn new(config: ReactiveConfiguration) -> Reactive {
        let mut node = Reactive {
            queued: BTreeMap::new(),
            held: HeldLocks::None,
            definition: None,
            value: None,
            reads: BasisStamp::empty(),
            subscribers: HashSet::new(),
            version: Version::ZERO,
            iteration: Iteration::ZERO,
        };

        match config {
            ReactiveConfiguration::Variable { value } => {
                node.value = Some(value);
            }
            ReactiveConfiguration::Definition { expr, inputs } => {
                node.definition = Some(Definition::new(expr, inputs));
            }
        }

        node
    }

    fn apply_changes<'a>(
        &mut self,
        basis: BasisStamp,
        shared_state: SharedLockState,
        exclusive_state: ExclusiveLockState,
        ctx: Context<'a>,
    ) -> Option<Context<'a>> {
        let SharedLockState {
            preempting: _,
            subscription_updates,
            read,
        } = shared_state;

        if let Some(Read::Complete) = read {
            self.reads.merge_from(&basis);
        }

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

                self.update_value(StampedValue { value, basis }, &ctx);

                Some(ctx)
            }
            ExclusiveLockState::Update(update) => {
                let new_value = match update {
                    ReactiveConfiguration::Variable { value } => {
                        self.definition = None;
                        Some(value)
                    }
                    ReactiveConfiguration::Definition { expr, inputs } => {
                        match &mut self.definition {
                            None => {
                                let definition = Definition::new(expr, inputs);
                                self.definition = Some(definition);
                                None
                            }
                            Some(existing) => existing.reconfigure(expr, inputs),
                        }
                    }
                };

                if let Some(new_value) = new_value {
                    self.update_value(new_value, &ctx);
                }

                self.version = self.version.increment();

                Some(ctx)
            }
        };

        for (subscriber, subscribe) in subscription_updates {
            if subscribe {
                self.subscribers.insert(subscriber);
            } else {
                self.subscribers.remove(&subscriber);
            }
        }

        ctx
    }

    fn grant_locks(&mut self, ctx: &Context) {
        let mut granted = Vec::new();

        for (txid, kind) in self.queued.iter() {
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

        for txid in granted {
            self.queued.remove(&txid);
            ctx.send(
                &txid.address,
                Message::LockGranted {
                    txid: txid.clone(),
                    address: ctx.me().clone(),
                    node_kind: if let Some(def) = &self.definition {
                        NodeKind::Definition {
                            ancestors: def
                                .ancestors()
                                .map(|(ad, an)| (ad.clone(), an.clone()))
                                .collect(),
                        }
                    } else {
                        NodeKind::Variable {
                            iteration: self.iteration,
                        }
                    },
                    version: self.version,
                },
            );
        }
    }

    fn update_value(&mut self, mut value: StampedValue, ctx: &Context) {
        value.basis.merge_from(&self.reads);

        self.iteration = self.iteration.max(value.basis.latest(ctx.me()));
        self.reads = BasisStamp::empty();

        for subscriber in &self.subscribers {
            ctx.send(
                subscriber,
                Message::Propagate {
                    sender: ctx.me().clone(),
                    value: value.clone(),
                },
            );
        }

        let my_ancestors = self
            .definition
            .iter()
            .flat_map(|def| def.ancestors().map(|(ad, an)| (ad.clone(), an.clone())))
            .collect();

        self.held.visit_shared(|txid, state| {
            if let Some(Read::Pending(basis)) = &state.read {
                // grant pending reads that are ready to be performed
                if basis.prec_eq_wrt_ancestors(&value.basis, &my_ancestors) {
                    ctx.send(
                        &txid.address,
                        Message::ReadResult {
                            txid: txid.clone(),
                            address: ctx.me().clone(),
                            value: value.clone(),
                        },
                    );
                    state.read = Some(Read::Complete);
                }
            }
        });

        self.value = Some(value);
    }
}

impl Actor for Reactive {
    fn handle(&mut self, message: Message, mut ctx: Context) {
        match message {
            Message::Lock { txid, kind } => {
                let btree_map::Entry::Vacant(e) = self.queued.entry(txid.clone()) else {
                    panic!("lock was double-requested")
                };

                e.insert(kind);

                self.grant_locks(&ctx);
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

                self.grant_locks(&ctx);
            }
            Message::Release { txid, basis: roots } => {
                match std::mem::replace(&mut self.held, HeldLocks::None) {
                    HeldLocks::None => panic!("release of unheld lock requested"),
                    HeldLocks::Shared(mut held) => {
                        let data = held.remove(&txid);

                        if held.len() != 0 {
                            // restore the remaining held shared locks
                            self.held = HeldLocks::Shared(held);
                        }

                        if let Some(data) = data {
                            if let Some(returned) =
                                self.apply_changes(roots, data, ExclusiveLockState::Unchanged, ctx)
                            {
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
                            if let Some(returned) =
                                self.apply_changes(roots, shared_data, exclusive_data, ctx)
                            {
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

                self.grant_locks(&ctx);
            }
            Message::Read { txid, basis } => {
                let Some(shared_state) = self.held.shared_mut(&txid) else {
                    panic!("requested read without shared lock")
                };

                if !basis.root_iterations.is_empty() || self.value.is_none() {
                    shared_state.read = Some(Read::Pending(basis));
                } else {
                    ctx.send(
                        &txid.address,
                        Message::ReadResult {
                            txid: txid.clone(),
                            address: ctx.me().clone(),
                            value: self.value.as_ref().unwrap().clone(),
                        },
                    );

                    shared_state.read = Some(Read::Complete);
                }
            }
            Message::ReadSubscriptions { txid } => {
                if self.held.shared(&txid).is_none() {
                    panic!("requested subscription update without shared lock")
                }

                ctx.send(
                    &txid.address,
                    Message::ReadSubscriptionsResult {
                        txid: txid.clone(),
                        subscriptions: self.subscribers.clone(),
                    },
                );
            }
            Message::UpdateSubscriptions { txid, changes } => {
                let Some(shared_state) = self.held.shared_mut(&txid) else {
                    panic!("requested subscription update without shared lock")
                };

                shared_state.subscription_updates.extend(changes);
            }
            Message::Reconfigure {
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
            Message::Write { txid, value } => {
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
                    ExclusiveLockState::Update(ReactiveConfiguration::Variable {
                        value: current_value,
                    }) => {
                        current_value.value = value;
                        todo!("what to do here about current_value.predecessors?")
                    }
                    ExclusiveLockState::Update(ReactiveConfiguration::Definition { .. })
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
            Message::Propagate { sender, value } => {
                let definition = self
                    .definition
                    .as_mut()
                    .expect("variable node received propagation");

                definition.add_update(sender, value);
                if let Some(new_value) = definition.find_and_apply_batch() {
                    self.update_value(new_value, &ctx);
                }
            }
            _ => todo!(),
        }
    }
}
