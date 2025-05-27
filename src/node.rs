use std::collections::{btree_map::Entry, hash_map, BTreeMap, HashMap, HashSet};

use held_locks::{ExclusiveLockState, HeldLocks, Read, SharedLockState};
use reactive::Reactive;

use crate::{
    actor::{Actor, Address, Context},
    message::{BasisStamp, LockKind, Message, StampedValue, TxId},
};

mod held_locks;
mod reactive;

pub struct Node {
    queued: BTreeMap<TxId, LockKind>,
    held: HeldLocks,
    preempted: HashSet<TxId>,

    reactives: HashMap<ReactiveId, Reactive>,
    exports: HashMap<ReactiveId, HashSet<Address>>,

    imports: HashMap<ReactiveAddress, HashSet<ReactiveId>>,
    subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,

    roots: HashMap<ReactiveAddress, HashSet<ReactiveAddress>>,
    topo: Vec<ReactiveId>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReactiveAddress {
    pub address: Address,
    pub id: ReactiveId,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReactiveId(usize);

impl Node {
    pub fn new() -> Node {
        Node {
            queued: BTreeMap::new(),
            held: HeldLocks::None,
            preempted: HashSet::new(),
            imports: HashMap::new(),
            reactives: HashMap::new(),
            exports: HashMap::new(),
            subscriptions: HashMap::new(),
            roots: HashMap::new(),
            topo: Vec::new(),
        }
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
                            ExclusiveLockState::default(),
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
                        for (shared_txid, _) in held.iter_mut().rev() {
                            if shared_txid < txid {
                                break;
                            }

                            Self::preempt(&mut self.preempted, shared_txid, ctx);
                        }

                        break;
                    }
                },

                // if an exclusive lock is held, we can grant no locks
                HeldLocks::Exclusive(held_txid, _, _) => {
                    // request preemption of the exclusive lock if it is younger than the queued lock
                    if txid < held_txid {
                        Self::preempt(&mut self.preempted, txid, ctx);
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
                },
            );
        }
    }

    fn apply_changes<'a>(
        &mut self,
        basis: BasisStamp,
        shared_state: SharedLockState,
        exclusive_state: ExclusiveLockState,
        ctx: Context<'a>,
    ) -> Option<Context<'a>> {
        for (id, read) in shared_state.reads {
            if !read.complete.is_empty() {
                self.reactives.get_mut(&id).unwrap().finished_read(&basis);
            }
        }

        let mut modified = exclusive_state
            .writes
            .keys()
            .cloned()
            .collect::<HashSet<_>>();

        for (id, value) in exclusive_state.writes {
            self.reactives.get_mut(&id).unwrap().write(StampedValue {
                value,
                basis: basis.clone(),
            });
        }

        for (id, config) in exclusive_state.reactives {
            if let Some(config) = config {
                match self.reactives.entry(id) {
                    hash_map::Entry::Vacant(e) => {
                        e.insert(Reactive::new(config));
                    }
                    hash_map::Entry::Occupied(mut e) => {
                        e.get_mut().reconfigure(config);
                    }
                }

                // TOOD: this does not actually force a propagation of this node's value. but we
                // want to do that. so, need to figure out a way to do that that actually works.
                modified.insert(id);
            } else {
                self.reactives.remove(&id);
            }
        }

        // TODO: update imports appropriately
        // TODO: update subscriptions appropriately
        // TODO: update roots appropriately
        // TODO: update topo appropriately
        //
        // (lots of computation!)
        // (this only needs to be done if there were code updates!)
        // (maybe for this coarse locking strategy an `Upgrade` lock kind would be good!)
        // (wouldn't really mean anything though, since Upgrade and Exclusive would still be mutex.)

        for (id, addrs) in exclusive_state.exports {
            if addrs.is_empty() {
                self.exports.remove(&id);
            } else {
                self.exports.insert(id, addrs);
            }
        }

        self.propagate(modified, &ctx);

        Some(ctx)
    }

    fn preempt(preempted: &mut HashSet<TxId>, txid: &TxId, ctx: &Context) {
        if preempted.insert(txid.clone()) {
            ctx.send(&txid.address, Message::Preempt { txid: txid.clone() });
        }
    }

    fn propagate(&mut self, modified: HashSet<ReactiveId>, ctx: &Context) {
        let mut found = false;
        for id in &self.topo {
            if !found {
                if modified.contains(id) {
                    found = true;
                } else {
                    continue;
                }
            }

            while let Some(value) = self
                .reactives
                .get_mut(id)
                .unwrap()
                .process_update(&self.roots)
            {
                for sub in self.subscriptions.get(id).unwrap() {
                    self.reactives.get_mut(sub).unwrap().add_update(
                        ReactiveAddress {
                            address: ctx.me().clone(),
                            id: *id,
                        },
                        value.clone(),
                    );
                }

                for addr in self.exports.get(id).iter().copied().flatten() {
                    ctx.send(
                        addr,
                        Message::Propagate {
                            sender: ReactiveAddress {
                                address: ctx.me().clone(),
                                id: *id,
                            },
                            value: value.clone(),
                        },
                    );
                }
            }
        }

        self.grant_reads(&ctx);
    }

    fn grant_reads(&mut self, ctx: &Context) {
        self.held.visit_shared(|txid, state| {
            for (id, read) in &mut state.reads {
                // If a read is completed, the `complete` stamp has to have SOMETHING in it -- no
                // value can have an empty basis stamp. Hence if `complete` is empty, the read is
                // pending and needs to be sent out.
                //
                // Alternatively, we may have already completed a read, but another is pending.
                if read.complete.is_empty() || !read.pending.is_empty() {
                    if let Some(value) = self.reactives.get(&id).unwrap().value() {
                        let address = ReactiveAddress {
                            address: ctx.me().clone(),
                            id: *id,
                        };

                        let roots = self.roots.get(&address).unwrap();

                        if read.pending.prec_eq_wrt_roots(&value.basis, roots) {
                            ctx.send(
                                &txid.address,
                                Message::ReadResult {
                                    txid: txid.clone(),
                                    reactive: address,
                                    value: value.clone(),
                                },
                            );

                            read.complete.merge_from(&value.basis);
                        }
                    }
                }
            }
        });
    }
}

impl Actor for Node {
    fn handle(&mut self, message: Message, mut ctx: Context) {
        match message {
            Message::Lock { txid, kind } => {
                let Entry::Vacant(e) = self.queued.entry(txid) else {
                    panic!("lock was double-requested");
                };

                e.insert(kind);

                self.grant_locks(&ctx);
            }
            Message::Abort { txid } => {
                match &mut self.held {
                    HeldLocks::None => panic!("abort of unheld lock requested"),
                    HeldLocks::Shared(held) => {
                        if held.remove(&txid).is_none() {
                            panic!("abort of unheld lock requested")
                        }

                        if held.is_empty() {
                            self.held = HeldLocks::None;
                        }
                    }
                    HeldLocks::Exclusive(held_txid, _, _) => {
                        if held_txid == &txid {
                            self.held = HeldLocks::None;
                        } else {
                            panic!("abort of unheld lock requested")
                        }
                    }
                }

                self.grant_locks(&ctx);
            }
            Message::PrepareCommit { txid } => {
                let state = self
                    .held
                    .shared(&txid)
                    .expect("attempted to prepare commit for unheld lock");

                let basis = state
                    .reads
                    .values()
                    .fold(BasisStamp::empty(), |mut basis, read| {
                        basis.merge_from(&read.complete);
                        basis
                    });

                // TODO: **comprehensively** validate the update (ideally equivalent to fully
                // executing it), perhaps by doing it and adding an 'undo log' entry, so that no
                // can occur after CommitPrepared is sent

                ctx.send(
                    &txid.address,
                    Message::CommitPrepared {
                        txid: txid.clone(),
                        basis,
                    },
                );
            }
            Message::Commit { txid, basis } => {
                match std::mem::replace(&mut self.held, HeldLocks::None) {
                    HeldLocks::None => panic!("release of unheld lock requested"),
                    HeldLocks::Shared(mut held) => {
                        let data = held.remove(&txid);

                        if !held.is_empty() {
                            // restore the remaining held shared locks
                            self.held = HeldLocks::Shared(held);
                        }

                        if let Some(data) = data {
                            if let Some(returned) =
                                self.apply_changes(basis, data, ExclusiveLockState::default(), ctx)
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
                                self.apply_changes(basis, shared_data, exclusive_data, ctx)
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
            Message::Read {
                txid,
                reactive,
                basis,
            } => {
                let Some(lock) = self.held.shared_mut(&txid) else {
                    panic!("attempted to read without a lock")
                };

                let Some(r) = self.reactives.get(&reactive) else {
                    panic!("attempted to read reactive that could not be found")
                };

                let e = lock.reads.entry(reactive);

                if let hash_map::Entry::Occupied(e) = &e {
                    let r = e.get();
                    if r.complete.is_empty() || !r.pending.is_empty() {
                        panic!("attempted to read while another read is still pending")
                    }
                }

                let read = e.or_insert(Read {
                    pending: BasisStamp::empty(),
                    complete: BasisStamp::empty(),
                });

                if let Some(value) = r.value() {
                    if basis.prec_eq_wrt_roots(
                        &value.basis,
                        self.roots
                            .get(&ReactiveAddress {
                                address: ctx.me().clone(),
                                id: reactive,
                            })
                            .unwrap(),
                    ) {
                        ctx.send(
                            &txid.address,
                            Message::ReadResult {
                                txid: txid.clone(),
                                reactive: ReactiveAddress {
                                    address: ctx.me().clone(),
                                    id: reactive,
                                },
                                value: value.clone(),
                            },
                        );

                        read.complete.merge_from(&value.basis);
                    } else {
                        read.pending = basis;
                    }
                } else {
                    read.pending = basis;
                }
            }
            Message::Write {
                txid,
                reactive,
                value,
            } => {
                let state = self
                    .held
                    .exclusive_mut(&txid)
                    .expect("attempted to write without an exclusive lock");
                assert!(self.reactives.contains_key(&reactive));
                state.writes.insert(reactive, value);
            }
            Message::Configure {
                txid,
                reactives,
                exports,
            } => {
                let state = self
                    .held
                    .exclusive_mut(&txid)
                    .expect("attempted to configure without an exclusive lock");
                state.reactives.extend(reactives);
                state.exports.extend(exports);
            }
            Message::Propagate { sender, value } => {
                let Some(importers) = self.imports.get(&sender) else {
                    return;
                };

                for id in importers {
                    self.reactives
                        .get_mut(id)
                        .unwrap()
                        .add_update(sender.clone(), value.clone());
                }

                self.propagate(importers.clone(), &ctx);
            }
            _ => todo!(),
        }
    }
}
