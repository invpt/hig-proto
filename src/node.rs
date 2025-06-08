use std::collections::{btree_map::Entry, hash_map, BTreeMap, HashMap, HashSet, VecDeque};

use held_locks::{ExclusiveLockState, HeldLocks, Read, SharedLockState};
use reactive::Reactive;

use crate::{
    actor::{Actor, Address, Context},
    message::{BasisStamp, Iteration, LockKind, Message, StampedValue, TxId},
};

mod held_locks;
mod reactive;

pub struct Node {
    queued: BTreeMap<TxId, LockKind>,
    held: HeldLocks,
    preempted: HashSet<TxId>,

    imports: HashMap<ReactiveAddress, Import>,
    reactives: HashMap<ReactiveId, Reactive>,
    iterations: HashMap<ReactiveId, Iteration>,
    exports: HashMap<ReactiveId, Export>,

    subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,
    roots: HashMap<ReactiveId, HashSet<ReactiveAddress>>,
    topo: VecDeque<ReactiveId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReactiveAddress {
    pub address: Address,
    pub id: ReactiveId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReactiveId(usize);

#[derive(Clone)]
pub struct Import {
    pub roots: HashSet<ReactiveAddress>,
    pub importers: HashSet<ReactiveId>,
}

pub struct Export {
    /// Exports' roots only contain cross-network roots, since they are themselves sources standing
    /// in for each of the local reactive state variables (if any).
    pub roots: HashSet<ReactiveAddress>,
    pub importers: HashSet<Address>,
}

#[derive(Debug)]
struct Cyclical;

impl Node {
    pub fn new() -> Node {
        Node {
            queued: BTreeMap::new(),
            held: HeldLocks::None,
            preempted: HashSet::new(),
            imports: HashMap::new(),
            reactives: HashMap::new(),
            iterations: HashMap::new(),
            exports: HashMap::new(),
            subscriptions: HashMap::new(),
            roots: HashMap::new(),
            topo: VecDeque::new(),
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

    fn commit<'a>(
        &mut self,
        mut basis: BasisStamp,
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
            // The direct writes won't necessarily be included in the basis since this reactive
            // might not be exported. Local-only basis roots like these are filtered out when
            // propagating basis stamps to other network nodes in propagate().
            basis.roots.insert(
                ReactiveAddress {
                    address: ctx.me().clone(),
                    id,
                },
                exclusive_state.prepared_iterations[&id],
            );

            self.reactives.get_mut(&id).unwrap().write(StampedValue {
                value,
                basis: basis.clone(),
            });
        }

        for (address, config) in exclusive_state.imports {
            if let Some(config) = config {
                match self.imports.entry(address) {
                    hash_map::Entry::Vacant(e) => {
                        e.insert(Import {
                            roots: config.roots,
                            importers: HashSet::new(),
                        });
                    }
                    hash_map::Entry::Occupied(e) => {
                        e.into_mut().roots = config.roots;
                    }
                }
            } else if let Some(removed) = self.imports.remove(&address) {
                assert!(
                    removed
                        .importers
                        .into_iter()
                        .all(|id| exclusive_state.reactives.contains_key(&id)),
                    "not all importers of a removed import {:?} are being updated",
                    address,
                );
            }
        }

        let reactives_changed = !exclusive_state.reactives.is_empty();

        for (id, config) in exclusive_state.reactives {
            if let Some(config) = config {
                self.iterations.entry(id).or_insert(Iteration::ZERO);

                let (reactive, mut prior_inputs) = match self.reactives.entry(id) {
                    hash_map::Entry::Vacant(e) => (e.insert(Reactive::new(config)), HashSet::new()),
                    hash_map::Entry::Occupied(e) => {
                        let reactive = e.into_mut();
                        let prior_inputs = reactive.inputs().cloned().collect::<HashSet<_>>();
                        reactive.reconfigure(config);
                        (reactive, prior_inputs)
                    }
                };

                for input in reactive.inputs() {
                    if prior_inputs.contains(input) {
                        prior_inputs.remove(input);
                        continue;
                    }

                    if &input.address == ctx.me() {
                        self.subscriptions
                            .get_mut(&input.id)
                            .expect("attempted to reference nonexistent local reactive")
                            .insert(id);
                    } else {
                        self.imports
                            .get_mut(input)
                            .expect("attempted to reference nonexistent import")
                            .importers
                            .insert(id);
                    }
                }

                for removed in prior_inputs {
                    if &removed.address == ctx.me() {
                        self.subscriptions.get_mut(&removed.id).unwrap().remove(&id);
                    } else {
                        let import = self.imports.get_mut(&removed).unwrap();
                        import.importers.remove(&id);
                        if import.importers.is_empty() {
                            self.imports.remove(&removed);
                        }
                    }
                }

                modified.insert(id);
            } else if let Some(removed) = self.reactives.remove(&id) {
                self.iterations.remove(&id);

                for input in removed.inputs() {
                    if &input.address == ctx.me() {
                        self.subscriptions.get_mut(&input.id).map(|i| i.remove(&id));
                    } else {
                        self.imports.get_mut(input).map(|i| i.importers.remove(&id));
                    }
                }
            }
        }

        if reactives_changed {
            self.recompute_topo();
            self.recompute_roots(&ctx);
        }

        for (id, addrs) in exclusive_state.exports {
            if addrs.is_empty() {
                self.exports.remove(&id);
            } else {
                self.exports.insert(
                    id,
                    Export {
                        roots: self.roots[&id]
                            .iter()
                            .filter(|r| &r.address != ctx.me())
                            .cloned()
                            .collect(),
                        importers: addrs,
                    },
                );
            }
        }

        self.iterations.extend(exclusive_state.prepared_iterations);

        self.propagate(modified, &ctx);

        Some(ctx)
    }

    fn recompute_topo(&mut self) {
        let mut visited = HashMap::new();
        self.topo.clear();
        for id in self.reactives.keys() {
            Self::topo_dfs(&self.subscriptions, &mut self.topo, &mut visited, *id)
                .expect("dependency graph is locally cyclical");
        }
    }

    fn topo_dfs(
        subscriptions: &HashMap<ReactiveId, HashSet<ReactiveId>>,
        topo: &mut VecDeque<ReactiveId>,
        visited: &mut HashMap<ReactiveId, bool>,
        id: ReactiveId,
    ) -> Result<(), Cyclical> {
        match visited.get(&id) {
            Some(true) => return Ok(()),
            Some(false) => return Err(Cyclical),
            None => (),
        }

        visited.insert(id, false);

        for sub in &subscriptions[&id] {
            Self::topo_dfs(subscriptions, topo, visited, *sub)?;
        }

        topo.push_front(id);
        visited.insert(id, true);

        Ok(())
    }

    fn recompute_roots(&mut self, ctx: &Context) {
        self.roots.clear();
        for id in &self.topo {
            let mut roots = HashSet::new();

            let mut has_inputs = false;
            for input in self.reactives[id].inputs() {
                has_inputs = true;

                if &input.address == ctx.me() {
                    roots.extend(self.roots[&input.id].iter().cloned());
                } else {
                    roots.extend(self.imports[input].roots.iter().cloned());
                }
            }

            if !has_inputs {
                roots.insert(ReactiveAddress {
                    address: ctx.me().clone(),
                    id: *id,
                });
            }

            self.roots.insert(*id, roots);
        }

        for (id, export) in &mut self.exports {
            export.roots = self.roots[id]
                .iter()
                .filter(|r| &r.address != ctx.me())
                .cloned()
                .collect();
        }
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

            let roots = |address: &ReactiveAddress| {
                if &address.address == ctx.me() {
                    self.roots.get(&address.id)
                } else {
                    self.imports.get(address).map(|i| &i.roots)
                }
            };

            while let Some(value) = self
                .reactives
                .get_mut(id)
                .unwrap()
                .next_value(roots)
                .cloned()
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

                let value_without_local_only_bases = StampedValue {
                    value: value.value,
                    basis: BasisStamp {
                        roots: value
                            .basis
                            .roots
                            .into_iter()
                            .filter(|(a, _)| {
                                &a.address != ctx.me() || self.exports.contains_key(&a.id)
                            })
                            .collect(),
                    },
                };

                for addr in self
                    .exports
                    .get(id)
                    .iter()
                    .copied()
                    .flat_map(|e| e.importers.iter())
                {
                    ctx.send(
                        addr,
                        Message::Propagate {
                            sender: ReactiveAddress {
                                address: ctx.me().clone(),
                                id: *id,
                            },
                            value: value_without_local_only_bases.clone(),
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
                        let roots = self.roots.get(id).unwrap();

                        if read.pending.prec_eq_wrt_roots(&value.basis, roots) {
                            ctx.send(
                                &txid.address,
                                Message::ReadResult {
                                    txid: txid.clone(),
                                    reactive: ReactiveAddress {
                                        address: ctx.me().clone(),
                                        id: *id,
                                    },
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

                let mut basis =
                    state
                        .reads
                        .values()
                        .fold(BasisStamp::empty(), |mut basis, read| {
                            basis.merge_from(&read.complete);
                            basis
                        });

                if let Some(exclusive) = self.held.exclusive_mut(&txid) {
                    // For any direct writes to local reactives, we want to increment the iterations
                    // of all transitively dependent local reactives, including the written nodes
                    // themselves.
                    for id in &self.topo {
                        if exclusive.writes.contains_key(id) {
                            exclusive
                                .prepared_iterations
                                .insert(*id, self.iterations[id].increment());
                        } else if self.reactives[id].inputs().any(|input| {
                            &input.address == ctx.me()
                                && exclusive.prepared_iterations.contains_key(&input.id)
                        }) {
                            exclusive
                                .prepared_iterations
                                .insert(*id, self.iterations[id].increment());
                        }
                    }

                    // Only include exported reactives as roots in the basis. Note that we have to
                    // take care to respect the set of exports that will be set following commit of
                    // the transaction, rather than the current self.exports.
                    basis.roots.extend(
                        exclusive
                            .prepared_iterations
                            .iter()
                            .filter(|(id, _)| {
                                exclusive
                                    .exports
                                    .get(id)
                                    .map_or(self.exports.contains_key(id), |export| {
                                        !export.is_empty()
                                    })
                            })
                            .map(|(id, iter)| {
                                (
                                    ReactiveAddress {
                                        address: ctx.me().clone(),
                                        id: *id,
                                    },
                                    *iter,
                                )
                            }),
                    );
                }

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
                                self.commit(basis, data, ExclusiveLockState::default(), ctx)
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
                                self.commit(basis, shared_data, exclusive_data, ctx)
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
                    if basis.prec_eq_wrt_roots(&value.basis, self.roots.get(&reactive).unwrap()) {
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
            Message::ReadConfiguration { txid } => {
                self.held
                    .exclusive(&txid)
                    .expect("attempted to read configuration without an exclusive lock");

                ctx.send(
                    &txid.address,
                    Message::ReadConfigurationResult {
                        imports: self.imports.clone(),
                    },
                );
            }
            Message::Configure {
                txid,
                imports,
                reactives,
                exports,
            } => {
                let state = self
                    .held
                    .exclusive_mut(&txid)
                    .expect("attempted to configure without an exclusive lock");
                state.imports.extend(imports);
                state.reactives.extend(reactives);
                state.exports.extend(exports);
            }
            Message::Propagate { sender, value } => {
                let Some(import) = self.imports.get(&sender) else {
                    return;
                };

                for id in &import.importers {
                    self.reactives
                        .get_mut(&id)
                        .unwrap()
                        .add_update(sender.clone(), value.clone());
                }

                self.propagate(import.importers.clone(), &ctx);
            }
            _ => todo!(),
        }
    }
}
