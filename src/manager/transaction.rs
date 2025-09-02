use std::{
    collections::{HashMap, HashSet},
    ops::Add,
};

use crate::{
    actor::{Address, Context, Version},
    expr::{Action, Upgrade},
    message::{BasisStamp, LockKind, Message, StampedValue, TxId},
    service::{Import, ReactiveAddress, ReactiveId},
};

pub struct Transaction {
    action: Action,
    upgrade: Upgrade,
    state: TransactionState,
}

struct TransactionState {
    id: TxId,
    expected_versions: HashMap<ReactiveAddress, Version>,
    pending_locks: HashMap<Address, PendingLock>,
    locks: HashMap<Address, Lock>,
    reads: HashMap<ReactiveAddress, Option<StampedValue>>,
    existing_configurations: HashMap<Address, Option<HashMap<ReactiveAddress, Import>>>,
}

enum PendingLock {
    Unsent(LockKind),
    Sent,
}

struct Lock {}

impl Transaction {
    pub fn new(
        id: TxId,
        action: Action,
        upgrade: Upgrade,
        expected_versions: HashMap<ReactiveAddress, Version>,
    ) -> Transaction {
        let mut tx = Transaction {
            action,
            upgrade,
            state: TransactionState::new(id, expected_versions),
        };

        tx.upgrade.visit_upgrades(|addr| {
            tx.state.pending_locks.insert(
                addr.service.clone(),
                PendingLock::Unsent(LockKind::Exclusive),
            );
            tx.state
                .existing_configurations
                .insert(addr.service.clone(), None);
        });

        tx.action.visit_writes(|addr, _| {
            tx.state.pending_locks.insert(
                addr.service.clone(),
                PendingLock::Unsent(LockKind::Exclusive),
            );
        });

        tx.action.visit_reads(|addr, _| {
            if !tx.state.pending_locks.contains_key(&addr.service) {
                tx.state
                    .pending_locks
                    .insert(addr.service.clone(), PendingLock::Unsent(LockKind::Shared));
            }
            tx.state.reads.insert(addr.clone(), None);
        });

        tx
    }

    pub fn eval(&mut self, ctx: &Context) {
        if !self.state.pending_locks.is_empty() {
            for (addr, status) in &mut self.state.pending_locks {
                if let PendingLock::Unsent(kind) = status {
                    ctx.send(
                        addr,
                        Message::Lock {
                            txid: self.state.id.clone(),
                            kind: *kind,
                        },
                    );
                    *status = PendingLock::Sent;
                }
            }

            return;
        }

        if !self.state.reads.values().all(|r| r.is_some()) {
            // need to wait to receive needed reads
            return;
        }

        if !self
            .state
            .existing_configurations
            .values()
            .all(|c| c.is_some())
        {
            // need to wait to receive needed configurations
            return;
        }

        self.action.eval(ctx);
    }

    pub fn lock_granted(
        &mut self,
        address: Address,
        reactives: HashMap<ReactiveId, Version>,
        ctx: &Context,
    ) {
        for (addr, version) in &self.state.expected_versions {
            if addr.service == address {
                let actual = reactives.get(&addr.id);
                if actual != Some(version) {
                    panic!("version mismatch: expected {version:?}, got {actual:?}")
                }
            }
        }

        self.state.pending_locks.remove(&address);
        self.state.locks.insert(address.clone(), Lock {});

        if self.state.existing_configurations.contains_key(&address) {
            ctx.send(
                &address,
                Message::ReadConfiguration {
                    txid: self.state.id.clone(),
                },
            );
        }

        for addr in self.state.reads.keys() {
            if addr.service == address {
                ctx.send(
                    &address,
                    Message::Read {
                        txid: self.state.id.clone(),
                        reactive: addr.id,
                        // TODO: reading from defs
                        basis: BasisStamp::empty(),
                    },
                );
            }
        }

        self.eval(ctx);
    }

    pub fn read_result(&mut self, reactive: ReactiveAddress, value: StampedValue, ctx: &Context) {
        *self
            .state
            .reads
            .get_mut(&reactive)
            .expect("received unexpected read result") = Some(value);

        self.eval(ctx);
    }

    pub fn read_configuration_result(
        &mut self,
        service: Address,
        imports: HashMap<ReactiveAddress, Import>,
        ctx: &Context,
    ) {
        *self
            .state
            .existing_configurations
            .get_mut(&service)
            .expect("received configuration") = Some(imports);

        self.eval(ctx);
    }
}

impl TransactionState {
    fn new(id: TxId, expected_versions: HashMap<ReactiveAddress, Version>) -> TransactionState {
        TransactionState {
            id,
            expected_versions,
            pending_locks: HashMap::new(),
            locks: HashMap::new(),
            reads: HashMap::new(),
            existing_configurations: HashMap::new(),
        }
    }
}
