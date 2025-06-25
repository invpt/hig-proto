use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    actor::{Address, Context, Version},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext, UpgradeEvalContext},
        Expr, Ident, TransactionKind, Value,
    },
    message::{BasisStamp, LockKind, Message, TxId},
    node::{Export, Import, ReactiveAddress, ReactiveId, VersionedReactiveAddress},
};

pub struct Transaction {
    kind: TransactionKind,
    state: TransactionState,
}

struct TransactionState {
    id: TxId,
    pending_locks: HashMap<Address, PendingLock>,
    locks: HashMap<Address, Lock>,
    reactives: HashMap<ReactiveAddress, Option<Reactive>>,
}

enum Reactive {
    Variable(Value),
    Definition(Expr<ReactiveAddress>),
}

struct PendingLock {
    kind: LockKind,
    expected_reactives: HashMap<ReactiveId, Version>,
    configuration_needed: bool,
}

struct Lock {
    kind: LockKind,
    values: HashMap<ReactiveId, ReactiveValue>,
    configuration: Configuration,
}

enum Configuration {
    Unneeded,
    PendingRead,
    Some {
        imports: HashMap<ReactiveAddress, Import>,
        exports: HashMap<ReactiveId, Export>,
        subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,
        roots: HashMap<ReactiveId, HashSet<ReactiveAddress>>,
    },
}

enum ReactiveValue {
    PendingRead,
    Some(Value),
}

impl Transaction {
    pub fn new(id: TxId, kind: TransactionKind) -> Transaction {
        Transaction {
            kind: kind,
            state: TransactionState::new(id),
        }
    }

    pub fn init(&mut self, ctx: &Context) {
        self.state.init(&self.kind, ctx);
    }

    pub fn lock_granted(
        &mut self,
        address: Address,
        reactives: HashMap<ReactiveId, Version>,
        ctx: &Context,
    ) {
        let pending = self
            .state
            .pending_locks
            .remove(&address)
            .expect("unknown lock granted");

        for (id, version) in pending.expected_reactives {
            let actual_version = reactives.get(&id).expect("missing expected reactive");
            assert_eq!(version, *actual_version);
        }

        self.state.locks.insert(
            address.clone(),
            Lock {
                kind: pending.kind,
                values: HashMap::new(),
                configuration: if pending.configuration_needed {
                    ctx.send(
                        &address,
                        Message::ReadConfiguration {
                            txid: self.state.id.clone(),
                        },
                    );

                    Configuration::PendingRead
                } else {
                    Configuration::Unneeded
                },
            },
        );

        self.kind.eval(&mut EvalContext {
            state: &mut self.state,
            ctx,
        });
    }

    pub fn returned_configuration(
        &mut self,
        address: Address,
        imports: HashMap<ReactiveAddress, Import>,
        exports: HashMap<ReactiveId, Export>,
        subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,
        roots: HashMap<ReactiveId, HashSet<ReactiveAddress>>,
        ctx: &Context,
    ) {
        let lock = self
            .state
            .locks
            .get_mut(&address)
            .expect("configuration for unlocked node");

        lock.configuration = Configuration::Some {
            imports,
            exports,
            subscriptions,
            roots,
        };

        self.kind.eval(&mut EvalContext {
            state: &mut self.state,
            ctx,
        });
    }
}

impl TransactionState {
    fn new(id: TxId) -> TransactionState {
        TransactionState {
            id,
            pending_locks: HashMap::new(),
            locks: HashMap::new(),
            reactives: HashMap::new(),
        }
    }

    fn init(&mut self, kind: &TransactionKind, ctx: &Context) {
        let mut required_locks: HashMap<Address, (LockKind, HashMap<ReactiveId, Version>, bool)> =
            HashMap::new();

        match kind {
            TransactionKind::Action(action) => {
                action.visit_writes(&mut |address: &VersionedReactiveAddress, _| {
                    let (_, expected_reactives, _) = required_locks
                        .entry(address.address.clone())
                        .or_insert_with(|| (LockKind::Exclusive, HashMap::new(), false));

                    let expected_version = expected_reactives
                        .entry(address.id)
                        .or_insert(address.version);

                    assert_eq!(*expected_version, address.version);
                });

                action.visit_reads(&mut |address, _| {
                    let (_, expected_reactives, _) = required_locks
                        .entry(address.address.clone())
                        .or_insert_with(|| (LockKind::Shared, HashMap::new(), false));

                    let expected_version = expected_reactives
                        .entry(address.id)
                        .or_insert(address.version);

                    assert_eq!(*expected_version, address.version);
                });
            }
            TransactionKind::Upgrade(upgrade) => {
                for item in &upgrade.items {
                    let (_, expected_reactives, _) = required_locks
                        .entry(item.address().clone())
                        .or_insert_with(|| (LockKind::Exclusive, HashMap::new(), true));

                    if let Some((id, version)) = item.expected_reactive() {
                        let expected_version = expected_reactives.entry(id).or_insert(version);
                        assert_eq!(*expected_version, version);
                    }
                }

                upgrade.visit_reads(&mut |ident, _| {
                    let Ident::Existing(address) = ident else {
                        return;
                    };

                    let (_, expected_reactives, _) = required_locks
                        .entry(address.address.clone())
                        .or_insert_with(|| (LockKind::Shared, HashMap::new(), false));

                    let expected_version = expected_reactives
                        .entry(address.id)
                        .or_insert(address.version);

                    assert_eq!(*expected_version, address.version);
                });
            }
        }

        for (address, (kind, expected_reactives, configuration_needed)) in required_locks {
            self.lock(address, kind, expected_reactives, configuration_needed, ctx);
        }
    }

    fn lock(
        &mut self,
        address: Address,
        kind: LockKind,
        expected_reactives: HashMap<ReactiveId, Version>,
        configuration_needed: bool,
        ctx: &Context,
    ) {
        if let Some(existing) = self.locks.get(&address) {
            if matches!(
                (kind, existing.kind),
                (LockKind::Exclusive, LockKind::Shared)
            ) {
                panic!("can't upgrade lock kind");
            }
        }

        match self.pending_locks.entry(address.clone()) {
            Entry::Occupied(mut e) => {
                let current = e.get_mut();

                if matches!(
                    (kind, current.kind),
                    (LockKind::Exclusive, LockKind::Shared)
                ) {
                    panic!("can't upgrade lock kind");
                }

                for (id, v) in expected_reactives {
                    match current.expected_reactives.entry(id) {
                        Entry::Occupied(current) => {
                            assert_eq!(*current.get(), v);
                        }
                        Entry::Vacant(current) => {
                            current.insert(v);
                        }
                    }
                }

                current.configuration_needed = configuration_needed || current.configuration_needed;
            }
            Entry::Vacant(e) => {
                e.insert(PendingLock {
                    kind,
                    expected_reactives,
                    configuration_needed,
                });

                ctx.send(
                    &address,
                    Message::Lock {
                        txid: self.id.clone(),
                        kind,
                    },
                );
            }
        };
    }
}

struct EvalContext<'a, 'c> {
    state: &'a mut TransactionState,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> UpgradeEvalContext for EvalContext<'a, 'c> {
    fn var(&mut self, ident: Ident, value: Value) {
        let reactive_address = match ident {
            Ident::New(address, name) => {
                // TODO: save name to resolve later
                _ = name;
                ReactiveAddress {
                    address,
                    id: ReactiveId(0), // TODO: random id or something
                }
            }
            Ident::Existing(address) => ReactiveAddress {
                address: address.address,
                id: address.id,
            },
        };

        self.state
            .reactives
            .insert(reactive_address, Some(Reactive::Variable(value)));
    }

    fn def(&mut self, ident: Ident, expr: Expr<Ident>) {
        let reactive_address = match ident {
            Ident::New(address, name) => {
                // TODO: save name to resolve later
                ReactiveAddress {
                    address,
                    id: ReactiveId(0), // TODO: random id
                }
            }
            Ident::Existing(address) => ReactiveAddress {
                address: address.address,
                id: address.id,
            },
        };

        let expr = expr.resolve(&mut |ident| match ident {
            Ident::New(address, name) => todo!("resolve {address:?} {name:?}"),
            Ident::Existing(address) => ReactiveAddress {
                address: address.address,
                id: address.id,
            },
        });

        self.state
            .reactives
            .insert(reactive_address, Some(Reactive::Definition(expr)));
    }

    fn del(&mut self, address: VersionedReactiveAddress) {
        self.state.reactives.insert(
            ReactiveAddress {
                address: address.address,
                id: address.id,
            },
            None,
        );
    }
}

impl<'a, 'c> ActionEvalContext for EvalContext<'a, 'c> {
    fn write(&mut self, address: &VersionedReactiveAddress, value: &crate::expr::Value) -> bool {
        if let Some(lock) = self.state.locks.get_mut(&address.address) {
            assert_eq!(lock.kind, LockKind::Exclusive);

            let Entry::Vacant(v) = lock.values.entry(address.id) else {
                // Can't write while read is pending
                return false;
            };

            v.insert(ReactiveValue::Some(value.clone()));

            self.ctx.send(
                &address.address,
                Message::Write {
                    txid: self.state.id.clone(),
                    reactive: address.id,
                    value: value.clone(),
                },
            );

            true
        } else {
            false
        }
    }
}

impl<'a, 'c> ExprEvalContext<Ident> for EvalContext<'a, 'c> {
    fn read(&mut self, ident: &Ident) -> Option<&Value> {
        match ident {
            Ident::New(address, name) => todo!("read new {address:?}.{name:?}"),
            Ident::Existing(address) => self.read(address),
        }
    }
}

impl<'a, 'c> ExprEvalContext for EvalContext<'a, 'c> {
    fn read(&mut self, address: &VersionedReactiveAddress) -> Option<&crate::expr::Value> {
        if let Some(lock) = self.state.locks.get_mut(&address.address) {
            match lock.values.entry(address.id) {
                Entry::Occupied(read) => match read.into_mut() {
                    ReactiveValue::PendingRead => None,
                    ReactiveValue::Some(value) => Some(value),
                },
                Entry::Vacant(read) => {
                    read.insert(ReactiveValue::PendingRead);

                    self.ctx.send(
                        &address.address,
                        Message::ReadValue {
                            txid: self.state.id.clone(),
                            reactive: address.id,
                            // TODO: large effort: read from defs (need to specify basis stamps)
                            basis: BasisStamp::empty(),
                        },
                    );

                    None
                }
            }
        } else {
            None
        }
    }
}
