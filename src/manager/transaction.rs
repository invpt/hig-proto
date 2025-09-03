use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    actor::{Address, Context, Version},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext, ExprResolveContext, UpgradeEvalContext},
        Action, Expr, Ident, Name, Upgrade, Value,
    },
    message::{BasisStamp, LockKind, Message, TxId},
    service::{Export, Import, ReactiveAddress, ReactiveId},
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
    names: HashMap<(Address, Name), ReactiveAddress>,
    reactives: HashMap<ReactiveAddress, Option<Reactive>>,
    prepared: HashMap<Address, BasisStamp>,
}

enum Reactive {
    Variable(Value),
    Definition(Expr),
}

struct PendingLock {
    kind: LockKind,
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
        next_id: ReactiveId,
        subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,
        roots: HashMap<ReactiveId, HashSet<ReactiveAddress>>,
    },
}

enum ReactiveValue {
    PendingRead,
    Some(Value),
}

impl Transaction {
    pub fn new(
        id: TxId,
        action: Action,
        upgrade: Upgrade,
        expected_versions: HashMap<ReactiveAddress, Version>,
        ctx: &Context,
    ) -> Transaction {
        let mut state = TransactionState::new(id, expected_versions);

        for item in &upgrade.items {
            state.lock(item.address().clone(), LockKind::Exclusive, true, ctx);
        }

        action.visit_writes(&mut |address, _| {
            state.lock(address.service.clone(), LockKind::Exclusive, false, ctx);
        });

        action.visit_reads(&mut |address, _| {
            state.lock(address.service.clone(), LockKind::Shared, false, ctx);
        });

        upgrade.visit_reads(&mut |ident, _| {
            let Ident::Existing(address) = ident else {
                return;
            };

            state.lock(address.service.clone(), LockKind::Shared, false, ctx);
        });

        Transaction {
            action,
            upgrade,
            state,
        }
    }

    pub fn lock_granted(
        &mut self,
        service: Address,
        reactives: HashMap<ReactiveId, Version>,
        ctx: &Context,
    ) {
        let pending = self
            .state
            .pending_locks
            .remove(&service)
            .expect("unknown lock granted");

        for (address, version) in &self.state.expected_versions {
            if address.service == service {
                assert_eq!(
                    reactives.get(&address.id),
                    Some(version),
                    "version mismatch"
                );
            }
        }

        self.state.locks.insert(
            service.clone(),
            Lock {
                kind: pending.kind,
                values: HashMap::new(),
                configuration: if pending.configuration_needed {
                    ctx.send(
                        &service,
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

        self.eval(ctx);
    }

    pub fn returned_configuration(
        &mut self,
        service: Address,
        imports: HashMap<ReactiveAddress, Import>,
        exports: HashMap<ReactiveId, Export>,
        next_id: ReactiveId,
        subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,
        roots: HashMap<ReactiveId, HashSet<ReactiveAddress>>,
        ctx: &Context,
    ) {
        let lock = self
            .state
            .locks
            .get_mut(&service)
            .expect("configuration for unlocked node");

        lock.configuration = Configuration::Some {
            imports,
            exports,
            next_id,
            subscriptions,
            roots,
        };

        self.eval(ctx);
    }

    pub fn returned_value(&mut self, reactive: ReactiveAddress, value: Value, ctx: &Context) {
        let lock = self
            .state
            .locks
            .get_mut(&reactive.service)
            .expect("value for unlocked node");

        let slot @ ReactiveValue::PendingRead = lock
            .values
            .get_mut(&reactive.id)
            .expect("unrequested value")
        else {
            panic!("reactive value in incorrect state")
        };

        *slot = ReactiveValue::Some(value);

        self.eval(ctx);
    }

    fn eval(&mut self, ctx: &Context) {
        self.action.eval(&mut EvalContext {
            state: &mut self.state,
            ctx,
        });
        self.upgrade.eval(&mut EvalContext {
            state: &mut self.state,
            ctx,
        });

        if matches!(self.action, Action::Nil) && self.upgrade.items.len() == 0 {
            for (service, lock) in &self.state.locks {
                ctx.send(
                    service,
                    Message::PrepareCommit {
                        txid: self.state.id.clone(),
                    },
                );
            }
        }
    }
}

impl TransactionState {
    fn new(id: TxId, expected_versions: HashMap<ReactiveAddress, Version>) -> TransactionState {
        TransactionState {
            id,
            expected_versions,
            pending_locks: HashMap::new(),
            locks: HashMap::new(),
            names: HashMap::new(),
            reactives: HashMap::new(),
            prepared: HashMap::new(),
        }
    }

    fn lock(
        &mut self,
        address: Address,
        kind: LockKind,
        configuration_needed: bool,
        ctx: &Context,
    ) {
        if let Some(existing) = self.locks.get(&address) {
            if matches!(
                (kind, existing.kind),
                (LockKind::Exclusive, LockKind::Shared)
            ) {
                panic!("can't upgrade lock kind");
            } else {
                return;
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

                current.configuration_needed = configuration_needed || current.configuration_needed;
            }
            Entry::Vacant(e) => {
                e.insert(PendingLock {
                    kind,
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

    fn declare(&mut self, ident: Ident, reactive: Reactive) {
        let address = match ident {
            Ident::New(service, name) => self
                .names
                .entry((service, name))
                .or_insert_with_key(|(service, _)| {
                    let Configuration::Some { next_id, .. } = &mut self
                        .locks
                        .get_mut(service)
                        .expect("couldn't find service")
                        .configuration
                    else {
                        panic!("missing service configuration")
                    };

                    let id = *next_id;
                    *next_id = next_id.increment();

                    ReactiveAddress {
                        service: service.clone(),
                        id,
                    }
                })
                .clone(),
            Ident::Existing(address) => address,
        };

        self.reactives.insert(address, Some(reactive));
    }
}

struct EvalContext<'a, 'c> {
    state: &'a mut TransactionState,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> UpgradeEvalContext for EvalContext<'a, 'c> {
    fn var(&mut self, ident: Ident, value: Value) {
        self.state.declare(ident, Reactive::Variable(value));
    }

    fn def(&mut self, ident: Ident, expr: Expr) {
        self.state.declare(ident, Reactive::Definition(expr));
    }

    fn del(&mut self, address: ReactiveAddress) {
        self.state.reactives.insert(address, None);
    }
}

impl<'a, 'c> ActionEvalContext for EvalContext<'a, 'c> {
    fn write(&mut self, address: &ReactiveAddress, value: &crate::expr::Value) -> bool {
        if let Some(lock) = self.state.locks.get_mut(&address.service) {
            assert_eq!(lock.kind, LockKind::Exclusive);

            let Entry::Vacant(v) = lock.values.entry(address.id) else {
                // Can't write while read is pending
                return false;
            };

            v.insert(ReactiveValue::Some(value.clone()));

            self.ctx.send(
                &address.service,
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
            Ident::New(service, name) => {
                // todo: dont clone
                let address = self
                    .state
                    .names
                    .get(&(service.clone(), name.clone()))
                    .expect("couldn't resolve");
                let Some(Reactive::Variable(value)) = self
                    .state
                    .reactives
                    .get(&address)
                    .expect("couldn't find reactive")
                else {
                    panic!("can't read from newly-created definition")
                };
                Some(value)
            }
            Ident::Existing(address) => self.read(address),
        }
    }
}

impl<'a, 'c> ExprEvalContext for EvalContext<'a, 'c> {
    fn read(&mut self, address: &ReactiveAddress) -> Option<&crate::expr::Value> {
        if let Some(lock) = self.state.locks.get_mut(&address.service) {
            match lock.values.entry(address.id) {
                Entry::Occupied(read) => match read.into_mut() {
                    ReactiveValue::PendingRead => None,
                    ReactiveValue::Some(value) => Some(value),
                },
                Entry::Vacant(read) => {
                    read.insert(ReactiveValue::PendingRead);

                    self.ctx.send(
                        &address.service,
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

impl<'a, 'c> ExprResolveContext<Ident> for EvalContext<'a, 'c> {
    fn resolve(&mut self, ident: Ident) -> Option<ReactiveAddress> {
        match ident {
            Ident::Existing(address) => Some(address),
            Ident::New(service, name) => self.state.names.get(&(service, name)).cloned(),
        }
    }
}
