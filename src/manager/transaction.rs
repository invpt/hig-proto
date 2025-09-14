use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    actor::{Address, Context, Version},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext, ExprResolveContext, UpgradeEvalContext},
        Action, Expr, Ident, Name, Upgrade, Value,
    },
    manager::graph,
    message::{BasisStamp, LockKind, Message, TxId},
    service::{Export, Import, ReactiveAddress, ReactiveId, ReactiveInputs},
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
    services: HashMap<Address, Configuration>,
    has_code_updates: bool,
    configuration_updates_ready: bool,
    prepared: HashMap<Address, BasisStamp>,
}

pub(super) enum Reactive {
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
    configuration: RequestedConfiguration,
}

enum RequestedConfiguration {
    Unneeded,
    PendingRead,
    Read,
}

struct ConfigurationUpdates {
    ready: bool,
    services: HashMap<Address, Configuration>,
}

pub(super) struct Configuration {
    pub(super) imports: HashMap<ReactiveAddress, Import>,
    pub(super) inputs: HashMap<ReactiveId, ReactiveInputs>,
    pub(super) reactives: HashMap<ReactiveId, Option<Reactive>>,
    pub(super) exports: HashMap<ReactiveId, Export>,
    pub(super) next_id: ReactiveId,
    pub(super) names: HashMap<Name, ReactiveId>,
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

        upgrade.visit_stmts(&mut |stmt| {
            state.lock(stmt.address().clone(), LockKind::Exclusive, true, ctx);
        });

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

                    RequestedConfiguration::PendingRead
                } else {
                    RequestedConfiguration::Unneeded
                },
            },
        );

        self.eval(ctx);
    }

    pub fn returned_configuration(
        &mut self,
        service: Address,
        imports: HashMap<ReactiveAddress, Import>,
        reactives: HashMap<ReactiveId, ReactiveInputs>,
        exports: HashMap<ReactiveId, Export>,
        next_id: ReactiveId,
        ctx: &Context,
    ) {
        let lock = self
            .state
            .locks
            .get_mut(&service)
            .expect("configuration for unlocked node");

        lock.configuration = RequestedConfiguration::Read;

        self.state.services.insert(
            service,
            Configuration {
                imports,
                inputs: reactives,
                reactives: HashMap::new(),
                exports,
                next_id,
                names: HashMap::new(),
            },
        );

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

        if let Upgrade::Nil = self.upgrade {
            if self.state.has_code_updates {
                if !self.state.configuration_updates_ready {
                    for (_, lock) in &mut self.state.locks {
                        if let RequestedConfiguration::Unneeded = lock.configuration {
                            continue;
                        }

                        if let RequestedConfiguration::PendingRead = lock.configuration {
                            // not ready yet
                            return;
                        }
                    }

                    let links = graph::cross_service_links(
                        self.state
                            .services
                            .iter()
                            .map(|(address, service)| (address, &service.inputs)),
                    );

                    // update the services based on these links
                    for link in &links {
                        // ensure src has an export
                        self.state
                            .services
                            .get_mut(&link.src.service)
                            .unwrap()
                            .exports
                            .entry(link.src.id)
                            .or_insert_with(|| Export {
                                roots: HashSet::new(),
                                importers: HashSet::new(),
                            })
                            .importers
                            .insert(link.dst.service.clone());
                        // ensure dst has an import
                        self.state
                            .services
                            .get_mut(&link.dst.service)
                            .unwrap()
                            .imports
                            .entry(link.src.clone())
                            .or_insert_with(|| Import {
                                roots: HashSet::new(),
                                importers: HashSet::new(),
                            })
                            .importers
                            .insert(link.dst.id);
                    }

                    graph::prune_unused_imports_exports(&mut self.state.services);

                    graph::compute_roots(&mut self.state.services);

                    self.state.configuration_updates_ready = true;
                }
                return;
            }
        }

        if let (Action::Nil, Upgrade::Nil) = (&self.action, &self.upgrade) {
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
            services: HashMap::new(),
            has_code_updates: false,
            configuration_updates_ready: false,
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

    fn declare(&mut self, ident: &Ident) -> Option<ReactiveId> {
        match ident {
            Ident::New(service_address, name) => {
                let Some(service) = self.services.get_mut(service_address) else {
                    return None;
                };
                let id = service.names.entry(name.clone()).or_insert_with(|| {
                    let id = service.next_id;
                    service.next_id = service.next_id.increment();

                    id
                });

                Some(*id)
            }
            Ident::Existing(address) => Some(address.id),
        }
    }
}

struct EvalContext<'a, 'c> {
    state: &'a mut TransactionState,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> UpgradeEvalContext for EvalContext<'a, 'c> {
    fn var(&mut self, ident: &Ident, value: &Value) -> bool {
        self.state.has_code_updates = true;

        let Some(id) = self.state.declare(ident) else {
            // don't have config for service yet
            return false;
        };

        let service = self.state.services.get_mut(ident.address()).unwrap();

        service.inputs.insert(
            id,
            ReactiveInputs {
                inputs: HashSet::new(),
            },
        );

        service
            .reactives
            .insert(id, Some(Reactive::Variable(value.clone())));

        true
    }

    fn def(&mut self, ident: &Ident, expr: &Expr) -> bool {
        self.state.has_code_updates = true;

        let Some(id) = self.state.declare(ident) else {
            // don't have config for service yet
            return false;
        };

        let service = self.state.services.get_mut(ident.address()).unwrap();

        service.inputs.insert(
            id,
            ReactiveInputs {
                inputs: todo!("inputs for def"),
            },
        );

        service
            .reactives
            .insert(id, Some(Reactive::Definition(expr.clone())));

        true
    }

    fn del(&mut self, address: &ReactiveAddress) -> bool {
        self.state.has_code_updates = true;

        let Some(service) = self.state.services.get_mut(&address.service) else {
            // don't have config for service yet
            return false;
        };

        service.inputs.remove(&address.id);
        service.reactives.insert(address.id, None);

        true
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
            Ident::New(service_address, name) => {
                let service = self
                    .state
                    .services
                    .get(service_address)
                    .expect("couldn't resolve service");
                let id = service.names.get(name).expect("couldn't resolve name");
                let Some(Reactive::Variable(value)) =
                    service.reactives.get(&id).expect("couldn't find reactive")
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
            Ident::New(service, name) => self
                .state
                .services
                .get(&service)
                .and_then(|s| s.names.get(&name))
                .map(|id| ReactiveAddress {
                    service: service.clone(),
                    id: *id,
                }),
        }
    }
}
