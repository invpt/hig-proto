use std::{
    collections::{HashMap, HashSet},
    f32::consts::PI,
    mem,
};

use crate::{
    actor::{Actor, Address, Context},
    expr::{
        eval::{
            ActionEvalContext, ActionTraversalContext, ExprEvalContext, ExprTraversalContext,
            Resolver, UpgradeEvalContext, UpgradeTraversalContext,
        },
        Action, Expr, Name, Upgrade, UpgradeIdent,
    },
    message::{LockKind, Message, MonotonicTimestampGenerator, TxId, TxKind, TxMeta},
    value::Value,
};

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    node_inputs: HashMap<Address, HashSet<Address>>,
    transactions: HashMap<TxId, Option<Transaction>>,
}

struct Transaction {
    id: TxId,
    kind: TransactionKind,
    may_write: HashSet<Address>,
    predecessors: HashMap<TxId, TxMeta>,
    locks: HashMap<Address, Lock>,
}

enum TransactionKind {
    Action(Action),
    Upgrade(Upgrade),
}

struct Lock {
    address: Address,
    kind: LockKind,
    value: LockValue,
    state: LockState,
    did_write: bool,
}

enum LockValue {
    None(ReadRequest),
    Some(Value, WriteRequest),
}

enum ReadRequest {
    None,
    Unsent,
    Pending,
}

enum WriteRequest {
    None,
    Unsent,
}

enum Request {
    None,
    Read,
    Write(Value),
}

enum LockState {
    Pending,
    Requested,
    Held(HashSet<TxId>),
}

impl Manager {
    pub fn new() -> Manager {
        Manager {
            timestamp_generator: MonotonicTimestampGenerator::new(),
            node_inputs: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    fn do_action(&mut self, action: Action<Address>, ctx: &Context) {
        let txid = TxId {
            kind: TxKind::Data,
            timestamp: self.timestamp_generator.generate_timestamp(),
            address: ctx.me().clone(),
        };

        let tx = Transaction {
            id: txid.clone(),
            kind: TransactionKind::Action(action),
            may_write: HashSet::new(),
            predecessors: HashMap::new(),
            locks: HashMap::new(),
        };

        if let Some(tx) = tx.eval(self, ctx) {
            self.transactions.insert(txid, Some(tx));
        }
    }
}

impl Actor for Manager {
    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            Message::Do { action } => self.do_action(action, &ctx),
            Message::LockRejected {
                txid,
                needs_predecessors_from_inputs,
            } => {
                self.node_inputs
                    .insert(txid.address.clone(), needs_predecessors_from_inputs);

                let tx = self.transactions.get_mut(&txid).unwrap().as_mut().unwrap();

                let lock = tx
                    .locks
                    .get_mut(&txid.address)
                    .expect("received lock granted from unknown lock");

                assert!(matches!(lock.state, LockState::Requested));

                lock.state = LockState::Pending;
            }
            Message::LockGranted { txid, predecessors } => {
                let mut tx = self.transactions.get_mut(&txid).unwrap().take().unwrap();

                let lock = tx
                    .locks
                    .get_mut(&txid.address)
                    .expect("received lock granted from unknown lock");

                assert!(matches!(lock.state, LockState::Requested));

                lock.state = LockState::Held(predecessors);

                lock.send_request(&txid, &ctx);

                if let Some(tx) = tx.eval(self, &ctx) {
                    self.transactions.insert(txid, Some(tx));
                }
            }
            Message::ReadValue {
                txid,
                value,
                predecessors,
            } => {
                let mut tx = self.transactions.get_mut(&txid).unwrap().take().unwrap();

                let lock = tx
                    .locks
                    .get_mut(&txid.address)
                    .expect("received value from unknown lock");

                assert!(matches!(lock.value, LockValue::None(ReadRequest::Pending)));

                lock.value = LockValue::Some(value, WriteRequest::None);

                for (txid, meta) in predecessors {
                    tx.predecessors.insert(txid, meta);
                }

                if let Some(tx) = tx.eval(self, &ctx) {
                    self.transactions.insert(txid, Some(tx));
                }
            }
            _ => todo!(),
        }
    }
}

impl Transaction {
    pub fn eval(mut self, mgr: &Manager, ctx: &Context) -> Option<Self> {
        match &mut self.kind {
            TransactionKind::Action(action) => {
                let mut action = mem::replace(action, Action::Nil);
                self.may_write.clear();
                action.traverse(&mut TransactionContext {
                    tx: &mut self,
                    mgr,
                    ctx,
                });
                action.eval(&mut TransactionContext {
                    tx: &mut self,
                    mgr,
                    ctx,
                });

                if let Action::Nil = action {
                    // complete the txn

                    let affected = self
                        .locks
                        .values()
                        .filter(|l| l.did_write)
                        .map(|l| l.address.clone())
                        .collect::<HashSet<_>>();

                    let mut predecessors = self.predecessors;
                    predecessors.insert(self.id.clone(), TxMeta { affected });

                    for (_, lock) in self.locks {
                        ctx.send(
                            &lock.address,
                            Message::Release {
                                txid: self.id.clone(),
                                predecessors: predecessors.clone(),
                            },
                        );
                    }

                    None
                } else {
                    self.kind = TransactionKind::Action(action);

                    Some(self)
                }
            }
            TransactionKind::Upgrade(upgrade) => {
                let mut upgrade = mem::replace(upgrade, Upgrade::Nil);
                self.may_write.clear();
                upgrade.traverse(&mut TransactionContext {
                    tx: &mut self,
                    mgr,
                    ctx,
                });
                upgrade.eval(&mut TransactionContext {
                    tx: &mut self,
                    mgr,
                    ctx,
                });

                if let Upgrade::Nil = upgrade {
                    todo!()
                } else {
                    todo!()
                }
            }
        }
    }

    fn lock(
        &mut self,
        address: &Address,
        mut kind: LockKind,
        request: Request,
        mgr: &Manager,
        ctx: &Context,
    ) -> &Lock {
        let lock = if self.locks.contains_key(address) {
            let lock = self.locks.get_mut(address).unwrap();

            assert!(lock.kind >= kind, "cannot upgrade lock kinds");

            lock
        } else {
            if self.may_write.contains(address) {
                // Even if a shared lock is requested, if we might at some point write this address,
                // we need to conservatively get an exclusive lock
                kind = LockKind::Exclusive;
            }

            let mut predecessors = HashSet::new();
            let mut all_held = true;
            for input in mgr
                .node_inputs
                .get(address)
                .iter()
                .flat_map(|inputs| inputs.iter())
            {
                if let LockState::Held(node_predecessors) =
                    &self.lock(input, kind, Request::None, mgr, ctx).state
                {
                    for pred in node_predecessors {
                        predecessors.insert(pred.clone());
                    }
                } else {
                    all_held = false;
                    break;
                }
            }

            if all_held {
                ctx.send(
                    address,
                    Message::Lock {
                        txid: self.id.clone(),
                        kind,
                        predecessors,
                    },
                );
            }

            self.locks.insert(
                address.clone(),
                Lock {
                    address: address.clone(),
                    kind,
                    value: LockValue::None(ReadRequest::None),
                    state: if all_held {
                        LockState::Requested
                    } else {
                        LockState::Pending
                    },
                    did_write: false,
                },
            );

            self.locks.get_mut(address).unwrap()
        };

        match request {
            Request::None => {}
            Request::Read => {
                if let LockValue::None(slot @ ReadRequest::None) = &mut lock.value {
                    *slot = ReadRequest::Unsent;
                }
            }
            Request::Write(value) => {
                assert_eq!(
                    kind,
                    LockKind::Exclusive,
                    "When a write is requested, the lock kind must be Exclusive"
                );

                lock.value = LockValue::Some(value, WriteRequest::Unsent);
            }
        }

        if let LockState::Held(_) = &lock.state {
            lock.send_request(&self.id, ctx);
        }

        lock
    }
}

impl Lock {
    pub fn send_request(&mut self, txid: &TxId, ctx: &Context) {
        assert!(
            matches!(self.state, LockState::Held(_)),
            "lock must be held to send its request"
        );

        match &mut self.value {
            LockValue::None(slot @ ReadRequest::Unsent) => {
                ctx.send(&self.address, Message::Read { txid: txid.clone() });
                *slot = ReadRequest::Pending;
            }
            LockValue::None(ReadRequest::None | ReadRequest::Pending) => {}
            LockValue::Some(value, slot @ WriteRequest::Unsent) => {
                ctx.send(
                    &self.address,
                    Message::Write {
                        txid: txid.clone(),
                        value: value.clone(),
                    },
                );

                *slot = WriteRequest::None;

                self.did_write = true;
            }
            LockValue::Some(_, WriteRequest::None) => {}
        }
    }
}

struct TransactionContext<'a, 'c> {
    tx: &'a mut Transaction,
    mgr: &'a Manager,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> UpgradeEvalContext for TransactionContext<'a, 'c> {
    fn var(&mut self, name: Name, value: Value) {
        todo!()
    }

    fn def(&mut self, name: Name, expr: Expr) {
        todo!()
    }

    fn del(&mut self, address: Address) {
        todo!()
    }
}

impl<'a, 'c> UpgradeTraversalContext for TransactionContext<'a, 'c> {
    fn will_var(&mut self, name: Name) {
        todo!()
    }

    fn will_def(&mut self, name: Name) {
        todo!()
    }

    fn will_del(&mut self, address: Address) {
        todo!()
    }
}

impl<'a, 'c> ActionEvalContext for TransactionContext<'a, 'c> {
    fn write(&mut self, address: &Address, value: Value) {
        self.tx.lock(
            address,
            LockKind::Exclusive,
            Request::Write(value),
            &self.mgr,
            self.ctx,
        );
    }
}

impl<'a, 'c> ActionTraversalContext for TransactionContext<'a, 'c> {
    fn will_write(&mut self, address: &Address) {
        // TODO: request some locks in advance?
        self.tx.may_write.insert(address.clone());
    }

    fn may_write(&mut self, address: &Address) {
        self.tx.may_write.insert(address.clone());
    }
}

impl<'a, 'c> ExprEvalContext for TransactionContext<'a, 'c> {
    fn read(&mut self, address: &Address) -> Option<Value> {
        let lock = self.tx.lock(
            address,
            LockKind::Shared,
            Request::Read,
            &self.mgr,
            self.ctx,
        );

        match &lock.value {
            LockValue::None(_) => None,
            LockValue::Some(value, _) => Some(value.clone()),
        }
    }
}

impl<'a, 'c> ExprTraversalContext for TransactionContext<'a, 'c> {
    fn will_read(&mut self, ident: &Address) {
        // TODO: request some locks in advance?
        _ = ident;
    }

    fn may_read(&mut self, ident: &Address) {
        _ = ident;
    }
}

impl<'a, 'c> Resolver<UpgradeIdent> for TransactionContext<'a, 'c> {
    fn resolve<'b>(&mut self, ident: &'b UpgradeIdent) -> Option<&'b Address> {
        match ident {
            UpgradeIdent::New(_) => todo!(),
            UpgradeIdent::Existing(address) => Some(address),
        }
    }
}
