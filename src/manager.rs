use std::{
    collections::{HashMap, HashSet},
    mem,
};

use crate::{
    actor::{Actor, Address, Context},
    directory::{Directory, DirectoryEvent},
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
    transactions: HashMap<TxId, Transaction>,
    directory: Directory,
}

struct Transaction {
    id: TxId,
    kind: TransactionKind,
    may_write: HashSet<Address>,
    predecessors: HashMap<TxId, TxMeta>,
    pending_locks: HashSet<Address>,
    locks: HashMap<Address, Lock>,
}

enum TransactionKind {
    Action(Action),
    Upgrade(Upgrade),
}

struct Lock {
    value: LockValue,
    did_write: bool,
    completed: HashSet<TxId>,
    ancestor_vars: HashSet<Address>,
}

enum LockValue {
    None(ReadRequest),
    Some(Value),
}

enum ReadRequest {
    None,
    Pending,
}

impl Manager {
    pub fn new(seed_peers: impl Iterator<Item = Address>) -> Manager {
        Manager {
            timestamp_generator: MonotonicTimestampGenerator::new(),
            node_inputs: HashMap::new(),
            transactions: HashMap::new(),
            directory: Directory::new(seed_peers),
        }
    }

    fn do_action(&mut self, action: Action<Address>, ctx: &Context) {
        let txid = TxId {
            kind: TxKind::Data,
            timestamp: self.timestamp_generator.generate_timestamp(),
            address: ctx.me().clone(),
        };

        let mut tx = Transaction {
            id: txid.clone(),
            kind: TransactionKind::Action(action),
            may_write: HashSet::new(),
            predecessors: HashMap::new(),
            pending_locks: HashSet::new(),
            locks: HashMap::new(),
        };

        tx.eval(ctx);

        self.transactions.insert(txid, tx);
    }
}

impl Actor for Manager {
    fn init(&mut self, ctx: Context) {
        self.directory.init(&ctx);
    }

    fn handle(&mut self, message: Message, ctx: Context) {
        let message = match self.directory.handle(message, &ctx) {
            DirectoryEvent::UpdatedState => return,
            DirectoryEvent::Unhandled(message) => message,
        };

        match message {
            Message::Do { action } => self.do_action(action, &ctx),
            Message::LockGranted {
                txid,
                address,
                completed,
                ancestor_vars,
            } => {
                let tx = self.transactions.get_mut(&txid).unwrap();

                if !tx.pending_locks.remove(&address) {
                    panic!("we were granted a lock we did not request")
                }

                tx.locks.insert(
                    address,
                    Lock {
                        value: LockValue::None(ReadRequest::None),
                        did_write: false,
                        completed,
                        ancestor_vars,
                    },
                );

                tx.eval(&ctx);
            }
            Message::ReadValue {
                txid,
                address,
                value,
                predecessors,
            } => {
                let tx = self
                    .transactions
                    .get_mut(&txid)
                    .expect("received message for unknown transaction");

                let lock = tx
                    .locks
                    .get_mut(&address)
                    .expect("received value from unknown lock");

                assert!(matches!(lock.value, LockValue::None(ReadRequest::Pending)));

                lock.value = LockValue::Some(value);

                for (txid, meta) in predecessors {
                    tx.predecessors.insert(txid, meta);
                }

                tx.eval(&ctx)
            }
            _ => todo!(),
        }
    }
}

impl Transaction {
    pub fn eval(&mut self, ctx: &Context) {
        match &mut self.kind {
            TransactionKind::Action(action) => {
                let mut action = mem::replace(action, Action::Nil);
                self.may_write.clear();
                action.traverse(&mut TransactionContext { tx: self, ctx });
                action.eval(&mut TransactionContext { tx: self, ctx });
                self.kind = TransactionKind::Action(action);

                /*if let Action::Nil = action {
                    // complete the txn

                    let affected = self
                        .locks
                        .iter()
                        .filter(|(_, l)| l.did_write)
                        .map(|(a, _)| a.clone())
                        .collect::<HashSet<_>>();

                    let mut predecessors = self.predecessors;
                    predecessors.insert(self.id.clone(), TxMeta { affected });

                    for (address, _) in self.locks {
                        ctx.send(
                            &address,
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
                }*/
            }
            TransactionKind::Upgrade(upgrade) => {
                let mut upgrade = mem::replace(upgrade, Upgrade::Nil);
                self.may_write.clear();
                upgrade.traverse(&mut TransactionContext { tx: self, ctx });
                upgrade.eval(&mut TransactionContext { tx: self, ctx });
                self.kind = TransactionKind::Upgrade(upgrade);

                /*if let Upgrade::Nil = upgrade {
                    todo!()
                } else {
                    todo!()
                }*/
            }
        }
    }

    fn lock(&mut self, address: &Address, mut kind: LockKind, ctx: &Context) -> Option<&mut Lock> {
        if let Some(lock) = self.locks.get_mut(address) {
            // already held
            return Some(lock);
        }

        if self.pending_locks.insert(address.clone()) {
            if self.may_write.contains(address) {
                kind = LockKind::Exclusive;
            }

            ctx.send(
                address,
                Message::Lock {
                    txid: self.id.clone(),
                    kind,
                },
            );
        }

        None
    }

    fn write(&mut self, address: &Address, value: &Value, ctx: &Context) -> bool {
        let Some(lock) = self.lock(address, LockKind::Exclusive, ctx) else {
            // cannot perform this write until the variable is locked
            return false;
        };

        if let LockValue::None(ReadRequest::Pending) = &lock.value {
            // cannot perform this write until we have gotten the value back
            return false;
        }

        lock.value = LockValue::Some(value.clone());

        ctx.send(
            address,
            Message::Write {
                txid: self.id.clone(),
                value: value.clone(),
            },
        );

        true
    }

    fn read(&mut self, address: &Address, ctx: &Context) -> Option<&Value> {
        let Some(lock) = self.lock(address, LockKind::Shared, ctx) else {
            // cannot perform this read until the variable is locked
            return None;
        };

        if let LockValue::Some(_value_that_is_exactly_what_we_need) = &lock.value {
            // can't use _value_that_is_exactly_what_we_need because rust is dumb without Polonius
            // this incantation grabs a fresh reference exactly equal to _value_that_is_exactly_what_we_need...
            let LockValue::Some(value) = &self.locks.get(address).unwrap().value else {
                unreachable!()
            };

            // we already have a value
            return Some(value);
        }

        if let LockValue::None(ReadRequest::None) = &lock.value {
            let mut predecessors = HashSet::new();
            for ancestor_address in lock.ancestor_vars.clone() {
                let Some(lock) = self.lock(&ancestor_address, LockKind::Shared, ctx) else {
                    // cannot perform this read until this ancestor is locked
                    return None;
                };

                for txid in &lock.completed {
                    predecessors.insert(txid.clone());
                }
            }

            ctx.send(
                address,
                Message::Read {
                    txid: self.id.clone(),
                    predecessors,
                },
            );

            self.locks.get_mut(address).unwrap().value = LockValue::None(ReadRequest::Pending);
        }

        None
    }
}

struct TransactionContext<'a, 'c> {
    tx: &'a mut Transaction,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> UpgradeEvalContext for TransactionContext<'a, 'c> {
    fn var(&mut self, name: Name, replace: Option<Address>, value: Value) {
        todo!()
    }

    fn def(&mut self, name: Name, replace: Option<Address>, expr: Expr) {
        todo!()
    }

    fn del(&mut self, address: Address) {
        todo!()
    }
}

impl<'a, 'c> UpgradeTraversalContext for TransactionContext<'a, 'c> {
    fn will_var(&mut self, name: Name, replace: Option<Address>) {
        todo!()
    }

    fn will_def(&mut self, name: Name, replace: Option<Address>) {
        todo!()
    }

    fn will_del(&mut self, address: Address) {
        todo!()
    }
}

impl<'a, 'c> ActionEvalContext for TransactionContext<'a, 'c> {
    fn write(&mut self, address: &Address, value: &Value) -> bool {
        self.tx.write(address, value, self.ctx)
    }
}

impl<'a, 'c> ActionTraversalContext for TransactionContext<'a, 'c> {
    fn will_write(&mut self, address: &Address) {
        self.tx.lock(address, LockKind::Exclusive, self.ctx);
    }

    fn may_write(&mut self, address: &Address) {
        self.tx.may_write.insert(address.clone());
    }
}

impl<'a, 'c> ExprEvalContext for TransactionContext<'a, 'c> {
    fn read(&mut self, address: &Address) -> Option<&Value> {
        self.tx.read(address, self.ctx)
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
