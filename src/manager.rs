use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    mem,
};

use crate::{
    actor::{Actor, Address, Context, Version, VersionedAddress},
    directory::{Directory, DirectoryEvent},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext, UpgradeEvalContext},
        Action, Expr, Ident, Name, Upgrade,
    },
    message::{BasisStamp, LockKind, Message, MonotonicTimestampGenerator, TxId, TxPriority},
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
    pending_locks: HashSet<Address>,
    locks: HashMap<Address, Lock>,
    nodes: HashMap<Ident, Node>,
}

enum TransactionKind {
    Action(Action),
    Upgrade(Upgrade),
}

struct Lock {
    value: LockValue,
    wrote: bool,
    basis: BasisStamp,
    roots: HashSet<Address>,
    version: Version,
}

enum LockValue {
    None(ReadRequest),
    Some(Value),
}

enum ReadRequest {
    None,
    Pending,
}

struct UpdatedNode {
    version: Version,
    node: Node,
}

enum Node {
    Var(Value),
    Def(Expr<Ident>, Option<Expr<Ident>>),
    Del,
}

#[derive(Debug)]
struct VersionMismatch;

impl Manager {
    pub fn new(seed_peers: impl Iterator<Item = Address>) -> Manager {
        Manager {
            timestamp_generator: MonotonicTimestampGenerator::new(),
            node_inputs: HashMap::new(),
            transactions: HashMap::new(),
            directory: Directory::new(seed_peers),
        }
    }

    fn do_action(&mut self, action: Action, ctx: &Context) {
        let txid = TxId {
            priority: TxPriority::Low,
            timestamp: self.timestamp_generator.generate_timestamp(),
            address: ctx.me().clone(),
        };

        let tx = Transaction {
            id: txid.clone(),
            kind: TransactionKind::Action(action),
            may_write: HashSet::new(),
            pending_locks: HashSet::new(),
            locks: HashMap::new(),
            nodes: HashMap::new(),
        };

        self.transactions.insert(txid.clone(), tx);

        self.eval(&txid, ctx);
    }

    fn eval(&mut self, txid: &TxId, ctx: &Context) {
        let tx = self.transactions.get_mut(txid).unwrap();
        tx.eval(&self.directory, ctx);

        match tx.kind {
            TransactionKind::Action(Action::Nil) => {
                let tx = self.transactions.remove(txid).unwrap();

                let txid = tx.id;
                let basis = tx
                    .locks
                    .values()
                    .fold(BasisStamp::empty(), |mut basis, lock| {
                        if let LockValue::Some(..) = lock.value {
                            basis.merge_from(&lock.basis);
                        }

                        basis
                    });

                for (address, _) in tx.locks {
                    ctx.send(
                        &address,
                        Message::Release {
                            txid: txid.clone(),
                            basis: basis.clone(),
                        },
                    );
                }
            }
            TransactionKind::Upgrade(Upgrade::Nil) => {
                let tx = self.transactions.remove(txid).unwrap();
            }
            _ => (),
        }
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
                basis,
                roots,
                version,
            } => {
                let tx = self.transactions.get_mut(&txid).unwrap();

                if !tx.pending_locks.remove(&address) {
                    panic!("we were granted a lock we did not request")
                }

                tx.locks.insert(
                    address,
                    Lock {
                        value: LockValue::None(ReadRequest::None),
                        wrote: false,
                        basis,
                        roots,
                        version,
                    },
                );

                self.eval(&txid, &ctx);
            }
            Message::ReadResult {
                txid,
                address,
                value,
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

                self.eval(&txid, &ctx);
            }
            _ => todo!(),
        }
    }
}

impl Transaction {
    pub fn eval(&mut self, directory: &Directory, ctx: &Context) {
        match &mut self.kind {
            TransactionKind::Action(action) => {
                let mut action = mem::replace(action, Action::Nil);

                self.may_write.clear();
                action.visit_writes(|address, definite| {
                    if definite {
                        self.lock_versioned(&address, LockKind::Exclusive, ctx)
                            .expect("invalid version (TODO: don't panic)");
                    } else {
                        self.may_write.insert(address.address.clone());
                    }
                });

                action.eval(&mut TransactionContext {
                    tx: self,
                    directory,
                    ctx,
                });

                self.kind = TransactionKind::Action(action);
            }
            TransactionKind::Upgrade(upgrade) => {
                let mut upgrade = mem::replace(upgrade, Upgrade::Nil);

                upgrade.visit_upgrades(|address| {
                    self.lock_versioned(address, LockKind::Exclusive, ctx)
                        .expect("invalid version (TODO: don't panic)");
                });

                self.may_write.clear();
                upgrade.visit_writes(|ident, definite| {
                    let Ident::Existing(address) = ident else {
                        return;
                    };

                    if definite {
                        self.lock_versioned(address, LockKind::Exclusive, ctx)
                            .expect("invalid version (TODO: don't panic)");
                    } else {
                        self.may_write.insert(address.address.clone());
                    }
                });

                upgrade.eval(&mut TransactionContext {
                    tx: self,
                    directory,
                    ctx,
                });

                self.kind = TransactionKind::Upgrade(upgrade);
            }
        }
    }

    fn lock_versioned(
        &mut self,
        address: &VersionedAddress,
        kind: LockKind,
        ctx: &Context,
    ) -> Result<Option<&mut Lock>, VersionMismatch> {
        match self.lock(&address.address, kind, ctx) {
            Some(lock) => {
                if lock.version == address.version {
                    Ok(Some(lock))
                } else {
                    Err(VersionMismatch)
                }
            }
            None => Ok(None),
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

    fn write(&mut self, ident: Ident, value: &Value, ctx: &Context) -> bool {
        if let Some(node) = self.nodes.get_mut(&ident) {
            match node {
                Node::Var(current_value) => {
                    *current_value = value.clone();

                    return true;
                }
                Node::Def(..) => panic!("can't write to def"),
                Node::Del => {}
            }
        }

        let Ident::Existing(address) = ident else {
            panic!("node not found")
        };

        let Some(lock) = self
            .lock_versioned(&address, LockKind::Exclusive, ctx)
            .expect("invalid version (TODO: don't panic)")
        else {
            // cannot perform this write until the variable is locked
            return false;
        };

        if let LockValue::None(ReadRequest::Pending) = lock.value {
            // cannot perform this write until we have gotten the value back
            return false;
        }

        if let LockValue::None(ReadRequest::None) = lock.value {
            let own_iteration = lock.basis.latest(&address.address);
            lock.basis = BasisStamp::empty();
            lock.basis.add(address.address.clone(), own_iteration);
        }

        lock.value = LockValue::Some(value.clone());
        lock.wrote = true;

        ctx.send(
            &address.address,
            Message::Write {
                txid: self.id.clone(),
                value: value.clone(),
            },
        );

        true
    }

    fn read(&mut self, ident: Ident, directory: &Directory, ctx: &Context) -> Option<&Value> {
        if let Some(node) = self.nodes.get_mut(&ident) {
            match node {
                Node::Var(_value_that_is_exactly_what_we_need) => {
                    // po. lo. ni. us.
                    let Some(Node::Var(value)) = self.nodes.get(&ident) else {
                        unreachable!()
                    };

                    return Some(value);
                }
                Node::Def(expr, computation) => {
                    let mut computation = computation.take().unwrap_or_else(|| expr.clone());
                    computation.eval(&mut TransactionContext {
                        tx: self,
                        directory,
                        ctx,
                    });

                    let Some(Node::Def(_, slot)) = self.nodes.get_mut(&ident) else {
                        unreachable!()
                    };

                    if let Expr::Value(value) = slot.insert(computation) {
                        return Some(value);
                    } else {
                        return None;
                    }
                }
                Node::Del => {}
            }
        }

        let Ident::Existing(address) = ident else {
            panic!("node not found")
        };

        let Some(lock) = self
            .lock_versioned(&address, LockKind::Shared, ctx)
            .expect("invalid version (TODO: don't panic)")
        else {
            // cannot perform this read until the variable is locked
            return None;
        };

        if let LockValue::Some(_value_that_is_exactly_what_we_need) = &lock.value {
            // can't use _value_that_is_exactly_what_we_need because rust is dumb without Polonius
            // this incantation grabs a fresh reference exactly equal to _value_that_is_exactly_what_we_need...
            let LockValue::Some(value) = &self.locks.get(&address.address).unwrap().value else {
                unreachable!()
            };

            // we already have a value
            return Some(value);
        }

        if let LockValue::None(ReadRequest::None) = &lock.value {
            let mut basis = BasisStamp::empty();
            for root_address in lock.roots.clone() {
                let Some(lock) = self.lock(&root_address, LockKind::Shared, ctx) else {
                    // cannot perform this read until this ancestor is locked
                    return None;
                };

                let latest = lock.basis.latest(&root_address);
                basis.add(root_address, latest);
            }

            ctx.send(
                &address.address,
                Message::Read {
                    txid: self.id.clone(),
                    basis,
                },
            );

            self.locks.get_mut(&address.address).unwrap().value =
                LockValue::None(ReadRequest::Pending);
        }

        None
    }
}

struct TransactionContext<'a, 'c> {
    tx: &'a mut Transaction,
    directory: &'a Directory,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> UpgradeEvalContext for TransactionContext<'a, 'c> {
    fn var(&mut self, ident: Ident, value: Value) {
        if self.tx.nodes.contains_key(&ident) {
            panic!("cannot redefine existing node")
        }

        if let Ident::New(name) = &ident {
            if self.directory.get(name).count() > 0 {
                panic!("cannot redefine existing node")
            }
        }

        self.tx.nodes.insert(ident, Node::Var(value));
    }

    fn def(&mut self, ident: Ident, expr: Expr<Ident>) {
        if self.tx.nodes.contains_key(&ident) {
            panic!("cannot redefine existing node")
        }

        if let Ident::New(name) = &ident {
            if self.directory.get(name).count() > 0 {
                panic!("cannot redefine existing node")
            }
        }

        self.tx.nodes.insert(ident, Node::Def(expr, None));
    }

    fn del(&mut self, address: VersionedAddress) {
        self.tx.nodes.insert(Ident::Existing(address), Node::Del);
    }
}

impl<'a, 'c, I: Clone + Into<Ident>> ActionEvalContext<I> for TransactionContext<'a, 'c> {
    fn write(&mut self, ident: &I, value: &Value) -> bool {
        self.tx.write(ident.clone().into(), value, self.ctx)
    }
}

impl<'a, 'c, I: Clone + Into<Ident>> ExprEvalContext<I> for TransactionContext<'a, 'c> {
    fn read(&mut self, ident: &I) -> Option<&Value> {
        self.tx.read(ident.clone().into(), self.directory, self.ctx)
    }
}
