use std::collections::{HashMap, HashSet};

use crate::{
    actor::{Address, Context, Version, VersionedAddress},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext, UpgradeEvalContext},
        Action, Expr, Ident, Upgrade, Value,
    },
    message::{BasisStamp, LockKind, Message, TxId},
};

use super::directory::Directory;

pub struct Transaction {
    kind: TransactionKind,
    state: TransactionState,
}

pub enum TransactionKind {
    Action(Action),
    Upgrade(Upgrade),
}

struct TransactionState {
    id: TxId,
    may_write: HashSet<Address>,
    pending_locks: HashMap<Address, Option<Version>>,
    locks: HashMap<Address, Lock>,
    nodes: HashMap<Ident, Node>,
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

enum Node {
    Var(Value),
    Def(Expr<Ident>, Option<Expr<Ident>>),
    Del,
}

struct EvalContext<'a, 'c> {
    state: &'a mut TransactionState,
    directory: &'a Directory,
    ctx: &'a Context<'c>,
}

#[derive(Debug)]
struct VersionMismatch;

impl Transaction {
    pub fn new(id: TxId, kind: TransactionKind) -> Transaction {
        Transaction {
            kind,
            state: TransactionState::new(id),
        }
    }

    pub fn eval(&mut self, directory: &Directory, ctx: &Context) {
        match &mut self.kind {
            TransactionKind::Action(action) => {
                self.state.may_write.clear();
                action.visit_writes(|address, definite| {
                    if definite {
                        self.state
                            .lock_versioned(&address, LockKind::Exclusive, ctx)
                            .expect("invalid version (TODO: don't panic)");
                    } else {
                        self.state.may_write.insert(address.address.clone());
                    }
                });

                action.eval(&mut EvalContext {
                    state: &mut self.state,
                    directory,
                    ctx,
                });

                if let Action::Nil = action {
                    self.finish_action(ctx);
                }
            }
            TransactionKind::Upgrade(upgrade) => {
                upgrade.visit_upgrades(|address| {
                    self.state
                        .lock_versioned(address, LockKind::Exclusive, ctx)
                        .expect("invalid version (TODO: don't panic)");
                });

                self.state.may_write.clear();
                upgrade.visit_writes(|ident, definite| {
                    let Ident::Existing(address) = ident else {
                        return;
                    };

                    if definite {
                        self.state
                            .lock_versioned(address, LockKind::Exclusive, ctx)
                            .expect("invalid version (TODO: don't panic)");
                    } else {
                        self.state.may_write.insert(address.address.clone());
                    }
                });

                upgrade.eval(&mut EvalContext {
                    state: &mut self.state,
                    directory,
                    ctx,
                });
            }
        }
    }

    fn finish_action(&mut self, ctx: &Context) {
        let txid = self.state.id.clone();
        let basis = self
            .state
            .locks
            .values()
            .fold(BasisStamp::empty(), |mut basis, lock| {
                if let LockValue::Some(..) = lock.value {
                    basis.merge_from(&lock.basis);
                }

                basis
            });

        for (address, _) in &self.state.locks {
            ctx.send(
                address,
                Message::Release {
                    txid: txid.clone(),
                    basis: basis.clone(),
                },
            );
        }
    }

    pub fn lock_granted(
        &mut self,
        address: Address,
        version: Version,
        basis: BasisStamp,
        roots: HashSet<Address>,
    ) {
        let Some(expected_version) = self.state.pending_locks.remove(&address) else {
            panic!("we were granted a lock we did not request")
        };

        if let Some(expected_version) = expected_version {
            if version != expected_version {
                panic!("we requested a non-current version")
            }
        }

        self.state.locks.insert(
            address,
            Lock {
                value: LockValue::None(ReadRequest::None),
                wrote: false,
                basis,
                roots,
                version,
            },
        );
    }

    pub fn read_result(&mut self, address: Address, value: Value) {
        let lock = self
            .state
            .locks
            .get_mut(&address)
            .expect("received value from unknown lock");

        assert!(matches!(lock.value, LockValue::None(ReadRequest::Pending)));

        lock.value = LockValue::Some(value);
    }
}

impl TransactionState {
    pub fn new(id: TxId) -> TransactionState {
        TransactionState {
            id,
            may_write: HashSet::new(),
            pending_locks: HashMap::new(),
            locks: HashMap::new(),
            nodes: HashMap::new(),
        }
    }

    fn lock_versioned(
        &mut self,
        address: &VersionedAddress,
        kind: LockKind,
        ctx: &Context,
    ) -> Result<Option<&mut Lock>, VersionMismatch> {
        match self.lock_inner(&address.address, Some(address.version), kind, ctx) {
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

    fn lock(&mut self, address: &Address, kind: LockKind, ctx: &Context) -> Option<&mut Lock> {
        self.lock_inner(address, None, kind, ctx)
    }

    fn lock_inner(
        &mut self,
        address: &Address,
        version: Option<Version>,
        mut kind: LockKind,
        ctx: &Context,
    ) -> Option<&mut Lock> {
        if let Some(lock) = self.locks.get_mut(address) {
            // already held
            return Some(lock);
        }

        if let Some(existing_version) = self.pending_locks.insert(address.clone(), version) {
            if let (Some(existing_version), Some(requested_version)) = (existing_version, version) {
                if existing_version != requested_version {
                    panic!("requested mismatched versions")
                }
            }
        } else {
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
                    computation.eval(&mut EvalContext {
                        state: self,
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

impl<'a, 'c> UpgradeEvalContext for EvalContext<'a, 'c> {
    fn var(&mut self, ident: Ident, value: Value) {
        if self.state.nodes.contains_key(&ident) {
            panic!("cannot redefine existing node")
        }

        if let Ident::New(name) = &ident {
            if self.directory.get(name).count() > 0 {
                panic!("cannot redefine existing node")
            }
        }

        self.state.nodes.insert(ident, Node::Var(value));
    }

    fn def(&mut self, ident: Ident, expr: Expr<Ident>) {
        if self.state.nodes.contains_key(&ident) {
            panic!("cannot redefine existing node")
        }

        if let Ident::New(name) = &ident {
            if self.directory.get(name).count() > 0 {
                panic!("cannot redefine existing node")
            }
        }

        self.state.nodes.insert(ident, Node::Def(expr, None));
    }

    fn del(&mut self, address: VersionedAddress) {
        self.state.nodes.insert(Ident::Existing(address), Node::Del);
    }
}

impl<'a, 'c, I: Clone + Into<Ident>> ActionEvalContext<I> for EvalContext<'a, 'c> {
    fn write(&mut self, ident: &I, value: &Value) -> bool {
        self.state.write(ident.clone().into(), value, self.ctx)
    }
}

impl<'a, 'c, I: Clone + Into<Ident>> ExprEvalContext<I> for EvalContext<'a, 'c> {
    fn read(&mut self, ident: &I) -> Option<&Value> {
        self.state
            .read(ident.clone().into(), self.directory, self.ctx)
    }
}
