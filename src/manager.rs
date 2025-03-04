use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap, HashSet},
    mem,
};

use crate::{
    actor::{Actor, Address, Context},
    expr::{
        eval::{ActionEvalContext, ExprEvalContext},
        Action,
    },
    message::{LockKind, Message, MonotonicTimestampGenerator, TxId, TxKind},
    value::Value,
};

pub struct Manager {
    timestamp_generator: MonotonicTimestampGenerator,
    node_inputs: HashMap<Address, HashSet<Address>>,
    transactions: HashMap<TxId, Option<Transaction>>,
}

struct Transaction {
    id: TxId,
    action: Action,
    will_write: HashSet<Address>,
    locks: HashMap<Address, Lock>,
}

struct Lock {
    address: Address,
    kind: LockKind,
    value: Option<Value>,
    request: Option<RequestState>,
    state: LockState,
}

struct RequestState {
    sent: bool,
    kind: RequestKind,
}

enum RequestKind {
    Read,
    Write,
}

enum Request {
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

    fn process_active(&mut self, ctx: Context) {
        for (txid, tx) in &mut self.transactions {}
    }

    fn do_action(&mut self, action: Action<Address>, ctx: &Context) {
        let txid = TxId {
            kind: TxKind::Data,
            timestamp: self.timestamp_generator.generate_timestamp(),
            address: ctx.me().clone(),
        };

        let mut tx = Transaction {
            id: txid.clone(),
            action,
            will_write: HashSet::new(),
            locks: HashMap::new(),
        };

        tx.eval(self, ctx);

        self.transactions.insert(txid, Some(tx));
    }
}

impl Actor for Manager {
    fn handle(&mut self, sender: Address, message: Message, ctx: Context) {
        match message {
            Message::Do { action } => self.do_action(action, &ctx),
            Message::LockRejected {
                txid,
                needs_predecessors_from_inputs,
            } => {
                self.node_inputs
                    .insert(sender.clone(), needs_predecessors_from_inputs);

                let tx = self.transactions.get_mut(&txid).unwrap().as_mut().unwrap();

                let lock = tx
                    .locks
                    .get_mut(&sender)
                    .expect("received lock granted from unknown lock");

                assert!(matches!(lock.state, LockState::Requested));

                lock.state = LockState::Pending;
            }
            Message::LockGranted { txid, predecessors } => {
                let tx = self.transactions.get_mut(&txid).unwrap().as_mut().unwrap();

                let lock = tx
                    .locks
                    .get_mut(&sender)
                    .expect("received lock granted from unknown lock");

                assert!(matches!(lock.state, LockState::Requested));

                lock.state = LockState::Held(predecessors);

                lock.send_request(&txid, &ctx);
            }
            _ => todo!(),
        }

        self.process_active(ctx);
    }
}

impl Transaction {
    pub fn eval(&mut self, mgr: &Manager, ctx: &Context) {
        let mut action = mem::replace(&mut self.action, Action::Nil);
        action.eval(&mut ActionContext { tx: self, mgr, ctx });
        self.action = action;
    }

    fn lock(
        &mut self,
        address: &Address,
        kind: LockKind,
        request: Option<Request>,
        mgr: &Manager,
        ctx: &Context,
    ) -> &Lock {
        let lock = if self.locks.contains_key(address) {
            let lock = self.locks.get_mut(address).unwrap();

            if lock.kind != kind {
                // TODO: is it possible to implement the rest of manager so that locks never need
                // to be upgrade? There is a degenerate way of doing this, always requesting
                // exclusive locks, but that's an especially inefficient way. Also have to consider
                // code updates not just actions.
                todo!("upgrading the kind of locks")
            }

            lock
        } else {
            let mut predecessors = HashSet::new();
            let mut all_held = true;
            for input in mgr
                .node_inputs
                .get(address)
                .iter()
                .flat_map(|inputs| inputs.iter())
            {
                if let LockState::Held(node_predecessors) =
                    &self.lock(input, kind, None, mgr, ctx).state
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
                    address.clone(),
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
                    value: None,
                    request: None,
                    state: if all_held {
                        LockState::Requested
                    } else {
                        LockState::Pending
                    },
                },
            );

            self.locks.get_mut(address).unwrap()
        };

        match request {
            Some(request) => match request {
                Request::Read => {
                    lock.request = Some(RequestState {
                        sent: false,
                        kind: RequestKind::Read,
                    });
                }
                Request::Write(value) => {
                    assert_eq!(
                        kind,
                        LockKind::Exclusive,
                        "When a write is requested, the lock kind must be Exclusive"
                    );

                    lock.value = Some(value);
                    lock.request = Some(RequestState {
                        sent: false,
                        kind: RequestKind::Write,
                    });
                }
            },
            None => (),
        };

        &self.locks[address]
    }
}

impl Lock {
    pub fn send_request(&mut self, txid: &TxId, ctx: &Context) {
        assert!(
            matches!(self.state, LockState::Held(_)),
            "lock must be held to send its request"
        );

        let Some(request) = &mut self.request else {
            return;
        };

        if request.sent {
            return;
        }

        match request.kind {
            RequestKind::Read => {
                ctx.send(self.address.clone(), Message::Read { txid: txid.clone() });
            }
            RequestKind::Write => {
                let Some(value) = self.value.clone() else {
                    panic!("write request without value to write")
                };

                ctx.send(
                    self.address.clone(),
                    Message::Write {
                        txid: txid.clone(),
                        value,
                    },
                )
            }
        }

        request.sent = true;
    }
}

struct ActionContext<'a, 'c> {
    tx: &'a mut Transaction,
    mgr: &'a Manager,
    ctx: &'a Context<'c>,
}

impl<'a, 'c> ExprEvalContext<Address> for ActionContext<'a, 'c> {
    fn read(&mut self, address: &Address) -> Option<Value> {
        self.tx
            .lock(
                address,
                LockKind::Shared,
                Some(Request::Read),
                &self.mgr,
                self.ctx,
            )
            .value
            .clone()
    }
}

impl<'a, 'c> ActionEvalContext<Address> for ActionContext<'a, 'c> {
    fn write(&mut self, address: &Address, value: Value) {
        self.tx.lock(
            address,
            LockKind::Exclusive,
            Some(Request::Write(value)),
            &self.mgr,
            self.ctx,
        );
    }

    fn will_write(&mut self, address: &Address) {
        self.tx
            .lock(address, LockKind::Exclusive, None, &self.mgr, self.ctx);
    }
}
