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
    kind: LockKind,
    replica: Option<Value>,
    state: LockState,
}

enum ReplicaState {
    Unneeded,
    Requestable,
    Requested,
}

enum LockState {
    Pending,
    Requested,
    Held(TxId),
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
    fn handle(&mut self, _sender: Address, message: Message, ctx: Context) {
        match message {
            Message::Do { action } => self.do_action(action, &ctx),
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

    fn lock(&mut self, address: &Address, kind: LockKind, mgr: &Manager, ctx: &Context) -> &Lock {
        // Early return if the lock has already been requested
        // TODO: ensure the already-requested lock has compatible kind
        if self.locks.contains_key(address) {
            return &self.locks[address];
        }

        let mut predecessors = HashSet::new();
        let mut all_held = true;
        for input in mgr
            .node_inputs
            .get(address)
            .iter()
            .flat_map(|inputs| inputs.iter())
        {
            if let LockState::Held(txid) = &self.lock(input, kind, mgr, ctx).state {
                predecessors.insert(txid.clone());
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
                kind,
                replica: None,
                state: if all_held {
                    LockState::Requested
                } else {
                    LockState::Pending
                },
            },
        );

        &self.locks[address]
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
            .lock(address, LockKind::Shared, &self.mgr, &self.ctx)
            .replica
            .clone()
    }
}

impl<'a, 'c> ActionEvalContext<Address> for ActionContext<'a, 'c> {
    fn write(&mut self, address: &Address, value: Value) {
        todo!();
    }

    fn will_write(&mut self, address: &Address) {
        self.tx.will_write.insert(address.clone());
    }
}
