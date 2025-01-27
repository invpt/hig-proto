use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use crate::{
    lock::Lock,
    message::{Message, TxId, TxMeta},
    router::{Actor, Address, Context},
    value::Value,
};

pub struct Definition {
    lock: Lock,
    replicas: HashMap<Address, Option<Value>>,
    ancestor_variable_to_inputs: HashMap<Address, Vec<Address>>,
    subscribers: HashSet<Address>,
    pending_updates: HashMap<UpdateLink, Arc<PendingUpdate>>,
    pending_transactions: HashMap<TxId, TxMeta>,
    handled_transactions: HashSet<TxId>,
}

pub struct InputMetadata {
    pub ancestor_variables: Box<[Address]>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct UpdateLink {
    address: Address,
    txid: TxId,
}

struct PendingUpdate {
    value: Value,
    links: HashSet<UpdateLink>,
}

impl Definition {
    fn new(inputs: HashMap<Address, InputMetadata>, subscribers: HashSet<Address>) -> Definition {
        let mut ancestor_variable_to_inputs = HashMap::<Address, Vec<Address>>::new();
        for (input, ancestors) in &inputs {
            for ancestor in &ancestors.ancestor_variables {
                match ancestor_variable_to_inputs.entry(*ancestor) {
                    Entry::Occupied(mut e) => {
                        e.get_mut().push(*input);
                    }
                    Entry::Vacant(e) => {
                        e.insert(Vec::from([*input]));
                    }
                }
            }
        }

        Definition {
            lock: Lock::new(),
            replicas: inputs.keys().map(|a| (*a, None)).collect(),
            ancestor_variable_to_inputs,
            subscribers,
            pending_updates: HashMap::new(),
            pending_transactions: HashMap::new(),
            handled_transactions: HashSet::new(),
        }
    }

    fn apply_batches(&mut self, ctx: Context) {
        _ = ctx;
        // simple DFS batch search
    }
}

impl Actor for Definition {
    fn handle(&mut self, sender: Address, message: Message, ctx: Context) {
        if self.lock.handle_lock_messages(&sender, &message, &ctx) {
            return;
        }

        match message {
            Message::Update {
                value,
                predecessors,
            } => {
                let mut links = HashSet::new();
                for (id, meta) in predecessors {
                    if self.handled_transactions.contains(&id) {
                        continue;
                    }

                    let mut affected_inputs = Vec::new();
                    for variable in &meta.affected {
                        for input in &self.ancestor_variable_to_inputs[&variable] {
                            if !self.pending_transactions.contains_key(&id) {}
                            affected_inputs.push(*input);
                            links.insert(UpdateLink {
                                address: *input,
                                txid: id,
                            });
                        }
                    }

                    if !affected_inputs.is_empty() {
                        self.pending_transactions.insert(
                            id,
                            TxMeta {
                                affected: affected_inputs.into_boxed_slice(),
                            },
                        );
                    }
                }
                let update = Arc::new(PendingUpdate { value, links });
                for link in &update.links {
                    self.pending_updates.insert(*link, update.clone());
                }
                self.apply_batches(ctx);
            }
            _ => todo!(),
        }
    }
}
