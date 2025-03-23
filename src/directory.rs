use std::collections::{hash_map::Entry, HashMap};

use crate::{
    actor::{Address, Context},
    expr::Name,
    message::{
        DirectoryEntryState, DirectoryState, EntryId, Message, MonotonicTimestampGenerator, TxId,
    },
};

pub struct Directory {
    state: DirectoryState,
}

pub enum DirectoryEvent {
    Unhandled(Message),
    UpdatedState,
}

impl Directory {
    pub fn new(seed_peers: impl Iterator<Item = Address>) -> Directory {
        Directory {
            state: DirectoryState {
                managers: seed_peers.map(|peer| (peer, false)).collect(),
                nodes: HashMap::new(),
            },
        }
    }

    pub fn init(&mut self, ctx: &Context) {
        self.state.managers.insert(ctx.me().clone(), false);
    }

    pub fn handle(&mut self, message: Message, ctx: &Context) -> DirectoryEvent {
        match message {
            Message::Directory { state } => {
                self.merge_and_update(state, ctx);
                DirectoryEvent::UpdatedState
            }
            _ => DirectoryEvent::Unhandled(message),
        }
    }

    fn merge_and_update(&mut self, new_state: DirectoryState, ctx: &Context) {
        let mut new_peers = Vec::new();

        for (peer, deleted) in new_state.managers {
            match self.state.managers.entry(peer.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(deleted);

                    if !deleted {
                        // for each non-deleted new peer, send an introduction
                        new_peers.push(peer);
                    }
                }
                Entry::Occupied(mut entry) => {
                    let local_deleted = entry.get_mut();
                    if deleted && !*local_deleted {
                        *local_deleted = true;
                    }
                }
            }
        }

        for (name, new_nodes) in new_state.nodes {
            match self.state.nodes.entry(name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(new_nodes);
                }
                Entry::Occupied(mut entry) => {
                    let old_nodes = entry.get_mut();
                    for (txid, new_node) in new_nodes {
                        match old_nodes.entry(txid.clone()) {
                            Entry::Vacant(entry) => {
                                entry.insert(new_node);
                            }
                            Entry::Occupied(mut entry) => {
                                let old_node = entry.get_mut();
                                match (new_node, old_node) {
                                    (_, DirectoryEntryState::Deleted) => {}
                                    (DirectoryEntryState::Deleted, current) => {
                                        *current = DirectoryEntryState::Deleted
                                    }
                                    (
                                        DirectoryEntryState::Existing {
                                            iteration: new_iteration,
                                            address: new_address,
                                        },
                                        DirectoryEntryState::Existing { iteration, address },
                                    ) => {
                                        if new_iteration > *iteration {
                                            *iteration = new_iteration;
                                            *address = new_address;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        for peer in new_peers {
            ctx.send(
                &peer,
                Message::Directory {
                    state: self.state.clone(),
                },
            );
        }
    }

    pub fn get<'a: 'b, 'b>(
        &'a self,
        name: &'b Name,
    ) -> impl Iterator<Item = (&'b EntryId, &'b Address)> {
        self.state.nodes.get(name).into_iter().flat_map(|entries| {
            entries.iter().flat_map(|(id, entry)| {
                if let DirectoryEntryState::Existing { address, .. } = entry {
                    Some((id, address))
                } else {
                    None
                }
            })
        })
    }

    pub fn create(&mut self, name: Name, address: Address, txid: TxId, ctx: &Context) {
        let entries = self
            .state
            .nodes
            .entry(name)
            .or_insert_with(|| HashMap::new());

        if entries
            .iter()
            .all(|(_, entry)| matches!(entry, DirectoryEntryState::Deleted))
        {
            entries.insert(
                EntryId { txid },
                DirectoryEntryState::Existing {
                    iteration: 0,
                    address,
                },
            );
        } else {
            panic!("there is already an entry for the given name")
        }

        self.disseminate_state(ctx);
    }

    pub fn update(&mut self, name: &Name, entry_id: &EntryId, new_address: Address, ctx: &Context) {
        let entries = self
            .state
            .nodes
            .get_mut(name)
            .expect("can not update name: no existing mappings");

        let entry = entries
            .get_mut(entry_id)
            .expect("can not update name: could not find mapping");

        let DirectoryEntryState::Existing { iteration, address } = entry else {
            panic!("can not update name: mapping was deleted");
        };

        *iteration += 1;
        *address = new_address;

        self.disseminate_state(ctx);
    }

    fn disseminate_state(&self, ctx: &Context) {
        for (peer, removed) in &self.state.managers {
            if *removed || peer == ctx.me() {
                continue;
            }

            ctx.send(
                peer,
                Message::Directory {
                    state: self.state.clone(),
                },
            );
        }
    }
}
