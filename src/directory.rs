use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    actor::{Address, Context},
    expr::Name,
    message::{DirectoryEntry, DirectoryState, Message, TxId},
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
                                    (_, DirectoryEntry::Deleted) => {}
                                    (DirectoryEntry::Deleted, current) => {
                                        *current = DirectoryEntry::Deleted
                                    }
                                    (
                                        DirectoryEntry::Exists {
                                            iteration: new_iteration,
                                            address: new_address,
                                        },
                                        DirectoryEntry::Exists { iteration, address },
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

    pub fn register(&mut self, name: Name, address: Address, txid: TxId, ctx: &Context) {
        todo!();
        /*self.state.nodes.insert(
            name,
            DirectoryEntry {
                txid,
                address,
                deleted: false,
            },
        );*/

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
