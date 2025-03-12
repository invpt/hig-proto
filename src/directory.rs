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
    pub fn new(&mut self, seed_peers: impl Iterator<Item = Address>) -> Directory {
        Directory {
            state: DirectoryState {
                peers: seed_peers.map(|peer| (peer, false)).collect(),
                nodes: HashMap::new(),
            },
        }
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
        let mut updated = false;
        for (peer, deleted) in new_state.peers {
            match self.state.peers.entry(peer.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert_entry(deleted);

                    if !deleted {
                        updated = true;
                    }
                }
                Entry::Occupied(mut entry) => {
                    let local_deleted = entry.get_mut();
                    *local_deleted = *local_deleted || deleted;
                }
            }
        }
        for (name, entry) in new_state.nodes {}
    }

    pub fn register(&mut self, name: Name, address: Address, txid: TxId, ctx: &Context) {
        self.state.nodes.insert(
            name,
            DirectoryEntry {
                txid,
                address,
                deleted: false,
            },
        );

        for (peer, removed) in &self.state.peers {
            if *removed {
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
