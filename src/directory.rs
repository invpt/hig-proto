use std::collections::{hash_map::Entry, HashMap};

use crate::{
    actor::{Address, Context, VersionedAddress},
    expr::Name,
    message::{DirectoryState, Message},
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

        for (name, addresses) in new_state.nodes {
            match self.state.nodes.entry(name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(addresses);
                }
                Entry::Occupied(mut entry) => {
                    let my_addresses = entry.get_mut();
                    for (address, version) in addresses {
                        match my_addresses.entry(address.clone()) {
                            Entry::Vacant(entry) => {
                                entry.insert(version);
                            }
                            Entry::Occupied(mut entry) => {
                                let my_version = entry.get_mut();
                                match (version, my_version) {
                                    (_, None) => (),
                                    (None, my_version) => *my_version = None,
                                    (Some(version), Some(my_version)) => {
                                        if version > *my_version {
                                            *my_version = version;
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

    pub fn get<'a>(&'a self, name: &Name) -> impl Iterator<Item = VersionedAddress> + 'a {
        self.state.nodes.get(name).into_iter().flat_map(|entries| {
            entries.iter().flat_map(|(address, version)| {
                if let Some(version) = version {
                    Some(VersionedAddress {
                        address: address.clone(),
                        version: version.clone(),
                    })
                } else {
                    None
                }
            })
        })
    }

    pub fn register(&mut self, name: Name, address: VersionedAddress, ctx: &Context) {
        match self
            .state
            .nodes
            .entry(name)
            .or_insert_with(|| HashMap::new())
            .entry(address.address)
        {
            Entry::Vacant(entry) => {
                entry.insert(Some(address.version));
            }
            Entry::Occupied(mut entry) => match entry.get_mut() {
                None => *entry.get_mut() = Some(address.version),
                Some(version) => {
                    if address.version > *version {
                        *version = address.version;
                    } else {
                        panic!("only new versions can be registered")
                    }
                }
            },
        }

        self.disseminate_state(ctx);
    }

    pub fn delete(&mut self, address: VersionedAddress, ctx: &Context) -> bool {
        let instances = self
            .state
            .nodes
            .values_mut()
            .flat_map(|addresses| addresses.get_mut(&address.address))
            .filter(|v| **v == Some(address.version))
            .map(|v| v);

        let mut deleted = false;
        for version in instances {
            *version = None;
            deleted = true;
        }

        if deleted {
            self.disseminate_state(ctx);
        }

        deleted
    }

    pub fn has(&self, address: &VersionedAddress) -> bool {
        self.state
            .nodes
            .values()
            .flat_map(|addresses| addresses.get(&address.address))
            .any(|v| *v == Some(address.version))
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
