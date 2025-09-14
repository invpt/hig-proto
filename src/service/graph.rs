use std::collections::{HashMap, HashSet, VecDeque};

use super::{ReactiveAddress, ReactiveId};

#[derive(Debug, Clone)]
pub struct ReactiveGraph {
    pub subscriptions: HashMap<ReactiveId, HashSet<ReactiveId>>,
    pub roots: HashMap<ReactiveId, HashSet<ReactiveAddress>>,
}

pub struct ReactiveGraphNode<I: Iterator<Item = ReactiveAddress>> {
    pub id: ReactiveId,
    pub inputs: I,
}

impl ReactiveGraph {
    pub fn empty() -> ReactiveGraph {
        ReactiveGraph {
            subscriptions: HashMap::new(),
            roots: HashMap::new(),
        }
    }

    pub fn add_or_update(&mut self, id: ReactiveId, inputs: impl Iterator<Item = ReactiveAddress>) {
        for input in inputs {}
    }

    pub fn remove(&mut self, id: ReactiveId) {}
}
