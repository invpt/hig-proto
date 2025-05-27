use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    actor::Address,
    expr::Value,
    message::{BasisStamp, ReactiveConfiguration, TxId},
};

use super::ReactiveId;

pub enum HeldLocks {
    None,
    Shared(BTreeMap<TxId, SharedLockState>),
    Exclusive(TxId, SharedLockState, ExclusiveLockState),
}

#[derive(Default)]
pub struct SharedLockState {
    pub reads: HashMap<ReactiveId, Read>,
}

pub struct Read {
    pub pending: BasisStamp,
    pub complete: BasisStamp,
}

#[derive(Default)]
pub struct ExclusiveLockState {
    pub writes: HashMap<ReactiveId, Value>,
    pub reactives: HashMap<ReactiveId, Option<ReactiveConfiguration>>,
    pub exports: HashMap<ReactiveId, HashSet<Address>>,
}

impl HeldLocks {
    pub fn exclusive(&self, txid: &TxId) -> Option<&ExclusiveLockState> {
        match self {
            HeldLocks::Exclusive(held_txid, _, exclusive_data) => {
                if held_txid == txid {
                    Some(exclusive_data)
                } else {
                    None
                }
            }
            HeldLocks::None | HeldLocks::Shared(_) => None,
        }
    }

    pub fn exclusive_mut(&mut self, txid: &TxId) -> Option<&mut ExclusiveLockState> {
        match self {
            HeldLocks::Exclusive(held_txid, _, exclusive_data) => {
                if held_txid == txid {
                    Some(exclusive_data)
                } else {
                    None
                }
            }
            HeldLocks::None | HeldLocks::Shared(_) => None,
        }
    }

    pub fn shared(&self, txid: &TxId) -> Option<&SharedLockState> {
        match self {
            HeldLocks::Shared(held) => held.get(txid),
            HeldLocks::Exclusive(held_txid, shared_data, _) => {
                if held_txid == txid {
                    Some(shared_data)
                } else {
                    None
                }
            }
            HeldLocks::None => None,
        }
    }

    pub fn shared_mut(&mut self, txid: &TxId) -> Option<&mut SharedLockState> {
        match self {
            HeldLocks::Shared(held) => held.get_mut(txid),
            HeldLocks::Exclusive(held_txid, shared_data, _) => {
                if held_txid == txid {
                    Some(shared_data)
                } else {
                    None
                }
            }
            HeldLocks::None => None,
        }
    }

    pub fn visit_shared(&mut self, mut visitor: impl FnMut(&TxId, &mut SharedLockState)) {
        match self {
            HeldLocks::Shared(held) => held
                .iter_mut()
                .for_each(|(txid, state)| visitor(txid, state)),
            HeldLocks::Exclusive(txid, state, _) => visitor(txid, state),
            HeldLocks::None => (),
        }
    }
}
