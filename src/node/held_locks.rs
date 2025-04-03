use std::collections::{BTreeMap, HashMap};

use crate::{
    actor::{Address, Context},
    message::{Message, NodeConfiguration, TxId},
    value::Value,
};

pub enum HeldLocks {
    None,
    Shared(BTreeMap<TxId, SharedLockState>),
    Exclusive(TxId, SharedLockState, ExclusiveLockState),
}

#[derive(Default)]
pub struct SharedLockState {
    pub preempting: bool,
    pub subscription_updates: HashMap<Address, bool>,
    pub was_read: bool,
}

pub enum ExclusiveLockState {
    Unchanged,
    Write(Value),
    Update(NodeConfiguration),
    Retire,
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
}

impl SharedLockState {
    pub fn preempt(&mut self, txid: &TxId, ctx: &Context) {
        if !self.preempting {
            self.preempting = true;
            ctx.send(&txid.address, Message::Preempt { txid: txid.clone() });
        }
    }
}
