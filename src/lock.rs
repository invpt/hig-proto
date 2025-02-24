use std::collections::{btree_map::Entry, BTreeMap, HashSet};

use crate::{
    message::{LockKind, Message, TxId},
    router::{Address, Context},
};

pub struct Lock<S, E> {
    queue: BTreeMap<TxId, QueuedLock>,
    held: HeldLocks<S, E>,
    preemptions: HashSet<TxId>,
}

pub enum LockEvent<S, E> {
    Unhandled(Message),
    Queued {
        txid: TxId,
        kind: LockKind,
    },
    Aborted {
        txid: TxId,
        data: LockData<S, E>,
    },
    Released {
        txid: TxId,
        data: LockData<S, E>,
        predecessors: HashSet<TxId>,
    },
}

pub struct LockData<S, E> {
    pub shared: S,
    pub exclusive: Option<E>,
}

enum HeldLocks<S, E> {
    None,
    Shared(BTreeMap<TxId, S>),
    Exclusive(TxId, S, E),
}

struct QueuedLock {
    kind: LockKind,
    predecessors: HashSet<TxId>,
}

impl<S, E> Lock<S, E>
where
    S: Default,
    E: Default,
{
    pub fn new() -> Lock<S, E> {
        Lock {
            queue: BTreeMap::new(),
            held: HeldLocks::None,
            preemptions: HashSet::new(),
        }
    }

    pub fn handle(
        &mut self,
        message: Message,
        ctx: &Context,
        completed: &HashSet<TxId>,
    ) -> LockEvent<S, E> {
        let event = match message {
            Message::Lock {
                txid,
                kind,
                predecessors,
            } => {
                let Entry::Vacant(e) = self.queue.entry(txid.clone()) else {
                    panic!("lock was double-requested")
                };

                e.insert(QueuedLock {
                    kind,
                    predecessors: predecessors.clone(),
                });

                LockEvent::Queued { txid, kind }
            }
            Message::Abort { txid } => match std::mem::replace(&mut self.held, HeldLocks::None) {
                HeldLocks::None => panic!("abort of unheld lock requested"),
                HeldLocks::Shared(mut held) => {
                    let data = held.remove(&txid);

                    if held.len() != 0 {
                        // restore the remaining held shared locks
                        self.held = HeldLocks::Shared(held);
                    }

                    if let Some(data) = data {
                        LockEvent::Aborted {
                            txid,
                            data: LockData {
                                shared: data,
                                exclusive: None,
                            },
                        }
                    } else {
                        panic!("abort of unheld lock requested")
                    }
                }
                HeldLocks::Exclusive(held_txid, shared_data, exclusive_data) => {
                    if held_txid == txid {
                        LockEvent::Aborted {
                            txid,
                            data: LockData {
                                shared: shared_data,
                                exclusive: Some(exclusive_data),
                            },
                        }
                    } else {
                        // restore the unmatched exclusive lock
                        self.held = HeldLocks::Exclusive(held_txid, shared_data, exclusive_data);

                        panic!("abort of unheld lock requested")
                    }
                }
            },
            Message::Release { txid, predecessors } => {
                match std::mem::replace(&mut self.held, HeldLocks::None) {
                    HeldLocks::None => panic!("release of unheld lock requested"),
                    HeldLocks::Shared(mut held) => {
                        let data = held.remove(&txid);

                        if held.len() != 0 {
                            // restore the remaining held shared locks
                            self.held = HeldLocks::Shared(held);
                        }

                        if let Some(data) = data {
                            LockEvent::Released {
                                txid,
                                data: LockData {
                                    shared: data,
                                    exclusive: None,
                                },
                                predecessors,
                            }
                        } else {
                            panic!("release of unheld lock requested")
                        }
                    }
                    HeldLocks::Exclusive(held_txid, shared_data, exclusive_data) => {
                        if held_txid == txid {
                            LockEvent::Released {
                                txid,
                                data: LockData {
                                    shared: shared_data,
                                    exclusive: Some(exclusive_data),
                                },
                                predecessors,
                            }
                        } else {
                            // restore the unmatched exclusive lock
                            self.held =
                                HeldLocks::Exclusive(held_txid, shared_data, exclusive_data);

                            panic!("release of unheld lock requested")
                        }
                    }
                }
            }
            _ => return LockEvent::Unhandled(message),
        };

        self.process_queue(ctx, completed);

        event
    }

    fn process_queue(&mut self, ctx: &Context, completed: &HashSet<TxId>) {
        for (txid, queued_lock) in self.queue.iter() {
            if !queued_lock.predecessors.is_subset(completed) {
                continue;
            }

            match &mut self.held {
                // if no locks are held, we can grant this queued lock unconditionally
                held @ HeldLocks::None => match queued_lock.kind {
                    LockKind::Shared => {
                        *held = HeldLocks::Shared(BTreeMap::from([(txid.clone(), S::default())]));
                    }
                    LockKind::Exclusive => {
                        *held = HeldLocks::Exclusive(txid.clone(), S::default(), E::default());
                    }
                },

                // if shared locks are held, we can grant only shared locks
                HeldLocks::Shared(held) => match queued_lock.kind {
                    LockKind::Shared => {
                        held.insert(txid.clone(), S::default());
                    }
                    LockKind::Exclusive => {
                        // request preemption of all held shared locks younger than the queued
                        // exclusive lock
                        for shared_txid in held.keys().rev() {
                            if shared_txid < txid {
                                break;
                            }

                            Self::preempt(shared_txid.clone(), &mut self.preemptions, ctx);
                        }

                        break;
                    }
                },

                // if an exclusive lock is held, we can grant no locks
                HeldLocks::Exclusive(held_txid, _, _) => {
                    // request preemption of the exclusive lock if it is younger than the queued lock
                    if txid < held_txid {
                        Self::preempt(held_txid.clone(), &mut self.preemptions, ctx);
                    }

                    break;
                }
            }

            // if control flow reaches here, the lock has now been granted
            // TODO: somehow remove it from the queue here!! this is currently broken
            ctx.send(
                txid.address.clone(),
                Message::LockGranted {
                    txid: txid.clone(),
                    predecessors: completed.clone(),
                },
            );
        }
    }

    // cannot take &mut self, must take ref to preemptions, because we might need to ref other parts
    // of self while calling this function
    fn preempt(txid: TxId, preemptions: &mut HashSet<TxId>, ctx: &Context) {
        if !preemptions.contains(&txid) {
            ctx.send(
                txid.address.clone(),
                Message::Preempt { txid: txid.clone() },
            );
            preemptions.insert(txid);
        }
    }

    pub fn exclusive_lock(&self, txid: &TxId) -> Option<&E> {
        match &self.held {
            HeldLocks::Exclusive(held_txid, _, exclusive_data) => {
                if held_txid == txid {
                    Some(exclusive_data)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn exclusive_lock_mut(&mut self, txid: &TxId) -> Option<&mut E> {
        match &mut self.held {
            HeldLocks::Exclusive(held_txid, _, exclusive_data) => {
                if held_txid == txid {
                    Some(exclusive_data)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn shared_lock(&self, txid: &TxId) -> Option<&S> {
        match &self.held {
            HeldLocks::Shared(held) => held.get(txid),
            HeldLocks::Exclusive(held_txid, shared_data, _) => {
                if held_txid == txid {
                    Some(shared_data)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn shared_lock_mut(&mut self, txid: &TxId) -> Option<&mut S> {
        match &mut self.held {
            HeldLocks::Shared(held) => held.get_mut(txid),
            HeldLocks::Exclusive(held_txid, shared_data, _) => {
                if held_txid == txid {
                    Some(shared_data)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
