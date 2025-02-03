use crate::{
    message::{LockKind, Message, TxId},
    router::{Address, Context},
};

pub struct Lock {}

pub enum LockEvent {
    Aborted(TxId, LockKind),
    Released(TxId, LockKind),
}

impl Lock {
    pub fn new() -> Lock {
        Lock {}
    }

    pub fn handle_lock_messages(
        &mut self,
        sender: &Address,
        message: &Message,
        ctx: &Context,
    ) -> Option<LockEvent> {
        _ = (sender, message, ctx);
        None
    }

    pub fn has_exclusive(&self, who: Address) -> bool {
        _ = who;
        false
    }

    pub fn has_shared(&self, who: Address) -> bool {
        _ = who;
        false
    }
}
