use crate::{
    message::Message,
    router::{Address, Context},
};

pub struct Lock {}

impl Lock {
    pub fn new() -> Lock {
        Lock {}
    }

    pub fn handle_lock_messages(
        &mut self,
        sender: &Address,
        message: &Message,
        ctx: &Context,
    ) -> bool {
        _ = (sender, message, ctx);
        false
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
