use crate::{
    message::Message,
    router::{Actor, Address, Context},
};

pub struct Transactor {
    address: Address,
    state: State,
}

enum State {
    Start,
}

impl Transactor {
    pub fn new(address: Address) -> Transactor {
        Transactor {
            address,
            state: State::Start,
        }
    }
}

impl Actor for Transactor {
    fn init(&mut self, ctx: Context) {}

    fn handle(&mut self, sender: Address, message: Message, ctx: Context) {}
}
