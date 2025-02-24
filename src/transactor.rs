use crate::{
    message::Message,
    router::{Actor, Address, Context},
    value::Value,
};

pub struct Transactor<T> {
    address: Address,
    transaction: T,
}

pub trait Transaction: Sized + Send {
    fn request(&self) -> Request<Self>;
}

pub enum Request<T> {
    Read {
        address: Address,
        fulfill: fn(&mut T, Value),
    },
    Write {
        address: Address,
        value: Value,
        fulfill: fn(&mut T),
    },
}

impl<T: Default> Transactor<T> {
    pub fn new(address: Address) -> Transactor<T> {
        Transactor {
            address,
            transaction: Default::default(),
        }
    }
}

impl<T: Transaction> Actor for Transactor<T> {
    fn init(&mut self, ctx: Context) {
        match self.transaction.request() {
            Request::Read { address, fulfill } => todo!(),
            Request::Write {
                address,
                value,
                fulfill,
            } => todo!(),
        }
    }

    fn handle(&mut self, sender: Address, message: Message, ctx: Context) {}
}
