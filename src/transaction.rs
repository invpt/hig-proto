use crate::{router::Address, value::Value};

pub struct Transaction {}

pub enum Request {
    Read { address: Address },
    Write { address: Address, value: Value },
}

impl Transaction {
    pub fn request(&self) -> Request {
        todo!()
    }

    pub fn fulfill_read(&mut self, address: Address, value: Value) {
        todo!()
    }

    pub fn fulfill_write(&mut self, address: Address) {
        todo!()
    }
}
