use std::collections::HashMap;

use crate::actor::Address;

#[derive(Debug, Clone)]
pub enum Value {
    Definition {
        address: Address,
        inputs: HashMap<Address, Value>,
    },
    Variable {
        address: Address,
        sequence: usize,
    },
}
