use std::collections::HashMap;

use crate::router::Address;

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
