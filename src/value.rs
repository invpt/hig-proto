use std::collections::HashMap;

use crate::router::Address;

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
