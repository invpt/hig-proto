use std::collections::HashMap;

use crate::actor::Address;

#[derive(Debug, Clone)]
pub enum Value {
    Tuple(Box<[Value]>),
    Integer(isize),
}
