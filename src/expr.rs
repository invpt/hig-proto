use crate::{
    actor::{Address, Version},
    service::{ReactiveAddress, ReactiveId},
};

pub mod eval;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Name {
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct Upgrade {
    pub items: Vec<UpgradeItem>,
}

#[derive(Debug, Clone)]
pub enum UpgradeItem {
    Var(Ident, Expr<Ident>),
    Def(Ident, Expr<Ident>),
    Del(ReactiveAddress),
}

impl UpgradeItem {
    pub fn address(&self) -> &Address {
        match self {
            UpgradeItem::Var(ident, _) => match ident {
                Ident::New(address, _) => address,
                Ident::Existing(address) => &address.service,
            },
            UpgradeItem::Def(ident, _) => match ident {
                Ident::New(address, _) => address,
                Ident::Existing(address) => &address.service,
            },
            UpgradeItem::Del(address) => &address.service,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Ident {
    New(Address, Name),
    Existing(ReactiveAddress),
}

impl Ident {
    pub fn address(&self) -> &Address {
        match self {
            Ident::New(address, _) => address,
            Ident::Existing(address) => &address.service,
        }
    }
}

impl From<ReactiveAddress> for Ident {
    fn from(value: ReactiveAddress) -> Self {
        Ident::Existing(value)
    }
}

#[derive(Debug, Clone)]
pub enum Expr<Ident = ReactiveAddress> {
    // TODO: more exprs
    Tuple(Box<[Expr<Ident>]>),
    Read(Ident),
    Value(Value),
}

#[derive(Debug, Clone)]
pub enum Action {
    Seq(Box<Action>, Box<Action>),
    Write(ReactiveAddress, Expr<ReactiveAddress>),
    Nil,
    // TODO: control flow
}

#[derive(Debug, Clone)]
pub enum Value {
    Tuple(Box<[Value]>),
    Integer(isize),
}

impl Value {
    pub fn compute_type(&self) -> Type {
        match self {
            Value::Tuple(items) => Type::Tuple(
                items
                    .iter()
                    .map(|item| item.compute_type())
                    .collect::<Box<[_]>>(),
            ),
            Value::Integer(_) => Type::Integer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Tuple(Box<[Type]>),
    Integer,
}
