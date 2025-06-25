use crate::{
    actor::{Address, Version},
    node::{ReactiveId, VersionedReactiveAddress},
};

pub mod eval;

#[derive(Debug, Clone)]
pub enum TransactionKind {
    Action(Action),
    Upgrade(Upgrade),
}

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
    Del(VersionedReactiveAddress),
}

impl UpgradeItem {
    pub fn expected_reactive(&self) -> Option<(ReactiveId, Version)> {
        match self {
            UpgradeItem::Var(ident, _) => match ident {
                Ident::New(..) => None,
                Ident::Existing(address) => Some((address.id, address.version)),
            },
            UpgradeItem::Def(ident, _) => match ident {
                Ident::New(..) => None,
                Ident::Existing(address) => Some((address.id, address.version)),
            },
            UpgradeItem::Del(address) => Some((address.id, address.version)),
        }
    }

    pub fn address(&self) -> &Address {
        match self {
            UpgradeItem::Var(ident, _) => match ident {
                Ident::New(address, _) => address,
                Ident::Existing(address) => &address.address,
            },
            UpgradeItem::Def(ident, _) => match ident {
                Ident::New(address, _) => address,
                Ident::Existing(address) => &address.address,
            },
            UpgradeItem::Del(address) => &address.address,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Ident {
    New(Address, Name),
    Existing(VersionedReactiveAddress),
}

impl Ident {
    pub fn address(&self) -> &Address {
        match self {
            Ident::New(address, _) => address,
            Ident::Existing(address) => &address.address,
        }
    }
}

impl From<VersionedReactiveAddress> for Ident {
    fn from(value: VersionedReactiveAddress) -> Self {
        Ident::Existing(value)
    }
}

#[derive(Debug, Clone)]
pub enum Expr<Ident = VersionedReactiveAddress> {
    // TODO: more exprs
    Tuple(Box<[Expr<Ident>]>),
    Read(Ident),
    Value(Value),
}

#[derive(Debug, Clone)]
pub enum Action {
    Seq(Box<Action>, Box<Action>),
    Write(VersionedReactiveAddress, Expr<VersionedReactiveAddress>),
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
