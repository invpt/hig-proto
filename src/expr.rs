use crate::actor::VersionedAddress;

pub mod eval;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Name {
    pub text: String,
}

#[derive(Clone)]
pub enum Upgrade {
    Seq(Box<Upgrade>, Box<Upgrade>),
    Var(Ident, Expr<Ident>),
    Def(Ident, Expr<Ident>),
    Del(VersionedAddress),
    Nil,
    // NOTE: control flow for upgrades is not planned
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub enum Ident {
    New(Name),
    Existing(VersionedAddress),
}

impl From<VersionedAddress> for Ident {
    fn from(value: VersionedAddress) -> Self {
        Ident::Existing(value)
    }
}

#[derive(Clone)]
pub enum Expr<Ident = VersionedAddress> {
    // TODO: more exprs
    Tuple(Box<[Expr<Ident>]>),
    Read(Ident),
    Value(Value),
}

#[derive(Clone)]
pub enum Action<Ident = VersionedAddress> {
    Seq(Box<Action<Ident>>, Box<Action<Ident>>),
    Write(Ident, Expr<Ident>),
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
