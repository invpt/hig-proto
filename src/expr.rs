use crate::node::VersionedReactiveAddress;

pub mod eval;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Name {
    pub text: String,
}

#[derive(Debug, Clone)]
pub enum Upgrade {
    Seq(Box<Upgrade>, Box<Upgrade>),
    Var(Ident, Expr<Ident>),
    Def(Ident, Expr<Ident>),
    Del(VersionedReactiveAddress),
    Nil,
    // NOTE: control flow for upgrades is not planned
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Ident {
    New(Name),
    Existing(VersionedReactiveAddress),
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
