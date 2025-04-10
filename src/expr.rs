use crate::{
    actor::{Address, VersionedAddress},
    value::Value,
};

pub mod eval;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Name {
    pub text: String,
}

#[derive(Clone)]
pub enum Upgrade {
    Seq(Box<Upgrade>, Box<Upgrade>),
    // NOTE: Var and Def always take names, since there's no way to predetermine the address for new or updated nodes.
    // They also take an optional address, which is used to update an existing node.
    Var(Name, Option<VersionedAddress>, Expr<Ident>),
    Def(Name, Option<VersionedAddress>, Expr<Ident>),
    // NOTE: on the other hand, since only preexisting nodes can be deleted (and newly-created ones cannot be), Del takes an address.
    Del(VersionedAddress),
    Do(Action<Ident>),
    Nil,
    // NOTE: control flow for upgrades is not planned
}

#[derive(Clone)]
pub enum Ident {
    New(Name),
    Existing(VersionedAddress),
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
