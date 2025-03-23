use crate::{actor::Address, value::Value};

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
    Var(Name, Option<Address>, Expr<UpgradeIdent>),
    Def(Name, Option<Address>, Expr<UpgradeIdent>),
    // NOTE: on the other hand, since only preexisting nodes can be deleted (and newly-created ones cannot be), Del takes an address.
    Del(Address),
    Do(Action<UpgradeIdent>),
    Nil,
    // NOTE: control flow for upgrades is not planned
}

#[derive(Clone)]
pub enum UpgradeIdent {
    New(Name),
    Existing(Address),
}

#[derive(Clone)]
pub enum Expr<Ident = Address> {
    // TODO: more exprs
    Tuple(Box<[Expr<Ident>]>),
    Read(Ident),
    Value(Value),
}

#[derive(Clone)]
pub enum Action<Ident = Address> {
    Seq(Box<Action<Ident>>, Box<Action<Ident>>),
    Write(Ident, Expr<Ident>),
    Nil,
    // TODO: control flow
}
