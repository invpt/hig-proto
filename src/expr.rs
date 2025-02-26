use crate::actor::Address;

#[derive(Clone)]
pub struct Name {
    pub text: String,
}

#[derive(Clone)]
pub enum Upgrade {
    Seq(Box<[Upgrade]>),
    // NOTE: Var and Def always take names, since there's no way to predetermine the address for new or updated nodes.
    Var(Name, Expr<UpgradeIdent>),
    Def(Name, Expr<UpgradeIdent>),
    // NOTE: on the other hand, since only preexisting nodes can be deleted (and newly-created ones cannot be), Del takes an address.
    Del(Address),
    Do(Action<UpgradeIdent>),
    // NOTE: control flow for upgrades is not planned
}

#[derive(Clone)]
pub enum UpgradeIdent {
    New(Name),
    Existing(Address),
}

#[derive(Clone)]
pub enum Expr<Ident> {
    Read(Ident),
}

#[derive(Clone)]
pub enum Action<Ident = Address> {
    Seq(Box<[Action<Ident>]>),
    Assign(Ident, Expr<Ident>),
    // TODO: control flow
}
