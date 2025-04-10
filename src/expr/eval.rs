use std::mem;

use crate::{
    actor::{Address, VersionedAddress},
    value::Value,
};

use super::{Action, Expr, Ident, Name, Upgrade};

pub trait UpgradeEvalContext: ActionEvalContext<Ident> {
    fn var(&mut self, name: Name, replace: Option<VersionedAddress>, value: Value) -> bool;
    fn def(&mut self, name: Name, replace: Option<VersionedAddress>, expr: Expr<Ident>) -> bool;
    fn del(&mut self, address: VersionedAddress) -> bool;
}

pub trait ActionEvalContext<Ident = Address>: ExprEvalContext<Ident> {
    /// Attempts to write to the node referenced by `ident` with the given `value`.
    ///
    /// Returns true if the write was performed.
    fn write(&mut self, ident: &Ident, value: &Value) -> bool;
}

pub trait ExprEvalContext<Ident = Address> {
    /// Reads the value held by the node referenced by `ident`.
    ///
    /// If the value is not yet ready, this function will return `None` instead of a value.
    fn read(&mut self, ident: &Ident) -> Option<&Value>;
}

impl Upgrade {
    pub fn eval(&mut self, ctx: &mut impl UpgradeEvalContext) {
        match self {
            Upgrade::Seq(a, b) => {
                a.eval(ctx);
                if let Upgrade::Nil = &**a {
                    b.eval(ctx);

                    *self = mem::replace(b, Upgrade::Nil);
                }
            }
            Upgrade::Var(_, _, expr) => {
                expr.eval(ctx);

                if let Expr::Value(_) = expr {
                    let Upgrade::Var(name, replace, Expr::Value(value)) =
                        mem::replace(self, Upgrade::Nil)
                    else {
                        unreachable!()
                    };

                    if !ctx.var(name, replace, value) {
                        panic!("var was invalid")
                    }
                } else {
                    panic!("var expr could not be fully evaluated")
                }
            }
            Upgrade::Def(..) => {
                let Upgrade::Def(name, replace, expr) = mem::replace(self, Upgrade::Nil) else {
                    unreachable!()
                };

                if !ctx.def(name, replace, expr) {
                    panic!("def was invalid")
                }
            }
            Upgrade::Del(_) => {
                let Upgrade::Del(address) = mem::replace(self, Upgrade::Nil) else {
                    unreachable!()
                };

                if !ctx.del(address) {
                    panic!("del was invalid")
                }
            }
            Upgrade::Do(action) => {
                action.eval(ctx);
                if let Action::Nil = action {
                    *self = Upgrade::Nil
                }
            }
            Upgrade::Nil => {}
        }
    }

    pub fn visit_upgrades(&self, mut visitor: impl FnMut(&VersionedAddress)) {
        match self {
            Upgrade::Seq(a, b) => {
                a.visit_upgrades(&mut visitor);
                b.visit_upgrades(&mut visitor);
            }
            Upgrade::Var(_, Some(address), _) => visitor(address),
            Upgrade::Def(_, Some(address), _) => visitor(address),
            Upgrade::Del(address) => visitor(address),
            _ => {}
        }
    }

    pub fn visit_writes(&self, mut visitor: impl FnMut(&Ident, bool)) {
        match self {
            Upgrade::Seq(a, b) => {
                a.visit_writes(&mut visitor);
                b.visit_writes(&mut visitor);
            }
            Upgrade::Var(..) | Upgrade::Def(..) | Upgrade::Del(..) => {}
            Upgrade::Do(action) => {
                action.visit_writes(visitor);
            }
            Upgrade::Nil => {}
        }
    }

    pub fn visit_reads(&self, mut visitor: impl FnMut(&Ident, bool)) {
        match self {
            Upgrade::Seq(a, b) => {
                a.visit_reads(&mut visitor);
                b.visit_reads(&mut visitor);
            }
            Upgrade::Var(.., expr) => {
                expr.visit_reads(visitor);
            }
            Upgrade::Def(.., expr) => {
                expr.visit_reads(|ident, _definite| visitor(ident, false));
            }
            Upgrade::Del(..) => {}
            Upgrade::Do(action) => {
                action.visit_writes(visitor);
            }
            Upgrade::Nil => {}
        }
    }
}

impl<Ident> Action<Ident> {
    /// Evaluates this action.
    ///
    /// When `self` is [`Action::Nil`], no further evaulation will be done.
    pub fn eval<C>(&mut self, ctx: &mut C)
    where
        C: ActionEvalContext<Ident>,
    {
        match self {
            Action::Seq(a, b) => {
                a.eval(ctx);
                if let Action::Nil = &**a {
                    b.eval(ctx);

                    *self = mem::replace(b, Action::Nil);
                }
            }
            Action::Write(ident, expr) => {
                expr.eval(ctx);

                if let Expr::Value(value) = expr {
                    if ctx.write(ident, value) {
                        *self = Action::Nil;
                    }
                }
            }
            Action::Nil => {}
        }
    }

    /// Traverses the expression, calling the callback with each Ident the Action might write to.
    pub fn visit_writes(&self, mut visitor: impl FnMut(&Ident, bool)) {
        match self {
            Action::Seq(a, b) => {
                a.visit_writes(&mut visitor);
                b.visit_writes(&mut visitor);
            }
            Action::Write(ident, _) => {
                visitor(ident, true);
            }
            Action::Nil => {}
        }
    }

    /// Traverses the expression, calling the callback with each Ident the Action might read from.
    pub fn visit_reads(&self, mut visitor: impl FnMut(&Ident, bool)) {
        match self {
            Action::Seq(a, b) => {
                a.visit_reads(&mut visitor);
                b.visit_reads(&mut visitor);
            }
            Action::Write(_, expr) => {
                expr.visit_reads(visitor);
            }
            Action::Nil => {}
        }
    }
}

impl<Ident> Expr<Ident> {
    /// Evaluates this expression.
    ///
    /// When `self` is an [`Expr::Value`], no further evaulation will be done.
    pub fn eval<C>(&mut self, ctx: &mut C)
    where
        C: ExprEvalContext<Ident>,
    {
        match self {
            Expr::Tuple(items) => {
                let mut all_evaled = true;
                for item in items.iter_mut() {
                    item.eval(ctx);
                    if !matches!(item, Expr::Value(_)) {
                        all_evaled = false;
                    }
                }

                if all_evaled {
                    let items = mem::replace(items, Box::from([]));
                    let mut values = Vec::with_capacity(items.len());
                    for item in items {
                        let Expr::Value(value) = item else {
                            unreachable!()
                        };

                        values.push(value);
                    }

                    *self = Expr::Value(Value::Tuple(values.into_boxed_slice()))
                }
            }
            Expr::Read(ident) => match ctx.read(ident) {
                Some(value) => *self = Expr::Value(value.clone()),
                None => (),
            },
            Expr::Value(_) => (),
        }
    }

    /// Traverses the expression, calling the callback with each Ident the Expr might read from.
    pub fn visit_reads(&self, mut visitor: impl FnMut(&Ident, bool)) {
        match self {
            Expr::Tuple(items) => {
                for item in items {
                    item.visit_reads(&mut visitor);
                }
            }
            Expr::Read(ident) => visitor(ident, true),
            Expr::Value(_) => (),
        }
    }
}
