use std::mem;

use crate::{actor::Address, expr::Value, node::VersionedReactiveAddress};

use super::{Action, Expr, Ident, Upgrade};

pub trait UpgradeEvalContext: ExprEvalContext<Ident> {
    fn var(&mut self, ident: Ident, value: Value);
    fn def(&mut self, ident: Ident, expr: Expr<Ident>);
    fn del(&mut self, address: VersionedReactiveAddress);
}

pub trait ActionEvalContext: ExprEvalContext<VersionedReactiveAddress> {
    /// Attempts to write to the node referenced by `address` with the given `value`.
    ///
    /// Returns true if the write was performed.
    fn write(&mut self, address: &VersionedReactiveAddress, value: &Value) -> bool;
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
            Upgrade::Var(_, expr) => {
                expr.eval(ctx);

                if let Expr::Value(_) = expr {
                    let Upgrade::Var(ident, Expr::Value(value)) = mem::replace(self, Upgrade::Nil)
                    else {
                        unreachable!()
                    };

                    ctx.var(ident, value);
                } else {
                    panic!("var expr could not be fully evaluated")
                }
            }
            Upgrade::Def(..) => {
                let Upgrade::Def(ident, expr) = mem::replace(self, Upgrade::Nil) else {
                    unreachable!()
                };

                ctx.def(ident, expr);
            }
            Upgrade::Del(_) => {
                let Upgrade::Del(address) = mem::replace(self, Upgrade::Nil) else {
                    unreachable!()
                };

                ctx.del(address);
            }
            Upgrade::Nil => {}
        }
    }

    pub fn visit_upgrades(&self, mut visitor: impl FnMut(&VersionedReactiveAddress)) {
        match self {
            Upgrade::Seq(a, b) => {
                a.visit_upgrades(&mut visitor);
                b.visit_upgrades(&mut visitor);
            }
            Upgrade::Var(Ident::Existing(address), _) => visitor(address),
            Upgrade::Def(Ident::Existing(address), _) => visitor(address),
            Upgrade::Del(address) => visitor(address),
            _ => {}
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
            Upgrade::Nil => {}
        }
    }
}

impl Action {
    /// Evaluates this action.
    ///
    /// When `self` is [`Action::Nil`], no further evaulation will be done.
    pub fn eval<C>(&mut self, ctx: &mut C)
    where
        C: ActionEvalContext,
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

    /// Traverses the expression, calling the callback with each VersionedAddress the Action might write to.
    pub fn visit_writes(&self, mut visitor: impl FnMut(&VersionedReactiveAddress, bool)) {
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

    /// Traverses the action, calling the callback with each VersionedAddress the Action might read from.
    pub fn visit_reads(&self, mut visitor: impl FnMut(&VersionedReactiveAddress, bool)) {
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
