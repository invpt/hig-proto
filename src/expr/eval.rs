use std::{collections::HashMap, mem};

use crate::{actor::Address, value::Value};

use super::{Action, Expr, Name, Upgrade, UpgradeIdent};

pub trait UpgradeEvalContext: ActionEvalContext + Resolver<UpgradeIdent> {
    fn var(&mut self, name: Name, replace: Option<Address>, value: Value);
    fn def(&mut self, name: Name, replace: Option<Address>, expr: Expr);
    fn del(&mut self, address: Address);
}

pub trait ActionEvalContext: ExprEvalContext {
    /// Writes to the node referenced by `address` with the given `value`.
    ///
    /// Returns true if the write was performed.
    fn write(&mut self, address: &Address, value: &Value) -> bool;
}

pub trait ExprEvalContext {
    /// Reads the value held by the node referenced by `address`.
    ///
    /// If the value is not yet ready, this function will return `None` instead of a value.
    fn read(&mut self, address: &Address) -> Option<&Value>;
}

pub trait Resolver<Ident> {
    fn resolve<'a>(&mut self, ident: &'a Ident) -> Option<&'a Address>;
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

                    ctx.var(name, replace, value);
                } else {
                    panic!("var expr could not be fully evaluated")
                }
            }
            Upgrade::Def(_, _, expr) => {
                let Some(expr) = (&*expr).resolve(ctx) else {
                    panic!("def expr could not be fully resolved")
                };

                let Upgrade::Def(name, replace, _) = mem::replace(self, Upgrade::Nil) else {
                    unreachable!()
                };

                ctx.def(name, replace, expr);
            }
            Upgrade::Del(_) => {
                let Upgrade::Del(address) = mem::replace(self, Upgrade::Nil) else {
                    unreachable!()
                };

                ctx.del(address);
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
}

impl<Ident> Action<Ident> {
    /// Evaluates this action.
    ///
    /// When `self` is [`Action::Nil`], no further evaulation will be done.
    pub fn eval<C>(&mut self, ctx: &mut C)
    where
        C: ActionEvalContext + Resolver<Ident>,
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
                    if let Some(address) = ctx.resolve(ident) {
                        if ctx.write(address, value) {
                            *self = Action::Nil;
                        }
                    }
                }
            }
            Action::Nil => (),
        }
    }
}

impl<Ident> Expr<Ident> {
    /// Evaluates this expression.
    ///
    /// When `self` is an [`Expr::Value`], no further evaulation will be done.
    pub fn eval<C>(&mut self, ctx: &mut C)
    where
        C: ExprEvalContext + Resolver<Ident>,
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
            Expr::Read(ident) => match ctx.resolve(ident) {
                Some(address) => match ctx.read(address) {
                    Some(value) => *self = Expr::Value(value.clone()),
                    None => (),
                },
                None => (),
            },
            Expr::Value(_) => (),
        }
    }

    fn resolve<C>(&self, ctx: &mut C) -> Option<Expr>
    where
        C: Resolver<Ident>,
    {
        match self {
            Expr::Tuple(items) => {
                let mut resolved = Vec::with_capacity(items.len());
                for item in items {
                    let Some(item) = item.resolve(ctx) else {
                        return None;
                    };
                    resolved.push(item);
                }

                Some(Expr::Tuple(resolved.into_boxed_slice()))
            }
            Expr::Read(ident) => match ctx.resolve(&ident) {
                Some(address) => Some(Expr::Read(address.clone())),
                None => None,
            },
            Expr::Value(value) => Some(Expr::Value(value.clone())),
        }
    }
}

impl<C> Resolver<Address> for C {
    fn resolve<'a>(&mut self, ident: &'a Address) -> Option<&'a Address> {
        Some(ident)
    }
}
