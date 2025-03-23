use std::{collections::HashMap, mem};

use crate::{actor::Address, value::Value};

use super::{Action, Expr, Name, Upgrade, UpgradeIdent};

pub trait UpgradeEvalContext: ActionEvalContext + Resolver<UpgradeIdent> {
    fn var(&mut self, name: Name, replace: Option<Address>, value: Value);
    fn def(&mut self, name: Name, replace: Option<Address>, expr: Expr);
    fn del(&mut self, address: Address);
}

pub trait UpgradeTraversalContext: ActionTraversalContext {
    fn will_var(&mut self, name: Name, replace: Option<Address>);
    fn will_def(&mut self, name: Name, replace: Option<Address>);
    fn will_del(&mut self, address: Address);
}

pub trait ActionEvalContext: ExprEvalContext {
    /// Writes to the node referenced by `address` with the given `value`.
    ///
    /// Returns true if the write was performed.
    fn write(&mut self, address: &Address, value: &Value) -> bool;
}

pub trait ActionTraversalContext: ExprTraversalContext {
    /// Indicates that the node referenced by `address` is guaranteed to be written to by a future
    /// call to `write`.
    fn will_write(&mut self, address: &Address) {
        _ = address;
    }

    /// Indicates that the node referenced by `address` may potentially be written to by a future
    /// call to `write`.
    fn may_write(&mut self, address: &Address) {
        _ = address;
    }
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

pub trait ExprTraversalContext {
    /// Indicates that the node referenced by `ident` is guaranteed to be read with a future call
    /// to `read`.
    ///
    /// An important distinction of this method compared to `read` is that reads indicated by
    /// calling this method may occur following a conflicting read. So, while `read` indicates that
    /// the *currently held* value of an `ident` needs to be read, `will_read` indicates that some
    /// *future* value of an `ident` will need to be read.
    fn will_read(&mut self, address: &Address) {
        _ = address;
    }

    /// Indicates that the node referenced by `ident` may potentially be read with a future call
    /// to `read`.
    fn may_read(&mut self, address: &Address) {
        _ = address;
    }
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

    pub fn traverse(&mut self, ctx: &mut impl UpgradeTraversalContext) {
        todo!()
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

    pub fn traverse<C>(&mut self, ctx: &mut C)
    where
        C: ActionTraversalContext + Resolver<Ident>,
    {
        self.traverse_inner(false, ctx);
    }

    fn traverse_inner<C>(&mut self, conditional: bool, ctx: &mut C)
    where
        C: ActionTraversalContext + Resolver<Ident>,
    {
        match self {
            Action::Seq(a, b) => {
                a.traverse_inner(conditional, ctx);
                b.traverse_inner(conditional, ctx);
            }
            Action::Write(ident, expr) => {
                expr.traverse_inner(conditional, ctx);

                if let Some(address) = ctx.resolve(ident) {
                    if conditional {
                        ctx.may_write(address);
                    } else {
                        ctx.will_write(address);
                    }
                }
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

    pub fn traverse<C>(&mut self, ctx: &mut C)
    where
        C: ExprTraversalContext + Resolver<Ident>,
    {
        self.traverse_inner(false, ctx);
    }

    fn traverse_inner<C>(&mut self, conditional: bool, ctx: &mut C)
    where
        C: ExprTraversalContext + Resolver<Ident>,
    {
        match self {
            Expr::Tuple(items) => {
                for item in items {
                    item.traverse_inner(conditional, ctx);
                }
            }
            Expr::Read(ident) => {
                let Some(address) = ctx.resolve(ident) else {
                    return;
                };

                if conditional {
                    ctx.may_read(address);
                } else {
                    ctx.will_read(address);
                }
            }
            Expr::Value(_) => {}
        }
    }
}

impl<C> Resolver<Address> for C {
    fn resolve<'a>(&mut self, ident: &'a Address) -> Option<&'a Address> {
        Some(ident)
    }
}

impl ExprEvalContext for HashMap<Address, Value> {
    fn read(&mut self, address: &Address) -> Option<&Value> {
        self.get(address)
    }
}
