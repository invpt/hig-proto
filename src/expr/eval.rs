use std::mem;

use crate::{
    expr::{UpgradeStmt, Value},
    service::ReactiveAddress,
};

use super::{Action, ActionStmt, Expr, Ident, Upgrade};

pub trait UpgradeEvalContext: ExprEvalContext<Ident> + ExprResolveContext<Ident> {
    fn var(&mut self, ident: &Ident, value: &Value) -> bool;
    fn def(&mut self, ident: &Ident, expr: &Expr) -> bool;
    fn del(&mut self, address: &ReactiveAddress) -> bool;
}

pub trait ActionEvalContext: ExprEvalContext {
    /// Attempts to write to the node referenced by `address` with the given `value`.
    ///
    /// Returns true if the write was performed.
    fn write(&mut self, address: &ReactiveAddress, value: &Value) -> bool;
}

pub trait ExprEvalContext<Ident = ReactiveAddress> {
    /// Reads the value held by the node referenced by `ident`.
    ///
    /// If the value is not yet ready, this function will return `None` instead of a value.
    fn read(&mut self, address: &Ident) -> Option<&Value>;
}

pub trait ExprResolveContext<Ident> {
    fn resolve(&mut self, ident: Ident) -> Option<ReactiveAddress>;
}

impl Upgrade {
    pub fn eval(&mut self, ctx: &mut impl UpgradeEvalContext) {
        match self {
            Upgrade::Stmt(stmt, rest) => {
                let done = match stmt {
                    UpgradeStmt::Var(ident, expr) => {
                        expr.eval(ctx);

                        if let Expr::Value(value) = expr {
                            ctx.var(ident, value)
                        } else {
                            panic!("var expr could not be fully evaluated")
                        }
                    }
                    UpgradeStmt::Def(ident, expr) => {
                        // TODO: something less expensive than cloning here :(
                        let resolved = expr.clone().resolve(ctx);

                        ctx.def(&ident, &resolved)
                    }
                    UpgradeStmt::Del(address) => ctx.del(&address),
                };

                if done {
                    *self = mem::take(&mut **rest);
                }

                self.eval(ctx);
            }
            Upgrade::Nil => {}
        }
    }

    pub fn visit_stmts(&self, visitor: &mut impl FnMut(&UpgradeStmt)) {
        match self {
            Upgrade::Stmt(stmt, upgrade) => {
                visitor(stmt);
                upgrade.visit_stmts(visitor);
            }
            Upgrade::Nil => {}
        }
    }

    pub fn visit_reads(&self, visitor: &mut impl FnMut(&Ident, bool)) {
        match self {
            Upgrade::Stmt(stmt, upgrade) => {
                match stmt {
                    UpgradeStmt::Var(.., expr) => {
                        expr.visit_reads(visitor);
                    }
                    UpgradeStmt::Def(.., expr) => {
                        expr.visit_reads(&mut |ident, _definite| visitor(ident, false));
                    }
                    UpgradeStmt::Del(..) => {}
                }

                upgrade.visit_reads(visitor);
            }
            Upgrade::Nil => (),
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
            Action::Stmt(stmt, _) => {
                match stmt {
                    ActionStmt::Write(ident, expr) => {
                        expr.eval(ctx);

                        if let Expr::Value(value) = expr {
                            if ctx.write(ident, value) {
                                let Action::Stmt(_, rest) = mem::take(self) else {
                                    unreachable!()
                                };

                                *self = *rest;
                            }
                        }
                    }
                }

                self.eval(ctx);
            }
            Action::Nil => {}
        }
    }

    /// Traverses the expression, calling the callback with each VersionedAddress the Action might write to.
    pub fn visit_writes(&self, visitor: &mut impl FnMut(&ReactiveAddress, bool)) {
        match self {
            Action::Stmt(stmt, rest) => {
                match stmt {
                    ActionStmt::Write(ident, _) => {
                        visitor(ident, true);
                    }
                }

                rest.visit_writes(visitor);
            }
            Action::Nil => {}
        }
    }

    /// Traverses the action, calling the callback with each VersionedAddress the Action might read from.
    pub fn visit_reads(&self, visitor: &mut impl FnMut(&ReactiveAddress, bool)) {
        match self {
            Action::Stmt(stmt, rest) => {
                match stmt {
                    ActionStmt::Write(_, expr) => {
                        expr.visit_reads(visitor);
                    }
                }

                rest.visit_reads(visitor);
            }
            Action::Nil => {}
        }
    }
}

impl<Ident> Expr<Ident> {
    pub fn resolve<C>(self, ctx: &mut C) -> Expr
    where
        C: ExprResolveContext<Ident>,
    {
        match self {
            Expr::Read(ident) => Expr::Read(ctx.resolve(ident).expect("failed to resolve")),
            Expr::Tuple(items) => Expr::Tuple(
                items
                    .into_vec()
                    .into_iter()
                    .map(|e| e.resolve(ctx))
                    .collect(),
            ),
            Expr::Value(value) => Expr::Value(value),
        }
    }

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
    pub fn visit_reads(&self, visitor: &mut impl FnMut(&Ident, bool)) {
        match self {
            Expr::Tuple(items) => {
                for item in items {
                    item.visit_reads(visitor);
                }
            }
            Expr::Read(ident) => visitor(ident, true),
            Expr::Value(_) => (),
        }
    }
}
