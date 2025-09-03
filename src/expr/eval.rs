use std::mem;

use crate::{
    expr::{UpgradeItem, Value},
    service::ReactiveAddress,
};

use super::{Action, Expr, Ident, Upgrade};

pub trait UpgradeEvalContext: ExprEvalContext<Ident> + ExprResolveContext<Ident> {
    fn var(&mut self, ident: Ident, value: Value);
    fn def(&mut self, ident: Ident, expr: Expr);
    fn del(&mut self, address: ReactiveAddress);
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
        while !self.items.is_empty() {
            match self.items.first_mut().unwrap() {
                UpgradeItem::Var(_, expr) => {
                    expr.eval(ctx);

                    if let Expr::Value(_) = expr {
                        let UpgradeItem::Var(ident, Expr::Value(value)) = self.items.remove(0)
                        else {
                            unreachable!()
                        };

                        ctx.var(ident, value);
                    } else {
                        panic!("var expr could not be fully evaluated")
                    }
                }
                UpgradeItem::Def(..) => {
                    let UpgradeItem::Def(ident, expr) = self.items.remove(0) else {
                        unreachable!()
                    };

                    let resolved = expr.resolve(ctx);
                    ctx.def(ident, resolved);
                }
                UpgradeItem::Del(_) => {
                    let UpgradeItem::Del(address) = self.items.remove(0) else {
                        unreachable!()
                    };

                    ctx.del(address);
                }
            }
        }
    }

    pub fn visit_reads(&self, visitor: &mut impl FnMut(&Ident, bool)) {
        for item in &self.items {
            match item {
                UpgradeItem::Var(.., expr) => {
                    expr.visit_reads(visitor);
                }
                UpgradeItem::Def(.., expr) => {
                    expr.visit_reads(&mut |ident, _definite| visitor(ident, false));
                }
                UpgradeItem::Del(..) => {}
                _ => (),
            }
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
    pub fn visit_writes(&self, visitor: &mut impl FnMut(&ReactiveAddress, bool)) {
        match self {
            Action::Seq(a, b) => {
                a.visit_writes(visitor);
                b.visit_writes(visitor);
            }
            Action::Write(ident, _) => {
                visitor(ident, true);
            }
            Action::Nil => {}
        }
    }

    /// Traverses the action, calling the callback with each VersionedAddress the Action might read from.
    pub fn visit_reads(&self, visitor: &mut impl FnMut(&ReactiveAddress, bool)) {
        match self {
            Action::Seq(a, b) => {
                a.visit_reads(visitor);
                b.visit_reads(visitor);
            }
            Action::Write(_, expr) => {
                expr.visit_reads(visitor);
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
