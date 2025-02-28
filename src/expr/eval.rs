use std::mem;

use crate::value::Value;

use super::{Action, Expr};

pub trait ExprEvalContext<Ident> {
    /// Reads the value held by the node referenced by `ident`.
    ///
    /// If the value is not yet ready, this function may return `None` instead of a value.
    fn read(&mut self, ident: &Ident) -> Option<Value>;
}

pub trait ActionEvalContext<Ident>: ExprEvalContext<Ident> {
    /// Writes to the node referenced by `ident` with the given `value`.
    ///
    /// If the value is not yet ready, this function may be called with `None` instead of a value.
    fn write(&mut self, ident: &Ident, value: Option<Value>);
}

impl<Ident> Expr<Ident> {
    /// Evaluates this expression.
    ///
    /// When `self` is an [`Expr::Value`], no further evaulation will be done.
    pub fn eval(&mut self, ctx: &mut impl ExprEvalContext<Ident>) {
        match self {
            Expr::Read(address) => match ctx.read(address) {
                Some(value) => *self = Expr::Value(value),
                None => (),
            },
            Expr::Value(_) => (),
        }
    }
}

impl<Ident> Action<Ident> {
    /// Evaluates this action.
    ///
    /// When `self` is [`Action::Nil`], no further evaulation will be done.
    pub fn eval(&mut self, ctx: &mut impl ActionEvalContext<Ident>) {
        match self {
            Action::Seq(a, b) => {
                a.eval(ctx);
                b.eval(ctx);

                match (&mut **a, &mut **b) {
                    (Action::Nil, b) => *self = mem::replace(b, Action::Nil),
                    (a, Action::Nil) => *self = mem::replace(a, Action::Nil),
                    (_, _) => (),
                }
            }
            Action::Write(address, expr) => {
                expr.eval(ctx);

                if let Expr::Value(_) = expr {
                    // take the current value of self, replacing it with Action::Nil to signify completion
                    let Action::Write(address, Expr::Value(value)) =
                        mem::replace(self, Action::Nil)
                    else {
                        unreachable!()
                    };

                    ctx.write(&address, Some(value));
                } else {
                    ctx.write(address, None);
                }
            }
            Action::Nil => (),
        }
    }
}
