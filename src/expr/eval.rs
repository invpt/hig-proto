use std::mem;

use crate::value::Value;

use super::{Action, Expr};

pub trait ExprEvalContext<Ident> {
    fn read(&mut self, address: &Ident) -> Option<Value>;
}

pub trait ActionEvalContext<Ident>: ExprEvalContext<Ident> {
    fn write(&mut self, ident: &Ident, value: Value);
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
            Action::Write(_, expr) => {
                expr.eval(ctx);

                if let Expr::Value(_) = expr {
                    // take the current value of self, replacing it with Action::Nil to signify completion
                    let Action::Write(address, Expr::Value(value)) =
                        mem::replace(self, Action::Nil)
                    else {
                        unreachable!()
                    };

                    ctx.write(&address, value);
                }
            }
            Action::Nil => (),
        }
    }
}
