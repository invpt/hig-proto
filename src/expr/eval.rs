use std::mem;

use crate::value::Value;

use super::{Action, Expr};

pub trait ActionEvalContext<Ident>: ExprEvalContext<Ident> {
    /// Writes to the node referenced by `ident` with the given `value`.
    fn write(&mut self, ident: &Ident, value: Value);
}

pub trait ActionTraversalContext<Ident>: ExprTraversalContext<Ident> {
    /// Indicates that the node referenced by `ident` is guaranteed to be written to by a future
    /// call to `write`.
    fn will_write(&mut self, ident: &Ident) {
        _ = ident;
    }

    /// Indicates that the node referenced by `ident` may potentially be written to by a future
    /// call to `write`.
    fn may_write(&mut self, ident: &Ident) {
        _ = ident;
    }
}

pub trait ExprEvalContext<Ident> {
    /// Reads the value held by the node referenced by `ident`.
    ///
    /// If the value is not yet ready, this function may return `None` instead of a value.
    fn read(&mut self, ident: &Ident) -> Option<Value>;
}

pub trait ExprTraversalContext<Ident> {
    /// Indicates that the node referenced by `ident` is guaranteed to be read with a future call
    /// to `read`.
    ///
    /// An important distinction of this method compared to `read` is that reads indicated by
    /// calling this method may occur following a conflicting read. So, while `read` indicates that
    /// the *currently held* value of an `ident` needs to be read, `will_read` indicates that some
    /// *future* value of an `ident` will need to be read.
    fn will_read(&mut self, ident: &Ident) {
        _ = ident;
    }

    /// Indicates that the node referenced by `ident` may potentially be read with a future call
    /// to `read`.
    fn may_read(&mut self, ident: &Ident) {
        _ = ident;
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
                if let Action::Nil = &**a {
                    b.eval(ctx);

                    *self = mem::replace(b, Action::Nil);
                }
            }
            Action::Write(_, expr) => {
                expr.eval(ctx);

                if let Expr::Value(_) = expr {
                    // take the current value of self, replacing it with Action::Nil to signify completion
                    let Action::Write(ident, Expr::Value(value)) = mem::replace(self, Action::Nil)
                    else {
                        unreachable!()
                    };

                    ctx.write(&ident, value);
                }
            }
            Action::Nil => (),
        }
    }

    pub fn traverse(&mut self, ctx: &mut impl ActionTraversalContext<Ident>) {
        self.traverse_inner(false, ctx);
    }

    fn traverse_inner(&mut self, conditional: bool, ctx: &mut impl ActionTraversalContext<Ident>) {
        match self {
            Action::Seq(a, b) => {
                a.traverse_inner(conditional, ctx);
                b.traverse_inner(conditional, ctx);
            }
            Action::Write(ident, expr) => {
                expr.traverse_inner(conditional, ctx);
                if conditional {
                    ctx.may_write(ident);
                } else {
                    ctx.will_write(ident);
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
    pub fn eval(&mut self, ctx: &mut impl ExprEvalContext<Ident>) {
        match self {
            Expr::Read(ident) => match ctx.read(ident) {
                Some(value) => *self = Expr::Value(value),
                None => (),
            },
            Expr::Value(_) => (),
        }
    }

    pub fn traverse(&mut self, ctx: &mut impl ExprTraversalContext<Ident>) {
        self.traverse_inner(false, ctx);
    }

    fn traverse_inner(&mut self, conditional: bool, ctx: &mut impl ExprTraversalContext<Ident>) {
        match self {
            Expr::Read(ident) => {
                if conditional {
                    ctx.may_read(ident);
                } else {
                    ctx.will_read(ident);
                }
            }
            Expr::Value(_) => {}
        }
    }
}
