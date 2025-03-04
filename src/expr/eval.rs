use std::mem;

use crate::value::Value;

use super::{Action, Expr};

pub trait ExprEvalContext<Ident> {
    /// Reads the value held by the node referenced by `ident`.
    ///
    /// If the value is not yet ready, this function may return `None` instead of a value.
    fn read(&mut self, ident: &Ident) -> Option<Value>;

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

pub trait ActionEvalContext<Ident>: ExprEvalContext<Ident> {
    /// Writes to the node referenced by `ident` with the given `value`.
    fn write(&mut self, ident: &Ident, value: Value);

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

impl<Ident> Expr<Ident> {
    /// Evaluates this expression.
    ///
    /// When `self` is an [`Expr::Value`], no further evaulation will be done.
    pub fn eval(&mut self, ctx: &mut impl ExprEvalContext<Ident>) {
        self.eval_inner(EvalTense::Present, ctx);
    }

    fn eval_inner(&mut self, tense: EvalTense, ctx: &mut impl ExprEvalContext<Ident>) {
        match self {
            Expr::Read(address) => match tense {
                EvalTense::Present => match ctx.read(address) {
                    Some(value) => *self = Expr::Value(value),
                    None => (),
                },
                EvalTense::Future => ctx.will_read(address),
                EvalTense::Conditional => ctx.may_read(address),
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
        self.eval_inner(EvalTense::Present, ctx);
    }

    fn eval_inner(&mut self, tense: EvalTense, ctx: &mut impl ActionEvalContext<Ident>) {
        match self {
            Action::Seq(a, b) => {
                a.eval_inner(tense, ctx);
                b.eval_inner(tense.weaken(EvalTense::Future), ctx);

                match (&mut **a, &mut **b) {
                    (Action::Nil, b) => *self = mem::replace(b, Action::Nil),
                    (a, Action::Nil) => *self = mem::replace(a, Action::Nil),
                    (_, _) => (),
                }
            }
            Action::Write(address, expr) => {
                expr.eval(ctx);

                match tense {
                    EvalTense::Present => {
                        if let Expr::Value(_) = expr {
                            // take the current value of self, replacing it with Action::Nil to signify completion
                            let Action::Write(address, Expr::Value(value)) =
                                mem::replace(self, Action::Nil)
                            else {
                                unreachable!()
                            };

                            ctx.write(&address, value);
                        } else {
                            ctx.will_write(address);
                        }
                    }
                    EvalTense::Future => ctx.will_write(address),
                    EvalTense::Conditional => ctx.may_write(address),
                }
            }
            Action::Nil => (),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum EvalTense {
    Present,
    Future,
    Conditional,
}

impl EvalTense {
    pub fn weaken(self, other: EvalTense) -> EvalTense {
        match (self, other) {
            (EvalTense::Present, _)
            | (EvalTense::Future, EvalTense::Future)
            | (EvalTense::Future, EvalTense::Conditional)
            | (EvalTense::Conditional, EvalTense::Conditional) => other,
            _ => self,
        }
    }
}
