use std::collections::{HashMap, HashSet};

use crate::{
    actor::Address,
    expr::{eval::ExprEvalContext, Expr, Value},
    message::{Ancestor, BasisStamp, InputConfiguration, StampedValue},
};

pub struct Definition {
    inputs: HashMap<Address, Input>,
    expr: Expr<Address>,
}

struct Input {
    ancestors: HashMap<Address, Ancestor>,
    value: StampedValue,
    updates: Vec<StampedValue>,
}

struct EvalContext<'a>(&'a HashMap<Address, Input>);

impl Definition {
    pub fn new(
        expr: Expr<Address>,
        inputs: impl IntoIterator<Item = (Address, InputConfiguration)>,
    ) -> (Definition, StampedValue) {
        let definition = Definition {
            inputs: inputs
                .into_iter()
                .map(|(address, cfg)| {
                    (
                        address,
                        Input {
                            ancestors: cfg.ancestors,
                            value: cfg.value,
                            updates: Vec::new(),
                        },
                    )
                })
                .collect(),
            expr,
        };

        let value = definition.compute();

        (definition, value)
    }

    pub fn reconfigure(
        &mut self,
        new_expr: Expr<Address>,
        new_inputs: impl IntoIterator<Item = (Address, InputConfiguration)>,
    ) -> StampedValue {
        self.expr = new_expr;
        self.inputs
            .extend(new_inputs.into_iter().map(|(address, cfg)| {
                (
                    address,
                    Input {
                        ancestors: cfg.ancestors,
                        value: cfg.value,
                        updates: Vec::new(),
                    },
                )
            }));

        let mut referenced_inputs = HashSet::new();
        self.expr.visit_reads(|address, _definite| {
            referenced_inputs.insert(address.clone());
        });

        self.inputs
            .retain(|address, _| referenced_inputs.contains(address));

        self.compute()
    }

    fn compute(&self) -> StampedValue {
        let mut expr = self.expr.clone();
        expr.eval(&mut EvalContext(&self.inputs));
        let Expr::Value(value) = expr else {
            panic!("expr did not fully evaluate")
        };

        StampedValue {
            value,
            basis: self.inputs.values().map(|input| &input.value.basis).fold(
                BasisStamp::empty(),
                |mut a, b| {
                    a.merge_from(&b);
                    a
                },
            ),
        }
    }

    pub fn ancestors(&self) -> impl Iterator<Item = (&Address, &Ancestor)> {
        self.inputs.values().flat_map(|i| i.ancestors.iter())
    }

    pub fn add_update(&mut self, sender: Address, value: StampedValue) {
        self.inputs
            .get_mut(&sender)
            .expect("received update from unknown input")
            .updates
            .push(value);
    }

    pub fn find_and_apply_batch(&mut self) -> Option<StampedValue> {
        let mut found = None;

        let mut explored = HashSet::new();
        'seeds: for seed in self.inputs.keys() {
            let mut inputs = self
                .inputs
                .iter()
                .map(|(address, input)| {
                    (
                        address,
                        BatchInput {
                            ancestors: &input.ancestors,
                            basis: &input.value.basis,
                            // Don't include any updates if this is an input we've already con-
                            // sidered as a seed. Since it was considered already, we know there
                            // are definitely no valid batches available now that involve this
                            // input.
                            remaining_updates: if explored.contains(address) {
                                &*input.updates
                            } else {
                                &[]
                            },
                            update_count: if explored.contains(address) {
                                input.updates.len()
                            } else {
                                0
                            },
                        },
                    )
                })
                .collect::<HashMap<_, _>>();

            let seed_input = inputs.get_mut(seed).unwrap();
            let Some((seed_update, rest)) = seed_input.remaining_updates.split_first() else {
                explored.insert(seed.clone());
                continue 'seeds;
            };
            seed_input.remaining_updates = rest;
            seed_input.basis = &seed_update.basis;

            let mut basis = seed_update.basis.clone();

            while {
                let mut changed = false;
                for (_, input) in inputs.iter_mut() {
                    while !basis.prec_eq_wrt_ancestors(&input.basis, &input.ancestors) {
                        let Some((update, rest)) = input.remaining_updates.split_first() else {
                            // We need an update from this input, but the input does not have an
                            // update to give us. That means there is no batch possible for the
                            // current seed.
                            explored.insert(seed.clone());
                            continue 'seeds;
                        };

                        input.remaining_updates = rest;
                        input.basis = &update.basis;
                        basis.merge_from(&update.basis);

                        changed = true;
                    }
                }
                changed
            } {}

            // Explanation: The number of updates we popped off the queue of each input.
            let update_counts = inputs
                .into_iter()
                .map(|(address, input)| {
                    (
                        address.clone(),
                        input.update_count - input.remaining_updates.len(),
                    )
                })
                .collect::<Vec<_>>();

            found = Some((update_counts, basis));
        }

        let Some((update_counts, mut basis)) = found else {
            return None;
        };

        for (address, update_count) in update_counts {
            let input = self.inputs.get_mut(&address).unwrap();

            debug_assert!(input.updates.len() <= update_count);

            if let Some(value) = input.updates.drain(0..update_count).last() {
                input.value = value;
            } else {
                // The basis we computed earlier only includes basis stamps from updated inputs.
                // But we need to include the basis stamp from every input. Since this one was not
                // updated, it has not been included yet, and so we need to add it.
                basis.merge_from(&input.value.basis);
            }
        }

        let mut expr = self.expr.clone();
        expr.eval(&mut EvalContext(&self.inputs));
        let Expr::Value(value) = expr else {
            panic!("expr did not fully evaluate")
        };

        Some(StampedValue { value, basis })
    }
}

struct BatchInput<'a> {
    ancestors: &'a HashMap<Address, Ancestor>,
    basis: &'a BasisStamp,
    remaining_updates: &'a [StampedValue],
    update_count: usize,
}

impl<'a> ExprEvalContext for EvalContext<'a> {
    fn read(&mut self, address: &Address) -> Option<&Value> {
        self.0.get(address).map(|input| &input.value.value)
    }
}
