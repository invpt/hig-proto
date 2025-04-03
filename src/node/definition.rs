use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
};

use crate::{
    actor::Address,
    expr::{eval::ExprEvalContext, Expr},
    message::{BasisStamp, InputConfiguration, Iteration, StampedValue, TxId},
    value::Value,
};

pub struct Definition {
    inputs: BTreeMap<Address, Input>,
    expr: Expr,
    counter: NonZeroUsize,
}

struct Input {
    roots: HashSet<Address>,
    value: StampedValue,
    updates: Vec<StampedValue>,
}

struct EvalContext<'a>(&'a BTreeMap<Address, Input>);

impl Definition {
    pub fn new(
        expr: Expr,
        inputs: HashMap<Address, InputConfiguration>,
    ) -> (Definition, StampedValue) {
        let definition = Definition {
            inputs: inputs
                .into_iter()
                .map(|(address, cfg)| {
                    (
                        address,
                        Input {
                            roots: cfg.roots,
                            value: cfg.value,
                            updates: Vec::new(),
                        },
                    )
                })
                .collect(),
            expr,
            counter: NonZeroUsize::MIN,
        };

        let value = definition.compute();

        (definition, value)
    }

    pub fn reconfigure(
        &mut self,
        new_expr: Expr,
        new_inputs: HashMap<Address, InputConfiguration>,
    ) -> StampedValue {
        /*self.expr = new_expr;
        self.inputs.extend(new_inputs);

        let mut referenced_inputs = HashSet::new();
        self.expr.may_read(|address| {
            referenced_inputs.insert(address.clone());
        });

        self.inputs
            .retain(|address, _| referenced_inputs.contains(address));

        self.compute()*/
        todo!()
    }

    fn compute(&self) -> StampedValue {
        /*let mut expr = self.expr.clone();
        expr.eval(&mut EvalContext(&self.inputs));
        let Expr::Value(value) = expr else {
            panic!("expr did not fully evaluate")
        };

        StampedValue {
            value,
            predecessors: self
                .inputs
                .values()
                .flat_map(|input| input.value.predecessors.iter())
                .map(|(txid, meta)| (txid.clone(), meta.clone()))
                .collect(),
        }*/
        todo!()
    }

    pub fn ancestor_vars(&self) -> impl Iterator<Item = &Address> {
        self.inputs.values().flat_map(|i| i.roots.iter())
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

        'seeds: for seed in self.inputs.keys() {
            let mut inputs = self
                .inputs
                .iter()
                .map(|(address, input)| {
                    (
                        address,
                        RemainingInput {
                            roots: &input.roots,
                            basis: &input.value.basis,
                            // Don't include any updates if this is an input we've already con-
                            // sidered as a seed. Since it was considered already, we know there
                            // are definitely no valid batches available now that involve this
                            // input.
                            remaining_updates: if *address >= *seed {
                                &*input.updates
                            } else {
                                &[]
                            },
                            update_count: if *address >= *seed {
                                input.updates.len()
                            } else {
                                0
                            },
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();

            let seed_input = inputs.get_mut(seed).unwrap();
            let Some((seed_update, rest)) = seed_input.remaining_updates.split_first() else {
                continue 'seeds;
            };
            seed_input.remaining_updates = rest;
            seed_input.basis = &seed_update.basis;

            let mut basis = seed_update.basis.clone();

            while {
                let mut changed = false;
                for (_, input) in inputs.iter_mut() {
                    while !basis.prec_eq_wrt_roots(&input.basis, &input.roots) {
                        let Some((update, rest)) = input.remaining_updates.split_first() else {
                            // We need an update from this input, but the input does not have an
                            // update to give us. That means there is no batch possible for the
                            // current seed.
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
                .map(
                    |(
                        address,
                        RemainingInput {
                            remaining_updates: updates,
                            update_count,
                            ..
                        },
                    )| { (address.clone(), update_count - updates.len()) },
                )
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
                // The basis we computed earlier only includes updated bases.
                // Since this input was not updated, we can add the basis now.
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

struct RemainingInput<'a> {
    roots: &'a HashSet<Address>,
    basis: &'a BasisStamp,
    remaining_updates: &'a [StampedValue],
    update_count: usize,
}

impl<'a> ExprEvalContext for EvalContext<'a> {
    fn read(&mut self, address: &Address) -> Option<&Value> {
        self.0.get(address).map(|input| &input.value.value)
    }
}
