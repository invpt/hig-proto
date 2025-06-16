use std::collections::{HashMap, HashSet};

use crate::{
    actor::{Actor, ActorConfiguration, Address, Context, System},
    expr::{Expr, Ident, Value},
    message::{
        BasisStamp, ImportConfiguration, LockKind, Message, MonotonicTimestampGenerator,
        ReactiveConfiguration, StampedValue, Timestamp, TxId, TxPriority,
    },
    node::{Node, ReactiveAddress, ReactiveId},
};

mod actor;
mod expr;
mod manager;
mod message;
mod node;

fn main() {
    let mut system = System::new();

    system.spawn(ScenarioConfiguration);

    system.run();
}

struct ScenarioConfiguration;

struct Scenario {
    gen: MonotonicTimestampGenerator,
    node1: Address,
    node2: Address,
    txid: TxId,
    node1_prepared: bool,
    node2_prepared: bool,
    basis: BasisStamp,
}

struct Stage2 {
    txid: TxId,
    node1: Address,
    node2: Address,
}

impl ActorConfiguration for ScenarioConfiguration {
    type Actor = Scenario;

    fn spawn(self, ctx: Context) -> Scenario {
        let mut gen = MonotonicTimestampGenerator::new();
        let node1 = ctx.spawn(Node::new());
        let node2 = ctx.spawn(Node::new());

        let timestamp = gen.generate_timestamp();
        let txid = TxId {
            priority: TxPriority::High,
            timestamp,
            address: ctx.me().clone(),
        };
        ctx.send(
            &node1,
            Message::Lock {
                txid: txid.clone(),
                kind: LockKind::Exclusive,
            },
        );
        ctx.send(
            &node2,
            Message::Lock {
                txid: txid.clone(),
                kind: LockKind::Exclusive,
            },
        );

        dbg!(&node1, &node2);

        Scenario {
            gen,
            node1,
            node2,
            txid,
            node1_prepared: false,
            node2_prepared: false,
            basis: BasisStamp::empty(),
        }
    }
}

impl Actor for Scenario {
    fn handle(&mut self, message: Message, ctx: actor::Context) {
        match message {
            Message::LockGranted { txid, address } => {
                assert_eq!(&txid, &self.txid);
                if &address == &self.node1 {
                    assert_eq!(&address, &self.node1);
                    ctx.send(
                        &address,
                        Message::Configure {
                            txid: self.txid.clone(),
                            imports: HashMap::new(),
                            reactives: HashMap::from([
                                (
                                    ReactiveId(0),
                                    Some(ReactiveConfiguration::Variable {
                                        value: StampedValue {
                                            value: Value::Integer(0),
                                            basis: BasisStamp::empty(),
                                        },
                                    }),
                                ),
                                (
                                    ReactiveId(1),
                                    Some(ReactiveConfiguration::Definition {
                                        expr: Expr::Read(ReactiveAddress {
                                            address: self.node1.clone(),
                                            id: ReactiveId(0),
                                        }),
                                    }),
                                ),
                            ]),
                            exports: HashMap::from([(
                                ReactiveId(1),
                                HashSet::from([self.node2.clone()]),
                            )]),
                        },
                    );
                } else {
                    ctx.send(
                        &address,
                        Message::Configure {
                            txid: self.txid.clone(),
                            imports: HashMap::from([(
                                ReactiveAddress {
                                    address: self.node1.clone(),
                                    id: ReactiveId(1),
                                },
                                Some(ImportConfiguration {
                                    roots: HashSet::from([ReactiveAddress {
                                        address: self.node1.clone(),
                                        id: ReactiveId(1),
                                    }]),
                                }),
                            )]),
                            reactives: HashMap::from([(
                                ReactiveId(0),
                                Some(ReactiveConfiguration::Definition {
                                    expr: Expr::Read(ReactiveAddress {
                                        address: self.node1.clone(),
                                        id: ReactiveId(1),
                                    }),
                                }),
                            )]),
                            exports: HashMap::new(),
                        },
                    );
                }
                ctx.send(
                    &address,
                    Message::PrepareCommit {
                        txid: self.txid.clone(),
                    },
                );
            }
            Message::CommitPrepared {
                address,
                txid,
                basis,
            } => {
                assert_eq!(txid, self.txid);

                self.basis.merge_from(&basis);

                if &address == &self.node1 {
                    assert!(!self.node1_prepared);
                    self.node1_prepared = true;
                } else if &address == &self.node2 {
                    assert!(!self.node2_prepared);
                    self.node2_prepared = true;
                } else {
                    unreachable!();
                }

                if self.node1_prepared && self.node2_prepared {
                    ctx.send(
                        &self.node1,
                        Message::Commit {
                            txid: self.txid.clone(),
                            basis: self.basis.clone(),
                        },
                    );
                    ctx.send(
                        &self.node2,
                        Message::Commit {
                            txid: self.txid.clone(),
                            basis: self.basis.clone(),
                        },
                    );

                    let t2 = TxId {
                        priority: TxPriority::Low,
                        timestamp: self.gen.generate_timestamp(),
                        address: ctx.me().clone(),
                    };
                    ctx.send(
                        &self.node1,
                        Message::Lock {
                            txid: t2.clone(),
                            kind: LockKind::Exclusive,
                        },
                    );
                    ctx.shift(Stage2 {
                        txid: t2,
                        node1: self.node1.clone(),
                        node2: self.node2.clone(),
                    });
                }
            }
            _ => todo!("unexpected message for test scenario: {:?}", message),
        }
    }
}

impl Actor for Stage2 {
    fn handle(&mut self, message: Message, ctx: Context) {
        match message {
            Message::LockGranted { txid, address } => {
                assert_eq!(address, self.node1);
                assert_eq!(txid, self.txid);
                ctx.send(
                    &address,
                    Message::Write {
                        txid: self.txid.clone(),
                        reactive: ReactiveId(0),
                        value: Value::Integer(2),
                    },
                );
                ctx.send(
                    &address,
                    Message::PrepareCommit {
                        txid: self.txid.clone(),
                    },
                );
            }
            Message::CommitPrepared {
                address,
                txid,
                basis,
            } => {
                assert_eq!(address, self.node1);
                assert_eq!(txid, self.txid);
                ctx.send(
                    &address,
                    Message::Commit {
                        txid: self.txid.clone(),
                        basis,
                    },
                );
            }
            _ => todo!("unexpected message for stage 2: {:?}", message),
        }
    }
}
