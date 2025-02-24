use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
};

use crate::message::Message;

pub struct Router {
    address_counter: usize,
    queue: VecDeque<QueuedMessage>,
    actors: HashMap<Address, Option<Box<dyn Actor>>>,
}

struct QueuedMessage {
    sender: Address,
    target: Address,
    message: Message,
}

pub struct Context<'a> {
    router: RefCell<&'a mut Router>,
    me: Address,
}

pub trait Actor: Send {
    fn init(&mut self, ctx: Context) {
        _ = ctx;
    }

    fn handle(&mut self, sender: Address, message: Message, ctx: Context);
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address {
    index: usize,
}

impl Router {
    pub fn run(&mut self) {
        while let Some(queued) = self.queue.pop_front() {
            let Some(actor) = self.actors.get_mut(&queued.target) else {
                // Prevent a back-and-forth unreachable message loop from occuring in the scenario
                // where there are two nodes that both get retired while there is a message queued
                // to go from one to the other.
                if !matches!(&queued.message, Message::Unreachable { .. }) {
                    // NOTE push_front to make this be the very next message sent
                    self.queue.push_front(QueuedMessage {
                        sender: queued.target,
                        target: queued.sender,
                        message: Message::Unreachable {
                            message: Box::new(queued.message),
                        },
                    });
                }

                continue;
            };

            let mut actor = actor
                .take()
                .expect("invariant broken: actor was checked out during run step");

            actor.handle(
                queued.sender,
                queued.message,
                Context {
                    router: RefCell::new(self),
                    me: queued.target.clone(),
                },
            );

            if let Some(entry) = self.actors.get_mut(&queued.target) {
                *entry = Some(actor);
            }
        }
    }

    pub fn spawn(&mut self, actor: impl Actor + 'static) -> Address {
        let address = Address {
            index: self.address_counter,
        };
        self.address_counter += 1;
        let mut actor = Box::new(actor);
        self.actors.insert(address.clone(), None);
        actor.init(Context {
            router: RefCell::new(self),
            me: address.clone(),
        });
        if let Some(entry) = self.actors.get_mut(&address) {
            *entry = Some(actor);
        }

        address
    }
}

impl<'a> Context<'a> {
    /// Gets this actor's address.
    pub fn me(&self) -> &Address {
        &self.me
    }

    /// Queues `message` to be sent to and handled by `target`.
    pub fn send(&self, target: Address, message: Message) {
        let message = message.into();
        self.router.borrow_mut().queue.push_back(QueuedMessage {
            sender: self.me.clone(),
            target,
            message,
        });
    }

    /// Spawns a new actor.
    pub fn spawn(&self, actor: impl Actor + 'static) -> Address {
        self.router.borrow_mut().spawn(actor)
    }

    /// Retires this actor, meaning it will no longer be asked to handle messages.
    pub fn retire(self) {
        self.router.borrow_mut().actors.remove(&self.me);
    }
}
