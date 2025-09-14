use std::collections::{HashMap, HashSet};

use crate::{
    actor::Address,
    manager::transaction::Configuration,
    service::{ReactiveAddress, ReactiveId, ReactiveInputs},
};

#[derive(PartialEq, Eq, Hash)]
pub struct CrossServiceLink {
    pub src: ReactiveAddress,
    pub dst: ReactiveAddress,
}

pub fn cross_service_links<'a>(
    services: impl Iterator<Item = (&'a Address, &'a HashMap<ReactiveId, ReactiveInputs>)>,
) -> HashSet<CrossServiceLink> {
    let mut links = HashSet::new();
    for (service, reactives) in services {
        for (id, inputs) in reactives {
            for input in &inputs.inputs {
                if &input.service != service {
                    links.insert(CrossServiceLink {
                        src: input.clone(),
                        dst: ReactiveAddress {
                            service: service.clone(),
                            id: *id,
                        },
                    });
                }
            }
        }
    }
    links
}

pub fn prune_unused_imports_exports(services: &mut HashMap<Address, Configuration>) {
    // case to remove an import/export:
    // remove imports first, then remove exports that have no more importers
    // when to remove an import:
    // - we see the import
    // - we see no importer of it exists anymorew
    // - so we remove it
    // simple

    // removing exporters is difficult because of borrowing reasons
    //
    // extra heap allocations easiest way around it for now
    let mut export_removal_candidates = HashMap::<ReactiveAddress, HashSet<Address>>::new();

    for (service_address, service) in services.iter_mut() {
        service.imports.retain(|address, import| {
            import.importers.retain(|id| match service.inputs.get(id) {
                Some(inputs) => inputs.inputs.contains(address),
                None => false,
            });

            if import.importers.is_empty() {
                export_removal_candidates
                    .entry(address.clone())
                    .or_insert_with(HashSet::new)
                    .insert(service_address.clone());
                false
            } else {
                true
            }
        });
    }

    for (service_address, service) in services.iter_mut() {
        service.exports.retain(|id, export| {
            if let Some(removed) = export_removal_candidates.get(&ReactiveAddress {
                service: service_address.clone(),
                id: *id,
            }) {
                export
                    .importers
                    .retain(|address| !removed.contains(address));
            }

            !export.importers.is_empty()
        });
    }
}

pub fn compute_roots(services: &mut HashMap<Address, Configuration>) {
    // NOTE: work on integration with meerkatREPL2.0 first
}

fn roots_dfs(
    address: ReactiveAddress,
    services: &mut HashMap<Address, Configuration>,
    roots: &mut HashMap<ReactiveAddress, HashSet<ReactiveAddress>>,
    visiting: &mut HashSet<ReactiveAddress>,
) {
}

// A { def x = B.y }
// B { var y = 0 }
// C { def z = B.y }
// D { def w = A.x + C.z }
//
// upgrade {
//   C { def z = 0 }
//   D <- update the roots of D
// }
//
