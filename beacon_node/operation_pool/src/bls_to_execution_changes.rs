use state_processing::SigVerifiedOp;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::sync::Arc;
use types::{
    AbstractExecPayload, BeaconState, ChainSpec, EthSpec, SignedBeaconBlock,
    SignedBlsToExecutionChange,
};

/// Indicates if a `BlsToExecutionChange` was received before or after the
/// Capella fork. This is used to know which messages we should broadcast at the
/// Capella fork epoch.
#[derive(Copy, Clone)]
pub enum ReceivedPreCapella {
    Yes,
    No,
}

/// Pool of BLS to execution changes that maintains a LIFO queue and an index by validator.
///
/// Using the LIFO queue for block production disincentivises spam on P2P at the Capella fork,
/// and is less-relevant after that.
#[derive(Debug, Default)]
pub struct BlsToExecutionChanges<T: EthSpec> {
    /// Map from validator index to BLS to execution change.
    by_validator_index: HashMap<u64, Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>>,
    /// Last-in-first-out (LIFO) queue of verified messages.
    queue: Vec<Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>>,
    /// Contains a set of validator indices which need to have their changes
    /// broadcast at the capella epoch.
    received_pre_capella_indices: HashSet<u64>,
    /// Contains a set of validator indices which are special and take priority
    /// in any block. These are *not* broadcast at the capella epoch.
    special_indices: HashSet<u64>,
    /// Contains queue of the special execution changes
    special_queue: Vec<Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>>,
}

impl<T: EthSpec> BlsToExecutionChanges<T> {
    pub fn existing_change_special_equals(
        &self,
        address_change: &SignedBlsToExecutionChange,
    ) -> Option<bool> {
        if !self
            .special_indices
            .contains(&address_change.message.validator_index)
        {
            return None;
        }
        self.existing_change_equals(address_change)
    }

    pub fn existing_change_equals(
        &self,
        address_change: &SignedBlsToExecutionChange,
    ) -> Option<bool> {
        self.by_validator_index
            .get(&address_change.message.validator_index)
            .map(|existing| existing.as_inner() == address_change)
    }

    // Returns whether or not the special already existed in the pool
    pub fn insert_special(
        &mut self,
        verified_change: SigVerifiedOp<SignedBlsToExecutionChange, T>,
    ) -> bool {
        let validator_index = verified_change.as_inner().message.validator_index;
        let verified_change = Arc::new(verified_change);
        let existed = match self.by_validator_index.entry(validator_index) {
            Entry::Vacant(entry) => {
                entry.insert(verified_change.clone());
                println!(
                    "Insert new SPECIAL bls execution change: {:?}",
                    verified_change.as_inner()
                );
                false
            }
            Entry::Occupied(mut entry) => {
                if entry.get().as_inner() == verified_change.as_inner() {
                    println!("Insert ignored duplicate SPECIAL bls execution change");
                } else {
                    *entry.get_mut() = verified_change.clone();
                    println!("Insert REPLACED CONFLICTING SPECIAL bls execution change\n    old {:?}\n    new {:?}\n", entry.get().as_inner(), verified_change.as_inner());
                }
                self.special_queue.retain(|address_change| {
                    address_change.as_inner().message.validator_index != validator_index
                });
                self.queue.retain(|address_change| {
                    address_change.as_inner().message.validator_index != validator_index
                });
                true
            }
        };

        self.special_queue.push(verified_change);
        self.special_indices.insert(validator_index);

        existed
    }

    pub fn insert(
        &mut self,
        verified_change: SigVerifiedOp<SignedBlsToExecutionChange, T>,
        received_pre_capella: ReceivedPreCapella,
    ) -> bool {
        let validator_index = verified_change.as_inner().message.validator_index;
        // Wrap in an `Arc` once on insert.
        let verified_change = Arc::new(verified_change);
        match self.by_validator_index.entry(validator_index) {
            Entry::Vacant(entry) => {
                self.queue.push(verified_change.clone());
                entry.insert(verified_change);
                if matches!(received_pre_capella, ReceivedPreCapella::Yes) {
                    self.received_pre_capella_indices.insert(validator_index);
                }
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// FIFO ordering, used for persistence to disk.
    pub fn iter_fifo(
        &self,
    ) -> impl Iterator<Item = &Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>> {
        self.queue.iter()
    }

    /// LIFO ordering, used for block packing.
    pub fn iter_lifo(
        &self,
    ) -> impl Iterator<Item = &Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>> {
        let lifo_queue = self.queue.iter().rev();
        let special_queue = self.special_queue.iter();
        special_queue.chain(lifo_queue)
    }

    /// Returns only those which are flagged for broadcasting at the Capella
    /// fork. Uses FIFO ordering, although we expect this list to be shuffled by
    /// the caller.
    pub fn iter_received_pre_capella(
        &self,
    ) -> impl Iterator<Item = &Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>> {
        self.queue.iter().filter(|address_change| {
            self.received_pre_capella_indices
                .contains(&address_change.as_inner().message.validator_index)
        })
    }

    /// Returns the set of indicies which should have their address changes
    /// broadcast at the Capella fork.
    pub fn iter_pre_capella_indices(&self) -> impl Iterator<Item = &u64> {
        self.received_pre_capella_indices.iter()
    }

    /// Prune BLS to execution changes that have been applied to the state more than 1 block ago.
    ///
    /// The block check is necessary to avoid pruning too eagerly and losing the ability to include
    /// address changes during re-orgs. This is isn't *perfect* so some address changes could
    /// still get stuck if there are gnarly re-orgs and the changes can't be widely republished
    /// due to the gossip duplicate rules.
    pub fn prune<Payload: AbstractExecPayload<T>>(
        &mut self,
        head_block: &SignedBeaconBlock<T, Payload>,
        head_state: &BeaconState<T>,
        spec: &ChainSpec,
    ) {
        let mut validator_indices_pruned = vec![];

        let mut already_eth1 =
            |address_change: &Arc<SigVerifiedOp<SignedBlsToExecutionChange, T>>| {
                let validator_index = address_change.as_inner().message.validator_index;
                head_state
                    .validators()
                    .get(validator_index as usize)
                    .map_or(true, |validator| {
                        let prune = validator.has_eth1_withdrawal_credential(spec)
                            && head_block
                                .message()
                                .body()
                                .bls_to_execution_changes()
                                .map_or(true, |recent_changes| {
                                    !recent_changes
                                        .iter()
                                        .any(|c| c.message.validator_index == validator_index)
                                });
                        if prune {
                            validator_indices_pruned.push(validator_index);
                        }
                        !prune
                    })
            };

        self.queue.retain(&mut already_eth1);
        self.special_queue.retain(already_eth1);

        for validator_index in validator_indices_pruned {
            self.by_validator_index.remove(&validator_index);
            self.special_indices.remove(&validator_index);
        }
    }

    /// Removes `broadcasted` validators from the set of validators that should
    /// have their BLS changes broadcast at the Capella fork boundary.
    pub fn register_indices_broadcasted_at_capella(&mut self, broadcasted: &HashSet<u64>) {
        self.received_pre_capella_indices = self
            .received_pre_capella_indices
            .difference(broadcasted)
            .copied()
            .collect();
    }
}
