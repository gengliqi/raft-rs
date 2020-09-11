// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The raw node async of the raft module.
//!
//! This module contains the value types for the node and it's connection to other
//! nodes but not the raft consensus itself. Generally, you'll interact with the
//! RawNodeAsync first and use it to access the inner workings of the consensus protocol.

use std::ops::{Deref, DerefMut};
use std::collections::VecDeque;

use crate::config::Config;
use crate::eraftpb::{Entry, EntryType, HardState, Message, MessageType, Snapshot};
use crate::errors::{Error, Result};
use crate::raw_node::RawNodeRaft;
use crate::read_only::ReadState;
use crate::{Raft, SoftState, Status, Storage, INVALID_ID};
use slog::Logger;

/// AsyncReady encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed or sent to other peers.
#[derive(Default, Debug, PartialEq)]
pub struct AsyncReady {
    number: u64,

    ss: Option<SoftState>,

    hs: Option<HardState>,

    read_states: Vec<ReadState>,

    entries: Vec<Entry>,

    snapshot: Snapshot,

    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Option<Vec<Entry>>,

    /// Messages specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    pub messages: Vec<Message>,
}

/// AsyncReadyRecord encapsulates the needed data for sync reply
struct AsyncReadyRecord {
    number: u64,
    last_log: Option<(u64, u64)>,
    snapshot: Snapshot,
    messages: Vec<Message>,
}

/// LastSyncResult encapsulates the committed entries and messages that are ready to be applied
/// or be sent to other peers.
#[derive(Default, Debug, PartialEq)]
pub struct LastSyncResult {
    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Option<Vec<Entry>>,

    /// Messages specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    pub messages: Vec<Message>,
}

/// RawNodeAsync is a thread-unsafe Node.
/// The methods of this struct correspond to the methods of Node and are described
/// more fully there.
pub struct RawNodeAsync<T: Storage> {
    core: RawNodeRaft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
    records: VecDeque<AsyncReadyRecord>,
    max_number: u64,
}

impl<T: Storage> Deref for RawNodeAsync<T> {
    type Target = RawNodeRaft<T>;

    fn deref(&self) -> &RawNodeRaft<T> {
        &self.core
    }
}

impl<T: Storage> DerefMut for RawNodeAsync<T> {
    fn deref_mut(&mut self) -> &mut RawNodeRaft<T> {
        &mut self.core
    }
}

impl<T: Storage> RawNodeAsync<T> {
    #[allow(clippy::new_ret_no_self)]
    /// Create a new RawNode given some [`Config`](../struct.Config.html).
    pub fn new(config: &Config, store: T, logger: &Logger) -> Result<Self> {
        let mut rn = RawNodeAsync {
            core: RawNodeRaft::<T>::new(config, store, logger)?,
            prev_hs: Default::default(),
            prev_ss: Default::default(),
            records: VecDeque::new(),
            max_number: 0, 
        };
        rn.prev_hs = rn.raft.hard_state();
        rn.prev_ss = rn.raft.soft_state();
        info!(
            rn.raft.logger,
            "RawNodeAsync created with id {id}.",
            id = rn.raft.id
        );
        Ok(rn)
    }

    /// Create a new RawNode given some [`Config`](../struct.Config.html) and the default logger.
    ///
    /// The default logger is an `slog` to `log` adapter.
    #[cfg(feature = "default-logger")]
    #[allow(clippy::new_ret_no_self)]
    pub fn with_default_logger(c: &Config, store: T) -> Result<Self> {
        Self::new(c, store, &crate::default_logger())
    }
}
