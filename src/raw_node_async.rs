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

use std::collections::VecDeque;
use std::{
    mem,
    ops::{Deref, DerefMut},
};

use crate::config::Config;
use crate::eraftpb::{Entry, EntryType, HardState, Message, MessageType, Snapshot};
use crate::errors::{Error, Result};
use crate::raw_node::RawNodeRaft;
use crate::read_only::ReadState;
use crate::{Raft, SoftState, StateRole, Status, Storage, INVALID_ID};
use slog::Logger;

/// ReadyAsync encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed or sent to other peers.
#[derive(Default, Debug, PartialEq)]
pub struct ReadyAsync {
    number: u64,

    ss: Option<SoftState>,

    hs: Option<HardState>,

    // TODO: change it to public
    read_states: Vec<ReadState>,

    /// Entries specifies entries to be saved to stable storage
    pub entries: Vec<Entry>,

    /// Snapshot specifies the snapshot to be applied to application
    pub snapshot: Snapshot,

    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Option<Vec<Entry>>,

    /// Messages specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    pub messages: Vec<Vec<Message>>,
}

impl ReadyAsync {
    /// The number of current ReadyAsync.
    /// It is used for .
    #[inline]
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The current volatile state of a Node.
    /// SoftState will be nil if there is no update.
    /// It is not required to consume or store SoftState.
    #[inline]
    pub fn ss(&self) -> Option<&SoftState> {
        self.ss.as_ref()
    }

    /// The current state of a Node to be saved to stable storage BEFORE
    /// Messages are sent.
    /// HardState will be equal to empty state if there is no update.
    #[inline]
    pub fn hs(&self) -> Option<&HardState> {
        self.hs.as_ref()
    }

    /// States can be used for node to serve linearizable read requests locally
    /// when its applied index is greater than the index in ReadState.
    /// Note that the read_state will be returned when raft receives MsgReadIndex.
    /// The returned is only valid for the request that requested to read.
    #[inline]
    pub fn read_states(&self) -> &[ReadState] {
        &self.read_states
    }
}

/// ReadyAsyncRecord encapsulates the needed data for sync reply
#[derive(Default, Debug, PartialEq)]
struct ReadyAsyncRecord {
    number: u64,
    // (index, term) of the last entry from the entries in ReadyAsync
    last_log: Option<(u64, u64)>,
    snapshot: Snapshot,
    messages: Vec<Message>,
}

/// SyncLastResult encapsulates the committed entries and messages that are ready to
/// be applied or be sent to other peers.
#[derive(Default, Debug, PartialEq)]
pub struct SyncLastResult {
    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Option<Vec<Entry>>,

    /// Messages specifies outbound messages to be sent
    pub messages: Vec<Vec<Message>>,
}

/// RawNodeAsync is a thread-unsafe Node.
/// The methods of this struct correspond to the methods of Node and are described
/// more fully there.
pub struct RawNodeAsync<T: Storage> {
    core: RawNodeRaft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
    records: VecDeque<ReadyAsyncRecord>,
    max_number: u64,
    // Index of last synced log
    last_synced_index: u64,
    // Messages that need to be sent to other peers
    messages: Vec<Vec<Message>>,
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
            last_synced_index: 0,
            messages: Vec::new(),
        };
        rn.last_synced_index = rn.raft.raft_log.last_index();
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

    /// Given an index, creates a new Ready value from that index.
    pub fn ready_since(&mut self, applied_idx: u64) -> ReadyAsync {
        let raft = &mut self.core.raft;

        self.max_number += 1;
        let number = self.max_number;
        let mut rd = ReadyAsync {
            number,
            ..Default::default()
        };
        let mut rd_record = ReadyAsyncRecord {
            number,
            ..Default::default()
        };

        if self.prev_ss.raft_state != StateRole::Leader && raft.state == StateRole::Leader {
            // TODO: Add more annotations
            assert_eq!(self.prev_ss.raft_state, StateRole::Candidate);
            for record in self.records.drain(..) {
                assert_eq!(record.last_log, None);
                assert!(record.snapshot.is_empty());
                if !record.messages.is_empty() {
                    self.messages.push(record.messages);
                }
            }
        }

        let ss = raft.soft_state();
        if ss != self.prev_ss {
            rd.ss = Some(ss);
        }
        let hs = raft.hard_state();
        if hs != self.prev_hs {
            rd.hs = Some(hs);
        }

        if !raft.read_states.is_empty() {
            rd.read_states = raft.read_states.clone();
        }

        rd.entries = raft.raft_log.unstable_entries().unwrap_or(&[]).to_vec();
        if let Some(e) = rd.entries.last() {
            rd_record.last_log = Some((e.get_index(), e.get_term()));
        }

        if raft.raft_log.unstable.snapshot.is_some() {
            rd.snapshot = raft.raft_log.unstable.snapshot.clone().unwrap();
            rd_record.snapshot = rd.snapshot.clone();
        }

        rd.committed_entries = raft
            .raft_log
            .next_entries_since(applied_idx, Some(self.last_synced_index));

        if !self.messages.is_empty() {
            mem::swap(&mut self.messages, &mut rd.messages);
        }
        if !raft.msgs.is_empty() {
            if raft.state == StateRole::Leader {
                let mut msgs = Vec::new();
                mem::swap(&mut raft.msgs, &mut msgs);
                rd.messages.push(msgs);
            } else {
                mem::swap(&mut raft.msgs, &mut rd_record.messages);
            }
        }
        self.records.push_back(rd_record);
        rd
    }

    fn check_has_ready(&self, applied_idx: Option<u64>) -> bool {
        let raft = &self.raft;
        if raft.soft_state() != self.prev_ss {
            return true;
        }
        let hs = raft.hard_state();
        if hs != HardState::default() && hs != self.prev_hs {
            return true;
        }

        if !raft.read_states.is_empty() {
            return true;
        }

        if raft.raft_log.unstable_entries().is_some() {
            return true;
        }

        if self.snap().map_or(false, |s| !s.is_empty()) {
            return true;
        }

        let has_unapplied_entries = match applied_idx {
            None => raft.raft_log.has_next_entries(),
            Some(applied_idx) => raft.raft_log.has_next_entries_since(applied_idx, None),
        };
        if has_unapplied_entries {
            return true;
        }

        if !raft.msgs.is_empty() || !self.messages.is_empty() {
            return true;
        }
        false
    }

    /// Given an index, can determine if there is a ready state from that time.
    #[inline]
    pub fn has_ready_since(&self, applied_idx: u64) -> bool {
        self.check_has_ready(Some(applied_idx))
    }

    fn commit_ready(&mut self, rd: ReadyAsync) {
        let rd_record = self.records.back().unwrap();
        assert!(rd_record.number == rd.number);
        if rd.ss.is_some() {
            self.prev_ss = rd.ss.unwrap();
        }
        if let Some(hs) = rd.hs {
            if hs != HardState::default() {
                self.prev_hs = hs;
            }
        }
        if let Some(e) = rd_record.last_log {
            self.raft.raft_log.stable_to(e.0, e.1);
        }
        if !rd.read_states.is_empty() {
            self.raft.read_states.clear();
        }
    }

    fn commit_apply(&mut self, applied: u64) {
        self.raft.commit_apply(applied);
    }

    /// Appends and commits the ready value.
    #[inline]
    pub fn advance_append(&mut self, rd: ReadyAsync) {
        self.commit_ready(rd);
    }

    /// Advance apply to the passed index.
    #[inline]
    pub fn advance_apply(&mut self, applied: u64) {
        self.commit_apply(applied);
    }

    /// Sync the ready
    pub fn synced_ready(&mut self, number: u64) {
        loop {
            let record = if let Some(record) = self.records.pop_front() {
                if record.number > number {
                    break;
                }
                record
            } else {
                break;
            };
            if let Some(last_log) = record.last_log {
                self.raft.on_sync_entries(last_log.0, last_log.1);
                self.last_synced_index = last_log.0;
            }
            if !record.snapshot.is_empty() {
                self.raft
                    .raft_log
                    .stable_snap_to(record.snapshot.get_metadata().index);
            }
            if !record.messages.is_empty() {
                self.messages.push(record.messages);
            }
        }
    }

    /// Sync the last ready and get the SyncLastResult
    pub fn synced_last_ready(&mut self, applied_idx: u64) -> SyncLastResult {
        self.synced_ready(self.max_number);

        let raft = &mut self.core.raft;
        let mut res = SyncLastResult {
            committed_entries: raft
                .raft_log
                .next_entries_since(applied_idx, Some(self.last_synced_index)),
            messages: vec![],
        };

        mem::swap(&mut res.messages, &mut self.messages);
        if !raft.msgs.is_empty() && raft.state == StateRole::Leader {
            let mut msgs = Vec::new();
            mem::swap(&mut raft.msgs, &mut msgs);
            res.messages.push(msgs);
        }
        res
    }
}
