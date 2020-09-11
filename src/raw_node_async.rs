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
    pub messages: Vec<Vec<Message>>,
}

/// ReadyAsyncRecord encapsulates the needed data for sync reply
#[derive(Default, Debug, PartialEq)]
struct ReadyAsyncRecord {
    number: u64,
    last_log: Option<(u64, u64)>,
    snapshot: Snapshot,
    messages: Vec<Message>,
}

/// SyncLastResult encapsulates the committed entries and messages that are ready to be applied
/// or be sent to other peers.
#[derive(Default, Debug, PartialEq)]
pub struct SyncLastResult {
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

/// RawNodeAsync is a thread-unsafe Node.
/// The methods of this struct correspond to the methods of Node and are described
/// more fully there.
pub struct RawNodeAsync<T: Storage> {
    core: RawNodeRaft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
    records: VecDeque<ReadyAsyncRecord>,
    max_number: u64,
    // (index, term) of synced last log
    synced_last_log: (u64, u64),
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
            synced_last_log: (0, 0),
            messages: Vec::new(),
        };
        rn.synced_last_log = (rn.raft.raft_log.last_index(), rn.raft.raft_log.last_term());
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
        if self.prev_ss.raft_state != StateRole::Leader && raft.state == StateRole::Leader {
            // TODO: clean up ReadyAsyncRecord
        }

        let mut rd = ReadyAsync {
            number,
            entries: raft.raft_log.unstable_entries().unwrap_or(&[]).to_vec(),
            ..Default::default()
        };
        let mut rd_record = ReadyAsyncRecord {
            number,
            ..Default::default()
        };
        if let Some(e) = rd.entries.last() {
            rd_record.last_log = Some((e.get_index(), e.get_term()));
        }
        mem::swap(&mut self.messages, &mut rd.messages);
        if !raft.msgs.is_empty() {
            if raft.state == StateRole::Leader {
                let mut msgs = Vec::new();
                mem::swap(&mut raft.msgs, &mut msgs);
                rd.messages.push(msgs);
            } else {
                mem::swap(&mut raft.msgs, &mut rd_record.messages);
            }
        }
        rd.committed_entries = raft
            .raft_log
            .next_entries_since(applied_idx, Some(self.synced_last_log.0));
        let ss = raft.soft_state();
        if ss != self.prev_ss {
            rd.ss = Some(ss);
        }
        let hs = raft.hard_state_for_ready();
        if hs != self.prev_hs {
            rd.hs = Some(hs);
        }
        if raft.raft_log.unstable.snapshot.is_some() {
            rd.snapshot = raft.raft_log.unstable.snapshot.clone().unwrap();
            rd_record.snapshot = rd.snapshot.clone();
        }
        if !raft.read_states.is_empty() {
            rd.read_states = raft.read_states.clone();
        }
        self.records.push_back(rd_record);
        rd
    }

    fn commit_ready(&mut self, rd: ReadyAsync) {
        if rd.ss.is_some() {
            self.prev_ss = rd.ss.unwrap();
        }
        if let Some(e) = rd.hs {
            if e != HardState::default() {
                self.prev_hs = e;
            }
        }
        if !rd.entries.is_empty() {
            let e = rd.entries.last().unwrap();
            self.raft.raft_log.stable_to(e.index, e.term);
        }
        if rd.snapshot != Snapshot::default() {
            self.raft
                .raft_log
                .stable_snap_to(rd.snapshot.get_metadata().index);
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
                self.synced_last_log = last_log;
            }
            self.raft
                .raft_log
                .stable_snap_to(record.snapshot.get_metadata().index);
            self.messages.push(record.messages);
        }
    }

    /// Sync the last ready and get the SyncLastResult
    pub fn synced_last_ready(&mut self, applied_idx: u64) -> SyncLastResult {
        self.synced_ready(self.max_number);
        let raft = &mut self.core.raft;
        let mut res = SyncLastResult {
            committed_entries: raft
                .raft_log
                .next_entries_since(applied_idx, Some(self.synced_last_log.0)),
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
