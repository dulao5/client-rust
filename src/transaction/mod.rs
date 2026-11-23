// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Transactional related functionality.
//!
//! Using the [`TransactionClient`](client::Client) you can utilize TiKV's transactional interface.
//!
//! This interface offers SQL-like transactions on top of the raw interface.
//!
//! **Warning:** It is not advisable to use both raw and transactional functionality in the same keyspace.

pub use client::Client;
pub use client::ProtoLockInfo;
pub use coprocessor::DEFAULT_COPROCESSOR_TIMEOUT;
pub use coprocessor::REQ_TYPE_ANALYZE;
pub use coprocessor::REQ_TYPE_CHECKSUM;
pub use coprocessor::REQ_TYPE_DAG;
pub(crate) use lock::expand_shared_lock;
pub(crate) use lock::resolve_locks;
pub(crate) use lock::resolve_locks_for_read;
pub(crate) use lock::resolve_locks_with_expiry;
pub(crate) use lock::HasLocks;
pub(crate) use lock::LockBackoff;
pub use lock::ReadLockHints;
pub use snapshot::Snapshot;
pub use sync_client::SyncTransactionClient;
pub use sync_snapshot::SyncSnapshot;
pub use sync_transaction::SyncTransaction;
pub use transaction::CheckLevel;
#[doc(hidden)]
pub use transaction::HeartbeatOption;
pub use transaction::Mutation;
pub use transaction::Scanner;
pub use transaction::Transaction;
pub use transaction::TransactionOptions;

mod buffer;
mod client;
mod copr_sender;
mod coprocessor;
pub use copr_sender::CopRpcContext;
pub use copr_sender::CopSendResult;
pub use copr_sender::CopSender;
pub use copr_sender::RegionLocation;
mod lock;
pub mod lowering;
mod requests;
pub(crate) use lock::reject_shared_locks;
pub use lock::LockResolver;
pub use lock::LockResolverState;
pub use lock::ResolveLocksContext;
pub use lock::ResolveLocksOptions;
mod snapshot;
mod sync_client;
mod sync_snapshot;
mod sync_transaction;
#[allow(clippy::module_inception)]
mod transaction;
