// Copyright (c) 2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::ops::{Deref, DerefMut};

use heed::{RoTxn, RwTxn, WithTls};

use super::error::Error;
use super::lmdb::Lmdb;

/// A read-only transaction for the LMDB database.
///
/// This struct provides safe access to read operations within a transaction.
pub struct ReadTransaction<'env> {
    txn: RoTxn<'env, WithTls>,
    db: &'env Lmdb,
}

impl<'env> ReadTransaction<'env> {
    pub(crate) fn new(db: &'env Lmdb) -> Result<Self, Error> {
        let txn = db.read_txn()?;
        Ok(Self { txn, db })
    }

    /// Get a reference to the database
    pub(crate) fn db(&self) -> &'env Lmdb {
        self.db
    }
}

impl<'env> Deref for ReadTransaction<'env> {
    type Target = RoTxn<'env, WithTls>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

/// A read-write transaction for the LMDB database.
///
/// This struct provides safe access to write operations within a transaction.
/// It implements RAII - the transaction will be automatically aborted on drop
/// if not explicitly committed.
pub struct WriteTransaction<'env> {
    txn: Option<RwTxn<'env>>,
    db: &'env Lmdb,
}

impl<'env> WriteTransaction<'env> {
    pub(crate) fn new(db: &'env Lmdb) -> Result<Self, Error> {
        let txn = db.write_txn()?;
        Ok(Self { txn: Some(txn), db })
    }

    /// Commit the transaction, making all changes permanent.
    ///
    /// After calling this method, the transaction can no longer be used.
    pub fn commit(mut self) -> Result<(), Error> {
        if let Some(txn) = self.txn.take() {
            txn.commit()?;
            Ok(())
        } else {
            Err(Error::TransactionAlreadyCommitted)
        }
    }

    /// Explicitly abort the transaction, discarding all changes.
    ///
    /// This is equivalent to dropping the transaction without calling commit.
    pub fn abort(mut self) {
        if let Some(txn) = self.txn.take() {
            txn.abort();
        }
    }

    /// Get a reference to the database
    pub(crate) fn db(&self) -> &'env Lmdb {
        self.db
    }

    /// Get a mutable reference to the underlying transaction
    ///
    /// Returns None if the transaction has already been committed or aborted
    pub(crate) fn as_mut(&mut self) -> Option<&mut RwTxn<'env>> {
        self.txn.as_mut()
    }
}

impl Drop for WriteTransaction<'_> {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            // Abort the transaction if it wasn't committed
            tracing::debug!("WriteTransaction dropped without commit - aborting transaction");
            txn.abort();
        }
    }
}

impl<'env> Deref for WriteTransaction<'env> {
    type Target = RwTxn<'env>;

    fn deref(&self) -> &Self::Target {
        self.txn
            .as_ref()
            .expect("Cannot access committed or aborted transaction")
    }
}

impl DerefMut for WriteTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.txn
            .as_mut()
            .expect("Cannot access committed or aborted transaction")
    }
}
