// Copyright (c) 2024 Michael Dilger
// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::{fmt, io};

use async_utility::tokio::task::JoinError;
use nostr::event::Error as NostrEventError;
use nostr::{key, secp256k1};
use nostr_database::flatbuffers;
use scoped_heed::ScopedDbError;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Error {
    /// An upstream I/O error
    Io(io::Error),
    /// An error from LMDB
    Heed(heed::Error),
    /// Flatbuffers error
    FlatBuffers(flatbuffers::Error),
    Thread(JoinError),
    Key(key::Error),
    Secp256k1(secp256k1::Error),
    OneshotRecv(oneshot::error::RecvError),
    /// MPSC send error
    MpscSend,
    /// The event kind is wrong
    WrongEventKind,
    /// Not found
    NotFound,
    /// Empty scope string
    EmptyScope,
    /// Error from scoped-heed crate
    ScopedHeed(ScopedDbError),
    /// Nostr Event error
    NostrEvent(NostrEventError),
    /// Transaction was already committed
    TransactionAlreadyCommitted,
    /// Transaction was aborted
    TransactionAborted,
    /// Other error
    Other(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Heed(e) => write!(f, "{e}"),
            Self::FlatBuffers(e) => write!(f, "{e}"),
            Self::Thread(e) => write!(f, "{e}"),
            Self::Key(e) => write!(f, "{e}"),
            Self::Secp256k1(e) => write!(f, "{e}"),
            Self::OneshotRecv(e) => write!(f, "{e}"),
            Self::MpscSend => write!(f, "mpsc channel send error"),
            Self::NotFound => write!(f, "Not found"),
            Self::WrongEventKind => write!(f, "Wrong event kind"),
            Self::EmptyScope => {
                write!(
                    f,
                    "Empty scope string is not allowed. Use unscoped view for default access."
                )
            }
            Self::ScopedHeed(e) => write!(f, "Scoped DB error: {e}"),
            Self::NostrEvent(e) => write!(f, "Nostr event error: {e}"),
            Self::TransactionAlreadyCommitted => write!(f, "Transaction was already committed"),
            Self::TransactionAborted => write!(f, "Transaction was aborted"),
            Self::Other(s) => write!(f, "{s}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<heed::Error> for Error {
    fn from(e: heed::Error) -> Self {
        Self::Heed(e)
    }
}

impl From<flatbuffers::Error> for Error {
    fn from(e: flatbuffers::Error) -> Self {
        Self::FlatBuffers(e)
    }
}

impl From<JoinError> for Error {
    fn from(e: JoinError) -> Self {
        Self::Thread(e)
    }
}

impl From<key::Error> for Error {
    fn from(e: key::Error) -> Self {
        Self::Key(e)
    }
}

impl From<secp256k1::Error> for Error {
    fn from(e: secp256k1::Error) -> Self {
        Self::Secp256k1(e)
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(e: oneshot::error::RecvError) -> Self {
        Self::OneshotRecv(e)
    }
}

impl From<ScopedDbError> for Error {
    fn from(e: ScopedDbError) -> Self {
        Self::ScopedHeed(e)
    }
}

impl From<NostrEventError> for Error {
    fn from(e: NostrEventError) -> Self {
        Self::NostrEvent(e)
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Self::Io(e) => Self::Io(io::Error::new(e.kind(), e.to_string())),
            Self::Heed(_) => Self::Other("LMDB error (cloned)".to_string()),
            Self::FlatBuffers(_) => Self::Other("FlatBuffers error (cloned)".to_string()),
            Self::Thread(_) => Self::Other("Thread error (cloned)".to_string()),
            Self::Key(_) => Self::Other("Key error (cloned)".to_string()),
            Self::Secp256k1(e) => Self::Secp256k1(*e),
            Self::OneshotRecv(_) => Self::Other("Oneshot receive error (cloned)".to_string()),
            Self::MpscSend => Self::MpscSend,
            Self::WrongEventKind => Self::WrongEventKind,
            Self::NotFound => Self::NotFound,
            Self::EmptyScope => Self::EmptyScope,
            Self::ScopedHeed(_) => Self::Other("ScopedHeed error (cloned)".to_string()),
            Self::NostrEvent(_) => Self::Other("Nostr event error (cloned)".to_string()),
            Self::TransactionAlreadyCommitted => Self::TransactionAlreadyCommitted,
            Self::TransactionAborted => Self::TransactionAborted,
            Self::Other(s) => Self::Other(s.clone()),
        }
    }
}
