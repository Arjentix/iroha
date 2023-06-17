//! Permission Token and related impls
#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeSet, format, string::String, vec::Vec};
#[cfg(feature = "std")]
use std::collections::BTreeSet;

use derive_more::Display;
use getset::Getters;
use iroha_data_model_derive::IdEqOrdHash;
use iroha_schema::IntoSchema;
use parity_scale_codec::{Decode, Encode};
use serde::{Deserialize, Serialize};

pub use self::model::*;
use crate::{Identifiable, Registered};

/// Collection of [`Token`]s
pub type Permissions = BTreeSet<PermissionToken>;

use super::*;

/// Unique id of [`PermissionTokenDefinition`]
pub type PermissionTokenId = String;

/// Defines a type of [`PermissionToken`] with given id
#[derive(
    Debug, Display, Clone, IdEqOrdHash, Getters, Decode, Encode, Deserialize, Serialize, IntoSchema,
)]
#[repr(C)]
#[display(fmt = "{id}")]
pub struct PermissionTokenDefinition {
    /// Token identifier
    #[getset(get = "pub")]
    pub id: PermissionTokenId,
    /// Description of the token type encoded as [`iroha_schema::MetaMap`] JSON
    #[getset(get = "pub")]
    pub metadata: String,
}

/// Stored proof of the account having a permission for a certain action.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Getters,
    Decode,
    Encode,
    Deserialize,
    Serialize,
    IntoSchema,
)]
#[repr(C)]
pub struct PermissionToken {
    /// Token identifier
    #[getset(get = "pub")]
    pub definition_id: PermissionTokenId,
    /// SCALE encoded token payload
    #[getset(get = "pub")]
    pub payload: Vec<u8>,
}

impl PermissionToken {
    /// Construct a permission token.
    #[inline]
    pub fn new(definition_id: PermissionTokenId, payload: Vec<u8>) -> Self {
        Self {
            definition_id,
            payload,
        }
    }
}

impl core::fmt::Display for PermissionToken {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.definition_id)
    }
}

impl Registered for PermissionTokenDefinition {
    type With = Self;
}

pub mod prelude {
    //! The prelude re-exports most commonly used traits, structs and macros from this crate.
    pub use super::{PermissionToken, PermissionTokenDefinition, PermissionTokenId};
}
