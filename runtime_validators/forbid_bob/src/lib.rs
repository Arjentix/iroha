//! This is a sample validator which forbids bob@wonderland to do anything

#![no_std]
#![no_main]

extern crate alloc;

use alloc::borrow::ToOwned as _;

use iroha_wasm::{validator::prelude::*, DebugExpectExt as _};

/// Forbid bob@wonderland to do anything
#[entrypoint]
pub fn validate(tx: SignedTransaction) -> Verdict {
    if tx.payload.account_id
        == "bob@wonderland"
            .parse()
            .dbg_expect("Failed to parse bob's id")
    {
        Verdict::Deny("Bob from Wonderland is not allowed to do anything".to_owned())
    } else {
        Verdict::Pass
    }
}
