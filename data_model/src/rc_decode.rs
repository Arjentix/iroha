//! Module with helper functions to deserialize/decode [`BTreeMap`] of `id -> identifiable` using
//! reference counting based on [`Name`].

#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeMap, format};
use core::fmt::Display;
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use serde::{de::Error as _, Deserialize};

use crate::Identifiable;

/// Deserialize map of `id -> identifiable` applying `f` as optimizer for keys and values.
///
/// # Errors
///
/// - Input is not a map
/// - Failed to deserialize key
/// - Failed to deserialize value
/// - Key id is not the same as value id
pub fn deserialize_map_with<'de, D, K, V, F>(
    deserializer: D,
    f: F,
) -> Result<BTreeMap<K, V>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    K: Deserialize<'de> + Clone + Ord + PartialEq + Display,
    V: Deserialize<'de> + Identifiable<Id = K>,
    F: FnMut(&mut K, &mut V),
{
    deserializer.deserialize_map(RefCountingVisitor(BTreeMap::default(), f))
}

struct RefCountingVisitor<K, V, F>(BTreeMap<K, V>, F);

impl<'de, K, V, F> serde::de::Visitor<'de> for RefCountingVisitor<K, V, F>
where
    K: Deserialize<'de> + Clone + Ord + PartialEq + Display,
    V: Deserialize<'de> + Identifiable<Id = K>,
    F: FnMut(&mut K, &mut V),
{
    type Value = BTreeMap<K, V>;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("a map")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        while let Some((mut id, mut value)) = map.next_entry::<K, V>()? {
            if id != *value.id() {
                return Err(A::Error::custom(format!(
                    "Inconsistent map: key has id `{id}`, but provided item has id `{}`",
                    value.id()
                )));
            }

            self.1(&mut id, &mut value);
            self.0.insert(id, value);
        }
        Ok(self.0)
    }
}
