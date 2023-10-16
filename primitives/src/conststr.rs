//! Const-string related implementation and structs.
#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString as _},
};
use core::{
    borrow::Borrow,
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    mem::{align_of, size_of, ManuallyDrop},
    ops::Deref,
    ptr::NonNull,
};

use arcstr::ArcStr;
use derive_more::{DebugCustom, Display};
use iroha_schema::{Ident, IntoSchema, MetaMap, TypeId};
use parity_scale_codec::{WrapperTypeDecode, WrapperTypeEncode};
use serde::{
    de::{Deserialize, Deserializer, Error, Visitor},
    ser::{Serialize, Serializer},
};

const MAX_INLINED_STRING_LEN: usize = 2 * size_of::<usize>() - 1;

/// Immutable inlinable string.
/// Strings shorter than 15/7/3 bytes (in 64/32/16-bit architecture) are inlined.
/// Union represents const-string variants: inlined or reference counted.
/// Distinction between variants are achieved by tagging most significant bit of field `len`:
/// - for inlined variant MSB of `len` is always equal to 1, it's enforced by `InlinedString` constructor;
/// - for reference counted variant MSB of `len` is always equal to 0, it's enforced by the fact
/// that `Box` and `Vec` never allocate more than`isize::MAX bytes`.
/// For little-endian 64bit architecture memory layout of [`Self`] is following:
///
/// ```text
/// +-------------------+-------+---------+---------------------------+
/// | Bits              | 0..63 | 64..118 | 119..126 | 127            |
/// +-------------------+-------+---------+----------+----------------+
/// | Inlined           | payload         | len      | tag (always 1) |
/// +-------------------+-------+---------+---------------------------+
/// | Reference counted | ptr   | len                | tag (always 0) |
/// +-------------------+-------+--------------------+----------------+
/// ```
#[derive(DebugCustom, Display)]
#[display(fmt = "{}", "&**self")]
#[debug(fmt = "{:?}", "&**self")]
#[repr(C)]
pub union ConstString {
    inlined: InlinedString,
    ref_counted: ManuallyDrop<ArcString>,
}

/// Test to ensure at compile-time that all [`ConstString`] variants have the same size.
const _: () = assert!(size_of::<InlinedString>() == size_of::<ManuallyDrop<ArcString>>());

/// Test [`ConstString`] layout
const _: () = assert!(size_of::<ConstString>() == size_of::<Box<str>>());
const _: () = assert!(align_of::<ConstString>() == align_of::<Box<str>>());

/// Test [`ArcStr`] layout
const _: () = assert!(size_of::<ArcStr>() == size_of::<NonNull<u8>>());
const _: () = assert!(align_of::<ArcStr>() == align_of::<NonNull<u8>>());

impl ConstString {
    /// Return the length of this [`Self`], in bytes.
    #[inline]
    #[allow(unsafe_code)]
    pub fn len(&self) -> usize {
        if self.is_inlined() {
            // Safety: `is_inlined()` returned `true`
            unsafe { self.inlined().len() }
        } else {
            // Safety: `is_inlined()` returned `false`
            unsafe { self.reference_counted().len() }
        }
    }

    /// Return `true` if [`Self`] is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Construct empty [`Self`].
    #[inline]
    pub const fn new() -> Self {
        Self {
            inlined: InlinedString::new(),
        }
    }

    /// Return `true` if [`Self`] is inlined.
    #[inline]
    #[allow(unsafe_code)]
    pub const fn is_inlined(&self) -> bool {
        // Safety: interpreting [`Self`] as [`InlinedString`] and calling
        // [`InlinedString::is_inlined()`] is always safe, because in fact
        // it's just [u8] cast and MSB checking.
        unsafe { self.inlined().is_inlined() }
    }

    #[allow(unsafe_code)]
    #[inline]
    const unsafe fn inlined(&self) -> &InlinedString {
        &self.inlined
    }

    #[allow(unsafe_code)]
    #[inline]
    unsafe fn reference_counted(&self) -> &ArcString {
        &self.ref_counted
    }
}

impl<T: ?Sized> AsRef<T> for ConstString
where
    InlinedString: AsRef<T>,
    ArcString: AsRef<T>,
{
    #[inline]
    #[allow(unsafe_code)]
    fn as_ref(&self) -> &T {
        if self.is_inlined() {
            // Safety: `is_inlined()` returned `true`
            unsafe { self.inlined().as_ref() }
        } else {
            // Safety: `is_inlined()` returned `false`
            unsafe { self.reference_counted().as_ref() }
        }
    }
}

impl Deref for ConstString {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl Borrow<str> for ConstString {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl Hash for ConstString {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl Ord for ConstString {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(&**self, &**other)
    }
}

/// Can't be derived.
impl PartialOrd for ConstString {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ConstString {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&**self, &**other)
    }
}

macro_rules! impl_eq {
    ($($ty:ty),*) => {$(
        impl PartialEq<$ty> for ConstString {
            // Not possible to write macro uniformly for different types otherwise.
            #[allow(clippy::string_slice, clippy::deref_by_slicing)]
            #[inline]
            fn eq(&self, other: &$ty) -> bool {
                PartialEq::eq(&self[..], &other[..])
            }
        }

        impl PartialEq<ConstString> for $ty {
            // Not possible to write macro uniformly for different types otherwise.
            #[allow(clippy::string_slice, clippy::deref_by_slicing)]
            #[inline]
            fn eq(&self, other: &ConstString) -> bool {
                PartialEq::eq(&self[..], &other[..])
            }
        }
    )*};
}

impl_eq!(String, str, &str);

/// Can't be derived.
impl Eq for ConstString {}

impl<T> From<T> for ConstString
where
    T: TryInto<InlinedString>,
    <T as TryInto<InlinedString>>::Error: Into<ArcString>,
{
    #[inline]
    fn from(value: T) -> Self {
        match value.try_into() {
            Ok(inlined) => Self { inlined },
            Err(value) => Self {
                ref_counted: ManuallyDrop::new(value.into()),
            },
        }
    }
}

impl Clone for ConstString {
    #[allow(unsafe_code)]
    fn clone(&self) -> Self {
        if self.is_inlined() {
            // Safety: `is_inlined()` returned `true`
            unsafe {
                Self {
                    inlined: *self.inlined(),
                }
            }
        } else {
            Self {
                // Safety: `is_inlined()` returned `false`
                ref_counted: unsafe { ManuallyDrop::new(self.reference_counted().clone()) },
            }
        }
    }
}

impl Drop for ConstString {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        if !self.is_inlined() {
            // SAFETY: safe because `is_inlined()` returned `false`.
            unsafe {
                ManuallyDrop::drop(&mut self.ref_counted);
            }
        }
    }
}

impl Serialize for ConstString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self)
    }
}

impl<'de> Deserialize<'de> for ConstString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(ConstStringVisitor)
    }
}

struct ConstStringVisitor;

impl Visitor<'_> for ConstStringVisitor {
    type Value = ConstString;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a string")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(v.into())
    }
}

impl WrapperTypeEncode for ConstString {}

impl WrapperTypeDecode for ConstString {
    type Wrapped = String;
}

// darling doesn't support unions, so this can't be derived
impl TypeId for ConstString {
    fn id() -> Ident {
        "ConstString".to_string()
    }
}

impl IntoSchema for ConstString {
    fn type_name() -> Ident {
        "String".to_string()
    }

    fn update_schema_map(map: &mut MetaMap) {
        if !map.contains_key::<Self>() {
            if !map.contains_key::<String>() {
                <String as iroha_schema::IntoSchema>::update_schema_map(map);
            }
            if let Some(schema) = map.get::<String>() {
                map.insert::<Self>(schema.clone());
            }
        }
    }
}

impl Default for ConstString {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(DebugCustom, Clone)]
#[debug(fmt = "{:?}", "&**self")]
#[repr(C)]
struct ArcString {
    /// [`ArcStr`] provides some optimization in comparison to just [`std::sync::Arc`].
    /// For example it has no `Weak` which allows to remove some overhead.
    #[cfg(target_endian = "little")]
    arc: ArcStr,
    /// Technically [`ArcStr`] also provides [`len()`](ArcStr::len) method, but
    /// we have some extra space in the struct, so we can store it here and remove needless
    /// pointer jump.
    len: usize,
    #[cfg(target_endian = "big")]
    arc: ArcStr,
}

impl ArcString {
    #[inline]
    const fn len(&self) -> usize {
        self.len
    }
}

impl AsRef<str> for ArcString {
    #[allow(unsafe_code)]
    #[inline]
    fn as_ref(&self) -> &str {
        self.arc.as_ref()
    }
}

impl Deref for ArcString {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl From<&str> for ArcString {
    #[allow(unsafe_code)]
    #[inline]
    fn from(value: &str) -> Self {
        Self {
            arc: ArcStr::from(value),
            len: value.len(),
        }
    }
}

impl From<String> for ArcString {
    #[inline]
    fn from(value: String) -> Self {
        Self {
            len: value.len(),
            arc: ArcStr::from(value),
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct InlinedString {
    #[cfg(target_endian = "little")]
    payload: [u8; MAX_INLINED_STRING_LEN],
    /// MSB is always 1 to distinguish inlined variant.
    len: u8,
    #[cfg(target_endian = "big")]
    payload: [u8; MAX_INLINED_STRING_LEN],
}

impl InlinedString {
    #[inline]
    const fn len(self) -> usize {
        (self.len - 128) as usize
    }

    #[inline]
    const fn is_inlined(self) -> bool {
        self.len >= 128
    }

    #[inline]
    const fn new() -> Self {
        Self {
            payload: [0; MAX_INLINED_STRING_LEN],
            // Set MSB to mark inlined variant.
            len: 128,
        }
    }
}

// TODO: Not safe
impl AsRef<str> for InlinedString {
    #[allow(unsafe_code)]
    #[inline]
    fn as_ref(&self) -> &str {
        // SAFETY: created from valid utf-8.
        unsafe { core::str::from_utf8_unchecked(&self.payload[..self.len()]) }
    }
}

impl<'value> TryFrom<&'value str> for InlinedString {
    type Error = &'value str;

    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    fn try_from(value: &'value str) -> Result<Self, Self::Error> {
        let len = value.len();
        if len > MAX_INLINED_STRING_LEN {
            return Err(value);
        }
        let mut inlined = Self::new();
        inlined.payload.as_mut()[..len].copy_from_slice(value.as_bytes());
        // Truncation won't happen because we checked that the length shorter than `MAX_INLINED_STRING_LEN`.
        // Addition here because we set MSB of len field in `Self::new` to mark inlined variant.
        inlined.len += len as u8;
        Ok(inlined)
    }
}

impl TryFrom<String> for InlinedString {
    type Error = String;

    #[inline]
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str()).map_or(Err(value.clone()), Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod api {
        use super::*;

        #[test]
        fn const_string_is_inlined() {
            run_with_strings(|string| {
                let len = string.len();
                let const_string = ConstString::from(string);
                let is_inlined = len <= MAX_INLINED_STRING_LEN;
                assert_eq!(const_string.is_inlined(), is_inlined, "with len {len}");
            });
        }

        #[test]
        fn const_string_len() {
            run_with_strings(|string| {
                let len = string.len();
                let const_string = ConstString::from(string);
                assert_eq!(const_string.len(), len);
            });
        }

        #[test]
        fn const_string_deref() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.as_str());
                assert_eq!(&*const_string, &*string);
            });
        }

        #[test]
        fn const_string_from_string() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.clone());
                assert_eq!(const_string, string);
            });
        }

        #[test]
        fn const_string_from_str() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.as_str());
                assert_eq!(const_string, string);
            });
        }

        #[test]
        #[allow(clippy::redundant_clone)]
        fn const_string_clone() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string);
                let const_string_clone = const_string.clone();
                assert_eq!(const_string, const_string_clone);
            });
        }
    }

    mod integration {
        use std::collections::hash_map::DefaultHasher;

        use parity_scale_codec::Encode;

        use super::*;

        #[test]
        fn const_string_hash() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.clone());
                let mut string_hasher = DefaultHasher::new();
                let mut const_string_hasher = DefaultHasher::new();
                string.hash(&mut string_hasher);
                const_string.hash(&mut const_string_hasher);
                assert_eq!(const_string_hasher.finish(), string_hasher.finish());
            });
        }

        #[test]
        fn const_string_eq_string() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.as_str());
                assert_eq!(const_string, string);
                assert_eq!(string, const_string);
            });
        }

        #[test]
        fn const_string_eq_str() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.as_str());
                assert_eq!(const_string, string.as_str());
                assert_eq!(string.as_str(), const_string);
            });
        }

        #[test]
        fn const_string_eq_const_string() {
            run_with_strings(|string| {
                let const_string_1 = ConstString::from(string.as_str());
                let const_string_2 = ConstString::from(string.as_str());
                assert_eq!(const_string_1, const_string_2);
                assert_eq!(const_string_2, const_string_1);
            });
        }

        #[test]
        fn const_string_cmp() {
            run_with_strings(|string_1| {
                run_with_strings(|string_2| {
                    let const_string_1 = ConstString::from(string_1.as_str());
                    let const_string_2 = ConstString::from(string_2.as_str());
                    assert!(
                        ((const_string_1 <= const_string_2) && (string_1 <= string_2))
                            || ((const_string_1 >= const_string_2) && (string_1 >= string_2))
                    );
                    assert!(
                        ((const_string_2 >= const_string_1) && (string_2 >= string_1))
                            || ((const_string_2 <= const_string_1) && (string_2 <= string_1))
                    );
                });
            });
        }

        #[test]
        fn const_string_scale_encode() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.as_str());
                assert_eq!(const_string.encode(), string.encode());
            });
        }

        #[test]
        fn const_string_serde_serialize() {
            run_with_strings(|string| {
                let const_string = ConstString::from(string.as_str());
                assert_eq!(
                    serde_json::to_string(&const_string).expect("valid"),
                    serde_json::to_string(&string).expect("valid"),
                );
            });
        }
    }

    fn run_with_strings(f: impl Fn(String)) {
        [
            // 0-byte
            "",
            // 1-byte
            "?",
            // 2-bytes
            "??",
            "Δ",
            // 3-bytes
            "???",
            "?Δ",
            "ン",
            // 4-bytes
            "????",
            "??Δ",
            "ΔΔ",
            "?ン",
            "🔥",
            // 7-bytes
            "???????",
            "???🔥",
            "Δ?🔥",
            "ン?ン",
            // 8-bytes
            "????????",
            "ΔΔΔΔ",
            "Δンン",
            "🔥🔥",
            // 15-bytes
            "???????????????",
            "?????????????Δ",
            "????????????ン",
            "???????????🔥",
            "Δ?🔥Δンン",
            // 16-bytes
            "????????????????",
            "????????Δンン",
            "ΔΔΔΔΔΔΔΔ",
            "🔥🔥🔥🔥",
            // 30-bytes
            "??????????????????????????????",
            "??????????????????????????ΔΔ",
            "Δ?🔥ΔンンΔ?🔥Δンン",
            // 31-bytes
            "???????????????????????Δンン",
            "Δ?🔥Δンン🔥🔥🔥🔥",
            "???????????????ΔΔΔΔΔΔΔΔ",
        ]
        .into_iter()
        .map(str::to_owned)
        .for_each(f);
    }
}
