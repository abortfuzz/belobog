use std::{cell::RefCell, collections::BTreeMap, rc::Rc, str::FromStr};

use move_binary_format::file_format::AbilitySet;
use move_core_types::{
    account_address::AccountAddress, language_storage::StructTag, runtime_value::MoveTypeLayout,
    u256::U256,
};
use move_vm_types::values::{Container, Value, ValueImpl};
use sui_json_rpc_types::{SuiMoveNormalizedStructType, SuiMoveNormalizedType};
use sui_types::{TypeTag, base_types::ObjectID};

use crate::meta::FuzzMetadata;

pub trait TypeUtils {
    fn is_tx_context(&self) -> bool;

    fn is_mutable(&self) -> bool;

    // not a regular hot potato, it does not have a drop and not a key, store struct
    fn is_hot_potato(&self, meta: &FuzzMetadata) -> bool;

    fn is_key_store(&self, meta: &FuzzMetadata) -> bool;

    fn is_balance(&self) -> bool;

    fn is_coin(&self) -> bool;

    fn needs_sample(&self) -> bool;

    fn has_copy(&self, meta: &FuzzMetadata) -> bool;

    fn to_type_layout(&self) -> MoveTypeLayout;

    fn ability(&self, meta: &FuzzMetadata) -> Option<AbilitySet>;

    fn gen_value(&self) -> Value;

    fn extract_ty_args(&self, instantiated_ty: &TypeTag) -> Option<BTreeMap<u16, TypeTag>>;

    // return: (self_ty_args, other_ty_args, mapping from self_ty_args index to other_ty_args index)
    fn partial_extract_ty_args(
        &self,
        other: &SuiMoveNormalizedType,
    ) -> Option<(
        BTreeMap<u16, TypeTag>,
        BTreeMap<u16, TypeTag>,
        Vec<(u16, u16)>,
    )>;

    fn subst(&self, ty_args: &BTreeMap<u16, TypeTag>) -> Option<TypeTag>;

    fn partial_subst(&self, ty_args: &BTreeMap<u16, TypeTag>) -> SuiMoveNormalizedType;

    fn from_type_tag(ty: &TypeTag) -> Self
    where
        Self: Sized;

    fn contains_type_param(&self, index: u16) -> bool;
}

impl TypeUtils for SuiMoveNormalizedType {
    fn to_type_layout(&self) -> MoveTypeLayout {
        match self {
            SuiMoveNormalizedType::Bool => MoveTypeLayout::Bool,
            SuiMoveNormalizedType::Address => MoveTypeLayout::Address,
            SuiMoveNormalizedType::U8 => MoveTypeLayout::U8,
            SuiMoveNormalizedType::U16 => MoveTypeLayout::U16,
            SuiMoveNormalizedType::U32 => MoveTypeLayout::U32,
            SuiMoveNormalizedType::U64 => MoveTypeLayout::U64,
            SuiMoveNormalizedType::U128 => MoveTypeLayout::U128,
            SuiMoveNormalizedType::U256 => MoveTypeLayout::U256,
            SuiMoveNormalizedType::Signer => MoveTypeLayout::Signer,
            SuiMoveNormalizedType::Vector(inner) => {
                MoveTypeLayout::Vector(Box::new(inner.to_type_layout()))
            }
            SuiMoveNormalizedType::Reference(inner) => inner.to_type_layout(),
            SuiMoveNormalizedType::MutableReference(inner) => inner.to_type_layout(),
            _ => panic!("Unsupported type: {:?}", self),
        }
    }

    fn is_mutable(&self) -> bool {
        match self {
            SuiMoveNormalizedType::Bool
            | SuiMoveNormalizedType::Address
            | SuiMoveNormalizedType::U8
            | SuiMoveNormalizedType::U16
            | SuiMoveNormalizedType::U32
            | SuiMoveNormalizedType::U64
            | SuiMoveNormalizedType::U128
            | SuiMoveNormalizedType::U256 => true,
            SuiMoveNormalizedType::Vector(inner) => inner.is_mutable(),
            _ => false,
        }
    }

    fn is_tx_context(&self) -> bool {
        match self {
            SuiMoveNormalizedType::Struct { inner } => {
                AccountAddress::from_str(inner.address.as_str()).unwrap() == AccountAddress::TWO
                    && inner.module == "tx_context"
                    && inner.name == "TxContext"
            }
            SuiMoveNormalizedType::Reference(inner) => inner.is_tx_context(),
            SuiMoveNormalizedType::MutableReference(inner) => inner.is_tx_context(),
            _ => false,
        }
    }

    fn is_hot_potato(&self, meta: &FuzzMetadata) -> bool {
        match self {
            SuiMoveNormalizedType::Struct { inner } => {
                if self.is_balance() || self.is_tx_context() {
                    return false;
                }
                let abilities = meta
                    .get_abilities(
                        &ObjectID::from_str(&inner.address).unwrap(),
                        &inner.module,
                        &inner.name,
                    )
                    .expect(&format!(
                        "Struct {}::{}::{} not found in module",
                        inner.address, inner.module, inner.name
                    ));
                !abilities.has_drop() && !self.is_key_store(meta)
            }
            SuiMoveNormalizedType::Vector(inner) => inner.is_hot_potato(meta),
            _ => false,
        }
    }

    fn is_balance(&self) -> bool {
        let SuiMoveNormalizedType::Struct { inner } = self else {
            return false;
        };
        ObjectID::from_str(&inner.address).unwrap() == AccountAddress::TWO.into()
            && inner.module == "balance"
            && inner.name == "Balance"
    }

    fn is_coin(&self) -> bool {
        let SuiMoveNormalizedType::Struct { inner } = self else {
            return false;
        };
        ObjectID::from_str(&inner.address).unwrap() == AccountAddress::TWO.into()
            && inner.module == "coin"
            && inner.name == "Coin"
    }

    // A, &A, &mut A, Vec<A>, T, &T, &mut T need sample
    fn needs_sample(&self) -> bool {
        if self.is_tx_context() {
            return false;
        }
        match self {
            SuiMoveNormalizedType::Struct { .. } => true,
            SuiMoveNormalizedType::Reference(b)
            | SuiMoveNormalizedType::MutableReference(b)
            | SuiMoveNormalizedType::Vector(b) => match b.as_ref() {
                SuiMoveNormalizedType::Struct { .. } => true,
                SuiMoveNormalizedType::TypeParameter(_) => true,
                _ => b.needs_sample(),
            },
            SuiMoveNormalizedType::TypeParameter(_) => true,
            _ => false,
        }
    }

    fn has_copy(&self, meta: &FuzzMetadata) -> bool {
        let SuiMoveNormalizedType::Struct { inner } = self else {
            panic!("Expected a struct type for copy check, got: {:?}", self);
        };
        let abilities = meta
            .get_abilities(
                &ObjectID::from_str(&inner.address).unwrap(),
                &inner.module,
                &inner.name,
            )
            .unwrap_or_else(|| {
                panic!(
                    "Struct {}::{}::{} not found in module",
                    inner.address, inner.module, inner.name
                )
            });
        abilities.has_copy()
    }

    fn is_key_store(&self, meta: &FuzzMetadata) -> bool {
        let SuiMoveNormalizedType::Struct { inner } = self else {
            panic!(
                "Expected a struct type for key store check, got: {:?}",
                self
            );
        };
        let abilities = meta
            .get_abilities(
                &ObjectID::from_str(&inner.address).unwrap(),
                &inner.module,
                &inner.name,
            )
            .expect("Struct not found in module");
        abilities.has_key() && abilities.has_store()
    }

    fn gen_value(&self) -> Value {
        match self {
            SuiMoveNormalizedType::Bool => Value::bool(false),
            SuiMoveNormalizedType::Address => Value::address(AccountAddress::ZERO),
            SuiMoveNormalizedType::U8 => Value::u8(0),
            SuiMoveNormalizedType::U16 => Value::u16(0),
            SuiMoveNormalizedType::U32 => Value::u32(0),
            SuiMoveNormalizedType::U64 => Value::u64(0),
            SuiMoveNormalizedType::U128 => Value::u128(0),
            SuiMoveNormalizedType::U256 => Value::u256(U256::zero()),
            SuiMoveNormalizedType::Signer => Value::signer(AccountAddress::ZERO),
            SuiMoveNormalizedType::Vector(inner) => match **inner {
                SuiMoveNormalizedType::Bool => Value::vector_bool(vec![false; 128]),
                SuiMoveNormalizedType::Address => {
                    Value::vector_address(vec![AccountAddress::ZERO; 128])
                }
                SuiMoveNormalizedType::U8 => Value::vector_u8(vec![0; 128]),
                SuiMoveNormalizedType::U16 => Value::vector_u16(vec![0; 128]),
                SuiMoveNormalizedType::U32 => Value::vector_u32(vec![0; 128]),
                SuiMoveNormalizedType::U64 => Value::vector_u64(vec![0; 128]),
                SuiMoveNormalizedType::U128 => Value::vector_u128(vec![0; 128]),
                SuiMoveNormalizedType::U256 => Value::vector_u256(vec![U256::zero(); 128]),
                SuiMoveNormalizedType::Vector(ref inner_inner) => {
                    let value = inner.gen_value();
                    let value2 = inner.gen_value();
                    Value(ValueImpl::Container(Container::Vec(Rc::new(RefCell::new(
                        vec![value.0, value2.0],
                    )))))
                }
                _ => {
                    unreachable!("Unsupported vector type: {:?}", inner);
                }
            },
            SuiMoveNormalizedType::Reference(inner)
            | SuiMoveNormalizedType::MutableReference(inner) => {
                let inner_value = inner.gen_value();
                if !inner.needs_sample() {
                    inner_value
                } else {
                    unreachable!(
                        "Mutable references are not supported in this context: {:?}",
                        inner_value
                    )
                }
            }
            _ => {
                unreachable!("Unsupported type: {:?}", self,);
            }
        }
    }

    fn ability(&self, meta: &FuzzMetadata) -> Option<AbilitySet> {
        match self {
            SuiMoveNormalizedType::Bool => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::Address => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::U8 => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::U16 => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::U32 => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::U64 => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::U128 => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::U256 => Some(AbilitySet::PRIMITIVES),
            SuiMoveNormalizedType::Signer => Some(AbilitySet::SIGNER),
            SuiMoveNormalizedType::Vector(_) => Some(AbilitySet::VECTOR),
            SuiMoveNormalizedType::Struct { inner } => {
                let abilities = meta
                    .get_abilities(
                        &ObjectID::from_str(&inner.address).unwrap(),
                        &inner.module,
                        &inner.name,
                    )
                    .expect(&format!(
                        "Struct {}::{}::{} not found in module",
                        inner.address, inner.module, inner.name
                    ));
                Some(abilities)
            }
            _ => None,
        }
    }

    // return a map of type parameter index to instantiated type tag
    fn extract_ty_args(&self, instantiated_ty: &TypeTag) -> Option<BTreeMap<u16, TypeTag>> {
        let mut ty_args = BTreeMap::new();
        match (self, instantiated_ty) {
            (SuiMoveNormalizedType::Vector(inner), TypeTag::Vector(inner_ty)) => {
                return inner.extract_ty_args(inner_ty);
            }
            (SuiMoveNormalizedType::Struct { inner }, TypeTag::Struct(tag)) => {
                // if AccountAddress::from_str(inner.address.as_str()).unwrap() != tag.address
                //     || inner.module != tag.module.to_string()
                //     || inner.name != tag.name.to_string()
                // currently ignore address check
                if inner.module != tag.module.to_string() || inner.name != tag.name.to_string() {
                    return None;
                }
                for (sub_generic_ty, sub_instantiated_ty) in
                    inner.type_arguments.iter().zip(tag.type_params.iter())
                {
                    if let Some(sub_ty_args) = sub_generic_ty.extract_ty_args(sub_instantiated_ty) {
                        for (index, sub_ty_arg) in sub_ty_args {
                            if ty_args.contains_key(&index) && ty_args[&index] != sub_ty_arg {
                                // If the type argument is already set and does not match, return None
                                return None;
                            }
                            ty_args.insert(index, sub_ty_arg);
                        }
                    } else {
                        return None;
                    }
                }
            }
            (SuiMoveNormalizedType::Reference(inner), _) => {
                return inner.extract_ty_args(instantiated_ty);
            }
            (SuiMoveNormalizedType::MutableReference(inner), _) => {
                return inner.extract_ty_args(instantiated_ty);
            }
            (SuiMoveNormalizedType::TypeParameter(index), _) => {
                ty_args.insert(*index, instantiated_ty.clone());
            }
            (SuiMoveNormalizedType::Bool, TypeTag::Bool)
            | (SuiMoveNormalizedType::Address, TypeTag::Address)
            | (SuiMoveNormalizedType::U8, TypeTag::U8)
            | (SuiMoveNormalizedType::U16, TypeTag::U16)
            | (SuiMoveNormalizedType::U32, TypeTag::U32)
            | (SuiMoveNormalizedType::U64, TypeTag::U64)
            | (SuiMoveNormalizedType::U128, TypeTag::U128)
            | (SuiMoveNormalizedType::U256, TypeTag::U256)
            | (SuiMoveNormalizedType::Signer, TypeTag::Signer) => {
                // No type arguments needed for these types
            }
            _ => return None,
        }
        Some(ty_args)
    }

    fn partial_extract_ty_args(
        &self,
        other: &SuiMoveNormalizedType,
    ) -> Option<(
        BTreeMap<u16, TypeTag>,
        BTreeMap<u16, TypeTag>,
        Vec<(u16, u16)>,
    )> {
        let mut ty_args = BTreeMap::new();
        let mut other_ty_args = BTreeMap::new();
        let mut mapping = Vec::new();
        match (self, other) {
            (
                SuiMoveNormalizedType::TypeParameter(index),
                SuiMoveNormalizedType::TypeParameter(other_index),
            ) => {
                mapping.push((*index, *other_index));
            }
            (SuiMoveNormalizedType::TypeParameter(index), _) => {
                // ignore nested type parameters case
                ty_args.insert(*index, other.subst(&BTreeMap::new())?);
            }
            (_, SuiMoveNormalizedType::TypeParameter(other_index)) => {
                // ignore nested type parameters case
                other_ty_args.insert(*other_index, self.subst(&BTreeMap::new())?);
            }
            (
                SuiMoveNormalizedType::Struct { inner },
                SuiMoveNormalizedType::Struct { inner: other_inner },
            ) => {
                if AccountAddress::from_str(inner.address.as_str()).unwrap()
                    != AccountAddress::from_str(other_inner.address.as_str()).unwrap()
                    || inner.module != other_inner.module
                    || inner.name != other_inner.name
                    || inner.type_arguments.len() != other_inner.type_arguments.len()
                {
                    return None;
                }
                for (sub_generic_ty, sub_other_ty) in inner
                    .type_arguments
                    .iter()
                    .zip(other_inner.type_arguments.iter())
                {
                    let (sub_ty_args, sub_other_ty_args, sub_mapping) =
                        sub_generic_ty.partial_extract_ty_args(sub_other_ty)?;
                    for (index, ty) in sub_ty_args {
                        if ty_args.contains_key(&index) && ty_args[&index] != ty {
                            // If the type argument is already set and does not match, return None
                            return None;
                        }
                        ty_args.insert(index, ty);
                    }
                    for (index, ty) in sub_other_ty_args {
                        if other_ty_args.contains_key(&index) && other_ty_args[&index] != ty {
                            // If the type argument is already set and does not match, return None
                            return None;
                        }
                        other_ty_args.insert(index, ty);
                    }
                    mapping.extend(sub_mapping);
                }
            }
            (
                SuiMoveNormalizedType::MutableReference(inner),
                SuiMoveNormalizedType::MutableReference(other_inner),
            )
            | (
                SuiMoveNormalizedType::Reference(inner),
                SuiMoveNormalizedType::MutableReference(other_inner),
            )
            | (
                SuiMoveNormalizedType::Reference(inner),
                SuiMoveNormalizedType::Reference(other_inner),
            )
            | (SuiMoveNormalizedType::Vector(inner), SuiMoveNormalizedType::Vector(other_inner)) => {
                // other indicates producer type, self indicates consumer type
                // e.g., &mut T can be passed in &T, &mut T, but not T
                return inner.partial_extract_ty_args(other_inner);
            }
            (SuiMoveNormalizedType::MutableReference(inner), _) => {
                return inner.partial_extract_ty_args(other);
            }
            (SuiMoveNormalizedType::Reference(inner), _) => {
                return inner.partial_extract_ty_args(other);
            }
            _ => {
                if self != other {
                    return None;
                }
            }
        }
        Some((ty_args, other_ty_args, mapping))
    }

    fn subst(&self, ty_args: &BTreeMap<u16, TypeTag>) -> Option<TypeTag> {
        match self {
            SuiMoveNormalizedType::Vector(inner) => inner
                .subst(ty_args)
                .map(|inner_ty| TypeTag::Vector(Box::new(inner_ty))),
            SuiMoveNormalizedType::Struct { inner } => {
                let type_params = inner
                    .type_arguments
                    .iter()
                    .map(|ty| ty.subst(ty_args))
                    .collect::<Option<Vec<_>>>()?;
                Some(TypeTag::Struct(Box::new(StructTag {
                    address: AccountAddress::from_str(inner.address.as_str()).unwrap(),
                    module: move_core_types::identifier::Identifier::new(inner.module.clone())
                        .unwrap(),
                    name: move_core_types::identifier::Identifier::new(inner.name.clone()).unwrap(),
                    type_params,
                })))
            }
            SuiMoveNormalizedType::Reference(inner) => inner.subst(ty_args),
            SuiMoveNormalizedType::MutableReference(inner) => inner.subst(ty_args),
            SuiMoveNormalizedType::TypeParameter(index) => ty_args.get(index).cloned(),
            SuiMoveNormalizedType::Bool => Some(TypeTag::Bool),
            SuiMoveNormalizedType::Address => Some(TypeTag::Address),
            SuiMoveNormalizedType::U8 => Some(TypeTag::U8),
            SuiMoveNormalizedType::U16 => Some(TypeTag::U16),
            SuiMoveNormalizedType::U32 => Some(TypeTag::U32),
            SuiMoveNormalizedType::U64 => Some(TypeTag::U64),
            SuiMoveNormalizedType::U128 => Some(TypeTag::U128),
            SuiMoveNormalizedType::U256 => Some(TypeTag::U256),
            SuiMoveNormalizedType::Signer => Some(TypeTag::Signer),
        }
    }

    fn partial_subst(&self, ty_args: &BTreeMap<u16, TypeTag>) -> SuiMoveNormalizedType {
        match self {
            SuiMoveNormalizedType::Vector(inner) => {
                SuiMoveNormalizedType::Vector(Box::new(inner.partial_subst(ty_args)))
            }
            SuiMoveNormalizedType::Struct { inner } => {
                let type_arguments = inner
                    .type_arguments
                    .iter()
                    .map(|ty| ty.partial_subst(ty_args))
                    .collect::<Vec<_>>();
                let mut inner = inner.clone();
                inner.type_arguments = type_arguments;
                SuiMoveNormalizedType::Struct { inner }
            }
            SuiMoveNormalizedType::Reference(inner) => {
                SuiMoveNormalizedType::Reference(Box::new(inner.partial_subst(ty_args)))
            }
            SuiMoveNormalizedType::MutableReference(inner) => {
                SuiMoveNormalizedType::MutableReference(Box::new(inner.partial_subst(ty_args)))
            }
            SuiMoveNormalizedType::TypeParameter(index) => ty_args
                .get(index)
                .cloned()
                .map(|ty_tag| SuiMoveNormalizedType::from_type_tag(&ty_tag))
                .unwrap_or(self.clone()),
            _ => self.clone(),
        }
    }

    fn from_type_tag(ty: &TypeTag) -> Self
    where
        Self: Sized,
    {
        match ty {
            TypeTag::Bool => SuiMoveNormalizedType::Bool,
            TypeTag::Address => SuiMoveNormalizedType::Address,
            TypeTag::U8 => SuiMoveNormalizedType::U8,
            TypeTag::U16 => SuiMoveNormalizedType::U16,
            TypeTag::U32 => SuiMoveNormalizedType::U32,
            TypeTag::U64 => SuiMoveNormalizedType::U64,
            TypeTag::U128 => SuiMoveNormalizedType::U128,
            TypeTag::U256 => SuiMoveNormalizedType::U256,
            TypeTag::Signer => SuiMoveNormalizedType::Signer,
            TypeTag::Vector(inner) => {
                SuiMoveNormalizedType::Vector(Box::new(SuiMoveNormalizedType::from_type_tag(inner)))
            }
            TypeTag::Struct(tag) => SuiMoveNormalizedType::Struct {
                inner: Box::new(SuiMoveNormalizedStructType {
                    address: tag.address.to_string(),
                    module: tag.module.to_string(),
                    name: tag.name.to_string(),
                    type_arguments: tag
                        .type_params
                        .iter()
                        .map(SuiMoveNormalizedType::from_type_tag)
                        .collect(),
                }),
            },
        }
    }

    fn contains_type_param(&self, index: u16) -> bool {
        match self {
            SuiMoveNormalizedType::TypeParameter(i) => *i == index,
            SuiMoveNormalizedType::Vector(inner) => inner.contains_type_param(index),
            SuiMoveNormalizedType::Reference(inner)
            | SuiMoveNormalizedType::MutableReference(inner) => inner.contains_type_param(index),
            SuiMoveNormalizedType::Struct { inner } => inner
                .type_arguments
                .iter()
                .any(|ty| ty.contains_type_param(index)),
            _ => false,
        }
    }
}
