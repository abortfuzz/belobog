#![allow(clippy::result_large_err)]

use std::collections::{BTreeSet, HashMap};

use belobog_types::error::BelobogError;
use sui_sdk::types::{
    base_types::{MoveObjectType, ObjectID, SuiAddress},
    digests::TransactionDigest,
    effects::TransactionEffects,
    inner_temporary_store::InnerTemporaryStore,
    object::{MoveObject, OBJECT_START_VERSION, Object, Owner},
    storage::{BackingPackageStore, BackingStore, ChildObjectResolver, ObjectStore, ParentSync},
};
use sui_types::TypeTag;

pub mod cache;
pub mod db;
pub mod dump;
pub mod empty;
pub mod file;
pub mod fork;
pub mod graphql;
pub mod schema;
pub mod snapshot;
pub mod types;

#[auto_impl::auto_impl(&, Arc, Box)]
pub trait ObjectStoreExt: BackingStore {
    fn get_object_at_checkpoint(
        &self,
        object_id: ObjectID,
        checkpoint: u64,
    ) -> Result<Option<Object>, BelobogError>;
}

#[auto_impl::auto_impl(&, Arc, Box)]
pub trait ObjectTypesStore {
    fn get_objects_by_ty_anyone(&self, ty: &MoveObjectType) -> Vec<(ObjectID, u64)>;
    fn get_objects_by_ty(&self, ty: &MoveObjectType, owner: SuiAddress) -> Vec<(ObjectID, u64)>;
    fn get_mutable_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)>;
    fn get_owned_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)>;
    fn tys(&self) -> Vec<MoveObjectType>;
    fn ty_cache(&self) -> HashMap<MoveObjectType, BTreeSet<(ObjectID, u64)>>;
}

pub trait ObjectStoreCommit {
    fn commit_single_object(&self, object: Object);
    fn commit_store(&self, store: InnerTemporaryStore, effects: &TransactionEffects);

    fn mint_gas_object(&self, gas_id: ObjectID, owner: ObjectID) -> Result<Object, BelobogError> {
        let gas_move =
            MoveObject::new_gas_coin(OBJECT_START_VERSION, gas_id, 1_000_000_000 * 1_000_000_000);
        let gas = Object::new_move(
            gas_move,
            Owner::AddressOwner(owner.into()),
            TransactionDigest::genesis_marker(),
        );
        self.commit_single_object(gas.clone());
        Ok(gas)
    }

    fn mint_coin(
        &self,
        coin_type: TypeTag,
        coin_id: ObjectID,
        owner: ObjectID,
    ) -> Result<Object, BelobogError> {
        let coin_move = MoveObject::new_coin(
            coin_type,
            OBJECT_START_VERSION,
            coin_id,
            1_000_000_000 * 1_000_000_000,
        );
        let coin = Object::new_move(
            coin_move,
            Owner::AddressOwner(owner.into()),
            TransactionDigest::genesis_marker(),
        );
        self.commit_single_object(coin.clone());
        Ok(coin)
    }
}

// Small utils to workaround traits bound
#[derive(Clone, Debug)]
pub enum TrivialBackStore<T1, T2> {
    T1(T1),
    T2(T2),
}

impl<T1, T2> BackingPackageStore for TrivialBackStore<T1, T2>
where
    T1: BackingPackageStore,
    T2: BackingPackageStore,
{
    fn get_package_object(
        &self,
        package_id: &ObjectID,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::storage::PackageObject>> {
        match self {
            Self::T1(t1) => t1.get_package_object(package_id),
            Self::T2(t2) => t2.get_package_object(package_id),
        }
    }
}

impl<T1, T2> ChildObjectResolver for TrivialBackStore<T1, T2>
where
    T1: ChildObjectResolver,
    T2: ChildObjectResolver,
{
    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: sui_sdk::types::base_types::SequenceNumber,
        epoch_id: sui_sdk::types::committee::EpochId,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        match self {
            Self::T1(t1) => t1.get_object_received_at_version(
                owner,
                receiving_object_id,
                receive_object_at_version,
                epoch_id,
            ),
            Self::T2(t2) => t2.get_object_received_at_version(
                owner,
                receiving_object_id,
                receive_object_at_version,
                epoch_id,
            ),
        }
    }
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: sui_sdk::types::base_types::SequenceNumber,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        match self {
            Self::T1(t1) => t1.read_child_object(parent, child, child_version_upper_bound),
            Self::T2(t2) => t2.read_child_object(parent, child, child_version_upper_bound),
        }
    }
}

impl<T1, T2> ParentSync for TrivialBackStore<T1, T2>
where
    T1: ParentSync,
    T2: ParentSync,
{
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        object_id: ObjectID,
    ) -> Option<sui_sdk::types::base_types::ObjectRef> {
        match self {
            Self::T1(t) => t.get_latest_parent_entry_ref_deprecated(object_id),
            Self::T2(t) => t.get_latest_parent_entry_ref_deprecated(object_id),
        }
    }
}

impl<T1, T2> ObjectStore for TrivialBackStore<T1, T2>
where
    T1: ObjectStore,
    T2: ObjectStore,
{
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        match self {
            Self::T1(t1) => t1.get_object(object_id),
            Self::T2(t2) => t2.get_object(object_id),
        }
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: sui_sdk::types::base_types::VersionNumber,
    ) -> Option<Object> {
        match self {
            Self::T1(t1) => t1.get_object_by_key(object_id, version),
            Self::T2(t2) => t2.get_object_by_key(object_id, version),
        }
    }

    fn multi_get_objects(&self, object_ids: &[ObjectID]) -> Vec<Option<Object>> {
        match self {
            Self::T1(t1) => t1.multi_get_objects(object_ids),
            Self::T2(t2) => t2.multi_get_objects(object_ids),
        }
    }

    fn multi_get_objects_by_key(
        &self,
        object_keys: &[sui_sdk::types::storage::ObjectKey],
    ) -> Vec<Option<Object>> {
        match self {
            Self::T1(t1) => t1.multi_get_objects_by_key(object_keys),
            Self::T2(t2) => t2.multi_get_objects_by_key(object_keys),
        }
    }
}

impl<T1, T2> ObjectStoreExt for TrivialBackStore<T1, T2>
where
    T1: ObjectStoreExt,
    T2: ObjectStoreExt,
{
    fn get_object_at_checkpoint(
        &self,
        object_id: ObjectID,
        checkpoint: u64,
    ) -> Result<Option<Object>, BelobogError> {
        match self {
            Self::T1(t1) => t1.get_object_at_checkpoint(object_id, checkpoint),
            Self::T2(t2) => t2.get_object_at_checkpoint(object_id, checkpoint),
        }
    }
}

impl<T1, T2> ObjectStoreCommit for TrivialBackStore<T1, T2>
where
    T1: ObjectStoreCommit,
    T2: ObjectStoreCommit,
{
    fn commit_single_object(&self, object: Object) {
        match self {
            Self::T1(t1) => t1.commit_single_object(object),
            Self::T2(t2) => t2.commit_single_object(object),
        }
    }

    fn commit_store(&self, store: InnerTemporaryStore, effects: &TransactionEffects) {
        match self {
            Self::T1(t1) => t1.commit_store(store, effects),
            Self::T2(t2) => t2.commit_store(store, effects),
        }
    }
}

impl<T1, T2> ObjectTypesStore for TrivialBackStore<T1, T2>
where
    T1: ObjectTypesStore,
    T2: ObjectTypesStore,
{
    fn get_objects_by_ty_anyone(&self, ty: &MoveObjectType) -> Vec<(ObjectID, u64)> {
        match self {
            Self::T1(t1) => t1.get_objects_by_ty_anyone(ty),
            Self::T2(t2) => t2.get_objects_by_ty_anyone(ty),
        }
    }

    fn get_objects_by_ty(&self, ty: &MoveObjectType, owner: SuiAddress) -> Vec<(ObjectID, u64)> {
        match self {
            Self::T1(t1) => t1.get_objects_by_ty(ty, owner),
            Self::T2(t2) => t2.get_objects_by_ty(ty, owner),
        }
    }

    fn get_mutable_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)> {
        match self {
            Self::T1(t1) => t1.get_mutable_objects_by_ty(ty, owner),
            Self::T2(t2) => t2.get_mutable_objects_by_ty(ty, owner),
        }
    }

    fn get_owned_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)> {
        match self {
            Self::T1(t1) => t1.get_owned_objects_by_ty(ty, owner),
            Self::T2(t2) => t2.get_owned_objects_by_ty(ty, owner),
        }
    }

    fn tys(&self) -> Vec<MoveObjectType> {
        match self {
            Self::T1(t1) => t1.tys(),
            Self::T2(t2) => t2.tys(),
        }
    }

    fn ty_cache(&self) -> HashMap<MoveObjectType, BTreeSet<(ObjectID, u64)>> {
        match self {
            Self::T1(t1) => t1.ty_cache(),
            Self::T2(t2) => t2.ty_cache(),
        }
    }
}
